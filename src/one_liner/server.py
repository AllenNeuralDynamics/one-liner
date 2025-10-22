"""Server for enabling remote control and broadcasting results of periodic function calls."""

import logging
import pickle
import zmq
from one_liner import Protocol, Encoding
from one_liner.stream_schema import Stream, PeriodicStream, Streams
from threading import Thread, Event, Lock
from time import sleep
from time import perf_counter as now
from typing import Callable, get_type_hints


class RouterServer:
    __slots__ = ("context", "streamer", "rpc")
    """Interface for enabling remote control/monitoring of one or more object
       instances. Heavy lifting is delegated to two subordinate objects."""

    def __init__(self,  protocol: Protocol = "tcp", interface: str = "*",
                 rpc_port: str = "5555", broadcast_port: str = "5556",
                 context: zmq.Context = None, **devices):
        """constructor.

        :param protocol:
        :param interface:
        :param rpc_port:
        :param broadcast_port:
        :param context: the zmq context. Will be created automatically if
            unspecified.

        .. warning::
           For sharing data within the same process using the `inproc` protocol,
           the zmq Context must be shared between `RouterServer` and `RouterClient`
           (or `RouterServer` and `RouterServer` if forwarding).

        .. note::
           For the `protocol` setting, some options are system-dependent (i.e: `ipc`
           is for unix-like OSes only).

        """
        self.context = context or zmq.Context()
        self.streamer = ZMQStreamServer(protocol=protocol, interface=interface,
                                        port=broadcast_port, context=self.context)
        # Pass streamer into RPC Server as another device so we can interact
        # with it remotely. Hide it with a "__" prefix.
        self.rpc = ZMQRPCServer(protocol=protocol, interface=interface,
                                port=rpc_port, context=self.context,
                                __streamer=self.streamer, **devices)

    def run(self):
        """Setup rpc listener and broadcaster."""
        self.rpc.run()
        self.streamer.run(run_in_thread=True)

    def add_broadcast(self, name: str, frequency_hz: float, func: Callable, *args, **kwargs):
        self.streamer.add(name, frequency_hz, func, *args, **kwargs)

    def get_broadcast_fn(self, name: str, set_timestamp: bool = False,
                         encoding: Encoding = "pickle"):
        return self.streamer.get_broadcast_fn(name, encoding=encoding,
                                              set_timestamp=set_timestamp)

    def remove_broadcast(self, func):
        self.streamer.remove(func)

    def close(self):
        self.rpc.close()
        self.streamer.close()
        # TODO: figure out why zmq proxy needs destroy instead of just term here
        self.context.destroy()


class ZMQRPCServer:
    """Remote Procedure Caller (RPC) Server. Call any method from a dict of
    object instances, and dispatch the serialized result to the connected
    RPC Client."""

    def __init__(self, protocol: Protocol = "tcp", interface: str = "*",
                 port: str = "5555", context: zmq.Context = None,
                 **devices):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.context = context or zmq.Context()
        self.context_managed_externally = context is not None
        self.socket = self.context.socket(zmq.REP)
        self.socket.setsockopt(zmq.RCVTIMEO, 100)  # milliseconds
        self.socket.setsockopt(zmq.LINGER, 0)
        address = f"{protocol}://{interface}:{self.port}"
        self.socket.bind(address)
        self._keep_receiving = Event()
        self._keep_receiving.set()
        self._receive_thread: Thread = None
        self.devices: dict[str, any] = devices

    def run(self):
        """Launch thread to execute RPCs."""
        self._keep_receiving.set()
        self._receive_thread = Thread(target=self._receive_worker,
                                     name=f"REQ_receive_worker",
                                     daemon=True)
        self._receive_thread.start()

    def _call(self, device_name: str, method_name: str, *args, **kwargs):
        """Lookup the call, invoke it, and return the result."""
        if device_name not in self.devices:
            raise ValueError(f"{device_name} is not present in devices.")
        device = self.devices[device_name]
        func = getattr(device, method_name)  # Might raise AttributeError
        # Call the function and return the result.
        return func(*args, **kwargs)

    def _receive_worker(self):
        """Wait for requests, call requested function, and return the reply.
        Launched in a thread.
        """
        while self._keep_receiving.is_set():
            try:
                pickled_request = self.socket.recv()
            except zmq.Again:
                continue
            request = pickle.loads(pickled_request)
            device_name, method_name, args, kwargs = request
            reply = self._call(device_name, method_name, *args, **kwargs)
            # Set copy=False since we have a pickled representation of the data.
            self.socket.send(pickle.dumps(reply), copy=False)

    def close(self):
        self._keep_receiving.clear()
        self._receive_thread.join()
        self.socket.close()
        if not self.context_managed_externally:
            self.context.term()


class ZMQStreamServer:
    """Broadcaster for periodically calling a callable with specific args/kwargs
       at a specified frequency."""

    def __init__(self, protocol: Protocol = "tcp", interface: str = "*",
                 port: str = "5556", context: zmq.Context = None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.context = context or zmq.Context()
        self.context_managed_externally = context is not None
        # To publish from multiple threads (one per publish frequency) in a
        # thread-safe way, we need to send published data through a zmq proxy
        # via internal process communication. Each thread has a zmq publisher
        # socket will publish through the proxy, making the result thread-safe!
        self.worker_url = f"inproc://workers:{id(self)}"  # make unique per instance
        self.xsub_socket = self.context.socket(zmq.XSUB)
        self.xsub_socket.setsockopt(zmq.LINGER, 0)
        self.xsub_socket.bind(self.worker_url)
        self.xpub_socket = self.context.socket(zmq.XPUB)
        self.xpub_socket.setsockopt(zmq.LINGER, 0)
        pub_address = f"{protocol}://{interface}:{self.port}"
        self.log.warning(f"Publishing from: {pub_address}")
        self.xpub_socket.bind(pub_address)
        self.log.debug(f"Creating zmq proxy to forward messages from: {self.worker_url} to {pub_address}")
        # zmq stream proxy threads and control sockets
        self.zmq_streams = {}
        self.zmq_stream_ctrl_sockets = {}
        # Periodic stream data structures
        self.call_signature: dict[str, tuple] = {}
        self.call_encodings: dict[str, Encoding] = {}
        self.call_enabled: dict[str, bool] = {}
        self.calls_by_frequency: dict[float, set] = {}
        self.call_frequencies: dict[str, float] = {}
        self.locks_by_frequency: dict[float, Lock] = {}
        self.threads: dict[float, Thread] = {}
        self.manual_broadcast_sockets: dict[str, zmq.Context.socket] = {}
        self.manual_broadcast_encodings: dict[str, Encoding] = {}

        self.keep_broadcasting = Event()
        self.keep_broadcasting.set()
        self.proxy_thread: Thread = None
        self.is_running: bool = False

    def run(self, run_in_thread: bool = True):
        """Launch proxy to handle multiple threads publishing."""
        if self.is_running:  # extra flag in case we put `run` in a thread elsewhere.
            raise RuntimeError("Streamer is already running!")
        if not run_in_thread:
            self.is_running = True
            zmq.proxy(self.xpub_socket, self.xsub_socket)  # Does not return.
        else:  # If we need to return
            self.proxy_thread = Thread(target=self.run, args=[False],
                                       daemon=True)
            self.proxy_thread.start()

    def add(self, name: str, frequency_hz: float, func: Callable, *args, **kwargs):
        """Setup periodic function call with specific arguments at a set
        frequency.

        If the function is already being broadcasted, update the broadcast
        parameters.
        """
        # Add/update func params and call frequency.
        self.call_signature[name] = (func, args, kwargs)
        self.call_frequencies[name] = frequency_hz
        self.call_encodings[name] = "pickle"
        self.enable(name)
        # Store call by frequency.
        call_names = self.calls_by_frequency.get(frequency_hz, set())
        self.log.debug(f"Adding stream: {name} @ {frequency_hz}[Hz].")
        if not call_names:  # Add to dict if nothing broadcasts at this freq.
            self.calls_by_frequency[frequency_hz] = call_names
            self.locks_by_frequency[frequency_hz] = Lock()  # Create a new lock
        with self.locks_by_frequency[frequency_hz]:
            call_names.add(name)
        if frequency_hz in self.threads:  # Thread already exists.
            return
        # Create a new thread for calls made at this frequency.
        broadcast_thread = Thread(target=self._stream_worker,
                                  name=f"{frequency_hz:.3f}[Hz]_broadcast_thread",
                                  args=[frequency_hz], daemon=True)
        self.log.debug(f"Launching new thread '{broadcast_thread.name}' to "
                       f"broadcast function call results at {frequency_hz}[Hz].")
        broadcast_thread.start()
        self.threads[frequency_hz] = broadcast_thread

    def get_broadcast_fn(self, name: str,  set_timestamp: bool = False,
                         encoding: Encoding = "pickle"):
        """Get a function to broadcast the specified stream name.
        Useful if the application is creating data at its
        own rate and needs a callback function to call upon producing new data.

        This implicitly adds a manual stream to the configuration.

        :param name: stream name
        :param encoding: `pickle` only for now.
        :param set_timestamp: if true, return a function who's first argument is
            the timestamp to be set for the packet.

        """
        if name not in self.manual_broadcast_sockets:
            self.log.debug(f"Creating socket to manually broadcast stream: {name} to {self.worker_url}.")
            socket = self.context.socket(zmq.PUB)
            socket.setsockopt(zmq.LINGER, 0)
            socket.connect(self.worker_url)
            self.manual_broadcast_sockets[name] = socket
        socket = self.manual_broadcast_sockets[name]
        self.manual_broadcast_encodings[name] = encoding
        tstamp_option = "manual" if set_timestamp else "autogenerated"
        msg = f"Creating send function for stream {name} with {tstamp_option} timestamp."
        self.log.debug(msg)
        if set_timestamp:
            return lambda data, timestamp, s=socket, n=name, e=encoding: \
                self._send(s, n, data, timestamp=timestamp, encoding=e)
        return lambda data, s=socket, n=name, e=encoding: \
            self._send(s, n, data, encoding=e)

    @staticmethod
    def _send(socket: zmq.Context.socket, name: str, data: any,
              timestamp: float = None, encoding: Encoding = "pickle"):
        # TODO: support packing protocols besides pickle
        timestamp = timestamp if timestamp is not None else now()
        packet = name.encode("utf-8") + pickle.dumps((timestamp, data))
        # Set copy=False since we have a pickled representation of the data.
        socket.send(packet, copy=False)

    def add_zmq_stream(self, name: str, address: str, enabled: bool = True,
                       log_chatter: bool = False):
        """ Add a stream from an existing zmq PUB socket source.

        :param name: stream name
        :param address: zmq socket address: `{protocol}://{interface}:{port}`
        """
        self.log.debug(f"Creating zmq proxy to forward messages from: {address} to {self.worker_url}")
        # Create XSUB and XPUB relay
        xsub_socket = self.context.socket(zmq.XSUB)
        xsub_socket.setsockopt(zmq.LINGER, 0)
        xsub_socket.connect(address)

        xpub_socket = self.context.socket(zmq.XPUB)
        xpub_socket.setsockopt(zmq.LINGER, 0)
        xpub_socket.connect(self.worker_url)

        capture_socket = None
        capture_socket_address = f"inproc://{name}_cap_socket_debug"
        if log_chatter:
            capture_socket = self.context.socket(zmq.PUB)
            capture_socket.setsockopt(zmq.LINGER, 0)
            capture_socket.bind(capture_socket_address)

        ctrl_socket = self.context.socket(zmq.PAIR)
        ctrl_socket.setsockopt(zmq.LINGER, 0)
        ctrl_socket_address = f"inproc://{name}_stream_proxy_control"
        ctrl_socket.bind(ctrl_socket_address)
        # Create a steerable zmq proxy and run it in a separate daemon thread.
        self.log.warning("Creating socket")
        self.zmq_streams[name] = Thread(target=zmq.proxy_steerable,
                                        args=[xsub_socket, xpub_socket],
                                        kwargs={"capture": capture_socket,
                                                "control": ctrl_socket},
                                        daemon=True)
        self.zmq_streams[name].start()
        ext_ctrl_socket = self.context.socket(zmq.PAIR)
        ext_ctrl_socket.connect(ctrl_socket_address)
        self.zmq_stream_ctrl_sockets[name] = ext_ctrl_socket
        # pause if not enabled. (We can't start paused, so approximate.)
        if not enabled:
            self.log.warning("Pausing stream: {name}.")
            self.zmq_stream_ctrl_sockets[name].send_string("PAUSE")
        if not log_chatter:
            return

        def log_socket_chatter(self):
            """Capture data from the relayed zmq stream and log it (unless it
            is "large"). Useful for diagnosing issues."""
            cap_sub_socket = self.context.socket(zmq.SUB)
            cap_sub_socket.subscribe("")
            cap_sub_socket.connect(capture_socket_address)
            while True:
                data = cap_sub_socket.recv()
                msg = "capture got data: " + \
                      (f"{data}" if len(data) < 128 else "(too large to display).")
                self.log.debug(msg)

        Thread(target=log_socket_chatter, args=[self], daemon=True).start()

    def remove(self, name: str):
        """Remove a broadcasting function call that was previously added."""
        if name in self.zmq_streams:
            # Delete zmq proxy stream related data.
            self.zmq_stream_ctrl_sockets[name].send_string("TERMINATE")
            del self.zmq_streams[name]
            del self.zmq_stream_ctrl_sockets[name]
            return
        if name in self.call_frequencies:
            # Delete periodic-stream-related data.
            call_frequency = self.call_frequencies[name]
            with self.locks_by_frequency[call_frequency]:
                self.calls_by_frequency[call_frequency].remove(name)
                del self.call_signature[name]
                del self.call_frequencies[name]
                del self.call_enabled[name]
                # Broadcast thread for this frequency will exit if it has
                # nothing to do and delete the lock.
                return
        # Raise an error if we got here and didn't (or can't) remove anything.
        if name in self.manual_broadcast_sockets:
            raise ValueError(f"Cannot remove stream: {name}. Stream is "
                             f"called manually by the application.")
        raise ValueError(f"Cannot remove non-existent stream: {name}. ")

    def _stream_worker(self, frequency_hz: float, ):
        """Periodically broadcast all functions at the specified frequency.
        If there's nothing to do, exit."""
        socket = self.context.socket(zmq.PUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(self.worker_url)
        sleep_interval_s = 1.0/frequency_hz
        try:
            while self.keep_broadcasting.is_set():
                # Prevent size change in self.calls_by_frequency during iteration.
                with self.locks_by_frequency[frequency_hz]:
                    if not self.calls_by_frequency[frequency_hz]: # exit.
                        break
                    for func_name in self.calls_by_frequency[frequency_hz]:
                        if func_name not in self.call_enabled:
                            continue
                        # Invoke the function and dispatch the result.
                        func, args, kwargs = self.call_signature[func_name]
                        try:
                            result = func(*args, **kwargs)
                        except Exception as e:
                            self.log.error(f"func: {func_name}("
                                           f"{', '.join([str(a) for a in args])}"
                                           f"{', ' if (len(args) and len(kwargs)) else ''}"
                                           f"{', '.join([str(k) + '=' + str(v) for k, v in kwargs.items()])}) "
                                           f"raised an exception while executing: {str(e)}")
                            result = e
                        # Send topic name and result as packed binary data in
                        # *one* message so Subscriber topic filtering works.
                        self._send(socket, func_name, result, timestamp=now())
                sleep(sleep_interval_s)
        finally:
            if self.calls_by_frequency[frequency_hz]:
                del self.locks_by_frequency[frequency_hz]
            socket.close()

    def enable(self, name: str):
        """Enable a previously-configured stream by name.

        .. note::
           Enabling/disabling streams applies to periodically called streams
           and relayed zmq streams but not streams sending data via
           manually-called broadcast functions.

        """
        # Enable zmq stream
        if name in self.zmq_streams:
            self.zmq_stream_ctrl_sockets[name].send_string("RESUME")
            return
        # Enable manual stream
        if name in self.call_signature:
            self.call_enabled[name] = True
            return
        # Error if we got here
        if name in self.manual_broadcast_sockets:
            raise KeyError(f"Stream: {name} cannot be enabled or disabled.")
        raise KeyError(f"Stream: {name} is not configured.")

    def disable(self, stream_name: str):
        """Disable a previously-configured stream by name."""
        del self.call_enabled[stream_name]

    def _get_return_type(self, stream_name: str) -> str | None:
        try:
            return get_type_hints(self.call_signature[stream_name][0])["return"].__name__
        except KeyError:
            return None

    def get_configuration(self, to_dict: bool = True) -> Streams | dict:
        """
        Get a breakdown of every stream with its broadcast settings.
        """
        manual_streams = {n: Stream(encoding=self.manual_broadcast_encodings[n])
                          for n in self.manual_broadcast_sockets.keys()}
        periodic_streams = {n: PeriodicStream(encoding=self.call_encodings[n],
                                              return_type=self._get_return_type(n),
                                              frequency_hz=self.call_frequencies[n],
                                              enabled=self.call_enabled[n])
                            for n in self.call_signature.keys()}
        # FIXME: how do we get the encoding for received arbitrary zmq data?
        zmq_streams = {n: Stream(encoding="unspecified") for n in self.zmq_streams}
        streams = Streams(manual_streams=manual_streams,
                          periodic_streams=periodic_streams,
                          zmq_streams=zmq_streams)
        return streams.model_dump() if to_dict else streams

    def close(self):
        """Exit all threads gracefully."""
        self.keep_broadcasting.clear()
        for thread in self.threads.values():
            thread.join()
        for socket in self.manual_broadcast_sockets.values():
            socket.close()
        for socket in self.zmq_stream_ctrl_sockets.values():
            socket.send_string("TERMINATE")
        if not self.context_managed_externally:
            self.context.term()
