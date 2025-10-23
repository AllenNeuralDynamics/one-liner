import json
import logging
import pickle
import zmq
from one_liner import Encoding, Protocol
from one_liner.stream_schema import Stream, PeriodicStream, Streams
from threading import Event, Lock, Thread
from time import perf_counter as now
from time import sleep
from typing import Callable, get_type_hints, Any


SERIALIZERS = \
    {
        None: lambda x: x,
        "pickle": pickle.dumps,
        "json": json.dumps
    }


class ZMQStreamServer:
    """Broadcaster for periodically calling a callable with specific args/kwargs
       at a specified frequency."""
    __slots__ = ("log", "port", "context_managed_externally", "worker_url",
                 "context", "xsub_socket", "xpub_socket",
                 "zmq_streams", "zmq_stream_ctrl_sockets",
                 "call_signature", "call_encodings", "call_enabled",
                 "calls_by_frequency", "call_frequencies", "locks_by_frequency",
                 "threads", "manual_broadcast_sockets",
                 "manual_broadcast_encodings", "keep_broadcasting",
                 "proxy_thread", "is_running")

    def __init__(self, protocol: Protocol = "tcp", interface: str = "*",
                 port: str = "5556", context: zmq.Context = None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.context = context or zmq.Context()
        self.context_managed_externally = context is not None
        # zmq sockets are not thread-safe!
        # To publish from multiple threads (one per publish frequency) in a
        # thread-safe way, we need to create a socket per-thread and send
        # published data through a zmq proxy via internal same-process
        # communication (zmq's "inproc"). The result is thread-safe by design
        # without needing to use any Python semantics (locks, etc).
        self.worker_url = f"inproc://workers:{id(self)}"  # unique per instance
        self.xsub_socket = self.context.socket(zmq.XSUB)
        self.xsub_socket.setsockopt(zmq.LINGER, 0)
        self.xsub_socket.bind(self.worker_url)
        self.xpub_socket = self.context.socket(zmq.XPUB)
        self.xpub_socket.setsockopt(zmq.LINGER, 0)
        pub_address = f"{protocol}://{interface}:{self.port}"
        self.log.warning(f"Publishing from: {pub_address}")
        self.xpub_socket.bind(pub_address)
        self.log.debug(f"Creating zmq proxy to forward messages from: "
                       f"{self.worker_url} to {pub_address}.")
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

        self.keep_broadcasting: Event = Event()
        self.keep_broadcasting.set()
        self.proxy_thread: Thread | None = None
        self.is_running: bool = False

    def run(self, run_in_thread: bool = True):
        """Launch proxy to handle multiple threads publishing.

        :param run_in_thread:  if True (default) run the proxy in a separate
           thread to prevent blocking.

        """
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
        frequency. If the function is already being broadcasted, update the
        broadcast parameters.
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
    def _send(socket: zmq.Context.socket, name: str, data: bytes | Any,
              timestamp: float = None, encoding: Encoding = "pickle"):
        """Send the data on tne specified socket prefixed with the specified
        stream name.

        :param socket: socket to do the sending.
        :param name: stream name. Under the hood, this is the topic.
        :param data: data to send. If the data is not `bytes`-like, the
           :paramref:`ZMQStreamServer._send.encoding` option cannot be None.
        :param encoding: the encoding option which to encode the data or `None`
           if the data is `bytes`-like. Default is `"pickle"`.

        """
        timestamp = timestamp if timestamp is not None else now()
        # Because the CONFLATE option (keep-last-message) only, doesn't work
        # with multipart messages where the first msg is the topic, we smush
        # the topic and data together before sending.
        packet = name.encode("utf-8") + SERIALIZERS[encoding]((timestamp, data))
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

        # To sniff zmq data in a proxy we need to create another socket.
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

    def _get_return_type(self, name: str) -> str | None:
        """Get the return type of the stream name if it was type-hinted or
        None if it was not type hinted."""
        try:
            return get_type_hints(self.call_signature[name][0])["return"].__name__
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
