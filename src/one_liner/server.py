"""Server for enabling remote control and broadcasting results of periodic function calls."""

import logging
import pickle
import zmq
from threading import Thread, Event, Lock
from time import sleep
from time import perf_counter as now
from typing import Callable


class RouterServer:
    __slots__ = ("context", "streamer", "rpc")

    """Interface for enabling remote control/monitoring of one or more object
       instances. Heavy lifting is delegated to two subordinate objects."""
    def __init__(self, rpc_port: str = "5555", broadcast_port: str = "5556",
                 **devices):
        self.context = zmq.Context()
        self.streamer = ZMQStreamServer(port=broadcast_port, context=self.context)
        # Pass streamer into RPC Server as another device so we can interact
        # with it remotely. Hide it with a "__" prefix.
        self.rpc = ZMQRPCServer(port=rpc_port, context=self.context,
                                __streamer=self.streamer,
                                **devices)

    def run(self):
        """Setup rpc listener and broadcaster."""
        self.rpc.run()
        self.streamer.run(run_in_thread=True)

    def add_broadcast(self, name: str, frequency_hz: float, func: Callable, *args, **kwargs):
        self.streamer.add(name, frequency_hz, func, *args, **kwargs)

    def get_broadcast_fn(self, name: str, set_timestamp: bool = False,
                         encoding: str = "pickle"):
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

    def __init__(self, port: str = "5555", context: zmq.Context = None,
                 **devices):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.context = context or zmq.Context()
        self.context_managed_externally = context is not None
        self.socket = self.context.socket(zmq.REP)
        self.socket.setsockopt(zmq.RCVTIMEO, 100)  # milliseconds
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.bind(f"tcp://*:{self.port}")
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
            self.socket.send(pickle.dumps(reply))

    def close(self):
        self._keep_receiving.clear()
        self._receive_thread.join()
        self.socket.close()
        if not self.context_managed_externally:
            self.context.term()


class ZMQStreamServer:
    """Broadcaster for periodically calling a callable with specific args/kwargs
       at a specified frequency."""

    def __init__(self, port: str = "5556", context: zmq.Context = None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.context = context or zmq.Context()
        self.context_managed_externally = context is not None
        # To publish from multiple threads (one per publish frequency) in a
        # thread-safe way, we need to send published data through a zmq proxy
        # via internal process communication. Each thread has a zmq publisher
        # socket will publish through the proxy, making the result thread-safe!
        self.worker_url = "inproc://workers"
        self.xsub_socket = self.context.socket(zmq.XSUB)
        self.xsub_socket.setsockopt(zmq.LINGER, 0)
        self.xsub_socket.bind(self.worker_url)
        self.xpub_socket = self.context.socket(zmq.XPUB)
        self.xpub_socket.setsockopt(zmq.LINGER, 0)
        self.xpub_socket.bind(f"tcp://*:{self.port}")
        self.call_signature: dict[str, tuple] = {}
        self.call_enabled: dict[str, bool] = {}
        self.calls_by_frequency: dict[float, set] = {}
        self.call_frequencies: dict[str, float] = {}
        self.locks_by_frequency: dict[float, Lock] = {}
        self.threads: dict[float, Thread] = {}
        self.manual_broadcast_sockets: dict[str, zmq.Context.socket] = {}

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
        self.enable(name)
        # Store call by frequency.
        call_names = self.calls_by_frequency.get(frequency_hz, set())
        if not call_names:  # Add to dict if nothing broadcasts at this freq.
            self.log.debug(f"Adding stream: {name} @ {frequency_hz}[Hz].")
            self.calls_by_frequency[frequency_hz] = call_names
            call_names.add(name)
            self.locks_by_frequency[frequency_hz] = Lock()  # Create a new lock
        else:
            with self.locks_by_frequency[frequency_hz]:
                call_names.add(name)
        if frequency_hz in self.threads:  # Thread already exists.
            return
        # Create a new thread for calls made at this frequency.
        broadcast_thread = Thread(target=self._stream_worker,
                                  name=f"{frequency_hz:.3f}[Hz]_broadcast_thread",
                                  args=[frequency_hz], daemon=True)
        broadcast_thread.start()
        self.threads[frequency_hz] = broadcast_thread

    def get_broadcast_fn(self, name: str,  set_timestamp: bool = False,
                         encoding: str = "pickle"):
        """Get a function to broadcast the specified stream name.
        Useful if the application is creating data at its
        own rate and needs a callback function to call upon producing new data.

        :param name: stream name
        :param encoding: `pickle` only for now.
        :param set_timestamp: if true, return a function who's first argument is
            the timestamp to be set for the packet.

        """
        if name not in self.manual_broadcast_sockets:
            self.log.debug(f"Creating socket to manually broadcast stream: {name}.")
            socket = self.context.socket(zmq.PUB)
            socket.setsockopt(zmq.LINGER, 0)
            socket.connect(self.worker_url)
            self.manual_broadcast_sockets[name] = socket
        socket = self.manual_broadcast_sockets[name]
        w_or_wo = "with" if set_timestamp else "without"
        msg = f"Creating send function for stream {name} {w_or_wo} timestamp."
        if set_timestamp:
            self.log.debug(msg)
            return lambda data, timestamp, s=socket, n=name, e=encoding: \
                self._send(s, n, data, timestamp=timestamp, encoding=e)
        return lambda data, s=socket, n=name, e=encoding: \
            self._send(s, n, data, encoding=e)

    @staticmethod
    def _send(socket: zmq.Context.socket, name: str, data: any,
              timestamp: float = None, encoding: str = "pickle"):
        # TODO: support packing protocols besides pickle
        timestamp = timestamp if timestamp is not None else now()
        packet = name.encode("utf-8") + pickle.dumps((timestamp, data))
        socket.send(packet)

    def remove(self, name: str):
        """Remove a broadcasting function call that was previously added."""
        if name not in self.call_signature:
            raise ValueError(f"Cannot remove {str(name)}. "
                             "Call is not being broadcasted.")
        # Delete all references!
        call_frequency = self.call_frequencies[name]
        with self.locks_by_frequency[call_frequency]:
            self.calls_by_frequency[call_frequency].remove(name)
            del self.call_signature[name]
            del self.call_frequencies[name]
            del self.call_enabled[name]
        # Broadcast thread for this frequency will exit if it has nothing to do
        # and delete the lock.

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
                            self.log.error(f"Function: {func}({args}, {kwargs}) "
                                           f"raised an exception while executing.")
                            result = e
                        # Send topic name and result as packed binary data in
                        # *one* message so Subscriber topic filtering works.
                        self._send(socket, func_name, result, timestamp=now())
                sleep(sleep_interval_s)
        finally:
            if self.calls_by_frequency[frequency_hz]:
                del self.locks_by_frequency[frequency_hz]
            socket.close()

    def enable(self, stream_name: str):
        """Enable a previously-configured stream by name."""
        if stream_name not in self.call_signature:
            raise KeyError(f"Stream: {stream_name} is not configured.")
        self.call_enabled[stream_name] = True

    def disable(self, stream_name: str):
        """Disable a previously-configured stream by name."""
        del self.call_enabled[stream_name]

    def close(self):
        """Exit all threads gracefully."""
        self.keep_broadcasting.clear()
        for thread in self.threads.values():
            thread.join()
        for socket in self.manual_broadcast_sockets.values():
            socket.close()
        if not self.context_managed_externally:
            self.context.term()
