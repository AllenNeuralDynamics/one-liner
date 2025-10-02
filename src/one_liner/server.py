"""Server for enabling remote control and broadcasting results of periodic function calls."""

import logging
import pickle
import zmq
from threading import Thread, Event, Lock
from time import sleep
from time import perf_counter as now
from typing import Callable


class RouterServer:
    """Interface for enabling remote control/monitoring of one or more object
       instances. Heavy lifting is delegated to two subordinate objects."""
    def __init__(self, rpc_port: str = "5555", broadcast_port: str = "5556",
                 **devices):
        self.streamer = ZMQStreamServer(port=broadcast_port)
        # Pass streamer into RPC Server as another device so we can interact
        # with it remotely. Hide it with a "__" prefix.
        self.rpc = ZMQRPCServer(port=rpc_port, __streamer=self.streamer,
                                **devices)

    def run(self):
        """Setup rpc listener and broadcaster."""
        self.rpc.run()
        self.streamer.run(run_in_thread=True)

    def add_broadcast(self, name: str, frequency_hz: float, func: Callable, *args, **kwargs):
        self.streamer.add(name, frequency_hz, func, *args, **kwargs)

    def remove_broadcast(self, func):
        self.streamer.remove(func)

    def close(self):
        self.streamer.close()


class ZMQRPCServer:
    """Remote Procedure Caller (RPC) Server. Call any method from a dict of
    object instances, and dispatch the serialized result to the connected
    RPC Client."""

    def __init__(self, port: str = "5555", **devices):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
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

    def stop(self):
        self._keep_receiving.clear()
        self._receive_thread.join()

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
            pickled_request = self.socket.recv()
            request = pickle.loads(pickled_request)
            device_name, method_name, args, kwargs = request
            reply = self._call(device_name, method_name, *args, **kwargs)
            self.socket.send(pickle.dumps(reply))


class ZMQStreamServer:
    """Broadcaster for periodically calling a callable with specific args/kwargs
       at a specified frequency."""

    def __init__(self, port: str = "5556"):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.context = zmq.Context()
        #self.socket = self.context.socket(zmq.PUB)
        #self.socket.bind(f"tcp://*:{self.port}")

        # To publish from multiple threads (one per publish frequency) in a
        # thread-safe way, we need a proxy setup via internal process
        # communication. Each thread will have a publisher that will publish
        # To the proxy, making this setup thread-safe!
        self.worker_url = "inproc://workers"
        self.xsub_socket = self.context.socket(zmq.XSUB)
        self.xsub_socket.bind(self.worker_url)
        self.xpub_socket = self.context.socket(zmq.XPUB)
        self.xpub_socket.bind(f"tcp://*:{self.port}")


        # Data structures.
        self.call_signature: dict[str, tuple] = {}
        self.call_enabled: dict[str, bool] = {}
        self.calls_by_frequency: dict[float, set] = {}
        self.call_frequencies: dict[str, float] = {}
        self.calls_lock = Lock()
        self.threads: dict[float, Thread] = {}
        self.keep_broadcasting = Event()
        self.keep_broadcasting.set()

    def run(self, run_in_thread: bool = True):
        """Launch proxy to handle multiple threads publishing."""

        if not run_in_thread:
            zmq.proxy(self.xpub_socket, self.xsub_socket)  # Does not return.
        else:  # If we need to return
            self.stream_thread = Thread(target = zmq.proxy,
                                        args = [self.xpub_socket,
                                                self.xsub_socket],
                                        daemon=True)
            self.stream_thread.start()

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
            self.calls_by_frequency[frequency_hz] = call_names
            call_names.add(name)
        else:
            with self.calls_lock:  # TODO: do this by frequency?
                call_names.add(name)
        if frequency_hz in self.threads: # Thread already exists.
            return
        # Create a new thread for calls made at this frequency.
        broadcast_thread = Thread(target=self._stream_worker,
                                  name=f"{frequency_hz:.3f}[Hz]_broadcast_thread",
                                  args=[frequency_hz], daemon=True)
        broadcast_thread.start()
        self.threads[frequency_hz] = broadcast_thread

    def remove(self, name: str):
        """Remove a broadcasting function call that was previously added."""
        if name not in self.call_signature:
            raise ValueError(f"Cannot remove {str(name)}. "
                             "Call is not being broadcasted.")
        # Delete all references!
        call_frequency = self.call_frequencies[name]
        with self.calls_lock:  # TODO: do this by frequency?
            self.calls_by_frequency[call_frequency].remove(name)
            del self.call_signature[name]
            del self.call_frequencies[name]
            del self.call_enabled[name]
       # Broadcast thread for this frequency will exit if it has nothing to do.

    def _stream_worker(self, frequency_hz: float, ):
        """Periodically broadcast all functions at the specified frequency.
        If there's nothing to do, exit."""
        socket = self.context.socket(zmq.PUB)
        socket.connect(self.worker_url)
        try:
            while self.keep_broadcasting.is_set():
                # Prevent size change in self.broadcast_calls during iteration.
                with self.calls_lock:
                    if not self.calls_by_frequency[frequency_hz]: # Nothing to do!
                        return
                    for func_name in self.calls_by_frequency[frequency_hz]:
                        if func_name not in self.call_enabled:
                            continue
                        # Invoke the function and dispatch the result.
                        func, args, kwargs = self.call_signature[func_name]
                        try:
                            # Send topic name and result as packed binary data in
                            # *one* message so Subscriber topic filtering works.
                            reply = (func_name.encode("utf-8") +
                                    pickle.dumps((now(), func(*args, **kwargs))))
                        except Exception as e:
                            self.log.error(f"Function: {func}({args}, {kwargs}) raised "
                                           f"an exception while executing.")
                            # FIXME: this is a brittle way to send an exception.
                            reply = (func_name.encode("utf-8") +
                                     pickle.dumps((now(), str(e))))
                        socket.send(reply)
                sleep(1.0/frequency_hz)
        finally:
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
        try:
            for thread in self.threads.values():
                thread.join()
        finally:
            self.socket.close()
            self.context.term()
