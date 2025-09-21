"""Instrument Server for enabling remote control/monitoring of the Instrument"""

import builtins
import logging
import pickle
import zmq
from threading import Thread, Event, Lock
from time import sleep
from time import perf_counter as now
from typing import Callable


# Note: Can we implement a WriteToken as an object instance that we pass
# into self.devices?


# TODO: Can *multiple* Router Clients connect to the RouterServer??


class RouterServer:
    """Interface for enabling remote control/monitoring of one or more object
       instances."""
    def __init__(self, rpc_port: str = "5555", broadcast_port: str = "5556",
                 **devices):
        self.streamer = ZMQStreamServer(port=broadcast_port)
        # Pass along streamer so we can configure it remotely.
        self.rpc = ZMQRPCServer(port=rpc_port, __streamer=self.streamer,
                                **devices)

    def run(self):
        """Setup rpc listener and broadcaster."""
        self.rpc.run()

    def add_broadcast(self, frequency_hz: float, func: Callable, *args, **kwargs):
        self.streamer.add(frequency_hz, func, *args, **kwargs)

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
        self.keep_receiving = Event()
        self.keep_receiving.set()
        self.devices = devices

    def run(self):
        """Launch thread to execute RPCs."""
        thread = Thread(target=self._receive_worker,
                        name=f"REQ_receive_thread",
                        daemon=True)
        thread.start()

    def stop(self):
        self.keep_receiving.clear()

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
        while self.keep_receiving.is_set():
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
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(f"tcp://*:{self.port}")

        self.call_signature: Dict[str, Tuple] = {}
        self.call_enabled: Dict[str, bool] = {}
        self.calls_by_frequency: Dict[float, set] = {}
        self.call_frequencies: Dict[Callable, float] = {}
        self.calls_lock = Lock()
        self.threads: Dict[float, Thread] = {}
        self.keep_broadcasting = Event()
        self.keep_broadcasting.set()

    def add(self, name: str, frequency_hz: float, func: Callable, *args, **kwargs):
        """Setup periodic function call with specific arguments at a set
        frequency.

        If the function is already being broadcasted, update the broadcast
        parameters.
        """
        # TODO: handle case where this broadcast is already stored, but at a
        # different frequency or with different parameters.
        # TODO: if calls already exist at this frequency, lock out access to
        # these containers.

        # Add/update func params and call frequency.
        # FIXME: dict storage strategy is kruft?
        self.call_signature[name] = (func, args, tuple(sorted(kwargs.items()))) # Dicts aren't hashable.
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
            raise ValueError(f"Cannot remove {str(func)}. "
                             "Call is not being broadcasted.")
        # Delete all references!
        call_frequency = self.call_frequencies[name]
        with self.calls_lock:  # TODO: do this by frequency?
            self.calls_by_frequency[call_frequency].remove(name)
            del self.call_signature[name]
            del self.call_frequencies[name]
            del self.call_enabled[name]
       # Broadcast thread for this frequency will exit if it has nothing to do.

    def _stream_worker(self, frequency_hz: float):
        """Periodically broadcast all functions at the specified frequency.
        If there's nothing to do, exit."""
        while self.keep_broadcasting.is_set():
            # Prevent size change in self.broadcast_calls during iteration.
            with self.calls_lock:
                if not self.calls_by_frequency[frequency_hz]: # Nothing to do!
                    return
                for func_name in self.calls_by_frequency[frequency_hz]:
                    if func_name not in self.call_enabled:
                        continue
                    # Invoke the function and dispatch the result.
                    params = self.call_signature[func_name]
                    func = params[0]
                    args = params[1]
                    kwargs = dict(params[2])
                    try:
                        # Send result as a tuple.
                        reply = (func_name.encode("utf-8"),
                                 pickle.dumps((now(), func(*args, **kwargs))))
                    except Exception as e:
                        self.log.error(f"Function: {func}({args}, {kwargs}) raised "
                                       f"an exception while executing.")
                        reply = str(e)
                    self.socket.send_multipart(reply)
            sleep(1.0/frequency_hz)

    def enable(self, stream_name: str):
        if stream_name not in self.call_signature:
            raise KeyError(f"Stream: {stream_name} is not configured.")
        self.call_enabled[stream_name] = True

    def disable(self, stream_name: str):
        del self.call_enabled[stream_name]

    def close(self):
        self.keep_broadcasting.clear()
        try:
            for thread in self.threads.values():
                thread.join()
        finally:
            self.socket.close()
            self.context.term()
