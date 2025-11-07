import logging
import pickle
import zmq
from one_liner.utils import _send, Protocol
from threading import Thread, Event
from typing import Any


class ZMQRPCServer:
    """Remote Procedure Caller (RPC) Server. Call any method from a dict of
    object instances, and dispatch the serialized result to the connected
    RPC Client."""

    def __init__(self, protocol: Protocol = "tcp", interface: str = "*",
                 port: str = "5555", context: zmq.Context = None,
                 devices: dict[str, Any] = None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.context = context or zmq.Context()
        self.context_managed_externally = context is not None
        self.socket = self.context.socket(zmq.REP)
        self.socket.setsockopt(zmq.RCVTIMEO, 100)  # timeout for recv() in [ms].
                                                   # >0 but value is kinda arbitrary.
        self.socket.setsockopt(zmq.LINGER, 0)  # close immediately when context terminates
        address = f"{protocol}://{interface}:{self.port}"
        self.socket.bind(address)
        self._keep_receiving = Event()
        self._keep_receiving.set()
        self._receive_thread: Thread = None
        self.devices: dict[str, Any] = {} if devices is None else devices

    def run(self):
        """Launch thread to execute RPCs."""
        self._keep_receiving.set()
        self._receive_thread = Thread(target=self._receive_worker,
                                     name=f"REQ_receive_worker",
                                     daemon=True)
        self._receive_thread.start()

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
            if device_name not in self.devices:
                error_msg = f"{device_name} is not present in devices."
                self.log.error(error_msg)
                _send(self.socket, name="", data=error_msg, success=False)
                continue
            device = self.devices[device_name]
            try:
                func = getattr(device, method_name)  # Might raise AttributeError
            except AttributeError as e:
                self.log.error(f"Instance {device} does not have attribute: "
                               f"{method_name}. Error: {str(e)}")
                _send(self.socket, name="", data=str(e), success=False)
                continue
            try:
                reply = func(*args, **kwargs)
                _send(self.socket, name="", data=reply)
            except Exception as e:  # catch-all error calling the function.
                self.log.error(f"func: {method_name}("
                               f"{', '.join([str(a) for a in args])}"
                               f"{', ' if (len(args) and len(kwargs)) else ''}"
                               f"{', '.join([str(k) + '=' + str(v) for k, v in kwargs.items()])}) "
                               f"raised an exception while executing: {str(e)}")
                _send(self.socket, name="", data=str(e), success=False)

    def close(self):
        self._keep_receiving.clear()
        self._receive_thread.join()
        self.socket.close()
        if not self.context_managed_externally:
            self.context.term()
