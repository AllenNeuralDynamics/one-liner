import logging
import pickle
import zmq
from one_liner import Protocol
from threading import Thread, Event


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
