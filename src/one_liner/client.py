"""client for controlling/monitoring one or more remote python objects."""

import logging
import pickle
import zmq
from typing import Literal, Tuple


class RouterClient:

    def __init__(self, ip_address: str = "localhost",
                 rpc_port: str = "5555", broadcast_port: str = "5556"):
        self._context = zmq.Context()  # Create shared context
        self.rpc_client = ZMQRPCClient(ip_address=ip_address, port=rpc_port,
                                       context=self._context)
        self.stream_client = ZMQStreamClient(ip_address=ip_address,
                                             port=broadcast_port,
                                             context=self._context)

    def call(self, name, *args, **kwds):
        """Call a function/method and return the response."""
        return self.rpc_client.call(name, *args, **kwds)

    def configure_stream(self, name: str,
                         storage_type: Literal["queue", "cache"] = "queue"):
        """Configure data received from a stream to either hold one the latest
        data (`cache`) or to hold onto all data (`queue`)."""
        self.stream_client.configure_stream(name, storage_type)

    def enable_stream(self, stream_name):
        """Enable broadcasting of a stream by name."""
        # Use rpc_client to enable/disable streams.
        self.rpc_client.call("__streamer", "enable", stream_name)

    def disable_stream(self, stream_name):
        """Disable broadcasting of a stream by name."""
        # Use rpc_client to enable/disable streams.
        self.rpc_client.call("__streamer", "disable", stream_name)

    def get_stream(self, stream_name: str, block: bool = False) -> Tuple[float, any]:
        """Receive the results of a configured stream."""
        return self.stream_client.get(stream_name, block=block)

    def close(self):
        self.stream_client.close()
        self.rpc_client.close()


class ZMQRPCClient:

    def __init__(self, ip_address: str = "localhost", port: str = "5555",
                 context: zmq.Context = None):
        self.context = context or zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.full_address = f"tcp://{ip_address}:{port}"
        self.socket.connect(self.full_address)

    def call(self, obj_name: str, method_name: str, *args, **kwargs):
        """Call a function and return the result."""
        self.socket.send(pickle.dumps((obj_name, method_name, args, kwargs)))
        # TODO: no real exception-handling here yet.
        return pickle.loads(self.socket.recv())

    def close(self):
        self.socket.close()


class ZMQStreamClient:
    """Connect to an instrument server (likely running on an actual instrument)
    and receive periodically broadcasted function call results."""

    def __init__(self, ip_address: str = "localhost", port: str = "5556",
                 stream_cfg: dict = None, context: zmq.Context = None):
        """
        :param stream_cfg: if specified, configure streams specified in the
            provided config.
        """
        # Receive periodic broadcasted messages setup.
        self.log = logging.getLogger(self.__class__.__name__)
        self.context = context or zmq.Context()
        self.full_address = f"tcp://{ip_address}:{port}"
        self.sub_sockets = {}
        if stream_cfg:
            for stream_name, storage_type in stream_cfg.items():
                self.configure_stream(stream_name, storage_type)

    def configure_stream(self, name: str,
                         storage_type: Literal["queue", "cache"] = "queue"):
        """Create a subscriber socket to receive a specific topic and setup
        how to buffer data.
        * queue -> FIFO.
        * cache -> only the latest data is received.
        """
        # Create zmq socket and configure to either queue or get-the-latest data.
        socket = self.context.socket(zmq.SUB)
        socket.subscribe(name)
        if storage_type == "cache":
            socket.setsockopt(zmq.CONFLATE, 1)  # last msg only
        else:
            socket.setsockopt(zmq.RCVHWM, 1000) # Buffer up to 1000 msgs.
        socket.connect(self.full_address)
        self.sub_sockets[name] = socket

    def get(self, stream_name: str, block: bool = False) -> Tuple[float, any]:
        """Return the timestamped data.

        :raises zmq.Again: if block is False (default) and no data is present.
        """
        flags = 0 if block else zmq.NOBLOCK
        pickled_data = self.sub_sockets[stream_name].recv(flags=flags)
        offset = len(stream_name)
        return pickle.loads(pickled_data[offset:])  # Strip off topic prefix.

    def close(self):
        for name, socket in self.sub_sockets.items():
            socket.close()

