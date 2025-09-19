"""Instrument client for controlling/monitoring a remote instrument. Basis for GUIs."""

import logging
import pickle
import zmq
from queue import SimpleQueue
from threading import Thread, Event
from time import perf_counter as now
from typing import Literal, Tuple


class RouterClient:

    def __init__(self, ip_address: str = "localhost",
                 rpc_port: str = "5555", broadcast_port: str = "5556"):
        self.rpc_client = ZMQRPCClient(ip_address=ip_address, port=rpc_port)
        self.stream_client = ZMQStreamClient(ip_address=ip_address,
                                             port=broadcast_port)

    def call(self, name, *args, **kwds):
        """Call a function/method and return the response."""
        return self.rpc_client.call(name, *args, **kwds)

    def receive_broadcast(self):
        """Receive the results of periodically called functions"""
        return self.stream_client.receive()

    def close(self):
        self.stream_client.close()
        self.rpc_client.close()


class ZMQRPCClient:

    def __init__(self, ip_address: str = "localhost", port: str = "5555"):
        # Receive periodic broadcasted messages setup.
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.full_address = f"tcp://{ip_address}:{port}"
        self.socket.connect(self.full_address)
        #self.socket.subscribe("")  # Subscribe to all topics.

    def call(self, obj_name: str, method_name: str, *args, **kwargs):
        """Call a function and return the result."""
        self.socket.send(pickle.dumps((obj_name, method_name, args, kwargs)))
        return pickle.loads(self.socket.recv())

    def close(self):
        self.socket.close()


class ZMQStreamClient:
    """Connect to an instrument server (likely running on an actual instrument)
    and receive periodically broadcasted function call results."""

    def __init__(self, ip_address: str = "localhost", port: str = "5556",
                 stream_cfg: dict = None):
        """
        :param stream_cfg: if specified, configure streams specified in the
            provided config.
        """
        # Receive periodic broadcasted messages setup.
        self.log = logging.getLogger(self.__class__.__name__)
        self.context = zmq.Context()
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
        socket.connect(self.full_address)
        socket.subscribe(name)
        if storage_type == "cache":
            socket.setsockopt(zmq.CONFLATE, 1)  # last msg only.
        else:
            socket.setsockopt(zmq.RCVHWM, 1000) # Buffer up to 1000 msgs.
        self.sub_sockets[name] = socket

    def get(self, stream_name: str) -> Tuple[float, any]:
        """Return the timestamped data."""
        topic_bytes, packet_bytes = self.sub_sockets[stream_name].recv_multipart(flags=zmq.NOBLOCK)
        return pickle.loads(packet_bytes)


    def close(self):
        for name, socket in self.sub_sockets.items():
            socket.close()

