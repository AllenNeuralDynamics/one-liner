"""Instrument client for controlling/monitoring a remote instrument. Basis for GUIs."""

import zmq
import pickle

# Notes:
# A UI using the Router needs to know what objects or methods are available
# on the other side of the client.


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
    and interact with it via remote control. (A remote procedure call interface)"""

    # TODO: Create a Thread to recv from the socket and cache the data!

    def __init__(self, ip_address: str = "localhost", port: str = "5556"):
        # Receive periodic broadcasted messages setup.
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.full_address = f"tcp://{ip_address}:{port}"
        self.socket.connect(self.full_address)
        self.socket.subscribe("")  # Subscribe to all topics.

    def receive(self):
        # Publish message
        return pickle.loads(self.socket.recv())

    def close(self):
        self.socket.close()

