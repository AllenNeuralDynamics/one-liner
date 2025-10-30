#!/usr/bin/env/python3

import pytest
from random import uniform
from one_liner.client import ZMQRPCClient
from one_liner.server import ZMQRPCServer
from time import sleep
from time import perf_counter as now


# Create a simple class to pass into the server.
class SensorArray:
    __test__ = False

    def __init__(self):
        pass

    def get_data(self, sensor_index: int = 0):
        """Get spoofed analog voltage sensor data: 0.0-5.0 [V] measurement from
            specified sensor index."""
        return {sensor_index: uniform(0., 5.)}


def test_rpc_server_creation():
    server = ZMQRPCServer()


def test_request_and_reply():
    sensors = SensorArray()  # Create an object
    server = ZMQRPCServer(sensors=sensors)  # Create a server.
    client = ZMQRPCClient()  # Create a server.
    server.run()

    data = client.call("sensors", "get_data", 0)
    try:
        assert 0 in data
        assert 0.0 <= data[0] <= 5.0
    finally:
        server.stop()
