#!/usr/bin/env/python3

import pytest
from random import uniform
from one_liner.client import RouterClient
from one_liner.server import RouterServer
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


def test_server_creation():
    server = RouterServer()  # Create a server.


def test_many_client_receive():
    sensor_index = 0
    sensors = SensorArray()  # Create an object
    server = RouterServer()  # Create a server.
    clients = [RouterClient(), RouterClient()]

    # Broadcast a method at 10 Hz.
    server.add_broadcast(10, sensors.get_data, sensor_index)

    received_data = None
    start_time = now()
    while ((now() - start_time) < 1):
        received_data0 = clients[0].receive_broadcast()
        received_data1 = clients[1].receive_broadcast()
        print(f"received: {received_data0} | {received_data1}")
        #assert 0.0 <= received_data[sensor_index] <= 5.0
    server.close()
