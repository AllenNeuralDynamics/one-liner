#!/usr/bin/env/python3

import pytest
import zmq
from random import uniform
from one_liner.client import ZMQStreamClient
from one_liner.server import ZMQStreamServer
from time import sleep
from time import perf_counter as now

import logging
logging.basicConfig(level=logging.DEBUG)


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
    server = ZMQStreamServer()  # Create a server.
    server.run()
    server.close()
    # Debugging: show all open sockets
    ctx = zmq.Context.instance()
    if hasattr(ctx, '_sockets'):
        # Extract the actual socket objects from the weak references
        open_sockets = [s for s in ctx._sockets if s is not None]
        print(f"Context is waiting for {len(open_sockets)} socket(s):")
        for s in open_sockets:
            # Print socket type and its memory address for identification
            print(f" - Socket Type: {s.socket_type}, Address: {hex(id(s))}")
    zmq.Context.instance().term()


def test_server_broadcast():
    sensors = SensorArray()  # Create an object
    server = ZMQStreamServer()  # Create a server.
    # broadcast a method at 100[Hz].
    server.add("sensor_0", 100, sensors.get_data, args=[0])
    server.add("sensor_1", 100, sensors.get_data, args=[1])
    server.run()
    sleep(0.05)
    server.close()
    zmq.Context.instance().term()


def test_client_receive():
    sensor_index = 0
    sensors = SensorArray()  # Create an object
    server = ZMQStreamServer()  # Create a server.
    client = ZMQStreamClient()  # Create a client.
    # broadcast a method at 10[Hz].
    server.add(f"sensor_{sensor_index}", 10, sensors.get_data, args=[sensor_index])
    server.run()
    client.configure_stream(f"sensor_{sensor_index}")
    start_time = now()
    try:
        while ((now() - start_time) < 1):
            received_data = client.get(f"sensor_{sensor_index}", block=True)
            #print(f"received: {received_data}")
            assert 0.0 <= received_data[1][sensor_index] <= 5.0
    finally:
        client.close()
        server.close()
        zmq.Context.instance().term()


def test_live_add_remove_broadcast():
    """Add, then remove a broadcast. Ensure that it has been removed."""
    sensor_index = 0
    broadcast_frequency = 10
    sensors = SensorArray()  # Create an object
    server = ZMQStreamServer()  # Create a server.
    client = ZMQStreamClient()  # Create a client.
    # broadcast a method at 10[Hz].
    server.run()
    server.add(f"sensor_{sensor_index}", broadcast_frequency, sensors.get_data,
               args=[sensor_index])
    server.remove(f"sensor_{sensor_index}")
    try:
        # Thread should exit after removing the only periodic function.
        server._threads[10].join(timeout=1)
    finally:
        client.close()
        server.close()
        zmq.Context.instance().term()
