#!/usr/bin/env/python3

import zmq
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
    server.run()
    server.close()


def test_many_client_receive():
    sensor_index = 0
    sensors = SensorArray()  # Create an object
    server = RouterServer()  # Create a server.
    clients = [RouterClient(), RouterClient()]

    # Broadcast a method at 10 Hz.
    server.add_stream_from_callable("sensor_data", 10, sensors.get_data, args=[0])
    server.run()

    for client in clients:
        client.configure_stream("sensor_data")
    start_time = now()
    while ((now() - start_time) < 1):
        received_data0 = clients[0].get_stream("sensor_data", block=True)
        received_data1 = clients[1].get_stream("sensor_data", block=True)
        print(f"received: {received_data0} | {received_data1}")
    server.close()
    for client in clients:
        client.close()
    zmq.Context.instance().term()
