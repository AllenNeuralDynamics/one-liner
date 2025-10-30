#!/usr/bin/env/python3

import pytest
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


def test_caching_option():
    sensor_index = 0
    sensors = SensorArray()  # Create an object
    server = RouterServer()  # Create a server.
    client = RouterClient()

    SEND_RATE = 1000
    RECV_RATE = 50  # Cannot get too large since we have to contend with
                    # system thread switch interval.
    RECV_INTERVAL = 1/RECV_RATE

    # Broadcast a method at 100 Hz.
    server.add_broadcast("measurement", SEND_RATE, sensors.get_data, sensor_index)
    # Add extraneous data to make sure topic filtering works.
    server.add_broadcast("noise", SEND_RATE, sensors.get_data, sensor_index+1)
    server.run()

    # Configure which topic to receive and how to store the data.
    client.configure_stream("measurement", storage_type="cache")
    start_time = now()

    # Read messages slower than we send them.
    # Ensure we are getting the latest data.
    # Check adjacent timestamps to make sure they are similar to RECV_RATE
    last_sample_time = now()
    old_tstamp = now()
    packet_deltas = []
    while ((now() - start_time) < 0.2):
        curr_time = now()
        # Read messages at a slower rate than the publisher is publishing them.
        if curr_time - last_sample_time < RECV_INTERVAL:
            continue
        last_sample_time = curr_time
        try:
            new_tstamp, new_data = client.get_stream("measurement")
            interpacket_delta_t = abs(new_tstamp - old_tstamp)
            packet_deltas.append(interpacket_delta_t)
            print(f"time: {new_tstamp:.3f} | data: {new_data} | delta: {interpacket_delta_t:.3f}")
            old_tstamp = new_tstamp
        except zmq.Again:
            pass
    average_recv_interval = sum(packet_deltas)/len(packet_deltas)
    print(f"average packet_delta: {average_recv_interval}")
    # average receive interval should be close to nominal receive interval and
    # *not* close to our send interval, which is faster.
    try:
        assert abs(average_recv_interval - RECV_INTERVAL) < RECV_INTERVAL * 0.1
    finally:
        client.close()
        server.close()
