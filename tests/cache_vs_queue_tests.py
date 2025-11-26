#!/usr/bin/env/python3

import pytest
import zmq
from collections import deque
from random import uniform
from one_liner.client import RouterClient
from one_liner.server import RouterServer
from time import sleep
from time import perf_counter as now


# Create a simple class to pass into the server.
class SensorArray:
    __test__ = False

    def __init__(self):
        self.queue = None
        self.reset_queue()

    def get_data(self, sensor_index: int = 0) -> dict[int, float]:
        """Get spoofed analog voltage sensor data: 0.0-5.0 [V] measurement from
            specified sensor index."""
        return {sensor_index: uniform(0., 5.)}

    def reset_queue(self):
        self.queue = deque(list(range(10)))

    def get_series_item(self) -> int:
        item = self.queue.popleft()
        return item


def test_caching_option():
    sensor_index = 0
    sensors = SensorArray()  # Create an object
    server = RouterServer()  # Create a server.
    client = RouterClient()

    SEND_RATE = 1000
    RECV_RATE = 50  # Cannot get too large since we have to contend with
                    # system thread switch interval.
    RECV_INTERVAL = 1/RECV_RATE

    try:
        # Broadcast a method at 100 Hz.
        server.add_stream("measurement", SEND_RATE, sensors.get_data, args=[sensor_index])
        server.run()

        # Configure which topic to receive and how to store the data.
        client.configure_stream("measurement", storage_type="cache")
        start_time = now()

        # Read messages slower than we send them.
        # Ensure we are getting the latest data.
        # Check adjacent timestamps to make sure they are similar to RECV_RATE
        old_tstamp = now()
        packet_deltas = []
        while ((now() - start_time) < 0.2):
            # Read messages at a slower rate than the publisher is publishing them.
            sleep(RECV_INTERVAL)
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
        assert abs(average_recv_interval - RECV_INTERVAL) < RECV_INTERVAL * 0.1
    finally:
        server.close()
        client.close()
        zmq.Context.instance().destroy() # We shouldn't have to do this.


def test_get_last_sent_value():
    sensor = SensorArray()
    server = RouterServer()
    client = RouterClient()
    try:
        # Configure stream to receive the **most recent** item only.
        client.configure_stream("series_data", storage_type="cache")
        send_element = server.get_stream_fn("series_data")
        server.run()  # Make the connection with the client first.
        sleep(0.1)  # Wait for all connections to take place
        # Send one element of the series
        last_element_sent = None
        for i in range(3):
            last_element_sent = sensor.get_series_item()
            send_element(last_element_sent)
            print(f"Sent: {last_element_sent}")
        sleep(0.01)  # Wait for data to be transmitted.
        # Receive and check the data.
        data = client.get_stream("series_data")[1]
        print(f"received: {data}")
        assert data == last_element_sent
        # Do it again:
        for i in range(3):
            last_element_sent = sensor.get_series_item()
            send_element(last_element_sent)
            print(f"Sent: {last_element_sent}")
        sleep(0.01)  # Wait for data to be transmitted.
        # Receive and check the data.
        data = client.get_stream("series_data")[1]
        print(f"Received: {data}")
        assert data == last_element_sent
    finally:
        client.close()
        server.close()
        zmq.Context.instance().destroy()
