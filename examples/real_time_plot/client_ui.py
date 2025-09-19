import numpy as np
from gr.pygr import *
from one_liner.client import ZMQStreamClient
from one_liner.server import ZMQStreamServer
from collections import deque

from random import random
from time import perf_counter, sleep
from math import sin, pi

import queue
import zmq


SAMPLES = 500
PLOT_INTERVAL_S = 1/60.


def sine_t(frequency_hz):
    return sin(2 * pi * frequency_hz * perf_counter())


start_time = perf_counter()

if __name__ == "__main__":
    server = ZMQStreamServer()
    for i in range(1, 10): # Create a few streams
        server.add(f"{i}hz_sine", 1000, sine_t, 1) # sample at 1KHz

    client = ZMQStreamClient()
    client.configure_stream("1hz_sine", storage_type="queue")

    buffer = deque(maxlen=SAMPLES)
    old_timestamp = None

    sleep(0.02)

    last_plot_time = perf_counter()
    # Display one stream.
    try:
        while perf_counter() - start_time < 3:
            while True:
                try:  # pull everything out.
                    buffer.append(client.get("1hz_sine")[1])
                except zmq.Again:
                    break
            if not len(buffer):
                continue
            curr_time = perf_counter()
            if curr_time - last_plot_time >= PLOT_INTERVAL_S:
                plot(range(SAMPLES), np.asarray(buffer), xlim=(0, SAMPLES), ylim=(-1, 1))
                last_plot_time = curr_time
    finally:
        client.close()
        server.close()
