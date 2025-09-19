from one_liner.server import ZMQStreamServer

from time import perf_counter, sleep
from math import sin, pi


SAMPLE_RATE_HZ = 1000.
SAMPLE_INTERVAL_S = 1/SAMPLE_RATE_HZ


def sine_t(frequency_hz):
    return sin(2 * pi * frequency_hz * perf_counter())


if __name__ == "__main__":
    start_time = perf_counter()
    server = ZMQStreamServer()
    for i in range(1, 10): # Create a few streams
        server.add(f"{i}hz_sine", SAMPLE_RATE_HZ, sine_t, 1) # set sample and signal rates
    try:
        while True:
            sleep(0.1)
    finally:
        server.close()
