from one_liner.server import ZMQStreamServer

from time import perf_counter, sleep
from math import sin, pi


NUM_STREAMS = 3

SAMPLE_RATE_HZ = 1000.
SAMPLE_INTERVAL_S = 1/SAMPLE_RATE_HZ


def sine_t(frequency_hz, phase_shift: float = 0):
    return sin(2 * pi * frequency_hz * perf_counter() + phase_shift)


if __name__ == "__main__":
    start_time = perf_counter()
    server = ZMQStreamServer()
    for i in range(NUM_STREAMS): # Create a few streams
        server.add(f"1hz_sine[{i}]", SAMPLE_RATE_HZ, sine_t, 1, i*2*pi/NUM_STREAMS) # set sample and signal rates
    try:
        while True:
            sleep(0.1)
    finally:
        server.close()
