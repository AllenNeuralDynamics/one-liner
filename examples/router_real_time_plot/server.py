from one_liner.server import RouterServer

from time import perf_counter, sleep
from math import sin, pi


NUM_STREAMS = 10

SAMPLE_RATE_HZ = 1000.
SAMPLE_INTERVAL_S = 1/SAMPLE_RATE_HZ


def sine_t(frequency_hz, phase_shift: float = 0):
    return sin(2 * pi * frequency_hz * perf_counter() + phase_shift)


if __name__ == "__main__":
    start_time = perf_counter()
    server = RouterServer()
    for i in range(NUM_STREAMS): # Create a few streams
        print(f"Adding broadcast: 1hz_side[{i}] at sampled at {SAMPLE_RATE_HZ} Hz")
        # Set sample and signal rates.
        server.add_broadcast(f"1hz_sine[{i}]", SAMPLE_RATE_HZ, sine_t, 1, i*2*pi/NUM_STREAMS)
    server.run()  # Start rpc thread.
    try:
        while True:
            sleep(0.1)
    finally:
        server.close()
