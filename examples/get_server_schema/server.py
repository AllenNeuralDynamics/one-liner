from one_liner.server import RouterServer
import random
random.seed(1)  # Make call to random values consistent across module usage.
from random import randint

from time import perf_counter, sleep
from math import sin, pi, ceil


NUM_STREAMS = 10
MAX_SAMPLE_RATE_HZ = 500
# Sample sine waves at different rates to produce different streaming threads.
SAMPLE_RATES_HZ = [randint(ceil(MAX_SAMPLE_RATE_HZ/10), MAX_SAMPLE_RATE_HZ) \
                   for _ in range(NUM_STREAMS)]


def sine_t(frequency_hz, phase_shift: float = 0) -> float:
    return sin(2 * pi * frequency_hz * perf_counter() + phase_shift)


if __name__ == "__main__":
    start_time = perf_counter()
    server = RouterServer()
    for i in range(NUM_STREAMS): # Create a few streams
        print(f"Adding broadcast: 1hz_side[{i}] sampled at {SAMPLE_RATES_HZ[i]} Hz")
        # Set sample and signal rates.
        server.add_broadcast(f"1hz_sine[{i}]", SAMPLE_RATES_HZ[i],
                             sine_t, 1, i*2*pi/NUM_STREAMS)
    server.run()  # Start broadcast and rpc threads.
    try:
        while True:
            sleep(0.1)
    finally:
        server.close()
