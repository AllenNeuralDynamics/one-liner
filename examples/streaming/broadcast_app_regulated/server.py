from one_liner.server import RouterServer
from time import perf_counter, sleep
from math import sin, pi, ceil


def sine_t(frequency_hz, phase_shift: float = 0):
    return sin(2 * pi * frequency_hz * perf_counter() + phase_shift)


if __name__ == "__main__":
    start_time = perf_counter()
    server = RouterServer()
    # Create a manual broadcast function that the application can call on its
    # own cadence.
    broadcast_sine_t = server.get_stream_fn(f"1hz_sine(t)", serializer="pickle",
                                            set_timestamp=False)
    server.run()
    try:
        while True:
            x = sine_t(1)  # sample sin(2*pi*1*t) (i.e: 1Hz sine wave).
            broadcast_sine_t(x)
            sleep(0.01)
    finally:
        server.close()
