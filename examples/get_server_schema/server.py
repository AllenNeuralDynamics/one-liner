import random

from one_liner.server import RouterServer

random.seed(1)  # Make call to random values consistent across module usage.
from enum import Enum
from math import ceil, pi, sin
from random import randint
from time import perf_counter, sleep

from pydantic import BaseModel

NUM_STREAMS = 3
MAX_SAMPLE_RATE_HZ = 500
# Sample sine waves at different rates to produce different streaming threads.
SAMPLE_RATES_HZ = [
    randint(ceil(MAX_SAMPLE_RATE_HZ / 3), MAX_SAMPLE_RATE_HZ)
    for _ in range(NUM_STREAMS)
]


################################################################################
#
#    RPC example
#
################################################################################


class HairType(Enum):
    STRAIGHT = "straight"
    WAVY = "wavy"
    CURLY = "curly"
    AFRO = "afro"


class Dancer(BaseModel):
    hair_type: HairType
    dance_moves: list[str]


class DiscoDevice:
    def __init__(self):
        self.tune = "Funky Town"

    # Example with custom type parameters and custom return type
    def get_dancer(self, hair_type: HairType = HairType.STRAIGHT) -> Dancer:
        return Dancer(hair_type=hair_type, dance_moves=["moonwalk", "robot", "salsa"])

    # Example with multiple return types
    def change_tune(self, tune: str) -> str | int:
        self.tune = tune
        if self.tune == "Boogie Wonderland":
            return 1
        return self.tune


################################################################################
#
#    Streams example
#
################################################################################


class BoogieWave(BaseModel):
    sin_wave: float
    motto: str


# Stream with model as return type
def boogie_wave(
    frequency_hz, phase_shift: float = 0, motto: str = "Let Dance!"
) -> BoogieWave:
    return BoogieWave(
        sin_wave=sin(2 * pi * frequency_hz * perf_counter() + phase_shift), motto=motto
    )


# Stream with primitive return type
def sine_t(frequency_hz, phase_shift: float = 0) -> float:
    return sin(2 * pi * frequency_hz * perf_counter() + phase_shift)


################################################################################
#
#    Client code
#
################################################################################


if __name__ == "__main__":
    start_time = perf_counter()

    disco_device = DiscoDevice()
    server = RouterServer(instances={"disco_device": disco_device})

    # Add RPCs
    server.add_named_call("get_dancer", "disco_device", "get_dancer", args=[HairType.CURLY])
    server.add_named_call("change_tune", "disco_device", "change_tune", args=["Boogie Wonderland"])


    # Add Streams
    for i in range(NUM_STREAMS):  # Create a few streams
        print(f"Adding broadcast: 1hz_side[{i}] sampled at {SAMPLE_RATES_HZ[i]} Hz")
        # Set sample and signal rates.
        server.add_stream_from_callable(
            f"1hz_sine[{i}]",
            SAMPLE_RATES_HZ[i],
            sine_t,
            args=[1, i * 2 * pi / NUM_STREAMS],
        )

    for i in range(NUM_STREAMS):  # Create a few streams
        print(f"Adding broadcast: 1hz_boogie_wave[{i}] sampled at {SAMPLE_RATES_HZ[i]} Hz")
        # Set sample and signal rates.
        server.add_stream_from_callable(
            f"1hz_boogie_wave[{i}]",
            SAMPLE_RATES_HZ[i],
            boogie_wave,
            args=[1, i * 2 * pi / NUM_STREAMS],
        )

    server.run()  # Start broadcast and rpc threads.
    try:
        while True:
            sleep(0.1)
    finally:
        server.close()
