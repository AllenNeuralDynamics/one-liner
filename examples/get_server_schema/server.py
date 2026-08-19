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


class DanceMoves(Enum):
    MOONWALK = "moonwalk"
    ROBOT = 123
    SALSA = "salsa"


class Dancer(BaseModel):
    name: str
    moves: list[DanceMoves]


class DiscoDevice:
    def __init__(self):
        self.tune = "Funky Town"

    # Example with custom type parameters and custom return type
    def get_dancer(self, name: str = "John Doe", moves: list[DanceMoves] = []) -> dict:
        """
        Get a groovy dancer to dance to some tunes.

        Parameters
        ----------
        name : str
            The name of the dancer.
        moves : list[DanceMoves]
            The dance moves the dancer can perform.

        Returns
        -------
        dict
            A dictionary representation of the dancer.

        Examples
        --------
        >>> disco_device = DiscoDevice()
        >>> disco_device.get_dancer(name="Jane Doe", moves=[DanceMoves.MOONWALK, DanceMoves.ROBOT])
        {'name': 'Jane Doe', 'moves': ['moonwalk', 'robot']}
        """
        return Dancer(name=name, moves=moves).model_dump()

    # Example with multiple return types
    def change_tune(self, tune: str) -> str | int:
        """Change the tune of the disco device. Returns 1 if the tune is "Boogie Wonderland"."""
        self.tune = tune
        if self.tune == "Boogie Wonderland":
            return 1
        return self.tune

    def no_annotation(self, test: None):
        return "what do I even do: " + str(test)


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
    server.add_named_call(
        "get_dancer",
        "disco_device",
        "get_dancer",
    )
    server.add_named_call(
        "change_tune",
        "disco_device",
        "change_tune",
    )
    server.add_named_call(
        "no_annotation",
        "disco_device",
        "no_annotation",
    )

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
        print(
            f"Adding broadcast: 1hz_boogie_wave[{i}] sampled at {SAMPLE_RATES_HZ[i]} Hz"
        )
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
