from one_liner.client import RouterClient
from random import randint
from server import NUM_STREAMS
from time import perf_counter, sleep


if __name__ == "__main__":
    client = RouterClient()
    try:
        while True:
            sleep(2)
            i = randint(0, NUM_STREAMS-1)
            print(f"disabling stream {i}.")
            client.disable_stream(f"1hz_sine[{i}]")
            sleep(2)
            print(f"enabling stream {i}.")
            client.enable_stream(f"1hz_sine[{i}]")
    finally:
        client.close()
