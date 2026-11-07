from one_liner.client import RouterClient
from concurrent.futures import ThreadPoolExecutor

if __name__ == "__main__":
    client = RouterClient()


    flock = {"Charlie", "Brandi", "Gertrude"}

    def make_sheep_sound(sheep_name: str):
        client.call(f"{sheep_name}", "bleat")

    with ThreadPoolExecutor(max_workers=len(flock)) as threadpool:
        threadpool.map(make_sheep_sound, flock)

    client.close()
