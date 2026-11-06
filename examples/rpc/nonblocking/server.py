from one_liner.server import RouterServer
from time import sleep


class Sheep:
    def __init__(self, name: str):
        self.name = name

    def bleat(self):
        print(f"{self.name} goes: \"baaah!\"")


if __name__ == "__main__":
    flock = \
    {
        "Charlie": Sheep("Charlie"),
        "Brandi": Sheep("Brandi"),
        "Gertrude": Sheep("Gertrude"),
    }
    server = RouterServer(instances=flock)
    server.run()
    try:
        while True:
            sleep(0.1)
    finally:
        server.close()
