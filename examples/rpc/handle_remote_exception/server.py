from one_liner.server import RouterServer
from time import sleep


class Horn:
    def malfunction(self):
        raise RuntimeError("Crunch!")


if __name__ == "__main__":
    horn = Horn()
    server = RouterServer(my_horn=horn)
    server.run()
    try:
        while True:
            sleep(0.1)
    finally:
        server.close()
