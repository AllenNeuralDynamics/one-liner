import zmq
import pprint
from one_liner.client import RouterClient


if __name__ == "__main__":
    client = RouterClient()
    pprint.pprint(client.get_stream_configurations())
