import logging
import zmq
from one_liner.utils import Protocol, Encoding, StreamException, _recv
from typing import Callable, Literal, Tuple



class ZMQStreamClient:
    """Connect to an instrument server (likely running on an actual instrument)
    and receive periodically broadcasted function call results."""
    __slots__ = ("log", "context", "address", "sub_sockets", "deserializers")

    def __init__(self, protocol: Protocol = "tcp", interface: str = "localhost",
                 port: str = "5556", context: zmq.Context | None = None):
        """
        """
        # Receive periodic broadcasted messages setup.
        self.log = logging.getLogger(self.__class__.__name__)
        self.context = context or zmq.Context()
        self.address = f"{protocol}://{interface}:{port}"
        self.sub_sockets: dict[str, zmq.Socket] = {}
        self.deserializers: dict[str, Encoding | Callable] = {}

    def configure_stream(self, name: str,
                         storage_type: Literal["queue", "cache"] = "queue",
                         deserializer: Encoding | Callable = "pickle"):
        """Create a subscriber socket to receive a specific topic and setup
        how to buffer data.

        Parameters
        ---------
        name:
            stream name.
        storage_type:
            * `"queue"` -> FIFO.
            * `"cache"` -> only the latest data is received.
        """
        self.deserializers[name] = deserializer
        # Create zmq socket and configure to either queue or get-the-latest data.
        socket = self.context.socket(zmq.SUB)
        socket.setsockopt(zmq.LINGER, 0)
        self.log.debug(f"Creating socket for {name} stream and subscribing to topic: {name}.")
        socket.subscribe(name)
        if storage_type == "cache":
            socket.setsockopt(zmq.CONFLATE, 1)  # last msg only
        else:
            socket.setsockopt(zmq.RCVHWM, 1000) # Buffer up to 1000 msgs.
        self.log.debug(f"Connecting socket for {name} to receive stream from: {self.address}.")
        socket.connect(self.address)
        self.sub_sockets[name] = socket

    def get(self, stream_name: str, block: bool = False) -> Tuple[float, any]:
        """Return the timestamped data.

        Raises
        ------
        `zmq.Again`
            if block is `False` (default) and no data is present.
        StreamException
            if the underlying function raised an exception while being executed.
        """
        flag = 0 if block else zmq.NOBLOCK
        success, timestamp, data = _recv(self.sub_sockets[stream_name], flag=flag,
                                         prefix=stream_name,
                                         deserializer=self.deserializers[stream_name])
        if not success:
            raise StreamException(str(data))
        return timestamp, data

    def close(self):
        for name, socket in self.sub_sockets.items():
            socket.close()
