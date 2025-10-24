"""client for controlling/monitoring one or more remote python objects."""

import logging
import pickle
import zmq
from typing import Any, Literal, Tuple
from one_liner import Protocol
from one_liner.stream_schema import Streams


class RouterClient:

    def __init__(self, protocol: Protocol = "tcp", interface: str = "localhost",
                 rpc_port: str = "5555", broadcast_port: str = "5556",
                 context: zmq.Context = None):
        """
        Create and return a `RouterClient` instance and connect it to an
        existing `RouterServer`.

        :param protocol: a valid zmq protocol (``"tcp"``, ``"inproc"``, etc.)
        :param interface: protocol interface. For `tcp`, this is the ip address
           of the PC running the :py:class:`~one_liner.server.RouterServer`.
           Default is ``"localhost"``.
        :param rpc_port: port to issue remote function calls to python object
           instances on the corresponding :py:class`~one_liner.server.RouterServer`.
        :param broadcast_port: port to receive streamed messages from the
           corresponding :py:class:`~one_liner.server.RouterServer`'s stream
           client.
        :param context: The zmq Context. If unspecified, this class instance
           will take the default global zmq context for this process.

        .. warning::
           ``rpc_port`` and ``broadcast_port`` values must match those of the
           :py:class:`~one_liner.server.RouterServer` that this ``RouterClient``
           is connecting to.

        .. note::
           For the `protocol` setting, some options are system-dependent (i.e:
           `ipc` is for unix-like OSes only).

        """
        self._context = context or zmq.Context.instance()
        # Share context between rpc and stream client
        self.rpc_client = ZMQRPCClient(protocol=protocol, interface=interface,
                                       port=rpc_port, context=self._context)
        self.stream_client = ZMQStreamClient(protocol=protocol,
                                             interface=interface,
                                             port=broadcast_port,
                                             context=self._context)

    def call(self, instance_name: str, callable_name: str, *args, **kwds) -> Any:
        """Call a function/method in the scope of the connected
        :py:class:`~one_liner.server.RouterServer` and return the response.

        .. note::
           This is a blocking call that returns after the response has been
           returned.
        """
        return self.rpc_client.call(instance_name, callable_name, *args, **kwds)

    def configure_stream(self, name: str,
                         storage_type: Literal["queue", "cache"] = "queue"):
        """Configure data received from a stream to either hold one the latest
        data (`cache`) or to hold onto all data in a buffer (`queue`) of
        size 1000.

        :param name: stream name
        :param storage_type: ``"cache"`` or ``"queue"``.
           If ``"cache"``, calling :func:`get_stream` will return the most
           recently received stream data and not buffer any incoming data.
           The ``"queue"`` option will buffer up to 1000 messages such that
           calling :func:`get_stream` will return data in a first-in-first-out
           (FIFO) manner.

        """
        self.stream_client.configure_stream(name, storage_type)

    def get_stream(self, name: str, block: bool = False) -> Tuple[float, Any]:
        """Receive the results of a configured stream as 2-tuple where the first
        value is a :py:class:`~one_liner.server.RouterServer`-specified
        timestamp and the second value is the stream data..

        .. note::
           This stream must first be configured with :func:`configure_stream`.
        """
        return self.stream_client.get(name, block=block)

    def enable_stream(self, name: str):
        """Enable broadcasting of a stream by name."""
        # Use rpc_client to enable/disable streams.
        return self.rpc_client.call("__streamer", "enable", name)

    def disable_stream(self, name: str):
        """Disable broadcasting of a stream by name."""
        # Use rpc_client to enable/disable streams.
        return self.rpc_client.call("__streamer", "disable", name)

    def get_stream_configurations(self) -> Streams | dict:
        return self.rpc_client.call("__streamer", "get_configuration")

    def close(self):
        """Close the connection to the :py:class:`~one_liner.server.RouterServer`."""
        self.stream_client.close()
        self.rpc_client.close()


class ZMQRPCClient:

    def __init__(self, protocol: Protocol = "tcp", interface: str = "localhost",
                 port: str = "5555", context: zmq.Context = None):
        self.context = context or zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        address = f"{protocol}://{interface}:{port}"
        self.socket.connect(address)

    def call(self, obj_name: str, method_name: str, *args, **kwargs):
        """Call a function and return the result."""
        self.socket.send(pickle.dumps((obj_name, method_name, args, kwargs)))
        # TODO: no real exception-handling here yet.
        return pickle.loads(self.socket.recv())

    def close(self):
        self.socket.close()


class ZMQStreamClient:
    """Connect to an instrument server (likely running on an actual instrument)
    and receive periodically broadcasted function call results."""

    def __init__(self, protocol: Protocol = "tcp", interface: str = "localhost",
                 port: str = "5556", stream_cfg: dict = None,
                 context: zmq.Context = None):
        """
        :param stream_cfg: if specified, configure streams specified in the
            provided config.
        """
        # Receive periodic broadcasted messages setup.
        self.log = logging.getLogger(self.__class__.__name__)
        self.context = context or zmq.Context()
        self.address = f"{protocol}://{interface}:{port}"
        self.sub_sockets = {}
        if stream_cfg:
            for stream_name, storage_type in stream_cfg.items():
                self.configure_stream(stream_name, storage_type)

    def configure_stream(self, name: str,
                         storage_type: Literal["queue", "cache"] = "queue"):
        """Create a subscriber socket to receive a specific topic and setup
        how to buffer data.
        * queue -> FIFO.
        * cache -> only the latest data is received.
        """
        # Create zmq socket and configure to either queue or get-the-latest data.
        socket = self.context.socket(zmq.SUB)
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

        :raises zmq.Again: if block is False (default) and no data is present.
        """
        flags = 0 if block else zmq.NOBLOCK
        pickled_data = self.sub_sockets[stream_name].recv(flags=flags)
        offset = len(stream_name)
        return pickle.loads(pickled_data[offset:])  # Strip off topic prefix.

    def close(self):
        for name, socket in self.sub_sockets.items():
            socket.close()

