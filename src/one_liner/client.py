"""client for controlling/monitoring one or more remote python objects."""

import logging
import pickle
import zmq
from one_liner.stream_schema import Streams
from one_liner.utils import Protocol, Encoding, RPCException, StreamException, DESERIALIZERS, _recv
from typing import Any, Callable, Literal, Tuple


class RouterClient:

    __slots__ = ("_context", "rpc_client", "stream_client")

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

    def call(self, obj_name: str, attr_name: str, args: list = None,
             kwargs: dict = None,
             deserializer: Encoding | Callable = "pickle") -> Tuple[float, Any]:
        """Call a function/method within the scope of the connected
        :py:class:`~one_liner.server.RouterServer` and return the result.

        :param obj_name: object name. (Class instance or module)
        :param attr_name: a callable attribute
        :param deserializer: callable function to deserialize the data or
            string-representation of one of the built-in options.
        :param args: list of positional arguments for function call
        :param kwargs: dict of keyword arguments for function call
        :raises RPCException: if the underlying function call raises an exception

        .. note::
           This is a blocking call that returns after the response has been
           returned.

        """
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        return self.rpc_client.call(obj_name, attr_name, args, kwargs,
                                    deserializer=deserializer)

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

    def get_stream(self, name: str, block: bool = False,
                   deserializer: Encoding | Callable = "pickle") -> Tuple[float, Any]:
        """Receive the results of a configured stream as 2-tuple where the first
        value is a :py:class:`~one_liner.server.RouterServer`-specified
        timestamp and the second value is the stream data..

        :param name: stream name
        :param block: if true, block until new data arrives.
        :param deserializer: callable function to deserialize the data or
            string-representation of one of the built-in options.
        :raises zmq.Again: if block is False (default) and no data is present.
        :raises StreamException: if the connected
           :py:class:`~one_liner.serverRouterServer`'s underlying function call
           raised an exception.

        .. warning::
           This stream must first be configured with :func:`configure_stream`.
        """
        return self.stream_client.get(name, block=block, deserializer=deserializer)

    def enable_stream(self, name: str):
        """Enable broadcasting of a stream by name. The connected
        :py:class:`~one_liner.server.RouterServer` will start periodically calling
        the underlying stream function, and calls to `get_stream(name)`
        will return new data.

        :param name: stream name

        .. note::
           Enabling streams only works for periodically-added streams
           added with :py:meth:`~one_liner.server.RouterServer.add_stream` and
           :py:meth:`~one_liner.server.RouterServer.add_zmq_stream` but
           *not* :py:meth:`~one_liner.server.RouterServer.get_stream_fn`.

        """
        # Use rpc_client to enable/disable streams.
        return self.rpc_client.call("__streamer", "enable", args=[name])

    def disable_stream(self, name: str):
        """Disable broadcasting of a stream by name. The connected
        :py:class:`~one_liner.server.RouterServer` will stop periodically calling
        the underlying stream function, and calls to `get_stream(name)`
        will return no new data.

        :param name: stream name.

        .. note::
           Disabling streams only works for periodically-added streams
           added with :py:meth:`~one_liner.server.RouterServer.add_stream` and
           :py:meth:`~one_liner.server.RouterServer.add_zmq_stream` but
           *not* :py:meth:`~one_liner.server.RouterServer.get_stream_fn`.

        """
        # Use rpc_client to enable/disable streams.
        return self.rpc_client.call("__streamer", "disable", args=[name])

    def get_stream_configurations(self, as_dict: bool = False) -> Streams | dict:
        """Get the configuration for all streams.

        :param as_dict: if `True`, get the schema representation as a dict.
           Otherwise, return a :py:class:`~one_liner.stream_schema.Streams`.

        """
        return self.rpc_client.call("__streamer", "get_configuration",
                                    kwargs={"as_dict": as_dict})[1]

    def close(self):
        """Close the connection to the :py:class:`~one_liner.server.RouterServer`."""
        self.stream_client.close()
        self.rpc_client.close()


class ZMQRPCClient:

    __slots__ = ("context", "socket")

    def __init__(self, protocol: Protocol = "tcp", interface: str = "localhost",
                 port: str = "5555", context: zmq.Context = None):
        self.context = context or zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        address = f"{protocol}://{interface}:{port}"
        self.socket.connect(address)

    def call(self, obj_name: str, attr_name: str, args: list = None, kwargs: dict = None,
             deserializer: Encoding | Callable = "pickle") -> Tuple[float, Any]:
        """Call a remote function available to the connected
        :py:class:`~one_liner.server.RouterServer` and return the result.

        """
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        self.socket.send(pickle.dumps((obj_name, attr_name, args, kwargs)), copy=False)
        success, timestamp, data = _recv(self.socket, deserializer=deserializer)
        if not success:
            raise RPCException(data)
        return timestamp, data

    def close(self):
        self.socket.close()


class ZMQStreamClient:
    """Connect to an instrument server (likely running on an actual instrument)
    and receive periodically broadcasted function call results."""
    __slots__ = ("log", "context", "address", "sub_sockets")

    def __init__(self, protocol: Protocol = "tcp", interface: str = "localhost",
                 port: str = "5556", context: zmq.Context = None):
        """
        """
        # Receive periodic broadcasted messages setup.
        self.log = logging.getLogger(self.__class__.__name__)
        self.context = context or zmq.Context()
        self.address = f"{protocol}://{interface}:{port}"
        self.sub_sockets = {}

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

    def get(self, stream_name: str, block: bool = False,
            deserializer: Encoding | Callable = None) -> Tuple[float, any]:
        """Return the timestamped data.


        :raises zmq.Again: if block is `False` (default) and no data is present.
        :raises StreamException: if the underlying function raised an exception
           while being executed.
        """
        flag = 0 if block else zmq.NOBLOCK
        success, timestamp, data = _recv(self.sub_sockets[stream_name], flag=flag,
                                         prefix=stream_name, deserializer=deserializer)
        if not success:
            raise StreamException(str(data))
        return timestamp, data

    def close(self):
        for name, socket in self.sub_sockets.items():
            socket.close()

