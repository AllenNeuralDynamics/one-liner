"""client for controlling/monitoring one or more remote python objects."""

import logging
import zmq
from contextlib import contextmanager
from one_liner.rpc_client import ZMQRPCClient
from one_liner.stream_client import ZMQStreamClient
from one_liner.socket_metadata_schema import Streams
from one_liner import __version__ as local_version
from one_liner.utils import Protocol, Encoding
from typing import Any, Callable, Literal, Tuple


class RouterClient:

    __slots__ = ("_log", "_context", "rpc_client", "stream_client", "name")


    def __init__(self, protocol: Protocol = "tcp", interface: str = "localhost",
                 rpc_port: str = "5555", broadcast_port: str = "5556",
                 context: zmq.Context | None = None,
                 name: str | None = None):
        """ Create and return a `RouterClient` instance and connect it to an
        existing `RouterServer`.

        Parameters
        ----------
        protocol:
            a valid zmq protocol (`"tcp"`, `"inproc"`, etc.)
        interface:
            protocol interface. For `"tcp"`, this is the ip address
            of the PC running the
            [`RouterServer`][one_liner.server.RouterServer].
        rpc_port:
            port to issue remote function calls to python object
            instances on the corresponding
            [`RouterServer`][one_liner.server.RouterServer].
        broadcast_port:
            port to receive streamed messages from the
            corresponding [`RouterServer`][one_liner.server.RouterServer]'s
            stream client.
        context:
            The zmq Context. If unspecified, this class instance
            will take the default global zmq context for this process.

        Warnings
        --------
        `rpc_port` and `broadcast_port` values must match those of the
        [`RouterServer`][one_liner.server.RouterServer] that this `RouterClient`
        is connecting to.

        Notes
        -----
        For the `protocol` setting, some options are system-dependent (i.e:
        `"ipc"` is for unix-like OSes only).
        """
        self._log = logging.getLogger(self.__class__.__name__)
        self._context = context or zmq.Context.instance()
        self.name = name
        # Share context between rpc and stream client
        self.rpc_client = ZMQRPCClient(protocol=protocol, interface=interface,
                                       port=rpc_port, context=self._context,
                                       name=name)
        self.stream_client = ZMQStreamClient(protocol=protocol,
                                             interface=interface,
                                             port=broadcast_port,
                                             context=self._context)

    def is_connected(self):
        """True if the client is connected to the server."""
        return self.rpc_client.is_connected()

    def call_by_name(self, call_name: str, args: list | None = None,
                     kwargs: dict | None = None,
                     deserializer: Encoding | Callable = "pickle",
                     timeout_s: float | None = None) \
            -> Any | Tuple[float, Any]:

        """Lookup a configured call by its `call_name`, call it with the specified
        args/kwargs, and return the result.

        Parameters
        ----------
        call_name :
            The name of the call specified in
            [`add_named_call`][one_liner.server.ZMQRPCServer.add_named_call].
        args :
            Args to pass to the underlying function. Note that any arg passed in
            will overwrite any existing pre-configured arg setup in
            [`add_named_call`][one_liner.server.ZMQRPCServer.add_named_call].
        kwargs :
            Kwargs to pass to the underlying function. Note that these kwargs
            will overwrite existing pre-configured args setup with
            [`add_named_call`][one_liner.server.ZMQRPCServer.add_named_call]
            via a standard dict update.
        deserializer :
            Callable function to deserialize the data or string-representation
            of one of the built-in options.
        timeout_s:
            max time in seconds to wait for a reply before raising a TimeoutError.

        Raises
        ------
        RPCException
            if the remotely-executed function raised an error.
        TimeoutError
            if the connected server does not issue a response within the
            specified timeout.
        RouterServerDisconnectedError
            if the associated RouterServer is disconnected

        Returns
        -------
            The result of the call with a timestamp.
        """
        return self.rpc_client.call_by_name(call_name=call_name, args=args,
                                            kwargs=kwargs, deserializer=deserializer,
                                            timeout_s=timeout_s)

    def call(self, obj_name: str, attr_name: str, args: list | None = None,
             kwargs: dict | None = None,
             deserializer: Encoding | Callable = "pickle",
             timeout_s: float | None = None) -> Any | Tuple[float, Any]:
        """Call a function/method within the scope of the connected
        [`RouterServer`][one_liner.server.RouterServer] and return the result.

        Parameters
        ----------
        obj_name:
            object name. (Class instance or module)
        attr_name:
            a callable attribute
        args:
            list of positional arguments for function call
        kwargs:
            dict of keyword arguments for function call
        deserializer:
            callable function to deserialize the data or
            string-representation of one of the built-in options.
        timeout_s:
            max time in seconds to wait for a reply before raising a TimeoutError.

        Raises
        ------
        RPCException
            if the remotely-executed function raised an error.
        TimeoutError
            if the connected server does not issue a response within the
            specified timeout.
        RouterServerDisconnectedError
            if the associated RouterServer is disconnected

        Notes
        -----
        This is a blocking call that returns after the response has been
        returned.

        """
        return self.rpc_client.call(obj_name=obj_name, attr_name=attr_name,
                                    args=args, kwargs=kwargs,
                                    deserializer=deserializer,
                                    timeout_s=timeout_s)

    def has_write_token(self):
        return self.rpc_client.has_write_token()

    def has_write_access(self):
        return self.rpc_client.has_write_access()

    def get_write_token_owner(self) -> str | None:
        return self.rpc_client.get_write_token_owner()

    def get_write_token(self, force: bool = False, timeout_s: float | None = None):
        return self.rpc_client.get_write_token(force=force, timeout_s=timeout_s)

    def release_write_token(self):
        return self.rpc_client.release_write_token()

    @contextmanager
    def lock_write_token(self, timeout_s: float | None = None):
        """ Context Manager to get access to the write token.

        Parameters
        ----------
        timeout_s:
            the timeout (in seconds) to wait to acquire the the token before
            raising an error.

        Raises
        ------
        PermissionError:
            if the write token never becomes available before the specified
            timeout.
        RouterServerDisconnectedError
            if the associated RouterServer is disconnected

        """
        with self.rpc_client.lock_write_token(timeout_s=timeout_s):
            yield

    def configure_stream(self, name: str,
                         storage_type: Literal["queue", "cache"] = "queue",
                         deserializer: Encoding | Callable = "pickle"):
        """Configure data received from a stream to either hold one the latest
        data (`"cache"`) or to hold onto all data in a buffer (`"queue"`) of
        size 1000.

        Parameters
        ----------
        name:
            stream name
        storage_type:
            ``"cache"`` or ``"queue"``. If `"cache"`, calling
           [`get_stream`][one_liner.client.RouterClient.get_stream]
           will return the most recently received stream data and not buffer any
           incoming data.
           The `"queue"` option will buffer up to 1000 messages such that
           calling [`get_stream`][one_liner.client.RouterClient.get_stream] will
           return data in a first-in-first-out (FIFO) manner.
        :param deserializer:
            callable function to deserialize the data or
            string-representation of one of the built-in options.

        """
        self.stream_client.configure_stream(name, storage_type, deserializer)

    def get_stream(self, name: str, block: bool = False) -> Tuple[float, Any]:
        """Receive the results of a configured stream as 2-tuple where the first
        value is a [`RouterServer`][one_liner.server.RouterServer]-specified
        timestamp and the second value is the stream data..

        Parameters
        ----------
        name:
            stream name
        block:
            if true, block until new data arrives.

        Raises
        ------
        zmq.Again
            if block is False (default) and no data is present.
        StreamException
            if the connected [`RouterServer`][one_liner.server.RouterServer]'s
            underlying function call raised an exception.

        Warnings
        --------
        This stream must first be configured on the `RouterClient`-side with
        [`configure_stream`][one_liner.client.RouterClient.configure_stream].
        """
        return self.stream_client.get(name, block=block)

    def enable_stream(self, name: str):
        """Enable broadcasting of a stream by name. The connected
        [`RouterServer`][one_liner.server.RouterServer] will start periodically
        calling the underlying stream function, and calls to `get_stream(name)`
        will return new data.

        Parameters
        ----------
        name:
            stream name

        Notes
        -----
        Enabling streams only works for periodically-added streams
        added with [`add_stream`][one_liner.server.RouterServer.add_stream]
        and [`add_zmq_stream`][one_liner.server.RouterServer.add_zmq_stream]
        but *not* [`get_stream_fn`][one_liner.server.RouterServer.get_stream_fn].
        """
        # Use rpc_client to enable/disable streams.
        return self.rpc_client.call("__streamer", "enable", args=[name])

    def disable_stream(self, name: str):
        """Disable broadcasting of a stream by name. The connected
        [`RouterServer`][one_liner.server.RouterServer] will stop periodically
        calling the underlying stream function, and calls to
        [`get_stream(name)`][one_liner.client.RouterClient.get_stream] will
        return no new data.

        Parameters
        ----------
        name:
            stream name.

        Notes
        -----
           Disabling streams only works for periodically-added streams added
           with [`add_stream`][one_liner.server.RouterServer.add_stream] and
           [`add_zmq_stream`][one_liner.server.RouterServer.add_zmq_stream] but
           *not* [`get_stream_fn`][one_liner.server.RouterServer.get_stream_fn].

        """
        # Use rpc_client to enable/disable streams.
        return self.rpc_client.call("__streamer", "disable", args=[name])

    def get_stream_configurations(self, as_dict: bool = False) -> Streams | dict:
        """Get the configuration for all streams.

        Parameters
        ----------
        as_dict:
            if `True`, get the schema representation as a dict. Otherwise,
            return a [`Streams`][one_liner.stream_schema.Streams] model.
        """
        return self.rpc_client.call("__streamer", "get_configuration",
                                    kwargs={"as_dict": as_dict})

    def get_rpc_configurations(self, as_dict: bool = False) -> dict:
        """Get the configuration for all RPCs."""
        return self.rpc_client.call("__router_server", "get_rpc", args=[as_dict])

    @property
    def version(self):
        """Return client version."""
        return local_version

    @property
    def server_version(self):
        """Return the server version."""
        return self.rpc_client.call("__router_server", "get_version")[-1]

    def close(self):
        """Close the connection to the
        [`RouterServer`][one_liner.server.RouterServer]"""
        self.stream_client.close()
        self.rpc_client.close()
