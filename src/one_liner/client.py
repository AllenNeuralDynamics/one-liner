"""client for controlling/monitoring one or more remote python objects."""

import logging
import pickle
import zmq
from contextlib import contextmanager
from one_liner.errors import RouterServerDisconnectedError
from one_liner.socket_metadata_schema import Streams
from one_liner import __version__ as local_version
from one_liner.utils import Protocol, Encoding, RPCException, StreamException, DESERIALIZERS, _recv
from threading import Thread, Event
from time import perf_counter
from typing import Any, Callable, Literal, Tuple
from zmq.utils.monitor import recv_monitor_message


class RouterClient:

    __slots__ = ("_log", "_context", "rpc_client", "stream_client")


    def __init__(self, protocol: Protocol = "tcp", interface: str = "localhost",
                 rpc_port: str = "5555", broadcast_port: str = "5556",
                 context: zmq.Context | None = None):
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
        # Share context between rpc and stream client
        self.rpc_client = ZMQRPCClient(protocol=protocol, interface=interface,
                                       port=rpc_port, context=self._context)
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


class ZMQRPCClient:


    __slots__ = ("context", "socket", "monitor_socket", "monitor_thread",
                 "connected", "_write_token", "log", "_default_rcv_timeout_ms")

    INNER_RECV_POLL_INTERVAL_MS = 500

    def __init__(self, protocol: Protocol = "tcp", interface: str = "localhost",
                 port: str = "5555", context: zmq.Context = None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.context = context or zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        ## Setup heartbeat settings
        self.socket.setsockopt(zmq.HEARTBEAT_IVL, 1000)
        self.socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, 2000)
        self.socket.setsockopt(zmq.HEARTBEAT_TTL, 2000)
        self.socket.setsockopt(zmq.LINGER, 0)
        # Setup a short inner-loop poll interval.
        self.socket.setsockopt(zmq.RCVTIMEO, self.__class__.INNER_RECV_POLL_INTERVAL_MS)
        address = f"{protocol}://{interface}:{port}"
        # Get monitor socket before connecting main socket.
        self.monitor_socket = self.socket.get_monitor_socket(zmq.EVENT_ALL)
        self.monitor_socket.setsockopt(zmq.LINGER, 0)
        self.monitor_thread = Thread(target=self._monitor_request_socket, daemon=True)
        self.monitor_thread.start()
        self.connected = Event()
        # Inproc protocol does not see "connected/disconnected" events.
        if protocol == "inproc":
            self.connected.set()
        self.socket.connect(address)
        self._write_token = ""

    def _monitor_request_socket(self):
        """Monitor thread to update the connection state."""
        while self.monitor_socket.poll(timeout=None, flags=zmq.POLLIN): # wait indefinitely
            event = recv_monitor_message(self.monitor_socket)
            event_id = event['event']
            endpoint = event['endpoint']
            if event_id == zmq.EVENT_CONNECTED:
                self.connected.set()
                self.log.debug(f"Connected to: {endpoint}")
            elif event_id == zmq.EVENT_DISCONNECTED:
                self.connected.clear()
                self.log.warning(f"Disconnected from: {endpoint}")
            elif event_id == zmq.EVENT_MONITOR_STOPPED:
                break

    def is_connected(self):
        """True if the client is connected to the server."""
        return self.connected.is_set()

    def call_by_name(self, call_name: str, args: list = None, kwargs: dict = None,
                     deserializer: Encoding | Callable = "pickle",
                     timeout_s: float | None = None) \
            -> Any | Tuple[float, Any]:
        return self.call("__rpc_server", "_call_by_name",
                         args=[call_name], kwargs={"args": args, "kwargs": kwargs},
                         deserializer=deserializer, timeout_s=timeout_s)

    def call(self, obj_name: str, attr_name: str, args: list | None = None,
             kwargs: dict | None = None,
             deserializer: Encoding | Callable = "pickle",
             timeout_s: float | None = None) -> Tuple[float, Any]:
        """Call a remote function available to the connected
        [`RouterServer`][one_liner.server.RouterServer] and return the result.

        Raises
        ------
        RPCException
            if the remotely-executed function raised an error.
        TimeoutError
            if the connected server does not issue a response within the
            specified timeout.
        RouterServerDisconnectedError
            if the associated RouterServer is disconnected

        """
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        pickled_req = pickle.dumps((self._write_token, obj_name, attr_name, args, kwargs))
        self.socket.send(pickled_req, copy=False)
        # inner poll loop. Update internal state (i.e: check connection)
        # outer poll loop. respect the user specified timeout.
        start_time_s = perf_counter()
        timeout_elapsed = False
        while not timeout_elapsed:
            timeout_elapsed = (False if timeout_s is None else
                (perf_counter() - start_time_s) > timeout_s)
            try:
                success, timestamp, data = _recv(self.socket, deserializer=deserializer)
                if not success:
                    raise RPCException(data) # data contains exception string.
                return timestamp, data
            except zmq.Again:
                if not self.is_connected():
                    error_msg = ("Did not get a reply from called function! "
                        "Server is disconnected!")
                    self.log.error(error_msg)
                    raise RouterServerDisconnectedError(error_msg)
        error_msg = (f"Server did not issue a respose within {timeout_s} seconds.")
        self.log.error(error_msg)
        raise TimeoutError(error_msg)

    def get_write_token(self, force: bool = False,
                        timeout_s: float | None = None):
        """Request a write token or acquire one by force.
        Idempotent if you already have the valid write token.

        Parameters
        ----------
        force:
            whether to force take the write token
        timeout_s:
            if not forcing, how long to wait for the write token before giving up
            (default is forever). A timeout of 0 will try exactly once.

        Raises
        ------
        PermissionError
            if the write token was not acquired.
        RouterServerDisconnectedError
            if the associated RouterServer is disconnected
        """
        timeout_elapsed = False
        start_time_s = perf_counter()
        while not timeout_elapsed:
            timeout_elapsed = (False if timeout_s is None else
                (perf_counter() - start_time_s) > timeout_s)
            try:
                self._write_token = self.call("__rpc_server", "_check_or_get_write_token",
                    kwargs={"force": force,
                        "curr_write_token": self._write_token})[-1]
                break
            except RPCException:
                if not timeout_elapsed:
                    continue
                error_msg = "Failed to acquire write token."
                self.log.error(error_msg)
                raise PermissionError(error_msg)
            except RouterServerDisconnectedError:
                self.log.error("Server disconnected while waiting to acquire write token!")
                raise

    def release_write_token(self):
        try:
            self.call("__rpc_server", "_release_write_token")
        except RouterServerDisconnectedError:
            self.log.error("Failed to return write token! Server is unresponsive.")
            return
        self._write_token = None

    def has_write_token(self):
        return True if self._write_token else False

    def has_write_access(self):
        return self.has_write_token()

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
        try:
            self.get_write_token(timeout_s=timeout_s)
            yield
        finally:
            self.release_write_token()

    def close(self):
        if self.has_write_token():
            self.release_write_token()
        self.socket.disable_monitor()
        self.monitor_thread.join()
        self.monitor_socket.close()
        self.socket.close()


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
        self.sub_sockets: dict[str, zmq.Context.socket] = {}
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
