"""ZMQ RPC Client handling request/reply calls"""

import logging
import pickle
import zmq

from contextlib import contextmanager
from one_liner.errors import RouterServerDisconnectedError
from one_liner.utils import Protocol, Encoding, RPCException, _recv
from threading import Thread, Event
from time import perf_counter, sleep
from typing import Any, Callable, Tuple
from zmq.utils.monitor import recv_monitor_message


class ZMQRPCClient:

    __slots__ = ("context", "socket", "monitor_socket", "monitor_thread", "address",
                 "name", "_connected", "_write_token", "log", "_default_rcv_timeout_ms")

    INNER_RECV_POLL_INTERVAL_MS = 500
    WRITE_TOKEN_RETRY_INTERVAL_S = 0.1

    def __init__(self, protocol: Protocol = "tcp", interface: str = "localhost",
                 port: str = "5555", context: zmq.Context | None = None,
                 name: str | None = None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.context = context or zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.name = name if name is not None else self.socket.getsockopt(zmq.IDENTITY)
        ## Setup heartbeat settings
        self.socket.setsockopt(zmq.HEARTBEAT_IVL, 1000)
        self.socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, 2000)
        self.socket.setsockopt(zmq.HEARTBEAT_TTL, 2000)
        self.socket.setsockopt(zmq.LINGER, 0)
        # Setup a short inner-loop poll interval.
        self.socket.setsockopt(zmq.RCVTIMEO, self.__class__.INNER_RECV_POLL_INTERVAL_MS)
        self.address = f"{protocol}://{interface}:{port}"
        # Get monitor socket before connecting main socket.
        self.monitor_socket = self.socket.get_monitor_socket(zmq.EVENT_ALL)
        self.monitor_socket.setsockopt(zmq.LINGER, 0)
        self.monitor_thread = Thread(target=self._monitor_request_socket, daemon=True)
        self.monitor_thread.start()
        self._connected = Event()
        self.socket.connect(self.address)
        self._write_token = None

    def _monitor_request_socket(self):
        """Monitor thread to update the connection state."""
        while self.monitor_socket.poll(timeout=None, flags=zmq.POLLIN): # wait indefinitely
            event = recv_monitor_message(self.monitor_socket)
            event_id = event['event']
            endpoint = event['endpoint']
            if event_id == zmq.EVENT_CONNECTED:
                self._connected.set()
                self.log.debug(f"Connected to: {endpoint}")
            elif event_id == zmq.EVENT_DISCONNECTED:
                self._connected.clear()
                self.log.warning(f"Disconnected from: {endpoint}")
            elif event_id == zmq.EVENT_MONITOR_STOPPED:
                break

    def is_connected(self):
        """True if the client is connected to the server."""
        # Inproc protocol does not see "connected/disconnected" events.
        if self.address.startswith("inproc"):
            raise NotImplementedError(
                "inproc protocol does not see ""connection/disconnection events.")
        return self._connected.is_set()

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

    def get_write_token_owner(self) -> str | None:
        return self.call("__rpc_server", "get_write_token_owner")[-1]

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
                self._write_token = self.call("__rpc_server", "_get_write_token",
                    kwargs={"force": force,
                        "curr_write_token": self._write_token,
                        "identity": self.name})[-1]
                break
            except RPCException:
                if not timeout_elapsed:
                    sleep(self.__class__.WRITE_TOKEN_RETRY_INTERVAL_S)
                    continue
                error_msg = "Failed to acquire write token."
                self.log.error(error_msg)
                raise PermissionError(error_msg)
            except RouterServerDisconnectedError:
                self.log.error("Server disconnected while waiting to acquire write token!")
                raise

    def release_write_token(self):
        # Idempotent Case:
        if self._write_token is None:
            self.log.error("No write token to release.")
            return
        try:
            # This named call will raise an exception if the write token is
            # invalid because it has "set" access_type.
            self.call_by_name("__release_write_token")
        except RPCException:
            msg = "Cannot release the write token! Token owner has changed!"
            self.log.error(msg)
            raise PermissionError(msg)
        except RouterServerDisconnectedError:
            self.log.error("Failed to return write token! Server is unresponsive.")
            # Consider whether we should still be releasing the write token
            # in this case.... Holding onto it for an intermittent connection
            # still seems like the best strategy here.
            return
        self._write_token = None

    def has_write_token(self):
        if self._write_token is None:
            return False
        # Ask the server if the current write token is still valid.
        return self.call("__rpc_server", "check_write_token",
            kwargs={"input_write_token": self._write_token})[-1]

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
        try:
            self.release_write_token()  # Idempotent
            # Don't raise these errors when trying to close:
            # * if another client force-claimed write token
            # * if the server is down
        except (PermissionError, RouterServerDisconnectedError):
            pass
        self.socket.disable_monitor()
        self.monitor_thread.join()
        self.monitor_socket.close()
        self.socket.close()
