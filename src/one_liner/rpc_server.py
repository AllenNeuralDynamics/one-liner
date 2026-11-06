import inspect
import logging
import pickle

import secrets
import zmq
from concurrent.futures import ThreadPoolExecutor
from one_liner.utils import _send, Protocol, get_func_sig_json_schema
from one_liner.models import AccessType
from one_liner.socket_metadata_schema import RPC
from threading import Thread, Event, get_ident
from typing import Any


class ZMQRPCServer:
    """Remote Procedure Caller (RPC) Server. Call any method from a dict of
    object instances, and dispatch the serialized result to the connected
    RPC Client."""

    NUM_RECEIVE_THREADS = 8

    def __init__(self, protocol: Protocol = "tcp", interface: str = "*",
                 port: str = "5555", context: zmq.Context | None = None,
                 instances: dict[str, Any] | None = None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.context = context or zmq.Context.instance()
        self._context_managed_externally = ((context is not None) or
                                            (self.context is zmq.Context.instance()))
        # Create a pool of threads and sockets available to receive requests.
        self._threadpool: ThreadPoolExecutor | None = None
        self._recv_sockets = []
        for i in range(self.__class__.NUM_RECEIVE_THREADS):
            socket = self.context.socket(zmq.REP)
            socket.setsockopt(zmq.RCVTIMEO, 100)  # timeout for recv() in [ms].
                                              # >0 but value is kinda arbitrary.
            socket.setsockopt(zmq.LINGER, 0)  # close immediately when context terminates
            self._recv_sockets.append(socket)
        self._address = f"{protocol}://{interface}:{self.port}"
        self._worker_address = f"inproc://workers:{id(self)}"  # unique per instance
        self._keep_receiving = Event()
        self._keep_receiving.set()
        self._proxy_thread: Thread | None = None
        self._rpc_proxy_ctrl_socket = self.context.socket(zmq.REP)
        self._rpc_proxy_ctrl_socket.setsockopt(zmq.LINGER, 0)
        self._rpc_proxy_ctrl_address = f"inproc://rpc_proxy_control:{id(self)}"
        self._rpc_proxy_ctrl_socket.bind(self._rpc_proxy_ctrl_address)

        self.instances: dict[str, Any] = {} if instances is None else instances
        self.instances.update({"__rpc_server": self})
        self.named_call_signatures = {}
        self.named_call_access_types = {}
        self.obj_attr_access_types = {}  # Same as above except by obj/attr
        self._write_token: str | None = None
        self._write_token_owner: str | None = None
        self._recv_write_token: str | None = None  # most recent request's write token

        # Make releasing the write token a named call so it respects write access
        # i.e: non-token owners cannot release the write token.
        self.add_named_call(
            "__release_write_token",
            "__rpc_server",
            "_release_write_token",
            access_type="set")

    @staticmethod
    def _refresh_token() -> str:
        """regenerate a token"""
        return secrets.token_urlsafe()

    def run(self):
        """Launch thread to execute RPCs."""
        self._keep_receiving.set()
        # Launch ROUTER-DEALER ZMQ Proxy
        self._proxy_thread = Thread(target=self._proxy_worker,
            name=f"ROUTER_DEALER_proxy_worker",
            daemon=True)
        self._proxy_thread.start()
        # Launch receive worker threads.
        self._threadpool = ThreadPoolExecutor(
            max_workers=self.__class__.NUM_RECEIVE_THREADS)
        for i in range(self.__class__.NUM_RECEIVE_THREADS):
            # Will block until the proxy is up.
            self._recv_sockets[i].connect(self._worker_address)
            self._threadpool.submit(self._receive_worker, self._recv_sockets[i])


    def _proxy_worker(self):
        """Create the steerable zmq proxy."""
        frontend = self.context.socket(zmq.ROUTER)
        frontend.setsockopt(zmq.LINGER, 0) # Close as soon as the context terminates.
        frontend.bind(self._address)
        backend = self.context.socket(zmq.DEALER)
        backend.setsockopt(zmq.LINGER, 0) # Close as soon as the context terminates.
        backend.bind(self._worker_address)
        # Does not return.
        zmq.proxy_steerable(frontend, backend, control=self._rpc_proxy_ctrl_socket)
        self.log.debug("RPC proxy exited.")

    def _receive_worker(self, socket):
        """Wait for requests, call requested function, and return the reply.
        Launched in a thread.
        """
        thread_id = get_ident()
        self.log.debug(f"Starting Worker thread {thread_id}")
        while self._keep_receiving.is_set():
            try:
                pickled_request = socket.recv()
            except zmq.Again:
                continue
            request = pickle.loads(pickled_request)
            # FYI: write_token can be empty string if requester doesn't have it.
            # _get_write_token does not have an access_type, so we
            #   can always call it.
            self._recv_write_token, obj_name, attr_name, args, kwargs = request
            try:
                reply = self._call(obj_name=obj_name, attr_name=attr_name,
                                   args=args, kwargs=kwargs)
                _send(socket, name="", data=reply, success=True)
            except Exception as e:
                _send(socket, name="", data=str(e), success=False)

    def _get_rpc_from_named_calls(self, named_call: str) -> RPC:
        """
        Extract RPC information from the named_call signature. Extracts parameters and return types
        from the underlying function
        """

        obj_name, attr_name, args, kwargs = self.named_call_signatures[named_call]
        func = getattr(self.instances[obj_name], attr_name)
        params_schema, return_schema, description = get_func_sig_json_schema(
            func, default_args=args, default_kwargs=kwargs).values()

        return RPC(
            instance=obj_name,
            access_type=self.named_call_access_types[named_call],
            params_schema=params_schema,
            return_schema=return_schema,
            description=description
        )

    def get_configuration(self, as_dict: bool) -> dict[str, RPC | dict]:
        """
        Get a breakdown of every RPC with its corresponding function signature.
        """
        configuration = {}
        for named_call in self.named_call_signatures.keys():
            rpc = self._get_rpc_from_named_calls(named_call)
            configuration[named_call] = rpc.model_dump() if as_dict else rpc

        return configuration

    def get_write_token_owner(self) -> str | None:
        return self._write_token_owner

    def check_write_token(self, input_write_token: str):
        """True if the input token is the current token"""
        return input_write_token == self._write_token

    def _get_write_token(self, curr_write_token: str | None = None,
        force: bool = False, identity: str | None = None):
        """
        Get the write token if not yet allocated and not forced or force
        creation of a new one.
        If allocated, return the write token if the input write token matches
        (makes this function idempotent).

        Raises
        ------
            PermissionError
                if the token has already been allocated and force=false
        """
        if force:
            self._write_token = self._refresh_token()
            self._write_token_owner = identity
            self.log.warning("Forcing creation of a new write token.")
            self.log.warning(f"FORCE allocating write token to {identity}.")
            return self._write_token
        if not self._write_token_allocated():
            self._write_token_owner = identity
            self._write_token = self._refresh_token()
            self.log.debug(f"Allocating write token to {identity}.")
            return self._write_token
        if curr_write_token == self._write_token:
            return self._write_token
        raise PermissionError(
            f"Write token already allocated to {self._write_token_owner}."
            "Cannot reallocate without forcing.")

    def _release_write_token(self):
        """Untrack the current write token owner. This function should only be
        called by clients who have the write token or by the server itself.
        """
        msg = "Unallocating write token" + (f" from {self._write_token_owner}."
            if self._write_token_owner is not None else ".")
        self.log.debug(msg)
        self._write_token_owner = None
        self._write_token = None # de-allocates write token.

    def _write_token_allocated(self):
        return self._write_token is not None

    def add_named_call(self, call_name: str,
                       obj_name: str, attr_name: str,
                       args: list | None = None, kwargs: dict | None = None,
                       access_type: AccessType | None = None):
        """Setup a call to be called with `call_by_name` on the
        :py:class:`~one_lner.client.ZMQRPCClient`

        :param call_name: string to save the function call signature under.
        :param obj_name: underlying object instance name. Must be present in
            the `objects` dict passed into the `__init__`.
        :param attr_name: name of the callable attribute (method).
        :param args: default args to save with the function call.
        :param kwargs: default kwargs to save with the function call.
        :param access_type: user annotation related to the effect that calling
            the function has on the remote object's state. This value is
            optional and appears as part of the metadata available from
            :py:meth:`~one_liner.server.rpc_server.ZMQRPCServer.get_configuration`

        .. note::
           `args` and `kwargs` can be overwritten by index or name respectively
           when actually calling the named function call with
           :py:meth:`~one_liner.server.rpc_server.ZMQRPCServer.call_by_name.`

        """
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        self.log.debug(f"Adding named call: {call_name} = "
                       f"{obj_name}.{attr_name}("
                       f"{', '.join([str(a) for a in args])}"
                       f"{', ' if (len(args) and len(kwargs)) else ''}"
                       f"{', '.join([str(k)+'='+str(v) for k,v in kwargs.items()])})")

        # Validate that the object and attribute exists
        if obj_name not in self.instances:
            raise KeyError(f"'{obj_name}' is not present in instances.")
        if not hasattr(self.instances[obj_name], attr_name):
            raise AttributeError(f"'{obj_name}' does not have attribute '{attr_name}'")

        # Validate name call is unique - warn user if not
        if call_name in self.named_call_signatures:
            self.log.warning(f"Overwriting existing named call: {call_name}")

        # Arity check: verify arg/kwargs fits function signature
        # (too many args, unknown kwargs, duplicate kwargs, etc)
        sig = inspect.signature(getattr(self.instances[obj_name], attr_name))
        sig.bind_partial(*args, **kwargs)

        self.named_call_signatures[call_name] = (obj_name, attr_name, args, kwargs)
        self.obj_attr_access_types[(obj_name, attr_name)] = access_type
        self.named_call_access_types[call_name] = access_type

    def _call_by_name(self, call_name: str, args: list | None = None,
                      kwargs: dict | None = None):
        """Lookup a configured call by its `'call_name'`, call it with the
        specified args/kwargs, and return the result.

        :pararm call_name: the name of the call specified in
            :py:meth:`~one_liner.server.ZMQRPCServer.add_named_call`.
        :param args: args to pass to the underlying function. Note that any arg
            passed in will overwrite any existing pre-configured arg setup in
            :py:meth:`~one_liner.server.ZMQRPCServer.add_named_call`.
        :param kwargs: kwargs to pass to the underlying function. Note that
            these kwargs will overwrite existing pre-configured args setup with
            :py:meth:`~one_liner.server.ZMQRPCServer.add_named_call` via
            a standard dict update.

        """
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        if call_name not in self.named_call_signatures:
            raise KeyError(f"'{call_name}' is not a named call.")
        obj_name, attr_name, default_args, default_kwargs = \
            self.named_call_signatures[call_name]

        args = args + default_args[len(args):]
        kwargs = default_kwargs | kwargs

        debug_msg = (f"Invoking named call: {obj_name}.{attr_name}("
                     f"{', '.join([str(a) for a in args])}"
                     f"{', ' if (len(args) and len(kwargs)) else ''}"
                     f"{', '.join([str(k) + '=' + str(v) for k, v in kwargs.items()])})")
        self.log.debug(debug_msg)
        return self._call(obj_name=obj_name, attr_name=attr_name, args=args,
                          kwargs=kwargs)

    def _call(self, obj_name: str, attr_name: str, args: list | None = None,
              kwargs: list | None = None):
        """Call the object attribute with the specified args/kwargs and return
        the result."""

        access_type = self.obj_attr_access_types.get((obj_name, attr_name), None)
        if access_type == "set" and self._recv_write_token != self._write_token:
            raise PermissionError("Cannot mutate local state with incorrect write token.")
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        if obj_name not in self.instances:
            error_msg = f"{obj_name} is not present in instances."
            self.log.error(error_msg)
            raise KeyError(error_msg)
        instance = self.instances[obj_name]
        try:
            func = getattr(instance, attr_name)  # Might raise AttributeError
        except AttributeError as e:
            error_msg = (f"Instance {instance} does not have attribute: "
                         f"{attr_name}. Error: {str(e)}")
            self.log.error(error_msg)
            raise AttributeError(error_msg) from e
        try:
            debug_msg = (f"Thread {get_ident()} Calling: {obj_name}.{attr_name}("
                         f"{', '.join([str(a) for a in args])}"
                         f"{', ' if (len(args) and len(kwargs)) else ''}"
                         f"{', '.join([str(k) + '=' + str(v) for k, v in kwargs.items()])})")
            self.log.debug(debug_msg)
            return func(*args, **kwargs)
        except Exception as e:  # catch-all error calling the function.
            error_msg = (f"func: {obj_name}.{attr_name}("
                         f"{', '.join([str(a) for a in args])}"
                         f"{', ' if (len(args) and len(kwargs)) else ''}"
                         f"{', '.join([str(k) + '=' + str(v) for k, v in kwargs.items()])}) "
                         f"raised an exception while executing: {str(e)}")
            self.log.error(error_msg)
            raise e

    def close(self):
        # _threadpool is only created by run(); may be None if never started.
        if self._threadpool:
            self._keep_receiving.clear()
            self._threadpool.shutdown(wait=True)
        if self._proxy_thread and self._proxy_thread.is_alive():
            # Create a one-off socket to shutdown the proxy.
            stream_proxy_ctrl_socket = self.context.socket(zmq.REQ)
            stream_proxy_ctrl_socket.setsockopt(zmq.LINGER, 0)
            stream_proxy_ctrl_socket.connect(self._rpc_proxy_ctrl_address)
            stream_proxy_ctrl_socket.send_string("TERMINATE")
            stream_proxy_ctrl_socket.recv() # Wait for empty reply to confirm.
            stream_proxy_ctrl_socket.close()
            self._proxy_thread.join()
        if not self._context_managed_externally:
            self.context.term()
