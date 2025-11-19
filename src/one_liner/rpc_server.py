import logging
import pickle
from ftplib import error_reply

import zmq
from one_liner.utils import _send, Protocol
from threading import Thread, Event
from typing import Any


class ZMQRPCServer:
    """Remote Procedure Caller (RPC) Server. Call any method from a dict of
    object instances, and dispatch the serialized result to the connected
    RPC Client."""

    def __init__(self, protocol: Protocol = "tcp", interface: str = "*",
                 port: str = "5555", context: zmq.Context = None,
                 instances: dict[str, Any] = None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.context = context or zmq.Context()
        self.context_managed_externally = context is not None
        self.socket = self.context.socket(zmq.REP)
        self.socket.setsockopt(zmq.RCVTIMEO, 100)  # timeout for recv() in [ms].
                                                   # >0 but value is kinda arbitrary.
        self.socket.setsockopt(zmq.LINGER, 0)  # close immediately when context terminates
        address = f"{protocol}://{interface}:{self.port}"
        self.socket.bind(address)
        self._keep_receiving = Event()
        self._keep_receiving.set()
        self._receive_thread: Thread = None
        self.instances: dict[str, Any] = {} if instances is None else instances
        self.instances.update({"__rpc_server": self})
        self.named_call_signatures = {}

    def run(self):
        """Launch thread to execute RPCs."""
        self._keep_receiving.set()
        self._receive_thread = Thread(target=self._receive_worker,
                                     name=f"REQ_receive_worker",
                                     daemon=True)
        self._receive_thread.start()

    def _receive_worker(self):
        """Wait for requests, call requested function, and return the reply.
        Launched in a thread.
        """
        while self._keep_receiving.is_set():
            try:
                pickled_request = self.socket.recv()
            except zmq.Again:
                continue
            request = pickle.loads(pickled_request)
            obj_name, attr_name, args, kwargs, get_timestamp = request
            print(f"unpacking: obj_name={obj_name}, attr_name={attr_name}, "
                  f"args={args}, kwargs={kwargs}, get_timestamp={get_timestamp}")
            try:
                reply = self._call(obj_name=obj_name, attr_name=attr_name,
                                   args=args, kwargs=kwargs)
                _send(self.socket, name="", data=reply, success=True,
                      send_timestamp=get_timestamp)
            except Exception as e:
                _send(self.socket, name="", data=str(e), success=False,
                      send_timestamp=get_timestamp)

    def add_named_call(self, call_name: str,
                             obj_name: str, attr_name: str,
                             args: list = None, kwargs: list = None):
        """Setup a call to be called with `call_by_name` on the
        :py:class:`~one_lner.client.ZMQRPCClient`

        :param call_name: string to save the function call signature under.
        :param obj_name: underlying object instance name. Must be present in
            the `objects` dict passed into the `__init__`.
        :param attr_name: name of the callable attribute (method).
        :param args: default args to save with the function call.
        :param kwargs: default kwargs to save with the function call.

        .. note::
           `args` and `kwargs` can be overwritten by index or name respectively
           when actually calling the named function call with
           :py:meth:`~one_liner.server.rpc_server.ZMQRPCServer.call_by_name.`

        """
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        self.named_call_signatures[call_name] = (obj_name, attr_name, args, kwargs)

    def _call_by_name(self, call_name: str, args: list = None,
                      kwargs: list = None):
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
        # Update args & kwargs from their defaults for this call if specified.
        args = args + default_args[len(args):]
        kwargs = default_kwargs | kwargs
        print(f"updated call: {obj_name}.{attr_name}, args={args}, kwargs={kwargs}")
        return self._call(obj_name=obj_name, attr_name=attr_name, args=args,
                          kwargs=kwargs)

    def _call(self, obj_name: str, attr_name: str, args: list = None,
              kwargs: list = None):
        """Call the object attribute with the specified args/kwargs and return
        the result."""
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
        self._keep_receiving.clear()
        self._receive_thread.join()
        self.socket.close()
        if not self.context_managed_externally:
            self.context.term()
