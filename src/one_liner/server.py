"""Server for enabling remote control and broadcasting results of periodic function calls."""

import zmq
from one_liner import __version__ as local_version
from one_liner.stream_server import ZMQStreamServer
from one_liner.rpc_server import ZMQRPCServer
from one_liner.utils import Protocol, Encoding
from typing import Any, Callable


class RouterServer:
    __slots__ = ("instances", "context", "streamer", "rpc",
                 "_context_managed_externally")
    """Interface for enabling remote control/monitoring of one or more object
       instances. Heavy lifting is delegated to two subordinate objects."""

    def __init__(self, protocol: Protocol = "tcp", interface: str = "*",
                 rpc_port: str = "5555", broadcast_port: str = "5556",
                 context: zmq.Context | None = None,
                 instances: dict[str, Any] | None = None,
                 config: dict[str, dict] | None = None):
        """ Constructor.

        Parameters
        ----------
        protocol : str
            A zmq supported protocol (tcp, inproc, etc).
        interface : str, optional
            The interface to bind or connect to.
        rpc_port : int
            Port to issue remote procedure calls to python objects.
        broadcast_port : int
            Port from which to stream periodic data.
        context : zmq.Context
            The zmq context. Will be created automatically if unspecified.
        instances : dict
            Dict of object instances, keyed by name.

        Warnings
        --------
        For sharing data within the same process using the `inproc` protocol,
        the zmq Context must be shared between
        [`RouterServer`][one_liner.server.RouterServer]
        and [`RouterClient`][one_liner.client.RouterClient]
        (or `RouterServer` and `RouterServer` if forwarding).

        Notes
        -----
        For the `protocol` setting, some options are system-dependent
        (i.e: `ipc` is for unix-like OSes only).
        """
        self.context = context or zmq.Context.instance()
        self._context_managed_externally = ((context is not None) or
                                            (self.context is zmq.Context.instance()))
        self.streamer = ZMQStreamServer(protocol=protocol, interface=interface,
                                        port=broadcast_port, context=self.context)
        # Pass streamer into RPC Server as another device so we can interact
        # with it remotely. Hide it with a "__" prefix.
        self.instances = {} if instances is None else instances
        self.instances.update({"__streamer": self.streamer,
                               "__router_server": self})
        self.rpc = ZMQRPCServer(protocol=protocol, interface=interface,
                                port=rpc_port, context=self.context,
                                instances=self.instances)
        if not config:
            return
        # Construct any streams or named calls from config spec.
        for name, specs in config.get("periodic_streams", {}).items():
            self.add_stream(stream_name=name, **specs)
        for name, specs in config.get("named_calls", {}).items():
            self.add_named_call(call_name=name, **specs)

    def run(self, block: bool = False):
        """Setup rpc listener and broadcaster.

        Parameters
        ----------
        block: if ``False``, run the underlying blocking calls in a thread and
            return immediately. Otherwise, run the streamer in the current
            thread and block (i.e: do not return).
        """
        self.rpc.run()
        self.streamer.run(run_in_thread=(not block))

    def add_named_call(self, call_name: str, obj_name: str, attr_name: str,
                       args: list | None = None, kwargs: dict | None = None):
        """ Setup a call to be called with
        [`call_by_name`][one_liner.client.RouterClient.call_by_name] on the
        [`RouterClient`][one_liner.client.RouterClient].

        Parameters
        ----------
        call_name : str
            String to save the function call signature under.
        obj_name : str
            Underlying object instance name. Must be present in the `objects`
            dict passed into the `__init__`.
        attr_name : str
            Name of the callable attribute (method).
        args : tuple
            Default args to save with the function call.
        kwargs : dict
            Default kwargs to save with the function call.

        Notes
        -----
        `args` and `kwargs` can be overwritten by index or name respectively
        when actually calling the named function call with
        [`call_by_name`][one_liner.client.RouterClient.call_by_name].
        """
        return self.rpc.add_named_call(call_name=call_name, obj_name=obj_name,
                                       attr_name=attr_name, args=args,
                                       kwargs=kwargs)

    def add_stream(self, stream_name: str, frequency_hz: float, obj_name: str,
                   attr_name: str, args: list | None = None,
                   kwargs: dict | None = None, enabled: bool = True,
                   serializer: Encoding | Callable = "pickle"):
        """Create a stream from a callable object attribute in the instance dict.

        Parameters
        ----------
        stream_name:
        frequency_hz:
            frequency at which to call the underlying function
        obj_name:
            name of instance in the instances dict
        attr_name:
            name of callable instance attribute (a method)
        args:
            any function arguments
        kwargs:
            any function keyword arguments
        enabled:
            if true, start with the stream enabled.
        serializer:
            callable function to serialize the data or string representation of
            built-in serializer (or None if the data is already serialized)
        """
        func = getattr(self.rpc.instances[obj_name], attr_name)
        self.streamer.add(name=stream_name, frequency_hz=frequency_hz,
                          func=func, args=args, kwargs=kwargs,
                          enabled=enabled, serializer=serializer)


    def add_stream_from_callable(self, stream_name: str, frequency_hz: float,
                   func: Callable, args: list = None, kwargs: dict = None,
                   enabled: bool = True,
                   serializer: Encoding | Callable = "pickle"):

        """ Create a stream.

        i.e: Setup a function to be called with specific arguments at a set frequency.
        If the function is already being broadcasted, update the broadcast parameters.

        Parameters
        ----------
        name : str
            Stream name.
        frequency_hz : float or int
            Frequency at which to call the underlying function.
        func : callable
            Function to call.
        args : tuple
            Any function arguments.
        kwargs : dict
            Any function keyword arguments.
        enabled : bool, default True
            If true, start with the stream enabled.
        serializer : callable or str, optional
            Callable function to serialize the data or string representation of
            built-in serializer (or None if the data is already serialized).

        Examples
        --------
        >>> import cv2
        >>> video = cv2.VideoCapture(0)  # Get the first available camera.
        >>> def get_frame():
        ...     return video.read()[1]  # just get the frame.
        >>> server = RouterServer()
        >>> server.add_stream(
        ...     "live_video",  # name of the stream
        ...     30,            # How fast to call this function.
        ...     get_frame,     # func to call.
        ... )
        >>> server.run()
        """
        self.streamer.add(name=stream_name, frequency_hz=frequency_hz,
                          func=func, args=args, kwargs=kwargs,
                          enabled=enabled, serializer=serializer)

    def add_zmq_stream(self, name: str, address: str, enabled: bool = True,
                       log_chatter: bool = False):
        """ Add a stream from an existing zmq PUB socket source (including
        another existing [`RouterServer`][one_liner.server.RouterServer].

        Parameters
        ----------
        name:
            stream name
        address:
            zmq socket address: `{protocol}://{interface}:{port}`
        enabled:
            if `True`, start enabled.
        log_chatter:
            if `True`, intercept messages in the connected PUB
            socket and add them to the logs (if the data length is short).
        """
        self.streamer.add_zmq_stream(name=name, address=address, enabled=enabled,
                                     log_chatter=log_chatter)

    def get_stream_fn(self, name: str, set_timestamp: bool = False,
                      serializer: Encoding | Callable = "pickle") -> Callable:
        """
        Get a function to broadcast the specified stream name.

        Useful if the application is creating data at its own rate and needs
        a callback function to call upon producing new data. This implicitly
        adds a manual stream to the configuration.

        Parameters
        ----------
        name : str
            Stream name.
        serializer : callable or str
            Callable function to serialize the data or string representation of
            a built-in serializer.
        set_timestamp : bool
            If true, return a function who's first argument is the timestamp
            to be set for the packet.

        Returns
        -------
        callable
            A broadcast function that accepts data to be streamed.

        Examples
        --------
        >>> send_func = server.get_stream_fn("live_video_feed")
        >>> video = cv2.VideoCapture(0)  # Connect to first available camera.
        >>> # Send images as soon as we can get them off the camera.
        >>> while True:
        ...     new_frame = video.read()[1]  # Get new video frame
        ...     send_func(new_frame)
        """
        return self.streamer.get_stream_fn(name, serializer=serializer,
                                           set_timestamp=set_timestamp)

    def enable_stream(self, name):
        """
        Enable broadcasting of a stream by name.

        Any connected [`RouterClient`][one_liner.client.RouterClient] will start
        receiving data from this stream after they have configured how to buffer
        the stream  data with
        [`configure_stream`][one_liner.client.RouterClient.configure_stream].

        Parameters
        ----------
        name : str
            Stream name.

        Raises
        ------
        KeyError
            If the stream name does not exist.
        ValueError
            If the stream exists but cannot be enabled/disabled.

        Notes
        -----
        Enabling streams only works for periodically-added streams added with
        [`add_stream`][one_liner.server.RouterServer.add_stream] and
        [`add_zmq_stream`][one_liner.server.RouterServer.add_zmq_stream] but
        *not* [`get_stream_fn`][one_liner.server.RouterServer.get_stream_fn].
        """
        return self.streamer.enable(name)

    def disable_stream(self, name):
        """disable broadcasting of a stream by name."""
        return self.streamer.disable(name)

    def remove_stream(self, name: str):
        """
        Remove an existing stream.

        The stream must be re-added if needed later. Consider using
        [`enable_stream`][one_liner.server.RouterServer.enable_stream] and
        [`disable_stream`][one_liner.server.RouterServer.disable_stream]
        instead if you need to conditionally throttle whether a stream is
        sending data.
        """
        self.streamer.remove(name)

    @property
    def version(self):
        """Get the server version."""
        return local_version

    def get_version(self):
        """Get the server version."""
        return local_version

    def close(self):
        """Close the RPC and Stream clients."""
        self.rpc.close()
        self.streamer.close()
        if not self._context_managed_externally:
            self.context.term()
