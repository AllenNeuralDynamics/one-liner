"""Server for enabling remote control and broadcasting results of periodic function calls."""

import zmq
from one_liner.stream_server import ZMQStreamServer
from one_liner.rpc_server import ZMQRPCServer
from one_liner.utils import Protocol, Encoding
from typing import Callable


class RouterServer:
    __slots__ = ("context", "streamer", "rpc", "_context_managed_externally")
    """Interface for enabling remote control/monitoring of one or more object
       instances. Heavy lifting is delegated to two subordinate objects."""

    def __init__(self,  protocol: Protocol = "tcp", interface: str = "*",
                 rpc_port: str = "5555", broadcast_port: str = "5556",
                 context: zmq.Context = None, **devices):
        """constructor.

        :param protocol: a zmq supported protocol (tcp, inproc, etc)
        :param interface:
        :param rpc_port: port to issue remote procedure calls to python objects.
        :param broadcast_port: port from which to stream periodic data.
        :param context: the zmq context. Will be created automatically if
            unspecified.

        .. warning::
           For sharing data within the same process using the `inproc` protocol,
           the zmq Context must be shared between
           :py:class:`~one_liner.server.RouterServer` and
           :py:class:`~one_liner.client.RouterClient`
           (or :py:class:`~one_liner.server.RouterServer` and
           :py:class:`~one_liner.server.RouterServer` if forwarding).

        .. note::
           For the `protocol` setting, some options are system-dependent (i.e:
           `ipc` is for unix-like OSes only).

        """
        self.context = context or zmq.Context.instance()
        self._context_managed_externally = context is None
        self.streamer = ZMQStreamServer(protocol=protocol, interface=interface,
                                        port=broadcast_port, context=self.context)
        # Pass streamer into RPC Server as another device so we can interact
        # with it remotely. Hide it with a "__" prefix.
        self.rpc = ZMQRPCServer(protocol=protocol, interface=interface,
                                port=rpc_port, context=self.context,
                                __streamer=self.streamer, **devices)

    def run(self, run_in_thread: bool = True):
        """Setup rpc listener and broadcaster.

        :param run_in_thread: if ``True`` (default), run the underlying blocking
           calls in a thread and return immediately.
        """
        self.rpc.run()
        self.streamer.run(run_in_thread=run_in_thread)

    def add_stream(self, name: str, frequency_hz: float, func: Callable,
                      *args, **kwargs):
        """Create a stream. i.e: Setup a function to be called with specific
        arguments at a set frequency. If the function is already being
        broadcasted, update the broadcast parameters.

        :param name: stream name
        :param frequency_hz: frequency at which to call the underlying function
        :param func: function to call
        :param \\*args: any function arguments
        :param \\*\\*kwargs: any function keyword arguments

        .. code-block:: python

           import cv2

           video = cv2.VideoCapture(0) # Get the first available camera.

           def get_frame():
             return video.read()[1] # just get the frame.

           server = RouterServer()
           server.add_stream("live_video", # name of the stream
                             30,  # How fast to call this function.
                             get_frame) # func to call.
           server.run()
        """
        self.streamer.add(name, frequency_hz, func, *args, **kwargs)

    def add_zmq_stream(self, name: str, address: str, enabled: True,
                       log_chatter: bool = False):
        """ Add a stream from an existing zmq PUB socket source (including
        another existing :py:class:`~one_liner.server.RouterServer`).

        :param name: stream name
        :param address: zmq socket address: `{protocol}://{interface}:{port}`
        :param enabled: if True (default) start enabled.
        :param log_chatter: if True, intercept messages in the connected PUB
           socket and add them to the logs (if the data length is short).

        """
        self.streamer.add_zmq_stream(name=name, address=address, enabled=enabled,
                                     log_chatter=log_chatter)

    def get_stream_fn(self, name: str, set_timestamp: bool = False,
                      serializer: Encoding | Callable = "pickle"):
        """Get a function to broadcast the specified stream name.
        Useful if the application is creating data at its
        own rate and needs a callback function to call upon producing new data.

        This implicitly adds a manual stream to the configuration.

        :param name: stream name
        :param serializer: callable function to serialize the data or string
            representation of a built-in serializer.
        :param set_timestamp: if true, return a function who's first argument is
            the timestamp to be set for the packet.

        .. code-block:: python

           send_func = server.get_stream_fn("live_video_feed")
           video = cv2.VideoCapture(0) # Connect to first available camera.

           # Send images as soon as we can get them off the camera.
           while True:
               new_frame = video.read()[1] # Get new video frame
               send_func(new_frame)

        """
        return self.streamer.get_stream_fn(name, serializer=serializer,
                                           set_timestamp=set_timestamp)

    def enable_stream(self, name):
        """Enable broadcasting of a stream by name. Any connected
        :py:class:`~one_liner.client.RouterClient` will start receiving data
        from this stream after they have configured how to buffer the stream
        data with :py:meth:`~one_liner.client.RouterClient.configure_stream`.

        :param name: stream name
        :raises KeyError: if the stream name does not exist.
        :raises ValueError: if the stream exists but cannot be enabled/disabled.

        .. note::
           Enabling streams only works for periodically-added streams
           added with :py:meth:`~one_liner.server.RouterServer.add_stream` and
           :py:meth:`~one_liner.server.RouterServer.add_zmq_stream` but
           *not* :py:meth:`~one_liner.server.RouterServer.get_stream_fn`.

        """
        return self.streamer.enable(name)

    def disable_stream(self, name):
        """disable broadcasting of a stream by name."""
        return self.streamer.disable(name)

    def remove_stream(self, name: str):
        """Remove an existing stream. The stream must be re-added if needed
        later. Consider using
        :py:meth:`~one_liner.server.RouterServer.enable_stream` and
        :py:meth:`~one_liner.server.RouterServer.disable_stream` instead if you
        need to conditionally throttle whether a stream is sending data.

        """
        self.streamer.remove(name)

    def close(self):
        """Close the RPC and Stream clients."""
        self.rpc.close()
        self.streamer.close()
        if not self._context_managed_externally:
            # TODO: figure out why zmq proxy needs destroy instead of just term.
            self.context.destroy()
