"""Server for enabling remote control and broadcasting results of periodic function calls."""

import zmq
from one_liner import Protocol, Encoding
from one_liner.stream_server import ZMQStreamServer
from one_liner.rpc_server import ZMQRPCServer
from typing import Callable


class RouterServer:
    __slots__ = ("context", "streamer", "rpc")
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
           the zmq Context must be shared between `RouterServer` and `RouterClient`
           (or `RouterServer` and `RouterServer` if forwarding).

        .. note::
           For the `protocol` setting, some options are system-dependent (i.e:
           `ipc` is for unix-like OSes only).

        """
        self.context = context or zmq.Context.instance()
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

    def add_broadcast(self, name: str, frequency_hz: float, func: Callable,
                      *args, **kwargs):
        self.streamer.add(name, frequency_hz, func, *args, **kwargs)

    def get_broadcast_fn(self, name: str, set_timestamp: bool = False,
                         encoding: Encoding = "pickle"):
        return self.streamer.get_broadcast_fn(name, encoding=encoding,
                                              set_timestamp=set_timestamp)

    def remove_broadcast(self, func):
        self.streamer.remove(func)

    def close(self):
        self.rpc.close()
        self.streamer.close()
        # TODO: figure out why zmq proxy needs destroy instead of just term here
        self.context.destroy()


