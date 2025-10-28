import json
import pickle
import zmq
from time import perf_counter as now
from typing import Any, Literal

Protocol = Literal["tcp", "inproc", "ipc", "ws", "wss"]
Encoding = Literal[None, "pickle", "json", "unspecified"]


SERIALIZERS = \
    {
        None: lambda x: x,
        "pickle": pickle.dumps,
        "json": json.dumps
    }


def _send(socket: zmq.Context.socket, name: str, data: bytes | Any,
          timestamp: float = None, success: bool = True,
          encoding: Encoding = "pickle"):
    """Send the data on tne specified socket prefixed with the specified
    stream name. Used in both RPC and StreamServer

    :param socket: socket to do the sending.
    :param name: stream name. Under the hood, this is the topic.
    :param data: data to send. If the data is not `bytes`-like, the
       :paramref:`ZMQStreamServer._send.encoding` option cannot be None.
    :param timestamp: if specified, send the data with a custom timestamp
       instead of the default ``time.perf_counter``.
    :param success: True if the data being sent was returned from a function
       that did not raise an exception. False otherwise.
       If False, the data is considered an exception string.
    :param encoding: the encoding option which to encode the data or `None`
       if the data is `bytes`-like. Default is `"pickle"`.

    """
    timestamp = timestamp if timestamp is not None else now()
    # Because the CONFLATE option (keep-last-message) only, doesn't work
    # with multipart messages where the first msg is the topic, we smush
    # the topic and data together as packed binary data before sending so that
    # topic filtering (i.e: subscriptions) work.
    packet = name.encode("utf-8") + SERIALIZERS[encoding]((success, timestamp, data))
    # Set copy=False since we have a pickled representation of the data.
    socket.send(packet, copy=False)


class RPCException(Exception):
    pass

class StreamException(Exception):
    pass
