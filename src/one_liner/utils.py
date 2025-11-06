import json
import struct
import pickle
import zmq
from time import perf_counter as now
from typing import Any, Literal, Callable, Tuple

Protocol = Literal["tcp", "inproc", "ipc", "ws", "wss"]
Encoding = Literal[None, "pickle", "json", "unspecified"]


SERIALIZERS = \
    {
        None: lambda x: x,
        "pickle": pickle.dumps,
        "json": lambda x: json.dumps(x).encode("utf-8")
    }

DESERIALIZERS = \
    {
        None: lambda x: x,
        "pickle": pickle.loads,
        "json": lambda x: json.loads(bytes(x))
    }


def _send(socket: zmq.Context.socket, name: str, data: bytes | Any,
          timestamp: float = None, success: bool = True,
          serializer: Encoding | Callable = "pickle"):
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
    :param serializer: the encoding option to encode the data, `None`
       if the data is `bytes`-like, or a user-supplied function to serialize
       a bytes-like object. Default is `"pickle"`.

    """
    timestamp = timestamp if timestamp is not None else now()
    # Because the zmq CONFLATE option (keep-last-message) only, doesn't work
    # with multipart messages where the first msg is the topic, we smush
    # the topic and data together as packed binary data before sending so that
    # topic filtering (i.e: subscriptions) work.

    # It's a little clunky that we need to send the size of the pickled
    # metadata, but it prevents us from doing an extra copy into a
    # io.BytesIO object on the receiving end.
    metadata_bytes = pickle.dumps((success, timestamp))
    metadata_num_bytes = len(metadata_bytes)
    serialize = SERIALIZERS.get(serializer, serializer)
    packet = name.encode("utf-8") + \
             struct.pack("<H", metadata_num_bytes) + metadata_bytes + \
             serialize(data)
    # Set copy=False since we have a pickled representation of the data.
    socket.send(packet, copy=False)


def _recv(socket: zmq.Context.socket, flag: zmq.Flag = 0, prefix: str | None = None,
          deserializer: Encoding | Callable = "pickle") -> Tuple[bool, float, Any]:
    """Receive data from a zmq socket and deserialize it.

    :param flag: additional zmq flag to pass to the socket
    :param deserializer: the encoding option to decode the data, `None`
       if the data is `bytes`-like, or a user-supplied function to deserialize
       a bytes-like object. Default is `"pickle"`.
    """
    raw_bytes = socket.recv(copy=False, flags=flag).buffer  # Get a view; don't copy yet.
    prefix_len = 0 if prefix is None else len(prefix)
    # Upack metadata first with pickle.
    metadata_num_bytes = struct.unpack("<H", raw_bytes[prefix_len:prefix_len + 2])[0]
    success, timestamp = pickle.loads(raw_bytes[prefix_len + 2:])
    # Unpack payload with deserializer of choice.
    deserialize = DESERIALIZERS.get(deserializer, deserializer)
    data = deserialize(raw_bytes[prefix_len + 2 + metadata_num_bytes:])
    return success, timestamp, data


class RPCException(Exception):
    pass


class StreamException(Exception):
    pass
