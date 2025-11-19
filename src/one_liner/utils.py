import orjson
import struct
import pickle
import zmq
from time import perf_counter as now
from typing import Any, Literal, Callable, Tuple

Protocol = Literal["tcp", "inproc", "ipc", "ws", "wss"]
Encoding = Literal[None, "pickle", "json", "unspecified"]


SERIALIZERS: dict[Encoding, Callable] = \
    {
        None: lambda x: x,
        "pickle": pickle.dumps,
        "json": orjson.dumps
    }

DESERIALIZERS: dict[Encoding, Callable] = \
    {
        None: lambda x: x,
        "pickle": pickle.loads,
        "json": orjson.loads
    }


def _send(socket: zmq.Context.socket, name: str, data: bytes | Any,
          send_timestamp: bool = True, timestamp: float = None, success: bool = True,
          serializer: Encoding | Callable = "pickle"):
    """Send the data on tne specified socket prefixed with the specified
    stream name. Used in both RPC and StreamServer

    :param socket: socket to do the sending.
    :param name: stream name. Under the hood, this is the topic.
    :param data: data to send. If the data is not `bytes`-like, the
       :paramref:`ZMQStreamServer._send.encoding` option cannot be None.
    :param send_timestamp: if True, send a timestamp associated with the data.
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
    metadata = success if not send_timestamp else (success, timestamp)
    metadata_bytes = pickle.dumps(metadata)
    metadata_num_bytes = len(metadata_bytes)
    serialize = SERIALIZERS.get(serializer, serializer)
    packet = name.encode("utf-8") + \
             struct.pack("<H", metadata_num_bytes) + metadata_bytes + \
             serialize(data)
    # Set copy=False since we have a pickled representation of the data.
    socket.send(packet, copy=False)


def _recv(socket: zmq.Context.socket, flag: zmq.Flag = 0, prefix: str | None = None,
          has_timestamp: bool = True, deserializer: Encoding | Callable = "pickle") -> (
        Tuple[bool, Any] | Tuple[bool, float, Any]):
    """Receive data from a zmq socket and deserialize it.

    :param flag: additional zmq flag to pass to the socket
    :param has_timestamp: if the
    :param prefix: a prefix to the data (usually a zmq topic) or `None` if unspecified.
    :param deserializer: the encoding option to decode the data, `None`
       if the data is `bytes`-like, or a user-supplied function to deserialize
       a bytes-like object. Default is `"pickle"`.
    """
    # Unpack payload with deserializer of choice.
    deserialize_fn = DESERIALIZERS.get(deserializer, deserializer)
    raw_bytes = socket.recv(copy=False, flags=flag).buffer  # Get a view; don't copy yet.
    prefix_len = 0 if prefix is None else len(prefix)
    # Upack metadata first with pickle.
    metadata_num_bytes = struct.unpack("<H", raw_bytes[prefix_len:prefix_len + 2])[0]
    # Unpack the timestamp if we know it came with the data.
    if has_timestamp:
        success, timestamp = pickle.loads(raw_bytes[prefix_len + 2:])
        data = deserialize_fn(raw_bytes[prefix_len + 2 + metadata_num_bytes:])
        return success, timestamp, data
    success = pickle.loads(raw_bytes[prefix_len + 2:])
    data = deserialize_fn(raw_bytes[prefix_len + 2 + metadata_num_bytes:])
    return success, data


class RPCException(Exception):
    pass


class StreamException(Exception):
    pass
