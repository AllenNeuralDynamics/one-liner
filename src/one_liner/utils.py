import inspect
import orjson
import struct
import pickle
import zmq
from pydantic import TypeAdapter, create_model, Field
from time import perf_counter as now
from typing import Any, Literal, Callable, Tuple

Protocol = Literal["tcp", "inproc", "ipc", "ws", "wss"]
Encoding = Literal[None, "pickle", "json", "unspecified"]

TOPIC_SUFFIX = b"\x00" # null-termination character


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
    metadata = (success, timestamp)
    metadata_bytes = pickle.dumps(metadata)
    metadata_num_bytes = len(metadata_bytes)
    serialize = SERIALIZERS.get(serializer, serializer)
    packet = name.encode("utf-8") + TOPIC_SUFFIX + \
             struct.pack("<H", metadata_num_bytes) + metadata_bytes + \
             serialize(data)
    # Set copy=False since we have a pickled representation of the data.
    socket.send(packet, copy=False)


def _recv(socket: zmq.Context.socket, flag: zmq.Flag = 0, prefix: str | None = None,
          deserializer: Encoding | Callable = "pickle") -> Tuple[bool, float, Any]:
    """Receive data from a zmq socket and deserialize it.

    :param flag: additional zmq flag to pass to the socket
    :param prefix: a prefix to the data (usually a zmq topic) or `None` if unspecified.
    :param deserializer: the encoding option to decode the data, `None`
       if the data is `bytes`-like, or a user-supplied function to deserialize
       a bytes-like object. Default is `"pickle"`.
    """
    # Unpack payload with deserializer of choice.
    deserialize_fn = DESERIALIZERS.get(deserializer, deserializer)
    raw_bytes = socket.recv(copy=False, flags=flag).buffer  # Get a view; don't copy yet.
    prefix_len = len(TOPIC_SUFFIX) if prefix is None else len(prefix) + len(TOPIC_SUFFIX)
    # Upack metadata first with pickle.
    metadata_num_bytes = struct.unpack("<H", raw_bytes[prefix_len:prefix_len + 2])[0]
    success, timestamp = pickle.loads(raw_bytes[prefix_len + 2:])
    data = deserialize_fn(raw_bytes[prefix_len + 2 + metadata_num_bytes:])
    return success, timestamp, data


def get_func_sig_json_schema(func: Callable, default_args: list = [], 
                             default_kwargs: dict = {}) -> dict:
    """
    Get the JSON schema for the parameters and return type of a function as well 
    as the docstring description. 
    
    NOTE: utilizes jsonref library to resolve any $ref references in the schema. 
    Since each schema is generated from its own function signature, there aren't
    any shared references between schemas, so this is a safe operation. 
    Plus this simplifies converting schemas -> models. 
    Pydantic may support this natively in the future according to these issues: 
    https://github.com/pydantic/pydantic/issues/889
    https://github.com/pydantic/pydantic/issues/12023
    """
    # Parameters schema
    # -----------------
    signature = inspect.signature(func)

    # Get default values for parameters
    #   <key: parameter name, value: default value>
    #   This should be the pre-filled parameters defined in configs or `add_named_call`
    #
    #   Also does arity check: verify arg/kwargs fits function signature
    #   (too many args, unknown kwargs, duplicate kwargs, etc)
    bound = signature.bind_partial(*default_args, **default_kwargs)
    default_param_values: dict[str, Any] = bound.arguments

    fields = {}
    for param_name, param in signature.parameters.items():
        if param_name == "self":
            continue
        annotation = param.annotation
        if param_name in default_param_values:
            # Default values from RouterServer 
            default = default_param_values[param_name]
        elif param.default is not inspect.Parameter.empty:
            # Default values from function signature
            default = param.default
        else:
            # No default
            default = ...
        if annotation is inspect.Parameter.empty:
            fields[param_name] = (Any, Field(default, description="Type unspecified — provide a value."))
        else:
            fields[param_name] = (annotation, default)
    # Build pydantic model with per-parameter fields with default values and `required`. 
    params_model = create_model(f"{func.__name__}_params", **fields)
    params_schema = params_model.model_json_schema()

    # Return type schema
    # ------------------
    return_annotation = signature.return_annotation
    if return_annotation is inspect.Signature.empty or return_annotation is None:
        # Default schema for no return type"
        return_schema = {}
    else:
        return_schema = TypeAdapter(return_annotation).json_schema()

    return {"params": params_schema, "return": return_schema,
            "description": inspect.getdoc(func)}


class RPCException(Exception):
    pass


class StreamException(Exception):
    pass
