from functools import partial
from typing import Type, TypeVar

from one_liner.client import RouterClient

T = TypeVar("API")


def get_connector(protocol: Type[T], remote_device_name: str) -> T:
    """Create a proxy instance that implements the given protocol"""
    client = RouterClient()

    funcs = protocol.__protocol_attrs__

    def func(funcname: str, *args, **kwargs):
        return client.call(remote_device_name, funcname, args, kwargs)[1]

    connector = type(
        f"{protocol.__name__}_sub",
        (protocol,),
        {f: partial(func, f) for f in funcs} | {"_client": client},
    )
    instance = connector()
    return instance
