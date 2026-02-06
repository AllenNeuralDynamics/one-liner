from functools import partial
import subprocess
from typing import Optional, Type, TypeVar

from one_liner.client import RouterClient

T = TypeVar("API")


def get_connector(
    protocol: Type[T],
    port: str = "5555",
    target_obj_name: str = "adapter",
    cmd_to_start_adapter_server: Optional[list[str]] = None,
) -> T:
    """Create a proxy instance that implements the given protocol

    :param protocol: Protocol class defining the API to connect to.
    :param target_obj_name: Name of the target object in the adapter server.
    :param port: Port where the adapter server is listening for RPC calls.
    :param cmd_to_start_adapter_server: If provided, will be run in a subprocess.
    """

    if cmd_to_start_adapter_server:
        adapter_process = subprocess.Popen(cmd_to_start_adapter_server)
    else:
        adapter_process = None

    client = RouterClient(rpc_port=port)

    api_funcs = protocol.__protocol_attrs__

    def call_remote(funcname: str, *args, **kwargs):
        return client.call(target_obj_name, funcname, args, kwargs)

    # dynamically create a new class that implements the protocol
    connector = type(
        f"{protocol.__name__}_connector",
        (protocol,),
        {f: partial(call_remote, f) for f in api_funcs}
        | {"_client": client, "_adapter_process": adapter_process},
    )
    instance = connector()
    return instance
