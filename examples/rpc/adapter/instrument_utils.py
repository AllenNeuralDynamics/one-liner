from functools import partial
import subprocess
from typing import Generic, Optional, Type, TypeVar

from one_liner.client import RouterClient


class ZmqRPCConnectorMixin:
    """A mixin class to create a ZMQ RPC connector for a given protocol.

    Usage:

    ```
    # Define an API protocol
    class MyProtocol(Protocol):
        api_name: str = "my_api"
        @staticmethod
        def my_method(param1: str) -> int:
            pass

    # Write one line to make a RPC implementation of your protocol
    class MyConnector(ZmqRPCConnectorMixin, MyProtocol): ...

    # Instantiate and use
    connector = MyConnector()
    connector.my_method("example")
    ```
    """

    _client: RouterClient
    api_name: str
    _cmd_to_start_adapter_server: Optional[list[str]]
    _adapter_process: subprocess.Popen | None

    def __init__(
        self,
        port: str = "5555",
        cmd_to_start_adapter_server: Optional[list[str]] = None,
        target_obj_name: Optional[str] = None,
    ):
        self._client = RouterClient(rpc_port=port)
        self._target_obj_name = target_obj_name or self.api_name

        self._cmd_to_start_adapter_server = cmd_to_start_adapter_server

        self._start_adapter_process()

        if hasattr(self, "__protocol_attrs__"):
            for funcname in self.__protocol_attrs__:
                setattr(
                    self,
                    funcname,
                    partial(self._call_remote, funcname),
                )

    def _start_adapter_process(self):
        if self._cmd_to_start_adapter_server:
            self._adapter_process = subprocess.Popen(self._cmd_to_start_adapter_server)
        else:
            self._adapter_process = None

    def _call_remote(self, funcname: str, *args, **kwargs):
        return self._client.call(self._target_obj_name, funcname, args, kwargs)[1]


T = TypeVar("API")


class ZmqRPCConnector(Generic[T]):
    """A parametrizable class to create a ZMQ RPC connector for a given protocol.
    This works by modifying the return type of __new__

    Usage:

    ```
    # Define an API protocol
    class MyProtocol(Protocol):
        api_name: str = "my_api"
        @staticmethod
        def my_method(param1: str) -> int:
            pass

    # Pass the protocol to ZmqRPCConnector and use
    connector = ZmqRPCConnector(protocol=MyProtocol)
    connector.my_method("example")
    ```
    """

    _client: RouterClient
    api_name: str
    _cmd_to_start_adapter_server: Optional[list[str]]
    _adapter_process: subprocess.Popen | None

    def __new__(cls, protocol: Type[T], *args, **kwargs) -> T:
        instance = super().__new__(cls)
        return instance

    def __init__(
        self,
        protocol: Type[T],
        port: str = "5555",
        cmd_to_start_adapter_server: Optional[list[str]] = None,
        target_obj_name: Optional[str] = None,
    ):
        self._protocol = protocol
        self._client = RouterClient(rpc_port=port)
        self._target_obj_name = target_obj_name or self._protocol.api_name

        self._cmd_to_start_adapter_server = cmd_to_start_adapter_server

        self._start_adapter_process()

        if hasattr(self, "_protocol"):
            for funcname in self._protocol.__protocol_attrs__:
                setattr(
                    self,
                    funcname,
                    partial(self._call_remote, funcname),
                )

    def _start_adapter_process(self):
        if self._cmd_to_start_adapter_server:
            self._adapter_process = subprocess.Popen(self._cmd_to_start_adapter_server)
        else:
            self._adapter_process = None

    def _call_remote(self, funcname: str, *args, **kwargs):
        return self._client.call(self._target_obj_name, funcname, args, kwargs)[1]


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
