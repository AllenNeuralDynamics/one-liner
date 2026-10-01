import logging
from one_liner.utils import RPCException
import pytest
from one_liner.server import RouterServer
from one_liner.client import RouterClient

config = {
    "named_calls": {
        "zmq_func_1": {
            "obj_name": "TestDevice",
            "attr_name": "func1",
            "access_type": "set"
        },
    },
    "periodic_streams": {},
}


class TestDevice:
    def func1(self, arg1, arg2):
        return arg1 + arg2


def test_get_write_token():
    server = RouterServer(protocol="inproc", interface="localhost",
                          instances={"TestDevice": TestDevice()}, config=config)
    server.run()
    client = RouterClient(protocol="inproc")
    assert client.has_write_token() is False
    client.get_write_token()
    assert client.has_write_token() is True
    client.close()
    server.close()


def test_multiple_clients_start_without_write_token():
    num_clients = 3
    server = RouterServer(protocol="inproc",
                          instances={"TestDevice": TestDevice()}, config=config)
    server.run()
    clients = [RouterClient(protocol="inproc", interface="localhost") for _ in range(num_clients)]
    assert not any([cli.has_write_token() for cli in clients])

    for client in clients:
        client.close()
    server.close()


def test_get_write_token_repeatedly_with_same_client_is_idempotent():
    num_tries = 5
    server = RouterServer(protocol="inproc", interface="localhost",
                          instances={"TestDevice": TestDevice()}, config=config)
    server.run()
    client = RouterClient(protocol="inproc")
    for i in range(num_tries):
        client.get_write_token()
    client.close()
    server.close()


def test_get_write_token_after_distributing_it_fails():
    num_clients = 2
    server = RouterServer(protocol="inproc", interface="localhost",
                          instances={"TestDevice": TestDevice()}, config=config)
    server.run()
    clients = [RouterClient(protocol="inproc") for _ in range(num_clients)]
    clients[0].get_write_token()
    with pytest.raises(PermissionError):
        clients[1].get_write_token()
    for client in clients:
        client.close()
    server.close()


def test_force_get_write_token():
    num_clients = 2
    server = RouterServer(protocol="inproc", interface="localhost",
                          instances={"TestDevice": TestDevice()}, config=config)
    server.run()
    clients = [RouterClient(protocol="inproc") for _ in range(num_clients)]
    clients[0].get_write_token()
    assert clients[0].has_write_token() is True
    clients[0].call_by_name("zmq_func_1", args=[1, 2])  # Should work with write token access.
    clients[1].get_write_token(force=True)
    assert clients[1].has_write_token() is True
    # Unfortunately, force-getting the write token doesn't tell the og client
    # that it doesn't have the write token anymore.
    # Write tokens will be different.
    assert clients[0].rpc_client._write_token != clients[1].rpc_client._write_token
    # Next "set" function with defunct write token will no longer work.
    with pytest.raises(RPCException):
        clients[0].call_by_name("zmq_func_1", args=[1, 2])
    for client in clients:
        client.close()
    server.close()


def test_release_write_token():
    server = RouterServer(protocol="inproc", interface="localhost",
                          instances={"TestDevice": TestDevice()}, config=config)
    server.run()
    client = RouterClient(protocol="inproc")
    assert client.has_write_token() is False
    client.get_write_token()
    client.release_write_token()
    assert client.has_write_token() is False
    client.close()
    server.close()
