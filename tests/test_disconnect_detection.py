import pytest
from one_liner.server import RouterServer
from one_liner.client import RouterClient
from tenacity import retry, stop_after_delay, wait_fixed
from one_liner.errors import RouterServerDisconnectedError
from time import sleep
from threading import Thread


class TestDevice:
    def sum(self, arg1, arg2):
        return arg1 + arg2

    def do_unbounded_task(self):
        while True:
            sleep(0.1)


def test_disconnection_detection_tcp():
    # Wait for client to eventually connect.
    # Retry every 0.1 seconds, stop after 1 seconds total
    @retry(wait=wait_fixed(0.1), stop=stop_after_delay(1))
    def poll_until_connected():
        assert client.is_connected() is True

    # Wait for client to eventually discconnect.
    @retry(wait=wait_fixed(0.1), stop=stop_after_delay(1))
    def poll_until_disconnected():
        assert client.is_connected() is False

    server = RouterServer(instances={"TestDevice": TestDevice()})
    client = RouterClient()
    assert client.is_connected() is False
    server.run()
    poll_until_connected()
    server.close()
    poll_until_disconnected()
    client.close()


def test_disconnection_detection_not_supported_in_inproc():
    server = RouterServer(protocol="inproc",
                instances={"TestDevice": TestDevice()})
    server.run()
    client = RouterClient(protocol="inproc")
    with pytest.raises(NotImplementedError):
        assert client.is_connected() is False
    server.close()
    client.close()


def test_server_disconnection_that_interrupts_client_call_raises_clientside_error(reraise):
    """Ensure the client side raises an exception if the RouterServer
    disconnects after the client issues an RPC that has not yet returned."""
    # Wait for client to eventually connect.
    # Retry every 0.1 seconds, stop after 1 seconds total
    @retry(wait=wait_fixed(0.1), stop=stop_after_delay(1))
    def poll_until_connected():
        assert client.is_connected() is True

    server = RouterServer(instances={"TestDevice": TestDevice()})
    client = RouterClient()
    server.run()

    poll_until_connected()

    @reraise.wrap # Forward failures in this thread to the main thread.
    def call_long_server_task_worker():
        # will raise DID NOT RAISE exception if this doesn't raise an
        # exception
        with pytest.raises(RouterServerDisconnectedError):
            client.call("TestDevice", "do_unbounded_task")

    t = Thread(target=call_long_server_task_worker, daemon=True)
    t.start()
    # Wait for client to actually issue the call
    sleep(0.25)
    # Kill the server, which should trigger the exception
    server.close(wait=False)
    t.join(timeout=3)
    assert not t.is_alive(), "client.call(...) did not return after server closed."
    client.close()
    reraise()  # Fail if the worker thread did not raise the correct exception


def test_many_threaded_client_connections_detect_server_disconnection(reraise):
    num_clients = 4
    server = RouterServer(instances={"TestDevice": TestDevice()})
    clients = [RouterClient() for i in range(num_clients)]
    server.run()
    # Wait all clients to connect.
    # Retry every 0.1 seconds, stop after 1 seconds total
    @retry(wait=wait_fixed(0.1), stop=stop_after_delay(1))
    def poll_until_connected():
        assert all([c.is_connected() for c in clients])

    poll_until_connected()

    @reraise.wrap # Forward failures in this thread to the main thread.
    def call_long_server_task_worker(client: RouterClient):
        with pytest.raises(RouterServerDisconnectedError):
            client.call("TestDevice", "do_unbounded_task")

    threads = []
    for client in clients:
        t = Thread(target=call_long_server_task_worker, args=[client],
            daemon=True)
        t.start()
        threads.append(t)
    # Wait for clients to actually issue the call
    sleep(0.25)
    # Kill the server, which should trigger the exception
    server.close(wait=False)
    for thread in threads:
        thread.join(timeout=3)
        assert not thread.is_alive(), "client.call(...) did not return after server closed."
    for client in clients:
        client.close()
    reraise()  # Fail if the worker thread did not raise the correct exception
