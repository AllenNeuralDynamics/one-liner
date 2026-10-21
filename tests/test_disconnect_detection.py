import pytest
from one_liner.server import RouterServer
from one_liner.client import RouterClient
from tenacity import retry, stop_after_delay, wait_fixed


class TestDevice:
    def sum(self, arg1, arg2):
        return arg1 + arg2


def test_disconnection_detection_tcp():
    server = RouterServer(instances={"TestDevice": TestDevice()})
    client = RouterClient()
    assert client.is_connected() is False
    server.run()
    # Wait for client to eventually connect.
    # Retry every 0.1 seconds, stop after 1 seconds total
    @retry(wait=wait_fixed(0.1), stop=stop_after_delay(1))
    def poll_until_connected():
        assert client.is_connected() is True
    server.close()
    # Wait for client to eventually discconnect.
    @retry(wait=wait_fixed(0.1), stop=stop_after_delay(1))
    def poll_until_disconnected():
        assert client.is_connected() is False
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
