#!/usr/bin/env/python3

from one_liner.client import RouterClient
from one_liner.server import RouterServer
from one_liner import __version__ as one_liner_version


def test_client_receive():
    """Ensure that the same version can be fetched across all one-liner
    components within the same process."""
    server = RouterServer()
    client = RouterClient()
    server.run()
    try:
        assert len({client.version, server.version, client.server_version}) == 1
    finally:
        server.close()
        client.close()
