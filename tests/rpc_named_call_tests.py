#!/usr/bin/env/python3

import pytest
from random import uniform
from one_liner.client import RouterClient
from one_liner.server import RouterServer
from one_liner.utils import RPCException



# Create a simple class to pass into the server.
class SensorArray:
    __test__ = False

    def __init__(self):
        pass

    def get_data(self, sensor_index: int = 0):
        """Get spoofed analog voltage sensor data: 0.0-5.0 [V] measurement from
            specified sensor index."""
        return {sensor_index: uniform(0., 5.)}


def test_many_client_receive():
    sensors = SensorArray()  # Create an object
    server = RouterServer(instances={"test_sensor_array": sensors})  # Create a server.
    client = RouterClient()
    server.run()

    server.add_named_call("get_sensor0", "test_sensor_array", "get_data", args=[0])
    data = client.call_by_name("get_sensor0")
    assert 0 in data
    # Ensure we can override args.
    data_from_sensor1 = client.call_by_name("get_sensor0", args=[1])
    assert 1 in data_from_sensor1
    # Ensure this throws an exception: we already specified sensor_index as an arg.
    with pytest.raises(RPCException):
        data_from_sensor1 = client.call_by_name("get_sensor0", kwargs={"sensor_index": 1})

    server.close()
    client.close()
