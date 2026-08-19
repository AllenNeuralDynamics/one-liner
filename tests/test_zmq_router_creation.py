import logging
import pytest
from one_liner.server import RouterServer

config = {
    "named_calls": {
        "zmq_func_1": {
            "obj_name": "TestDevice",
            "attr_name": "func1",
        },
    },
    "periodic_streams": {
        "zmq_stream_1": {
            "frequency_hz": 1,
            "obj_name": "TestDevice",
            "attr_name": "stream1",
            "kwargs": {"arg1": 1, "arg2": 2},
            # "args": [1, 2],
        },
    },
}


class TestDevice:
    def func1(self, arg1, arg2):
        return arg1 + arg2

    def stream1(self, arg1, arg2):
        return arg1 + arg2


def test_create_server_with_config():
    server = RouterServer(instances={"TestDevice": TestDevice()}, config=config)
    server.run()
    server.close()


def test_create_server_missing_object_errors():
    with pytest.raises(KeyError):
        server = RouterServer(instances={"TestDeeViCe": TestDevice()}, config=config)


def test_create_server_missing_attribute_errors():
    with pytest.raises(AttributeError):
        server = RouterServer(
            instances={"TestDevice": TestDevice()},
            config={
                "named_calls": {
                    "zmq_func_1": {
                        "obj_name": "TestDevice",
                        "attr_name": "non_existent_func",
                    },
                },
                "periodic_streams": {},
            },
        )


def test_create_server_non_unique_named_calls(caplog):
    caplog.set_level(logging.WARNING)
    server = RouterServer(instances={"TestDevice": TestDevice()}, config=config)

    # Add a duplicate named call to the server configuration
    server.add_named_call(
        "zmq_func_1",
        "TestDevice",
        "func1",
    )
    server.run()
    server.close()

    assert (
        "ZMQRPCServer",
        logging.WARNING,
        "Overwriting existing named call: zmq_func_1",
    ) in caplog.record_tuples


# TODO: test_create_server_non_unique_streams


################################################################################
#
#   RPC args/kwargs tests
#
################################################################################


def test_create_named_call_invalid_args_and_kwargs():
    """Test when args and kwargs conflict with the function signature."""
    server = RouterServer(instances={"TestDevice": TestDevice()}, config=config)
    with pytest.raises(TypeError):
        server.add_named_call(
            "zmq_func_1",
            "TestDevice",
            "func1",
            args=[1, 2],
            kwargs={"arg1": 1, "arg2": 2},
        )
    server.close()


def test_create_named_call_too_many_args():
    server = RouterServer(instances={"TestDevice": TestDevice()}, config=config)
    with pytest.raises(TypeError):
        server.add_named_call(
            "zmq_func_1",
            "TestDevice",
            "func1",
            args=[1, 2, 3],  # Too many args
        )
    server.close()


def test_create_named_call_non_existing_kwargs():
    server = RouterServer(instances={"TestDevice": TestDevice()}, config=config)
    with pytest.raises(TypeError):
        server.add_named_call(
            "zmq_func_1",
            "TestDevice",
            "func1",
            kwargs={"non_existing_arg": 1},  # Non-existing kwarg
        )
    server.close()


################################################################################
#
#   Streams args/kwargs tests
#
################################################################################


def test_create_stream_invalid_args_and_kwargs():
    server = RouterServer(instances={"TestDevice": TestDevice()}, config=config)
    """Stream: args and kwargs conflict with the function signature."""
    with pytest.raises(TypeError):
        server.add_stream(
            "zmq_stream_1",
            frequency_hz=1,
            obj_name="TestDevice",
            attr_name="stream1",
            args=[1, 2],
            kwargs={"arg1": 1, "arg2": 2},
        )
    server.close()


def test_create_stream_too_many_args():
    server = RouterServer(instances={"TestDevice": TestDevice()}, config=config)
    with pytest.raises(TypeError):
        server.add_stream(
            "zmq_stream_1",
            frequency_hz=1,
            obj_name="TestDevice",
            attr_name="stream1",
            args=[1, 2, 3],  # Too many args
        )
    server.close()


def test_create_stream_non_existing_kwargs():
    server = RouterServer(instances={"TestDevice": TestDevice()}, config=config)
    with pytest.raises(TypeError):
        server.add_stream(
            "zmq_stream_1",
            frequency_hz=1,
            obj_name="TestDevice",
            attr_name="stream1",
            kwargs={"non_existing_arg": 1},  # Non-existing kwarg
        )
    server.close()
