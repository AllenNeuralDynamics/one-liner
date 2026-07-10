import pytest
from one_liner.client import RouterClient
from one_liner.server import RouterServer

# NOTE: Tests only cover JSON schema rules that I implemented. Any other JSON
# schema conversion rules were done by pydantic functions and are not tested here.
# Examples of my implementation:
#   - Stripping "self" from parameters
#   - Handling unannotated parameters
#   - Handling optional parameters
#   - Handling missing return annotations
#   - Handling docstrings for descriptions


# Test class to pass into server
class RPCClass:
    def rpc(self):
        return str(input)

    def rpc_with_params(self, input):
        return str(input)

    def rpc_annotated(self, input: int, opt: str | None = None) -> str:
        """Test RPC function that is fully annotated"""
        return str(input) + opt


@pytest.fixture
def rpc_configs():
    server = RouterServer(instances={"test_rpc": RPCClass()})
    client = RouterClient()
    server.run()
    for name in ("rpc", "rpc_with_params", "rpc_annotated"):
        server.add_named_call(name, "test_rpc", name)
    try:
        _, data = client.get_rpc_configurations()
        yield data
    finally:
        server.close()
        client.close()


################################################################################
#
#   PARAM ANNOTATION TESTS
#
################################################################################


def test_all_registered_calls_are_returned(rpc_configs):
    assert set(rpc_configs) == {"rpc", "rpc_with_params", "rpc_annotated"}


def test_self_is_stripped_from_params(rpc_configs):
    for entry in rpc_configs.values():
        assert "self" not in entry.params_schema.get("properties", {})


def test_no_params_yields_empty_properties(rpc_configs):
    assert rpc_configs["rpc"].params_schema.get("properties", {}) == {}
    assert "required" not in rpc_configs["rpc"].params_schema


def test_unannotated_param_yields_no_type_and_custom_description(rpc_configs):
    """When users don't annotate a parameter, don't guess the type, surface
    a description to notify downstream"""
    prop = rpc_configs["rpc_with_params"].params_schema["properties"]["input"]
    assert "type" not in prop
    assert prop.get("description")  # custom description
    assert "input" in rpc_configs["rpc_with_params"].params_schema["required"]


def test_annotated_param_yields_type(rpc_configs):
    prop = rpc_configs["rpc_annotated"].params_schema["properties"]["input"]
    assert prop["type"] == "integer"
    assert "input" in rpc_configs["rpc_annotated"].params_schema["required"]


def test_annotated_params_with_optional_yields_defaults(rpc_configs):
    prop = rpc_configs["rpc_annotated"].params_schema["properties"]["opt"]
    types = [types["type"] for types in prop["anyOf"]]
    assert "string" in types
    assert "null" in types
    assert "opt" not in rpc_configs["rpc_annotated"].params_schema.get("required", [])


################################################################################
#
#   RETURN ANNOTATION TESTS
#
################################################################################


def test_return_annotation(rpc_configs):
    assert rpc_configs["rpc_annotated"].return_schema.get("type") == "string"


def test_missing_return_annotation_yields_empty_schema(rpc_configs):
    assert rpc_configs["rpc"].return_schema == {}
    assert rpc_configs["rpc_with_params"].return_schema == {}


################################################################################
#
#  DOCSTRING ANNOTATION TESTS
#
################################################################################


def test_docstring_becomes_description(rpc_configs):
    assert rpc_configs["rpc"].description is None
    assert rpc_configs["rpc_with_params"].description is None
    assert (
        rpc_configs["rpc_annotated"].description
        == "Test RPC function that is fully annotated"
    )


################################################################################
#
#   Single stream test for posterity
#
#   Doesn't test all the stream functionality, just that the schema is generated
#   correctly. Streams and RPCs use the same function to convert function
#   signatures to JSON schema, so testing one is sufficient.
#
################################################################################


def test_annotated_stream_yields_schema():
    def test_stream(a: int) -> int:
        """test stream function"""
        return a + 1

    server = RouterServer()
    client = RouterClient()

    server.add_stream_from_callable("test_stream", 100, test_stream, args=[1])
    server.run()

    _, data = client.get_stream_configurations()

    stream = data.periodic_streams["test_stream"]
    param_schema = stream.params_schema
    return_schema = stream.return_schema
    description = stream.description

    assert param_schema["properties"]["a"]["type"] == "integer"
    assert return_schema["type"] == "integer"
    assert description == "test stream function"
    assert "a" in param_schema["required"]

    server.close()
    client.close()


################################################################################
#
#   DEFAULT ARGS / KWARGS TESTS
#
#   When add_named_call is given `args` and/or `kwargs`, the matching parameters
#   should be optional in the generated JSON schema, with the default value as
#   the schema `default`.
#
################################################################################


@pytest.fixture
def rpc_configs_with_defaults():
    server = RouterServer(instances={"test_rpc": RPCClass()})
    client = RouterClient()
    server.run()
    server.add_named_call("with_args", "test_rpc", "rpc_annotated", args=[10])
    server.add_named_call(
        "with_kwargs", "test_rpc", "rpc_annotated", kwargs={"input": 10}
    )
    server.add_named_call(
        "with_both", "test_rpc", "rpc_annotated", args=[10], kwargs={"input": 11}
    )
    try:
        _, data = client.get_rpc_configurations()
        yield data
    finally:
        server.close()
        client.close()


def test_default_args_make_param_optional(rpc_configs_with_defaults):
    """Check default value is set for parameter when args is provided in add_named_call"""
    schema = rpc_configs_with_defaults["with_args"].params_schema
    assert "input" not in schema.get("required", [])
    assert schema["properties"]["input"]["default"] == 10


def test_default_kwargs_make_param_optional(rpc_configs_with_defaults):
    """Check default value is set for parameter when kwargs is provided in add_named_call"""
    schema = rpc_configs_with_defaults["with_kwargs"].params_schema
    assert "input" not in schema.get("required", [])
    assert schema["properties"]["input"]["default"] == 10


def test_default_args_override_default_kwargs(rpc_configs_with_defaults):
    """When args and kwargs provided, use args as the default value"""
    schema = rpc_configs_with_defaults["with_both"].params_schema
    assert "input" not in schema.get("required", [])
    assert schema["properties"]["input"]["default"] == 10
