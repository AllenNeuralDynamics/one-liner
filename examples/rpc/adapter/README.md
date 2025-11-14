# Adapter Protocol Example


<img src="./adapter_diagram.svg">

This example shows a pattern that can be used to decouple generic code (e.g. for running an instrument) from infrastructure-specific code (e.g. querying external databases).

The Instrument code defines a [Protocol](https://typing.python.org/en/latest/spec/protocol.html) and models that it will use to communicate, then uses the `get_connector` utility method to set up an RPC client to broadcast the function calls over zmq.

In a separate application, an Adapter is written that imports the API from the main instrument and implements the specific logic to carry out the desired functions. It runs a RPC server that connects to the instrument.

The `get_connector` method in `instrument_utils.py` takes an abstract Protocol class and creates a concrete connector object that replaces the api methods with calls through the RPC client. It also can run a subprocess, so it can start the adapter based on a command provided by the instrument config. The connector object is also fully type-hinted and supports IDE autocomplete.

## 

This essentially allows dependency injection, where the injected code can be in its own repository and can be deployed and run completely separately from the generic instrument or application.
