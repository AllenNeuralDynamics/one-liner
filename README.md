# one-liner
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)

<img src="./assets/train-conductor-mouse.png" width="160">


a ZMQ-based Router pattern for interacting with remote python objects.

High level features:
* Remote execution of Python code
* Streaming of periodically called functions with the ability to enable/disable them
* caching and queuing options for receiving data from `RouterClient`
* Multiple topologies for `RouterServer` and `RouterClient` communication
  * between processes on the same PC
  * across multiple PCs.
  * Multiple `RouterClient`s can connect to a single `RouterServer`

## Why build this?
The router pattern provides a bridge between distinct applications.

For our use cases we mostly apply this pattern to separate instrument control code from its respective GUI.

This separation provides several advantages:
* GUIs can be developed independently of standalone projects.
* GUIs can run on separate processes or on different machines altogether, providing more flexibility where some machines are resource-constrained.
* Failures are siloed. A GUI can crash independent of the application code it is interfacing with.

## Package Installation with uv

To install and develop this package run:
```bash
uv sync
```

To install all optional dependencies to play with the examples, run:
```bash
uv sync --extra examples
```

## Package Installation with Pip

To install this package, in the root directory, run
```bash
pip install .
```

To install in editable mode, in the root directory, run:
```bash
pip install -e .
```
To install with supplementary dependencies for running the examples, run:
```bash
pip install -e .[examples]
```

## Quickstart
### Remote Function Execution
TODO

### Streaming Data
TODO

### Handling Received Data
TODO: cache vs queue

### Controlling Data Streams
TODO

## Implementation Details

### Streamer
Streaming is done by aggregating all calls of the same frequency and creating one thread per frequency.
Streaming using threads simplifies the problem of scheduling when certain functions would be call using strategies like a priority queue.
Because sockets are explicitly _not_ threadsafe, each streamer thread instead creates its own PUB socket.
To simplify connections on the client side, streams are aggregated together through a single socket such that the client only needs to know one address.
Socket-to-socket communication is done using zmq's shared memory implementation (`inproc`).
Creating this proxy makes the system threadsafe.

<div align="center">
<img src="./assets/streamer_architecture.png" width="240px">
</div>

### Package/Project Management

This project utilizes [uv](https://docs.astral.sh/uv/) to handle installing dependencies as well as setting up environments for this project. It replaces tool like pip, poetry, virtualenv, and conda.

This project also uses [tox](https://tox.wiki/en/latest/index.html) for orchestrating multiple testing environments that mimics the github actions CI/CD so that you can test the workflows locally on your machine before pushing changes.

<!--

### Code Quality Check

The following are tools used to ensure code quality in this project.

- Unit Testing

```bash
uv run pytest tests
```

- Linting

```bash
uv run ruff check
```

- Type Check

```bash
uv run mypy src/mypackage
```

## Documentation
To generate the rst files source files for documentation, run
```bash
sphinx-apidoc -o docs/source/ src
```
Then to create the documentation HTML files, run
```bash
sphinx-build -b html docs/source/ docs/build/html
```
-->
