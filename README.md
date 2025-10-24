# one-liner
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)

<img src="./assets/train-conductor-mouse.png" width="160">

a ZMQ-based Router pattern for interacting with remote python objects.

## High level features:
* Remote execution of Python code
* Streaming of periodically called functions with the ability to enable/disable them
* caching and queuing options for receiving data from `RouterClient`
* Multiple topologies for `RouterServer` and `RouterClient` communication
  * between processes on the same PC
  * across multiple PCs.
  * Many `RouterClient`s can connect to a single `RouterServer`
  * `RouterServer`s can cascade: (i.e: a `RouterServer` can forward to another `RouterServer`)

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
There are three ways to stream data from a `RouterServer` to one or more `RouterClient` objects.

#### Periodic Broadcasting
In the PC acting as the server:
```python
server = RouterServer()
```

In the PC acting as the client:
```python
import cv2

video = cv2.VideoCapture(0)

def get_frame():
    return video.read()[1]

client = RouterClient()
client.configure_stream("live_video")
```

In the PC running **

#### Application-Controlled
TODO

#### from another zmq socket
TODO

### Handling Received Data
TODO: cache vs queue

### Controlling Data Streams
TODO

## Implementation Details

### Bird's Eye View
High level, the `RouterServer` and `RouterClient` support two ways of sending and receiving data remotely.

<div align="center">
<img src="./assets/one_liner_socket_architecture.png" width="240px">
</div>

It is also OK to connect multiple `RouterClient`s to a single `RouterServer` like so:
<div align="center">
<img src="./assets/many_sub_socket_architecture.png" width="550px">
</div>


> [!WARNING]
> There are no restrictions for connecting multiple clients at this level.
> Any restrictions or limitations on what functions can be called when multiple clients are connected needs to be applied at a higher level.


### Streamer
Streaming is done by aggregating all calls of the same frequency and creating one thread per frequency.
Streaming using threads simplifies the problem of scheduling when certain functions would be call using strategies like a priority queue.
Because sockets are explicitly _not_ threadsafe, each streamer thread instead creates its own PUB socket.
To simplify connections on the client side, streams are aggregated together through a single socket such that the client only needs to know one address.
Socket-to-socket communication is done using zmq's same-process shared memory implementation (`inproc`).
Creating this proxy makes the system threadsafe.

<div align="center">
<img src="./assets/streamer_architecture.png" width="240px">
</div>

### Relaying data from another ZMQ Socket

It's also possible to stream data from an existing zmq socket (including another `RouterServer`).
This is done with a zmq proxy.

<div align="center">
<img src="./assets/one_liner_relay_stream_architecture.png" width="300px">
</div>

By relaying data from a completely separate zmq socket, it is possible to cascade `RouterServer`s.

## Package/Project Management

This project utilizes [uv](https://docs.astral.sh/uv/) to handle installing dependencies as well as setting up environments for this project. It replaces tool like pip, poetry, virtualenv, and conda.

This project also uses [tox](https://tox.wiki/en/latest/index.html) for orchestrating multiple testing environments that mimics the github actions CI/CD so that you can test the workflows locally on your machine before pushing changes.

<!--
## Code Quality Check

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
