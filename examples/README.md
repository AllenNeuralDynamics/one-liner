# one-liner Example Suite

Each of these examples has, at minimum,

* a client.py
* a server.py

Some examples also have a **ctrl_client.py** demonstrating how to enable/disable streams from the server.

To run these examples, make sure you have all the dependencies installed with:

```bash
uv sync --extra examples
```

## Running these examples

### on the same PC
Each of these examples is meant to run in a separate python process (i.e: 2 console windows or similar).
From the first window, launch **server.py** with `uv run server.py`.
Then launch **client.py** with `uv run client.py`.

### on different PCs
By changing the client address (`interface`) to the ip address of the PC running the server, it's possible to run these examples across two different PCs.
(Make sure the client PC can ping the PC running **server.py**.)

### with multiple clients on different pcs
Multiple clients (running on the same or different PCs) can also connect to a single server.
Like before, make sure the client PC(s) specify the server address with the `interface` argument, and make sure they can ping the PC running **server.py**.