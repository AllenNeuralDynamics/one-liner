# Multi-Stream Frequency Time-Series Livestream Plot

This example demonstrates sampling a 1Hz sine wave at different rates, forcing the ZMQStreamer internally to designate a unique thread per streaming rate.

Make sure you have optional dependencies installed first with:
```bash
uv sync --extra examples
```

First run `server.py`.
Then, in another Python instance, run `client.py`.


