# Time-Series Livestream Plot

Make sure you have optional dependencies installed first with:
```bash
uv sync --extra examples
```

First run `server.py`.
Then, in another Python instance, run `client.py`.


Some notes:
* This example is divided into 2 processes to reduce overhead plotting the data while trying to sample it at a consistent rate.
