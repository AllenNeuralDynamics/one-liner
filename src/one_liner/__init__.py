__version__ = "0.1.0"
from typing import Literal

Protocol = Literal["tcp", "inproc", "ipc", "ws", "wss"]
Encoding = Literal[None, "pickle", "json"]
