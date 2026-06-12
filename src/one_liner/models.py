"""RouterServerAPI Pydantic Model for creating a RouterServer from a dict"""
from pydantic import BaseModel
from typing import Any, Optional


class PeriodicStream(BaseModel):
    frequency_hz: float
    obj_name: str
    attr_name: str

class NamedCall(BaseModel):
    obj_name: str
    attr_name: str
    args: Optional[list[Any]] = None
    kwargs: Optional[dict[str, Any]] = None

class RouterServerConfig(BaseModel):
    periodic_streams: dict[str, PeriodicStream] = {}
    named_calls: dict[str, NamedCall] = {}
