from typing import Optional

from pydantic import (
    BaseModel,
    Field,
)

from one_liner.utils import Encoding


class SocketMetadata(BaseModel):
    params_schema: Optional[dict] = None
    return_schema: Optional[dict] = None
    description: Optional[str] = None


class RPC(SocketMetadata):
    instance: str


class Stream(SocketMetadata):
    encoding: Encoding


class PeriodicStream(Stream):
    frequency_hz: float
    enabled: bool


class Streams(BaseModel):
    manual_streams: Optional[dict[str, Stream]] = Field(default_factory=dict)
    zmq_streams: Optional[dict[str, Stream]] = Field(default_factory=dict)
    periodic_streams: Optional[dict[str, PeriodicStream]] = Field(default_factory=dict)
