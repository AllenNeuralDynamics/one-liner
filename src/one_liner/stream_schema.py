from one_liner.utils import Encoding
from pydantic import BaseModel, computed_field, field_serializer, model_serializer, AfterValidator, Field
from pydantic import ValidationError
from typing import Optional


class Stream(BaseModel):
    encoding: Encoding


class PeriodicStream(Stream):
    return_type: str | None  # None indicates unspecified whereas "None" means it returns None
    frequency_hz: float
    enabled: bool


class Streams(BaseModel):
    manual_streams: Optional[dict[str, Stream]] = Field(default_factory=dict)
    zmq_streams: Optional[dict[str, Stream]] = Field(default_factory=dict)
    periodic_streams: Optional[dict[str, PeriodicStream]] = Field(default_factory=dict)

    # Maybe we do a calls-by-frequency getter fn?