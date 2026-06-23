from pydantic import BaseModel
from typing import Optional


class RPC(BaseModel):
    instance: str
    params_schema: Optional[dict] = None
    return_schema: Optional[list] = None
