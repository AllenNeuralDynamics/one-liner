"""This is an example pattern"""

from pathlib import Path
from typing import Protocol
from pydantic import BaseModel

from instrument_utils import (
    ZmqRPCConnector,
    ZmqRPCConnectorMixin,
    get_connector,
)

### Define the models and protocol that the instrument will use to communicate
### Maybe this is in an api.py or something


class ProjectInfo(BaseModel):
    project_id: str
    project_name: str
    project_data_storage: str


class SubjectInfo(BaseModel):
    subject_id: str
    age: int
    project_id: str


class InstrumentInfrastructureAPI(Protocol):
    api_name: str = "infrastructure"

    @staticmethod
    def start_new_session(user_id: str, subject_id: str) -> str:
        pass

    @staticmethod
    def get_project_info(project_id: str) -> ProjectInfo:
        pass

    @staticmethod
    def get_subject_info(subject_id: str) -> SubjectInfo:
        pass


###


class InstrumentConfig(BaseModel):
    infra_adapter_port: str = "5678"
    infra_adapter_cmd: list[str] = [
        "uv",
        "run",
        str(Path(__file__).parent / "adapter.py"),
        "5678",  # run the adapter on the same port
    ]


if __name__ == "__main__":
    config = InstrumentConfig()

    # Option 1: Using a mixin class and requiring the user to define InsfrastructureConnector
    class InfrastructureConnector(
        ZmqRPCConnectorMixin, InstrumentInfrastructureAPI
    ): ...

    connector1 = InfrastructureConnector(
        port=config.infra_adapter_port,
        cmd_to_start_adapter_server=config.infra_adapter_cmd,
    )

    # Option 2: A parametrizable connector class that overrides __new__
    connector2 = ZmqRPCConnector(
        protocol=InstrumentInfrastructureAPI,
        port=config.infra_adapter_port,
        cmd_to_start_adapter_server=config.infra_adapter_cmd,
    )

    # Option 3: A helper function that dynamically creates a subclass of the API
    connector3 = get_connector(
        InstrumentInfrastructureAPI,
        port=config.infra_adapter_port,
        target_obj_name=InstrumentInfrastructureAPI.api_name,
        cmd_to_start_adapter_server=config.infra_adapter_cmd,
    )

    # Using connector methods has type hints and autocompletion
    print(connector1.get_project_info("example_project_id"))
    print(connector2.start_new_session("user123", "subject456"))
    print(connector3.get_subject_info("subject456"))
