from typing import Protocol

from instrument_utils import get_connector


class InstrumentInfrastructureAPI(Protocol):
    @staticmethod
    def start_new_session(user_id: str, subject_id: str) -> str:
        pass

    @staticmethod
    def get_project_info(project_id: str) -> dict:
        pass

    @staticmethod
    def get_subject_info(subject_id: str) -> dict:
        pass


if __name__ == "__main__":
    connector = get_connector(InstrumentInfrastructureAPI, "infrastructure")
    print(connector.get_project_info("example_project_id"))
    print(connector.start_new_session("user123", "subject456"))
    print(connector.get_subject_info("subject456"))


## TODO: Add functionality to pass configurable `cmd_to_start_adapter_server`
# to the connector so that it auto-starts and restarts
# That way the instrument isn't dependent on another separate process
