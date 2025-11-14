import sys
from instrument import InstrumentInfrastructureAPI, ProjectInfo, SubjectInfo

from one_liner.server import RouterServer

# import some_very_specific_dependency


class InfrastructureAdapter(InstrumentInfrastructureAPI):
    @staticmethod
    def start_new_session(user_id: str, subject_id: str) -> str:
        return f"session_{user_id}_{subject_id}"

    @staticmethod
    def get_project_info(project_id: str) -> ProjectInfo:
        # Query infrastructure database for project info
        project_info_from_database = {
            "name": "Example Project",
            "code": project_id,
            "storage": "s3://example-bucket/projects/" + project_id,
        }
        # Translate into language understood by the instrument
        project_info = ProjectInfo(
            project_id=project_info_from_database["code"],
            project_name=project_info_from_database["name"],
            project_data_storage=project_info_from_database["storage"],
        )
        return project_info

    @staticmethod
    def get_subject_info(subject_id: str) -> SubjectInfo:
        # Query infrastructure database for subject info
        subject_info_from_database = {
            "age": 30,
            "project_id": "example_project_id",
        }
        # Translate into language understood by the instrument
        subject_info = SubjectInfo(
            subject_id=subject_id,
            age=subject_info_from_database["age"],
            project_id=subject_info_from_database["project_id"],
        )
        return subject_info


if __name__ == "__main__":
    port = sys.argv[1] if len(sys.argv) > 1 else "5555"
    adapter = InfrastructureAdapter()
    server = RouterServer(devices={adapter.api_name: adapter}, rpc_port=port)
    server.run()
    try:
        import time

        while True:
            time.sleep(0.1)
    finally:
        server.close()
