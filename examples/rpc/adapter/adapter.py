from instrument import InstrumentInfrastructureAPI

from one_liner.server import RouterServer

class InfrastructureAdapter(InstrumentInfrastructureAPI):

    @staticmethod
    def start_new_session(user_id: str, subject_id: str) -> str:
        return f"session_{user_id}_{subject_id}"

    @staticmethod
    def get_project_info(project_id: str) -> dict:
        return {"project_id": project_id, "name": "Example Project"}

    @staticmethod
    def get_subject_info(subject_id: str) -> dict:
        return {"subject_id": subject_id, "age": 30}
    
if __name__ == "__main__":
    adapter = InfrastructureAdapter()
    server = RouterServer(devices={"infrastructure": adapter})
    server.run()
    try:
        import time
        while True:
            time.sleep(0.1)
    finally:
        server.close()