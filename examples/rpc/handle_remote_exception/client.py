from one_liner.client import RouterClient

if __name__ == "__main__":
    client = RouterClient()
    try:
        client.call("my_horn", "malfunction")
    except Exception as e:
        print(f"calling function raised an exception in the response: {str(e)}")