from one_liner.client import RouterClient


if __name__ == "__main__":
    print("Creating client.")
    client = RouterClient(name="client1")
    client.get_write_token()
    print(f"client1 has write token: {client.has_write_token()}")
    print("Creating second client")
    client2 = RouterClient(name="client2")
    print(f"client2 has write token: {client2.has_write_token()}")

    print("Try to get the write token as client2.")
    try:
        client2.get_write_token(timeout_s=0)
    except PermissionError:
        print("Failed to get the write token as client2.")

    print("Force-getting the write token as client2.")
    client2.get_write_token(force=True)
    print(f"client1 has write token: {client.has_write_token()}")
    print(f"client2 has write token: {client2.has_write_token()}")

    print("Closing all clients. Client2 will also release the token.")
    client.close()
    client2.close()
