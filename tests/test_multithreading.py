from concurrent.futures import ThreadPoolExecutor
from one_liner.client import RouterClient
from one_liner.server import RouterServer
from threading import Lock
from time import sleep


class Counter:
    """Threadsafe counter."""
    __test__ = False

    def __init__(self):
        self._lock = Lock()
        self.count = 0

    def increment(self):
        with self._lock:
            count = self.count
            sleep(0.001)  # Widen the read-modify-write race window.
            self.count = count + 1
            return self.count

    def get_count(self):
        with self._lock:
            return self.count


def test_concurrent_client_threads_increment_server_counter():
    num_threads = 8
    increments_per_thread = 50
    server = RouterServer(instances={"counter": Counter()})
    client = RouterClient()
    server.run()

    def increment_many():
        return [client.call("counter", "increment")[-1]
                for _ in range(increments_per_thread)]

    try:
        with ThreadPoolExecutor(max_workers=num_threads) as pool:
            futures = [pool.submit(increment_many) for _ in range(num_threads)]
            # result() re-raises any exception from the client threads.
            returned_counts = [c for f in futures for c in f.result(timeout=30)]

        expected_total = num_threads * increments_per_thread
        # Every increment was applied exactly once, with no lost updates.
        assert sorted(returned_counts) == list(range(1, expected_total + 1))
        assert client.call("counter", "get_count")[-1] == expected_total
    finally:
        client.close()
        server.close()
