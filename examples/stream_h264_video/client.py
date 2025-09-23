import cv2
import zmq
from one_liner.client import RouterClient
from time import perf_counter as now

from server import VIDEO_FEED_NAME

if __name__ == "__main__":

    client = RouterClient()
    client.configure_stream(VIDEO_FEED_NAME, storage_type="cache")
    try:
        while True:
            try:
                timestamp, frame = client.get_stream(VIDEO_FEED_NAME)
            except zmq.Again:
                continue
            print(f"Got frame at: {now()}")
            cv2.imshow(VIDEO_FEED_NAME, frame)
            cv2.waitKey(1) # Required short wait (1-ms).
    finally:
        cv2.destroyAllWindows()
        client.close()

