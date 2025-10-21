import cv2
import zmq
from one_liner.client import RouterClient
from time import perf_counter as now
from server import VIDEO_FEED_NAME


import logging
logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":

    client = RouterClient()
    client.configure_stream(VIDEO_FEED_NAME, storage_type="cache")

    curr_time = now()
    last_time = curr_time
    try:
        while True:
            try:
                timestamp, frame = client.get_stream(VIDEO_FEED_NAME)
            except zmq.Again:
                continue
            curr_time = now()
            print(f"Execution time: {curr_time - last_time: .3f} seconds")
            last_time = curr_time
            cv2.imshow(VIDEO_FEED_NAME, frame)
            cv2.waitKey(1000) # Required short wait (1-ms).
    finally:
        cv2.destroyAllWindows()
        client.close()

