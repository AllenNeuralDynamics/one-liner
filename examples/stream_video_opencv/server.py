import cv2
import numpy as np
from one_liner.server import RouterServer
from time import sleep


VIDEO_SOURCE = 0 #"/dev/video4"
FPS = 30
VIDEO_FEED_NAME = "live_video"


if __name__ == "__main__":

    # Start video stream.
    video = cv2.VideoCapture(VIDEO_SOURCE)

    def get_frame():
        return video.read()[1]

    server = RouterServer()
    # We need to call this faster than FPS, or it will clog up.
    server.add_broadcast(VIDEO_FEED_NAME, FPS, get_frame)
    server.run()  # Start rpc thread.

    try:
        while True:  # Do do nothing.
            sleep(0.1)
    finally:
        server.close()
