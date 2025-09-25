# On Linux:
#     sudo apt install v4l-utils

import ffmpeg
import platform
import numpy as np
from one_liner.server import RouterServer
from time import perf_counter as now
from time import sleep

# Adapted from:
# https://kkroening.github.io/ffmpeg-python/

VIDEO_SOURCE = "/dev/video4"

WIDTH = 640
HEIGHT = 480
FPS = 30

VIDEO_FEED_NAME = "live_video"

os_name = platform.system()
input_lib = "dshow" if os_name == "Windows" else "v4l2"


if __name__ == "__main__":


    # Start video stream.
    stream_process = (
        ffmpeg
        .input(VIDEO_SOURCE, f=input_lib, s=f"{WIDTH}x{HEIGHT}", framerate=FPS)
        .output('pipe:', format='rawvideo', pix_fmt='bgr24')
        .run_async(pipe_stdout=True)
    )

    def get_frame():
        while True:  # block until we get a frame.
            in_bytes = stream_process.stdout.read(WIDTH * HEIGHT * 3)
            if not in_bytes:
                continue
            return np.frombuffer(in_bytes, np.uint8).reshape([HEIGHT, WIDTH, 3])


    server = RouterServer()
    # We need to call this faster than FPS, or it will clog up.
    server.add_broadcast(VIDEO_FEED_NAME, FPS*2, get_frame)
    server.run()  # Start rpc thread.

    try:
        while True:  # Do do nothing.
            sleep(0.1)
    finally:
        stream_process.terminate()
        server.close()
