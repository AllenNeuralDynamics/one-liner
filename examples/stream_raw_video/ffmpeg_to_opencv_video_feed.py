# On Linux:
#     sudo apt install v4l-utils

import cv2
import ffmpeg
import platform
import numpy as np
from time import perf_counter as now
import pickle

# Adapted from:
# https://kkroening.github.io/ffmpeg-python/

VIDEO_SOURCE = "/dev/video0"
OUTPUT_FILE = "video.mp4"

WIDTH = 640
HEIGHT = 480
FPS = 30

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

    start_time = now()
    try:
        while True:
            in_bytes = stream_process.stdout.read(WIDTH * HEIGHT * 3)
            if not in_bytes:
                continue
            # Capture frame
            frame = np.frombuffer(in_bytes, np.uint8).reshape([HEIGHT, WIDTH, 3])
            cv2.imshow('Real-time Video', frame)
            cv2.waitKey(1) # 1-ms wait.
    finally:
        stream_process.terminate()
        cv2.destroyAllWindows()

