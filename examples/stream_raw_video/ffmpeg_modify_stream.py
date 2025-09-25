# On Linux:
#     sudo apt install v4l-utils

import ffmpeg
import platform
import numpy as np
from one_liner.server import RouterServer
from time import perf_counter as now

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

    stream_process = (
        ffmpeg
        .input(VIDEO_SOURCE, f=input_lib, s=f"{WIDTH}x{HEIGHT}", framerate=FPS)
        #.input(VIDEO_SOURCE, f=input_lib, s=f"{WIDTH}x{HEIGHT}", framerate=FPS)
        .output('pipe:', format='h264', pix_fmt='bgr24')
        .run_async(pipe_stdout=True)
    )

    recv_process = (
        ffmpeg
        .input('pipe:', format='rawvideo', pix_fmt='bgr24', s=f'{WIDTH}x{HEIGHT}', framerate=FPS)
        #.input('pipe:', format='h264', pix_fmt='bgr24', s=f'{WIDTH}x{HEIGHT}', framerate=FPS)
        .output(OUTPUT_FILE, pix_fmt='yuv420p')
        .overwrite_output()
        .run_async(pipe_stdin=True)
    )

    print(f"streaming process is: {stream_process.pid}")
    print(f"recv_process is: {recv_process.pid}")
    start_time = now()
    try:
        while now() - start_time < 2:
            in_bytes = stream_process.stdout.read(WIDTH * HEIGHT * 3)
            if not in_bytes:
                continue
            # Capture frame
            in_frame = np.frombuffer(in_bytes, np.uint8).reshape([HEIGHT, WIDTH, 3])
            out_frame = in_frame# * 0.3
            recv_process.stdin.write(out_frame.astype(np.uint8).tobytes())
    finally:
        stream_process.terminate()
        recv_process.terminate()
