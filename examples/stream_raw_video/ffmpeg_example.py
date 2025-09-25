# On Linux:
#     sudo apt install v4l-utils

import ffmpeg

# From:
# https://github.com/kkroening/ffmpeg-python/blob/master/examples/read_frame_as_jpeg.py
def read_frame_as_jpeg(video_source="/dev/video0"):
    out, err = (
        ffmpeg
        .input(video_source, f="v4l2")  # f="dshow" on windows
        .output('pipe:', vframes=1, format='image2', vcodec='mjpeg')
        .run(capture_stdout=True)
    )
    return out



if __name__ == "__main__":

    out = read_frame_as_jpeg()
    from pathlib import Path
    file_path = Path("image.jpg")
    file_path.parent.mkdir(exist_ok=True, parents=True)
    file_path.write_bytes(out)

