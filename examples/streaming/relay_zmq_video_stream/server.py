import cv2
import zmq
from one_liner.server import RouterServer

import logging
logging.basicConfig(level=logging.DEBUG)


VIDEO_SOURCE = 0 #"/dev/video4"
VIDEO_FEED_NAME = "live_video"


if __name__ == "__main__":
    context = zmq.Context()     # For sending data via inproc, RouterServers
                                # must share the same zmq context.
    # Create an internal server for distributing camera data.
    internal_server = RouterServer(protocol="inproc",
                                   interface="video",
                                   rpc_port="",  # unused, but must be distinct from default option.
                                   broadcast_port="5558",
                                   context=context)
    # Create broadcast function for sending video.
    broadcast_live_video = internal_server.get_stream_fn(VIDEO_FEED_NAME)
    internal_server.run()  # Start rpc thread.

    # Create a RouterServer to connect to GUI clients.
    server = RouterServer(context=context)
    # Forward the broadcast from the internal RouterServer to the Client-facing one
    server.streamer.add_zmq_stream(VIDEO_FEED_NAME,
                                   address=f"inproc://video:5558",
                                   enabled=True)
    server.run()


    try:
        # Start video stream.
        video = cv2.VideoCapture(VIDEO_SOURCE)

        while True:  # Send video data.
            #print(f"Sending data on topic: {VIDEO_FEED_NAME}")
            broadcast_live_video(video.read()[1])
    finally:
        server.close()
        internal_server.close()
