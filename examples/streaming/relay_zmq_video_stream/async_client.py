import  matplotlib.pyplot as plt
import asyncio
from one_liner.async_client import AsyncRouterClient as RouterClient
from server import VIDEO_FEED_NAME

import logging
logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":

    client = RouterClient()
    client.configure_stream(VIDEO_FEED_NAME, storage_type="cache")

    async def display_images():
        fig, ax = plt.subplots()
        # Get an initial image to figure out the size:
        _, frame = await client.get_stream(VIDEO_FEED_NAME)
        im = ax.imshow(frame)
        plt.show(block=False)
        while True:
            _, frame = await client.get_stream(VIDEO_FEED_NAME)
            im.set_data(frame)
            fig.canvas.draw_idle()
            fig.canvas.flush_events()

    asyncio.run(display_images())

