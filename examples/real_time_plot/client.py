import queue
import zmq
import matplotlib
matplotlib.use('QtAgg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np

from one_liner.client import ZMQStreamClient
from collections import deque

from random import random
from time import perf_counter, sleep
from math import sin, pi

from server import SAMPLE_INTERVAL_S

SAMPLES = 500
PLOT_INTERVAL_S = 1/60.

if __name__ == "__main__":
    client = ZMQStreamClient()
    client.configure_stream("1hz_sine", storage_type="queue")

    x_buffer = deque(maxlen=SAMPLES)
    buffer = deque(maxlen=SAMPLES)

    # Create figure and axes
    fig, ax = plt.subplots()
    line, = ax.plot([], [], 'r-') # Initialize an empty line

    # Set axis limits (important for real-time plotting)
    ax.set_xlim(0, SAMPLES)
    ax.set_ylim(-1, 1)

    # Plot one stream.
    def animate(i):
        while True:
            try:  # pull everything out.
                data_pt = client.get("1hz_sine")
                if len(x_buffer) and data_pt[0] - x_buffer[-1] >= 2*SAMPLE_INTERVAL_S:
                    print(f"dropped packet! delta = {data_pt[0] - x_buffer[-1]:.3f}")
                x_buffer.append(data_pt[0])
                buffer.append(data_pt[1])
            except zmq.Again:
                if len(buffer):
                    break
                else:
                    continue
        # Update the line data
        #line.set_xdata(range(len(buffer)))
        line.set_xdata(x_buffer)
        line.set_ydata(buffer)
        ax.set_xlim(x_buffer[0], x_buffer[-1])
        return line,

    try:
        # Create the animation
        ani = animation.FuncAnimation(fig, animate, interval=PLOT_INTERVAL_S, blit=True)
        plt.show()
    finally:
        client.close()
