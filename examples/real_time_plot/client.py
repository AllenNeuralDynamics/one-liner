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

from server import SAMPLE_INTERVAL_S, NUM_STREAMS

SAMPLES = 500
PLOT_INTERVAL_S = 1/60.

if __name__ == "__main__":
    client = ZMQStreamClient()
    for i in range(NUM_STREAMS):
        client.configure_stream(f"1hz_sine[{i}]", storage_type="queue")

    # Create figure and axes
    fig, ax = plt.subplots()

    x_buffers = []
    y_buffers = []
    lines = []
    for i in range(NUM_STREAMS):
        x_buffers.append(deque(maxlen=SAMPLES))
        y_buffers.append(deque(maxlen=SAMPLES))

        line, = ax.plot([], [], 'r-') # Initialize an empty line
        lines.append(line)

    # Set axis limits (important for real-time plotting)
    ax.set_xlim(0, SAMPLES)
    ax.set_ylim(-1, 1)

    # Plot one stream.
    def animate(i):
        for i in range(NUM_STREAMS):
            while True:
                try:  # pull everything out.
                    data_pt = client.get(f"1hz_sine[{i}]")
                    if len(x_buffers[i]) and data_pt[0] - x_buffers[i][-1] >= 2*SAMPLE_INTERVAL_S:
                        print(f"1hz_sine[{i}] source_slow or receiver dropped "
                              f"packet! Δ = {data_pt[0] - x_buffers[i][-1]:.3f}[s]")
                    x_buffers[i].append(data_pt[0])
                    y_buffers[i].append(data_pt[1])
                except zmq.Again:
                    break
            # Update the line data
            lines[i].set_xdata(x_buffers[i])
            lines[i].set_ydata(y_buffers[i])
        ax.set_xlim(x_buffers[0][0], x_buffers[0][-1])
        return lines
    try:
        # Create the animation
        ani = animation.FuncAnimation(fig, animate, interval=PLOT_INTERVAL_S, blit=True)
        plt.show()
    finally:
        client.close()
