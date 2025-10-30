import queue
import zmq
import matplotlib
matplotlib.use('QtAgg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.colors as mcolors
import numpy as np

from one_liner.client import RouterClient
from collections import deque

from time import perf_counter, sleep
from math import sin, pi

from server import NUM_STREAMS, SAMPLE_RATES_HZ
SAMPLE_INTERVALS_S = [1.0/i for i in SAMPLE_RATES_HZ]

PLOT_INTERVAL_MS = 1/60. * 1000

if __name__ == "__main__":
    client = RouterClient()
    for i in range(NUM_STREAMS):
        client.configure_stream(f"1hz_sine[{i}]", storage_type="queue")

    # Create figure and axes
    fig, axes = plt.subplots(NUM_STREAMS, 1, figsize=(4, 8))

    x_buffers = []
    y_buffers = []
    lines = []
    # generate random color options
    color_options = list(mcolors.CSS4_COLORS.keys())
    # Buffer the last 1-second worth of data for each stream.
    for i in range(NUM_STREAMS):
        x_buffers.append(deque(maxlen=SAMPLE_RATES_HZ[i]))
        y_buffers.append(deque(maxlen=SAMPLE_RATES_HZ[i]))
        color = np.random.choice(color_options, replace=False)
        line, = axes[i].plot([], [], color=f'{color}', linestyle='-') # empty line
        lines.append(line)
        # Set axis limits for 1-second worth of data for each stream.
        axes[i].set_xlim(0, SAMPLE_RATES_HZ[i])
        axes[i].set_ylim(-1, 1)

    # Plot one stream.
    def animate(frame):
        for i in range(NUM_STREAMS):
            while True:
                try:  # pull everything out.
                    data_pt = client.get_stream(f"1hz_sine[{i}]")
                    if len(x_buffers[i]) and data_pt[0] - x_buffers[i][-1] >= 2*SAMPLE_INTERVALS_S[i]:
                        print(f"1hz_sine[{i}] source_slow or receiver dropped "
                              f"packet! Δ = {data_pt[0] - x_buffers[i][-1]:.3f}[s]")
                    x_buffers[i].append(data_pt[0])
                    y_buffers[i].append(data_pt[1])
                except zmq.Again:
                    break
            # Update the line data
            lines[i].set_data(x_buffers[i], y_buffers[i])
            # adjust xlimits to see the current data for each stream.
            axes[i].set_xlim(x_buffers[i][0], x_buffers[i][-1])
        return lines
    try:
        # Create the animation
        ani = animation.FuncAnimation(fig, animate, interval=PLOT_INTERVAL_MS, blit=True)
        plt.show()
    finally:
        client.close()
