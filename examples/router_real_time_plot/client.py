import zmq
import matplotlib
matplotlib.use('QtAgg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from one_liner.client import RouterClient
from collections import deque
from server import SAMPLE_INTERVAL_S, NUM_STREAMS

import logging
logging.basicConfig(level=logging.DEBUG)

SAMPLES = 500
PLOT_INTERVAL_MS = 1/60. * 1000

if __name__ == "__main__":
    client = RouterClient()
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
                    data_pt = client.get_stream(f"1hz_sine[{i}]")
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
        # Note that visualization is relative to stream 0.
        ax.set_xlim(x_buffers[0][0], x_buffers[0][-1])
        return lines
    try:
        # Create the animation
        ani = animation.FuncAnimation(fig, animate, interval=PLOT_INTERVAL_MS, blit=True)
        plt.show()
    finally:
        client.close()
