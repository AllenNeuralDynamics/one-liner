import zmq
import matplotlib
matplotlib.use('QtAgg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from one_liner.client import RouterClient
from collections import deque

SAMPLES = 500
PLOT_INTERVAL_MS = 1/60. * 1000

if __name__ == "__main__":
    client = RouterClient()
    client.configure_stream(f"1hz_sine(t)", storage_type="queue")

    # Create figure and axes
    fig, ax = plt.subplots()

    x_buffer = deque(maxlen=SAMPLES)
    y_buffer = deque(maxlen=SAMPLES)

    line, = ax.plot([], [], 'r-')  # Initialize an empty line

    # Set starting axis limits
    ax.set_xlim(0, SAMPLES)
    ax.set_ylim(-1, 1)

    # Plot one stream.
    def animate(i):
        while True:
            try:  # pull everything out.
                data_pt = client.get_stream(f"1hz_sine(t)")
                x_buffer.append(data_pt[0])
                y_buffer.append(data_pt[1])
            except zmq.Again:
                break
        # Update the line data
        line.set_xdata(x_buffer)
        line.set_ydata(y_buffer)
        # Note that visualization is relative to stream 0.
        ax.set_xlim(x_buffer[0], x_buffer[-1])
        return line,
    try:
        # Create the animation
        ani = animation.FuncAnimation(fig, animate, interval=PLOT_INTERVAL_MS, blit=True)
        plt.show()
    finally:
        client.close()