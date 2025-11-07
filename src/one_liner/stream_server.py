import logging
import zmq
from one_liner.stream_schema import Stream, PeriodicStream, Streams
from one_liner.utils import Encoding, Protocol, _send
from threading import Event, Lock, Thread
from time import sleep
from typing import Callable, get_type_hints


class ZMQStreamServer:
    """Broadcaster for periodically calling a callable with specific args/kwargs
       at a specified frequency."""
    __slots__ = ("log", "port", "_context_managed_externally", "_worker_url",
                 "_context", "_xsub_socket", "_xpub_socket",
                 "_zmq_streams", "_zmq_stream_ctrl_sockets",
                 "_call_signature", "_call_encodings", "_call_enabled",
                 "_calls_by_frequency", "_call_frequencies",
                 "_locks_by_frequency", "_threads", "_manual_broadcast_sockets",
                 "_manual_broadcast_encodings", "_keep_broadcasting",
                 "_proxy_thread", "_is_running")

    def __init__(self, protocol: Protocol = "tcp", interface: str = "*",
                 port: str = "5556", context: zmq.Context = None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.port: str = port
        self._context: zmq.Context = context or zmq.Context()
        self._context_managed_externally = context is not None
        # zmq sockets are not thread-safe!
        # To publish from multiple threads (one per publish frequency) in a
        # thread-safe way, we need to create a socket per-thread and send
        # published data through a zmq proxy via internal same-process
        # communication (zmq's "inproc"). The result is thread-safe by design
        # without needing to use any Python semantics (locks, etc).
        self._worker_url = f"inproc://workers:{id(self)}"  # unique per instance
        self._xsub_socket = self._context.socket(zmq.XSUB)
        self._xsub_socket.setsockopt(zmq.LINGER, 0) # Close as soon as the context terminates.
        self._xsub_socket.bind(self._worker_url)
        self._xpub_socket = self._context.socket(zmq.XPUB)
        self._xpub_socket.setsockopt(zmq.LINGER, 0)
        pub_address = f"{protocol}://{interface}:{self.port}"
        self.log.warning(f"Publishing from: {pub_address}")
        self._xpub_socket.bind(pub_address)
        self.log.debug(f"Creating zmq proxy to forward messages from: "
                       f"{self._worker_url} to {pub_address}.")
        # zmq stream proxy threads and control sockets
        self._zmq_streams = {}
        self._zmq_stream_ctrl_sockets = {}
        # Periodic stream data structures
        self._call_signature: dict[str, tuple] = {}
        self._call_encodings: dict[str, Encoding | Callable] = {}
        self._call_enabled: dict[str, bool] = {}
        self._calls_by_frequency: dict[float, set] = {}
        self._call_frequencies: dict[str, float] = {}
        self._locks_by_frequency: dict[float, Lock] = {}
        self._threads: dict[float, Thread] = {}
        self._manual_broadcast_sockets: dict[str, zmq.Context.socket] = {}
        self._manual_broadcast_encodings: dict[str, Encoding] = {}

        self._keep_broadcasting: Event = Event()
        self._keep_broadcasting.set()
        self._proxy_thread: Thread | None = None
        self._is_running: bool = False

    def run(self, run_in_thread: bool = True):
        """Launch proxy to handle multiple threads publishing.

        :param run_in_thread:  if True (default) run the zmq proxy in a separate
           thread to prevent blocking.

        """
        if self._is_running:  # extra flag in case we put `run` in a thread elsewhere.
            raise RuntimeError("Streamer is already running!")
        if not run_in_thread:
            self._is_running = True
            zmq.proxy(self._xpub_socket, self._xsub_socket)  # Does not return.
        else:  # If we need to return
            self._proxy_thread = Thread(target=self.run, args=[False],
                                        daemon=True)
            self._proxy_thread.start()

    def add(self, name: str, frequency_hz: float, func: Callable,
            args: list = None, kwargs: dict = None, enabled: bool = True,
            serializer: Encoding | Callable = "pickle"):
        """Create a stream. i.e: Setup a function to be called with specific
        arguments at a set frequency. If the function is already being
        broadcasted, update the broadcast parameters.

        :param name:
        :param frequency_hz:
        :param func:
        :param args:
        :param kwargs:
        :param enabled:
        :param serializer:

        """
        # Add/update func params and call frequency.
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        self._call_signature[name] = (func, args, kwargs)
        self._call_frequencies[name] = frequency_hz
        self._call_encodings[name] = serializer
        if enabled:
            self.enable(name)
        # Store call by frequency.
        call_names = self._calls_by_frequency.get(frequency_hz, set())
        self.log.debug(f"Adding stream: {name} @ {frequency_hz}[Hz].")
        if not call_names:  # Add to dict if nothing broadcasts at this freq.
            self._calls_by_frequency[frequency_hz] = call_names
            self._locks_by_frequency[frequency_hz] = Lock()  # Create a new lock
        with self._locks_by_frequency[frequency_hz]:
            call_names.add(name)
        if frequency_hz in self._threads:  # Thread already exists.
            return
        # Create a new thread for calls made at this frequency.
        broadcast_thread = Thread(target=self._stream_worker,
                                  name=f"{frequency_hz:.3f}[Hz]_broadcast_thread",
                                  args=[frequency_hz], daemon=True)
        self.log.debug(f"Launching new thread '{broadcast_thread.name}' to "
                       f"broadcast function call results at {frequency_hz}[Hz].")
        broadcast_thread.start()
        self._threads[frequency_hz] = broadcast_thread

    def get_stream_fn(self, name: str, set_timestamp: bool = False,
                      serializer: Encoding | Callable = "pickle") -> Callable:
        """Get a function to broadcast the specified stream name.
        Useful if the application is creating data at its
        own rate and needs a callback function to call upon producing new data.

        This implicitly adds a manual stream to the configuration.

        :param name: stream name
        :param serializer: how to encode the data.
        :param set_timestamp: if true, return a function who's first argument is
            the timestamp to be set for the packet.

        """
        if name not in self._manual_broadcast_sockets:
            self.log.debug(f"Creating socket to manually broadcast stream: "
                           f"{name} to {self._worker_url}.")
            socket = self._context.socket(zmq.PUB)
            socket.setsockopt(zmq.LINGER, 0)
            socket.connect(self._worker_url)
            self._manual_broadcast_sockets[name] = socket
        socket = self._manual_broadcast_sockets[name]
        self._manual_broadcast_encodings[name] = serializer
        tstamp_option = "manual" if set_timestamp else "autogenerated"
        msg = f"Creating send function for stream {name} with {tstamp_option} timestamp."
        self.log.debug(msg)
        if set_timestamp:
            return lambda data, timestamp, s=socket, n=name, success=True, e=serializer: \
                _send(s, n, data, timestamp=timestamp, success=success, serializer=e)
        return lambda data, s=socket, n=name, success=True, e=serializer: \
            _send(s, n, data, success=success, serializer=e)

    def add_zmq_stream(self, name: str, address: str, enabled: bool = True,
                       log_chatter: bool = False):
        """ Add a stream from an existing zmq PUB socket source.

        :param name: stream name
        :param address: zmq socket address: `{protocol}://{interface}:{port}`
        :param enabled: if True (default) enable relaying of the upstream
           zmq topic data.
        :param log_chatter:

        """
        self.log.debug(f"Creating zmq proxy to forward messages from: "
                       f"{address} to {self._worker_url}")
        # Create XSUB and XPUB relay
        xsub_socket = self._context.socket(zmq.XSUB)
        xsub_socket.setsockopt(zmq.LINGER, 0)
        xsub_socket.connect(address)

        xpub_socket = self._context.socket(zmq.XPUB)
        xpub_socket.setsockopt(zmq.LINGER, 0)
        xpub_socket.connect(self._worker_url)

        capture_socket = None
        capture_socket_address = f"inproc://{name}_cap_socket_debug"
        if log_chatter:
            capture_socket = self._context.socket(zmq.PUB)
            capture_socket.setsockopt(zmq.LINGER, 0)
            capture_socket.bind(capture_socket_address)

        ctrl_socket = self._context.socket(zmq.PAIR)
        ctrl_socket.setsockopt(zmq.LINGER, 0)
        ctrl_socket_address = f"inproc://{name}_stream_proxy_control"
        ctrl_socket.bind(ctrl_socket_address)
        # Create a steerable zmq proxy and run it in a separate daemon thread.
        self.log.warning("Creating socket")
        self._zmq_streams[name] = Thread(target=zmq.proxy_steerable,
                                         args=[xsub_socket, xpub_socket],
                                         kwargs={"capture": capture_socket,
                                                "control": ctrl_socket},
                                         daemon=True)
        self._zmq_streams[name].start()
        ext_ctrl_socket = self._context.socket(zmq.PAIR)
        ext_ctrl_socket.connect(ctrl_socket_address)
        self._zmq_stream_ctrl_sockets[name] = ext_ctrl_socket
        # pause if not enabled. (We can't start paused, so approximate.)
        if not enabled:
            self.log.warning("Pausing stream: {name}.")
            self._zmq_stream_ctrl_sockets[name].send_string("PAUSE")
        if not log_chatter:
            return

        # To sniff zmq data in a proxy we need to create another socket.
        def log_socket_chatter(self):
            """Capture data from the relayed zmq stream and log it (unless it
            is "large"). Useful for diagnosing issues."""
            cap_sub_socket = self._context.socket(zmq.SUB)
            cap_sub_socket.subscribe("")
            cap_sub_socket.connect(capture_socket_address)
            while True:
                data = cap_sub_socket.recv()
                msg = "capture got data: " + \
                      (f"{data}" if len(data) < 128 else "(too large to display).")
                self.log.debug(msg)

        Thread(target=log_socket_chatter, args=[self], daemon=True).start()

    def remove(self, name: str):
        """Remove a broadcasting function call that was previously added."""
        if name in self._zmq_streams:
            # Delete zmq proxy stream related data.
            self._zmq_stream_ctrl_sockets[name].send_string("TERMINATE")
            del self._zmq_streams[name]
            del self._zmq_stream_ctrl_sockets[name]
            return
        if name in self._call_frequencies:
            # Delete periodic-stream-related data.
            call_frequency = self._call_frequencies[name]
            with self._locks_by_frequency[call_frequency]:
                self._calls_by_frequency[call_frequency].remove(name)
                del self._call_signature[name]
                del self._call_frequencies[name]
                del self._call_enabled[name]
                # Broadcast thread for this frequency will exit if it has
                # nothing to do and delete the lock.
                return
        # Raise an error if we got here and didn't (or can't) remove anything.
        if name in self._manual_broadcast_sockets:
            raise ValueError(f"Cannot remove stream: {name}. Stream is "
                             f"called manually by the application.")
        raise ValueError(f"Cannot remove non-existent stream: {name}. ")

    def _stream_worker(self, frequency_hz: float, ):
        """Periodically broadcast all functions at the specified frequency.
        If there's nothing to do, exit."""
        socket = self._context.socket(zmq.PUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(self._worker_url)
        sleep_interval_s = 1.0/frequency_hz
        try:
            while self._keep_broadcasting.is_set():
                # Prevent size change in self.calls_by_frequency during iteration.
                with self._locks_by_frequency[frequency_hz]:
                    if not self._calls_by_frequency[frequency_hz]: # exit.
                        break
                    for stream_name in self._calls_by_frequency[frequency_hz]:
                        if stream_name not in self._call_enabled:
                            continue
                        # Invoke the function and dispatch the result.
                        func, args, kwargs = self._call_signature[stream_name]
                        try:
                            success = True
                            result = func(*args, **kwargs)
                        except Exception as e:
                            self.log.error(f"For stream: {stream_name}, "
                                           f"func: {func.__name__}("
                                           f"{', '.join([str(a) for a in args])}"
                                           f"{', ' if (len(args) and len(kwargs)) else ''}"
                                           f"{', '.join([str(k) + '=' + str(v) for k, v in kwargs.items()])}) "
                                           f"raised an exception while executing: {str(e)}")
                            success = False
                            result = e
                        _send(socket, name=stream_name, data=result, success=success,
                              serializer=self._call_encodings[stream_name])
                sleep(sleep_interval_s)
        finally:
            if self._calls_by_frequency[frequency_hz]:
                del self._locks_by_frequency[frequency_hz]
            socket.close()

    def enable(self, name: str):
        """Enable broadcasting of a stream by name. Any connected
        :py:class:`~one_liner.client.RouterClient` will start receiving data
        from this stream after they have configured how to buffer the stream
        data with :py:meth:`~one_liner.client.RouterClient.configure_stream`.

        :param name: stream name
        :raises KeyError: if the stream name does not exist.
        :raises ValueError: if the stream exists but cannot be enabled/disabled.

        .. note::
           Enabling streams only works for periodically-added streams
           added with :py:meth:`~one_liner.server.RouterServer.add_stream` and
           :py:meth:`~one_liner.server.RouterServer.add_zmq_stream` but
           *not* :py:meth:`~one_liner.server.RouterServer.get_stream_fn`.

        """

        # Enable zmq stream
        if name in self._zmq_streams:
            self._zmq_stream_ctrl_sockets[name].send_string("RESUME")
            return
        # Enable manual stream
        if name in self._call_signature:
            self._call_enabled[name] = True
            return
        # Error if we got here
        if name in self._manual_broadcast_sockets:
            raise ValueError(f"Stream: {name} cannot be enabled or disabled.")
        raise KeyError(f"Stream: {name} does not exist.")

    def disable(self, name: str):
        """Disable a previously-configured stream by name.

        :param name: the stream name

        """
        # FIXME: handle other edge cases like in enable.
        del self._call_enabled[name]

    def _get_return_type(self, name: str) -> str | None:
        """Get the return type of the stream name if it was type-hinted or
        None if it was not type hinted."""
        try:
            return get_type_hints(self._call_signature[name][0])["return"].__name__
        except KeyError:
            return None

    def get_configuration(self, as_dict: bool = True) -> Streams | dict:
        """
        Get a breakdown of every stream with its broadcast settings.
        """
        manual_streams = {n: Stream(encoding=str(self._manual_broadcast_encodings[n]))
                          for n in self._manual_broadcast_sockets.keys()}
        periodic_streams = {n: PeriodicStream(encoding=str(self._call_encodings[n]),
                                              return_type=self._get_return_type(n),
                                              frequency_hz=self._call_frequencies[n],
                                              enabled=self._call_enabled[n])
                            for n in self._call_signature.keys()}
        # FIXME: how do we get the encoding for received arbitrary zmq data?
        zmq_streams = {n: Stream(encoding="unspecified") for n in self._zmq_streams}
        streams = Streams(manual_streams=manual_streams,
                          periodic_streams=periodic_streams,
                          zmq_streams=zmq_streams)
        return streams.model_dump() if as_dict else streams

    def close(self):
        """Exit all threads gracefully."""
        self._keep_broadcasting.clear()
        for thread in self._threads.values():
            thread.join()
        for socket in self._manual_broadcast_sockets.values():
            socket.close()
        for socket in self._zmq_stream_ctrl_sockets.values():
            socket.send_string("TERMINATE")
        if not self._context_managed_externally:
            self._context.term()
