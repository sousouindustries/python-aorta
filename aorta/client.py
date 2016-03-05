import importlib
import inspect

from aorta.connection import BaseConnection
from aorta.listener import Listener
from aorta.publisher import Publisher


class Aorta:

    def __init__(self, host, port, connection_class=None):
        self.host = host
        self.port = port
        self.listeners = {}

        if connection_class is None:
            raise TypeError("The connection_class argument is required.")
        self._setup_connection(connection_class)

    def create_publisher(self, url):
        pass

    def create_listener(self, channel, block=False):
        """Create a new event listener.

        Args:
            channel: the channel to listen for messages on.
            block: a boolean indicating if control must block until
                the listener has succesfully connected.

        Returns:
            aorta.listener.Listener
        """
        listener = self.listeners.get(channel)
        if listener is None:
            listener = Listener(self, channel)
            self.listeners[channel] = listener
            listener.start()

        if block:
            listener.wait(listener.EVNT_CONNECTED)


    def _setup_connection(self, connection_class):
        if isinstance(connection_class, str):
            try:
                module_name, class_name = connection_class.rsplit('.', 1)
            except ValueError:
                raise ValueError(
                    "Please specify a connection class or a dotted-path.")
            else:
                module = importlib.import_module(module_name)
                try:
                    connection_class = getattr(module, class_name)
                    if not inspect.isclass(connection_class):
                        raise TypeError(
                            class_name + "must inherit from aorta.connection.BaseConnection")
                except AttributeError:
                    raise ImportError("No such class: " + class_name)

        assert inspect.isclass(connection_class)
        assert issubclass(connection_class, BaseConnection)
        self.connection = connection_class(self.host, self.port)
