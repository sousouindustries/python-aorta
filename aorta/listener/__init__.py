import queue
import threading

from aorta.backends import load
from aorta.backends.base import BaseMessagingBackend


class Listener:
    """The :class:`Listener` object subscribes to a channel and starts
    receiving all messages published to that channel.
    """

    @property
    def receiver_id(self):
        return self.__receiver.receiver_id

    def __init__(self, dsn, backend='aorta.backends.mock'):
        """Initialize a new :class:`Listener` instance.

        Args:
            dsn: a string holding a Data Source Name (DSN) identifying
                the AMQP 1.0 server and channel.
            backend: indicates the backend to use. May be a string pointing
                to the module holding the backend, or an actual instance.
        """
        self.__dsn = dsn
        self.__backend = load(backend)
        self.__receiver = None
        #self.__handlers = MessageHandlersProvider()
        self.__lock = threading.RLock()
        self.__event = threading.Event()

        assert isinstance(self.__backend, BaseMessagingBackend)

    def start(self):
        """Starts listening on the address specified by :attr:`dsn`
        and processing messages.
        """
        if not self.__backend.is_running():
            self.__backend.start()
        self.__backend.add_listener(self)

    def setup(self, create_receiver):
        self.__receiver = create_receiver(self.__dsn, options=self.get_options())

    def get_options(self):
        return None

    def dispatch(self, message):
        """Dispatches an incoming message to the appropriate message
        handlers.
        """
        self.__event.set()

    def wait(self):
        """Block until the :class:`Listener` has received a message."""
        if self.__event.wait():
            self.__event.clear()
