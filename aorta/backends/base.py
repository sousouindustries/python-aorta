import itertools
import logging
import re
import threading
import uuid
import queue


EXC_NOTIMPLEMENTED = NotImplementedError("Subclasses must override this method.")


class BaseMessagingBackend:
    """The base class for all messaging backends."""
    sender_class = None
    receiver_class = None
    logger = logging.getLogger('aorta')

    @property
    def senders(self):
        return dict(self.__senders)

    @property
    def receivers(self):
        return dict(self.__receivers)

    @property
    def listeners(self):
        return dict(self.__listeners)

    @property
    def deliveries(self):
        return self.__deliveries

    def __init__(self):
        self.__senders = {}
        self.__listeners = {}
        self.__receivers = {}
        self.__lock = threading.RLock()
        self.__deliveries = 0
        self.__incoming = queue.Queue()
        self.__stopped = False
        self.__thread = threading.Thread(target=self.__main__, daemon=True)
        self.__opts = {}

    def start(self):
        self.__thread.start()

    def register_delivery(self):
        with self.__lock:
            self.__deliveries += 1

    def generate_message_id(self):
        return uuid.uuid4().hex

    def get(self):
        """Return the latest message in the queue."""
        return self.__incoming.get()

    def put(self, *args):
        """Put a message on the incoming message queue."""
        self.__incoming.put(args)

    def get_sender(self, dsn):
        """Get a sender for the specified Data Source Name (DSN) `dsn`."""
        host, port, channel = re.match('^(.*)\:([0-9]{1,5})/(.*)$', dsn).groups()
        with self.__lock:
            if dsn not in self.__senders:
                self.__senders[dsn] = self.create_sender(host, port, channel)
            sender = self.__senders[dsn]
        return sender

    def listen(self, dsn, options=None):
        """Listen for messages coming from the specified `dsn`."""
        host, port, channel = re.match('^(.*)\:([0-9]{1,5})/(.*)$', dsn).groups()
        with self.__lock:
            if dsn not in self.__receivers:
                self.__receivers[dsn] = self\
                    ._create_receiver(host, port, channel, options=options)
            receiver = self.__receivers[dsn]
        return receiver

    def _create_receiver(self, host, port, channel, *args, **kwargs):
        return self.receiver_class\
            .create(self, host, port, channel, *args, **kwargs)

    def create_sender(self, host, port, channel, *args, **kwargs):
        return self.sender_class\
            .create(self, host, port, channel, *args, **kwargs)

    def send_message(self, dsn, message, block=False, timeout=None, forward=False):
        """Send a message to the specified `channel`.

        Args:
            dsn: a string specifying the host, port and channel.
            message: the message to send.
            forward: a boolean indicating is the message is being forwared.

        Returns:
            None
        """
        if message.id is None and not forward:
            message.id = self.generate_message_id()
        sender = self.get_sender(dsn)
        return sender.send(message, blocking=block, timeout=timeout)

    def destroy(self):
        handlers = itertools.chain(self.senders.values(), self.receivers.values())
        for handler in handlers:
            handler.destroy()

        self.__stopped = True
        if self.__thread.is_alive():
            self.__thread.join()

    def dispatch(self, receiver, message):
        """Dispatches a message to the appropriate listener."""
        try:
            with self.__lock:
                listener = self.listeners.get(receiver.receiver_id)
            if listener is None:
                self.logger.warning(
                    "Orphaned receiver (id: {0})".format(receiver.receiver_id))
                return

            listener.dispatch(message)
        except Exception:
            self.logger.exception("Caught fatal exception")

    def __main__(self):
        while True:
            try:
                receiver, msg = self.__incoming.get(True, 0.1)
            except queue.Empty:
                if self.__stopped:
                    break
            else:
                self.dispatch(receiver, msg)
