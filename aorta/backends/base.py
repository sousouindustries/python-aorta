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
                    .create_receiver(host, port, channel, options=options)
            receiver = self.__receivers[dsn]
        return receiver

    def create_receiver(self, host, port, channel, *args, **kwargs):
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
