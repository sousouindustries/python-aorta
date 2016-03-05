import logging
import re
import threading
import uuid


EXC_NOTIMPLEMENTED = NotImplementedError("Subclasses must override this method.")


class BaseMessagingBackend:
    """The base class for all messaging backends."""
    sender_class = None

    @property
    def senders(self):
        return dict(self.__senders)

    @property
    def listeners(self):
        return dict(self.__listeners)

    @property
    def deliveries(self):
        return self.__deliveries

    def __init__(self):
        self.__senders = {}
        self.__listeners = {}
        self.__lock = threading.RLock()
        self.__deliveries = 0

    def register_delivery(self):
        with self.__lock:
            self.__deliveries += 1

    def generate_message_id(self):
        return uuid.uuid4().hex

    def get_sender(self, dsn):
        """Get a sender for the specified Data Source Name (DSN) `dsn`."""
        host, port, channel = re.match('^(.*)\:([0-9]{1,5})/(.*)$', dsn).groups()
        with self.__lock:
            if dsn not in self.__senders:
                self.__senders[dsn] = self.create_sender(host, port, channel)
            sender = self.__senders[dsn]
        return sender

    def create_sender(self, host, port, channel, *args, **kwargs):
        """Create a new sender using the specified `dsn`."""
        raise EXC_NOTIMPLEMENTED

    def create_listener(self, dsn, *args, **kwargs):
        """Create a new listener using the specified `dsn`."""
        raise EXC_NOTIMPLEMENTED

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
        for sender in self.senders.values():
            sender.destroy()
