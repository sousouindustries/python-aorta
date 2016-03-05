import logging
import queue
import threading
import time

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

from aorta.backends.isender import ISender
from aorta.backends.base import BaseMessagingBackend


class MessagingBackend(BaseMessagingBackend):

    def create_sender(self, host, port, channel, *args, **kwargs):
        return Sender.create(self, host, port, channel)

    def destroy(self):
        for sender in self.senders.values():
            sender.destroy()


class Sender(MessagingHandler, threading.Thread, ISender):

    @property
    def address(self):
        return "{0}:{1}".format(self.host, self.port)

    @property
    def dsn(self):
        return "{0}/{1}".format(self.address, self.channel)

    @classmethod
    def create(cls, backend, *args):
        sender = cls(backend, *args)
        sender.start()
        return sender

    def destroy(self):
        """Ceases the senders' activity and releases all resources."""
        self.logger.info("Tearing down connection {0}".format(self.dsn))
        self.connection.close()
        self.join()

    def __init__(self, backend, host, port, channel):
        MessagingHandler.__init__(self)
        self.backend = backend
        self.host = host
        self.port = port
        self.channel = channel
        self.container = Container(self)
        self.connection = self.container.connect(self.address)
        self.sender = self.container.create_sender(self.connection, self.channel)
        self.events = {}
        self.lock = threading.RLock()
        threading.Thread.__init__(self, target=self.container.run, daemon=True)
        self._event_started = threading.Event()

    def send(self, message, blocking=False, timeout=None):
        """Sends a message to the AMQP server."""
        assert message.id is not None
        self._event_started.wait()
        timeout = ((timeout or 0) / 1000) or None

        message = Message(**message)
        delivery = self.sender.send(message)

        log_msg = "Message (id: {0}, delivery: {1}) dispatched to {2}."\
            .format(message.id, delivery.tag, self.dsn)
        self.logger.debug(log_msg)


        event = threading.Event()
        with self.lock:
            self.events[delivery.tag] = (message, event)

        if (blocking or timeout) and not event.wait(timeout):
            pass

        # TODO: when send() returns succesfully this does not mean that the
        # message has been succesfully accepted by the server. This is only
        # known when on_accepted() receives an event for the corresponding
        # delivery. There needs to be some kind of persistence mechanism
        # that stores messages until there is a guaranted delivery to
        # an acceptance by the server.

    def on_start(self, event):
        self._event_started.set()

    def on_accepted(self, event):
        self.notify_accepted(event.delivery.tag)

    def notify_accepted(self, tag):
        with self.lock:
            msg, event = self.events.pop(tag, [None, threading.Event()])
        if msg is None:
            return

        log_msg = "Message (id: {0}, delivery: {1}) accepted by {2}"\
            .format(msg.id, tag, self.dsn)
        self.logger.debug(log_msg)
        self.backend.register_delivery()
        event.set()
