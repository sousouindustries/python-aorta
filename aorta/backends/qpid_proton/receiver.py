import collections
import threading
import uuid
import queue

from proton.handlers import MessagingHandler
from proton.reactor import Container

from aorta.backends.ireceiver import IReceiver



class Receiver(MessagingHandler, threading.Thread, IReceiver):

    @property
    def address(self):
        return "{0}:{1}".format(self.host, self.port)

    @property
    def dsn(self):
        return "{0}/{1}".format(self.address, self.channel)

    @classmethod
    def create(cls, backend, *args, **kwargs):
        receiver = cls(backend, *args)
        receiver.start()
        return receiver

    def __init__(self, backend, host, port, channel, options=None):
        MessagingHandler.__init__(self)
        self.backend = backend
        self.host = host
        self.port = port
        self.channel = channel
        self.container = Container(self)
        self.connection = self.container.connect(self.address)
        self.receiver = self.container\
            .create_receiver(self.connection, self.channel, options=options)
        self.seen = collections.deque([], 2000)
        self.receiver_id = uuid.uuid4().hex
        threading.Thread.__init__(self, target=self.container.run, daemon=True)

    def destroy(self):
        """Ceases the receiver activity and releases all resources."""
        self.logger.info("Tearing down connection {0}".format(self.dsn))
        self.receiver.close()
        self.connection.close()
        self.join()

    def on_start(self, event):
        log_msg = "Receiver (id: {0}) connecting to {1}"\
            .format(self.receiver_id, self.dsn)
        self.logger.info(log_msg)

    def on_message(self, event):
        msg = event.message
        if msg in self.seen:
            return
        log_msg = "Message (id: {0}, receiver: {1}) received from {2}"\
            .format(msg.id, self.receiver_id, self.dsn)
        self.logger.debug(log_msg)
        self.backend.put(self.receiver_id, msg)
