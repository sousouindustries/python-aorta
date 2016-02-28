import logging

from proton.reactor import Container
from proton.handlers import MessagingHandler

from aorta.listener.base import Listener
from aorta.listener.ireceiver import IReceiver


class Receiver(IReceiver, MessagingHandler):

    def on_start(self, event):
        connection = event.container.connect(self.listener.address)
        event.container.create_receiver(connection, self.listener.channel,
            options=self.get_options())
        self.logger.info("Aorta connection established.")


class ProtonListener(Listener):
    receiver_class = Receiver

    def __init__(self, *args, **kwargs):
        Listener.__init__(self, *args, **kwargs)
        self.receiver = self.receiver_class(self)
        self.logger.info("Initialized ProtonListener")

    def on_start(self):
        self.container = Container(self.receiver)
        self.logger.info("Connecting to Aorta.")
        self.container.run()
        time.sleep(0.1)
