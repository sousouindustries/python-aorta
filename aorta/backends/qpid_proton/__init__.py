from aorta.backends.base import BaseMessagingBackend
from aorta.backends.qpid_proton.sender import Sender


class MessagingBackend(BaseMessagingBackend):

    def create_sender(self, host, port, channel, *args, **kwargs):
        return Sender.create(self, host, port, channel)
