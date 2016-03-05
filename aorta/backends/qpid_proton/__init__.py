from aorta.backends.base import BaseMessagingBackend
from aorta.backends.qpid_proton.sender import Sender
from aorta.backends.qpid_proton.receiver import Receiver


class MessagingBackend(BaseMessagingBackend):
    receiver_class = Receiver
    sender_class = Sender
