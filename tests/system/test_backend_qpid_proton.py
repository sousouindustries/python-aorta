import logging
import os
import sys
import threading
import time
import unittest

from aorta.const import CHANNEL
from aorta.const import HOST
from aorta.const import PORT
from aorta.message import Message
from aorta.listener import Listener
import aorta.backends

logger = logging.getLogger()
logger.level = logging.DEBUG
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)
PROTON_INSTALLED = hasattr(aorta.backends, 'qpid_proton')


@unittest.skipIf(not PROTON_INSTALLED, "Skipping Apache Proton tests (not installed).")
class AortaTestCase(unittest.TestCase):
    channel = CHANNEL

    def setUp(self):
        self.backend = aorta.backends.qpid_proton.MessagingBackend()
        self.url = "{0}:{1}/{2}".format(HOST, PORT, CHANNEL)

    def test_destroy(self):
        self.backend.listen(self.url)
        self.backend.start()
        for i in range(10):
            self.backend.send_message(self.url, Message(body="Hello world!"), block=True)
        self.backend.destroy()

    def test_send(self):
        self.backend.send_message(self.url, Message(body="Hello world!"), block=True)
        self.backend.send_message(self.url, Message(body="Hello world!"), block=True)
        self.backend.send_message(self.url, Message(body="Hello world!"), block=True)
        self.backend.send_message(self.url, Message(body="Hello world!"), block=True)
        self.assertEqual(self.backend.deliveries, 4)

    def test_recv(self):
        self.backend.listen(self.url)
        self.backend.send_message(self.url, Message(body="Hello world!"))
        receiver_id, msg = self.backend.get()
        self.assertEqual(msg.body, "Hello world!")

    def test_orphaned_received(self):
        self.backend.listen(self.url)
        self.backend.start()
        self.backend.send_message(self.url, Message(body="Hello world!"), block=True)

    def test_listener(self):
        listener = Listener(self.url, backend=self.backend)
        listener.start()
        self.backend.send_message(self.url, Message(body="Hello world!"), block=True)
        listener.wait()

    def test_backend_dispatch_catches_fatal_exception(self):
        listener = ExceptionOnDispatchRaisingListener(
            self.url, backend=self.backend)
        listener.start()
        self.backend.send_message(self.url, Message(body="Hello world!"), block=True)
        listener.wait()


class ExceptionOnDispatchRaisingListener(Listener):

    def dispatch(self, *args, **kwargs):
        self._Listener__event.set() # to prevent wait() from blocking indefinatly.
        raise Exception


if __name__ == '__main__':
    unittest.main()
