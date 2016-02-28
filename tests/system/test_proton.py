import copy
import logging
import os
import sys
import time
import uuid
import unittest

import eda

import aorta.listener


#logger = logging.getLogger()
#logger.level = logging.DEBUG
#stream_handler = logging.StreamHandler(sys.stdout)
#logger.addHandler(stream_handler)


PROTON_INSTALLED = hasattr(aorta.listener, 'ProtonListener')
HOST = os.getenv('AORTA_TEST_PROTON_HOST') or 'localhost'
try:
    PORT = int(os.getenv('AORTA_TEST_PROTON_PORT') or 5672)
except ValueError:
    print("Provide a valid integer for AORTA_TEST_PROTON_PORT")

# The channel must be an exchange of type fanout
CHANNEL = 'aorta.test'


@unittest.skipIf(not PROTON_INSTALLED, "Skipping Apache Proton tests (not installed).")
class ProtonTestCase(unittest.TestCase):
    events = [
        'test.TestEvent'
    ]
    headers = {
        'type': 'event',
        'event_id': uuid.uuid4().hex,
        'event_type': 'test.TestEvent',
        'sender_id': uuid.uuid4().hex
    }
    body = {'foo': 1, 'bar': 2, 'baz': 3}

    def send_event(self, event_class):
        headers = copy.deepcopy(self.headers)
        headers['event_type'] = event_class.event_type
        send_message(self.url, headers, self.body)
        time.sleep(0.1)

    def setUp(self):
        self.handler1 = TestEventHandler(self)
        self.handler2 = IgnoredEventHandler(self)
        self.handler3 = FailingEventHandler(self)
        self.listener = aorta.listener.ProtonListener(
            'test', HOST, PORT, channel=CHANNEL, events=self.events,
            handlers=[self.handler3, self.handler2, self.handler1]
        )
        self.listener.start()
        self.url = self.listener.url
        self.host = HOST
        self.port = PORT
        time.sleep(0.05)

    def test_recv_event(self):
        self.send_event(eda.events.test.TestEvent)
        self.assertEqual(self.listener.events_received, 1)

    def test_recv_event_non_handled(self):
        self.send_event(eda.events.test.TestEvent)
        self.assertTrue(self.handler2.event is None)

    def test_graceful_error_handling(self):
        self.send_event(eda.events.test.EventWithFailingHandler)

        # The AnyEventHandler is always invoked last in this test;
        # so we consider an exception handled gracefully if it is
        # invoked.
        self.assertTrue(self.handler1.event is not None)


    def tearDown(self):
        pass


class TestEventHandler(eda.event.AnyEventHandler):

    def __init__(self, test):
        self.test = test
        self.event = None

    def handle(self, event):
        self.event = event


@eda.event.handles(eda.events.test.IgnoredEvent)
class IgnoredEventHandler(eda.event.EventHandler):

    def __init__(self, test):
        self.test = test
        self.event = None

    def handle(self, event):
        self.event = event


@eda.event.handles(eda.events.test.EventWithFailingHandler)
class FailingEventHandler(eda.event.EventHandler):

    def __init__(self, test):
        self.test = test
        self.event = None

    def handle(self, event):
        raise Exception


if PROTON_INSTALLED:
    import threading
    import queue
    import uuid

    from proton import Message
    from proton.handlers import MessagingHandler
    from proton.reactor import Container

    def send_message(url, properties, body):
        sender = Send(url, properties, body)
        Container(sender).run()
        return sender.message_id


    class Send(MessagingHandler):

        def __init__(self, url, properties, body):
            super(Send, self).__init__()
            self.url = url
            self.message_id = uuid.uuid4().hex
            self.body = body
            self.properties = properties
            self.total = 0

        def on_start(self, event):
            event.container.create_sender(self.url)

        def on_sendable(self, event):
            if self.total == 0:
                msg = Message(id=self.message_id, properties=self.properties, body=self.body)
                event.sender.send(msg)
                self.total += 1

        def on_accepted(self, event):
            event.connection.close()


if __name__ == '__main__':
    #logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.disable(logging.CRITICAL)
    unittest.main()
