import logging
import threading

from eda.event import EventFactory

from aorta.const import EVENTS_CHANNEL
from aorta.dto import EventAdapter
from aorta.exc import MalformedEvent


class IListener(object):
    pass


class Listener(IListener):
    """Creates a listener for a single source (channel) of messages.
    """
    event_adapter = EventAdapter()

    @property
    def url(self):
        return "{0}:{1}/{2}".format(self.host, self.port, self.channel)

    @property
    def address(self):
        return "{0}:{1}".format(self.host, self.port)

    def __init__(self, sender_id, host, port, channel=None, logger='aorta.listener',
        events=None, handlers=None):
        """
        Initialize a new :class:`Listener` instance.

        Args:
            sender_id: a string identifying the sender for all messages that
                are published through this listener instance.
            host: a string specifying the host that is running the AMQP message
                broker.
            port: an integer specifying the port at which the AMQP message broker
                is listening.
            channel: a string specifying the channel to listen to.
            logger: specifies the logger name. Default is ``aorta.listener``.
            events: a list of event names that the :class:`Listener` must subscribe
                to.
            handlers: a list of :class:`eda.event.EventHandler` instances.
        """
        self.sender_id = sender_id
        self.host = host
        self.port = port
        self.channel = channel or EVENTS_CHANNEL
        self.logger = logging.getLogger(logger)
        self.thread = threading.Thread(target=self.on_start, daemon=True)
        self.events = events or []
        self.events_received = 0
        self.exception = None
        self.handlers = handlers or []
        self.consumer_events = []
        self.lock = threading.RLock()

    def start(self):
        self.thread.start()

    def get_event_handlers(self, event):
        for handler in self.handlers:
            try:
                if not handler.is_valid_event(event)\
                or not handler.can_handle(event):
                    continue
            except Exception:
                self.logger.exception(
                    "Caught fatal exception during event handling.")
                continue
            yield handler

    def on_start(self):
        raise NotImplementedError("Subclasses must override this method.")

    def on_event(self, msg):
        """Handles a message of type 'event'."""
        self.events_received += 1
        try:
            header, event = self._parse_event(msg)
        except Exception as e:
            self.logger.exception("Received a malformed event.")
        else:
            for handler in self.get_event_handlers(event):
                try:
                    handler.handle(event)
                except Exception as e:
                    self.logger.exception(
                        "Caught fatal exception during event handling.")
                    continue

    def _parse_event(self, msg):
        header, errors = self.event_adapter.load(msg.properties)
        if errors:
            self.on_malformed_event(msg)
            raise MalformedEvent
        return header, EventFactory(header['event_type'])(**msg.body)


    def on_malformed_event(self, msg):
        pass
