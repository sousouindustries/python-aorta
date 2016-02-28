import collections
import logging


class IReceiver(object):

    def __init__(self, listener):
        self.listener = listener
        self.logger = self.listener.logger
        self.seen = collections.deque([], 1000)
        super(IReceiver, self).__init__()

    def get_options(self):
        """Return the options used when connecting to the AMQP message
        broker.
        """
        return None

    def on_start(self, event):
        raise NotImplementedError("Subclasses must override this method.")

    def on_message(self, event):
        self.logger.debug(
            "Received message {0} from {1}"\
                .format(event.message.id, self.listener.url)
            )
        msg = event.message
        if msg.id in self.seen:
            return

        self.seen.append(msg.id)

        # Currently only messages of type 'event' are supported.
        assert msg.properties.get('type') == 'event'

        self.listener.on_event(msg)
