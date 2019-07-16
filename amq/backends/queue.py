import time
import itertools
from queue import Queue
from amq.backends.base import BaseMessage, BaseBackend

mqueue = Queue()


class Message(BaseMessage):
    """Message received from the backend.
    See :class:`amq.backends.base.BaseMessage`.
    """


class Backend(BaseBackend):
    def get(self, *args, **kwargs):
        """Get the next waiting message from the queue.
        """
        if not mqueue.qsize():
            return None
        message_data, content_type, content_encoding = mqueue.get()
        return Message(backend=self, body=message_data,
                       content_type=content_type,
                       content_encoding=content_encoding)

    def consume(self, queue, no_ack, callback, consumer_tag, limit=None):
        """Go into consume mode."""
        for total_message_count in itertools.count():
            message = self.get()
            if message:
                callback(message.decode(), message)
            time.sleep(0.1)

    def purge(self, queue, **kwargs):
        """Discard all messages in the queue."""
        mqueue = Queue()
        return mqueue

    def prepare_message(self, message_data, delivery_mode,
                        content_type, content_encoding, **kwargs):
        """Prepare message for sending."""
        return (message_data, content_type, content_encoding)

    def publish(self, message, exchange, routing_key, **kwargs):
        """Publish a message to the queue."""
        mqueue.put(message)
