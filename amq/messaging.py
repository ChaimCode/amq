from functools import partial
from amqplib import client_0_8 as amqp
from json import dumps as serialize
from json import loads as deserialize


class Message(object):
    """wrapper around amqplib.client_0_8.Message."""

    def __init__(self, amqp_message, channel):
        self.amqp_message = amqp_message
        self.channel = channel

    def ack(self):
        """Acknowledge this message as being processed.,
        This will remove the message from the queue."""
        return self.channel.basic_ack(self.delivery_tag)

    def reject(self):
        """Reject this message.
        The message will then be discarded by the server.
        """
        return self.channel.basic_reject(self.delivery_tag, requeue=False)

    def requeue(self):
        """Reject this message and put it back on the queue.
        You must not use this method as a means of selecting messages
        to process."""
        return self.channel.basic_reject(self.delivery_tag, requeue=True)

    @property
    def body(self):
        return self.amqp_message.body

    @property
    def delivery_tag(self):
        return self.amqp_message.delivery_tag


class Consumer(object):
    queue = ""
    exchange = ""
    routing_key = ""
    durable = True
    exclusive = False
    auto_delete = False
    exchange_type = "direct"
    # to show zhe channel status
    channel_open = False

    def __init__(self, connection, queue=None, exchange=None, routing_key=None,
                 **kwargs):
        self.connection = connection.connection()
        self.queue = queue or self.queue
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.durable = kwargs.get("durable", self.durable)
        self.exclusive = kwargs.get("exclusive", self.exclusive)
        self.auto_delete = kwargs.get("auto_delete", self.auto_delete)
        self.exchange_type = kwargs.get("exchange_type", self.exchange_type)
        self.channel = self._build_channel()

    def _build_channel(self):
        """constructe channel:
            1. create channel
            2. declare queue
            3. declare exchange
            4. bind queue and exchange
        """
        channel = self.connection.connection.channel()
        if self.queue:
            channel.queue_declare(queue=self.queue,
                                  durable=self.durable,
                                  exclusive=self.exclusive,
                                  auto_delete=self.auto_delete)
        if self.exchange:
            channel.exchange_declare(exchange=self.exchange,
                                     type=self.exchange_type,
                                     durable=self.durable,
                                     auto_delete=self.auto_delete)
        if self.queue:
            channel.queue_bind(queue=self.queue,
                               exchange=self.exchange,
                               routing_key=self.routing_key)
        return channel

    def _receive_callback(self, raw_message):
        """function/method called with each delivered message
            message_data: message content
            message: message object
        """
        if not self.channel.connection:
            self.channel = self._build_channel()
        message = Message(raw_message, self.channel)
        message_data = deserialize(message.body)
        self.receive(message_data, message)

    def fetch(self):
        """fetch Message obj"""
        if not self.channel.connection:
            self.channel = self._build_channel()
        raw_message = self.channel.basic_get(self.queue)
        if not raw_message:
            return None
        return Message(raw_message, channel=self.channel)

    def process_next(self, ack=True):
        """returns a pending message from the queue."""
        message = self.fetch()
        if message:
            self._receive_callback(message)
            if ack:
                message.ack()
        return message

    def discard_all(self):
        """Discard all waiting messages.
        Returns the number of messages discarded.
        *WARNING*: All incoming messages will be ignored and not processed.
        """
        discarded_count = 0
        while True:
            message = self.fetch()
            if message is None:
                return discarded_count
            message.ack()
            discarded_count = discarded_count + 1

    def receive(self, message_data, message):
        """depend on register_callback hook"""
        raise NotImplementedError(
            "Consumers must implement the receive method")

    def register_callback(self, func):
        """implement receive"""
        if not isinstance(func, object):
            raise Exception("Type Error, need function")
        self.receive = func

    def wait(self):
        """maybe disconnect connection, auto connect when receive msg"""
        if not self.channel.connection:
            self.channel = self._build_channel()
        self.channel_open = True
        # consumer_tag is alias of consumer
        # when no_ack=False, callback is needed.
        self.channel.basic_consume(queue=self.queue, no_ack=False,
                                   callback=self._receive_callback,
                                   consumer_tag=self.__class__.__name__)
        while self.channel.callbacks:
            self.channel.wait()

    def close(self):
        """end a queue consumer, close channel"""
        if self.channel_open:
            self.channel.basic_cancel(self.__class__.__name__)
            self.channel_open = False
        if getattr(self, "channel") and self.channel.is_open:
            self.channel.close()


class Publisher:
    exchange = ""
    routing_key = ""
    # persistent
    delivery_mode = 2

    def __init__(self, connection, exchange=None, routing_key=None, **kwargs):
        self.connection = connection.connection()
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.delivery_mode = kwargs.get("delivery_mode", self.delivery_mode)
        self.channel = self._build_channel()

    def _build_channel(self):
        return self.connection.connection.channel()

    def create_message(self, message_data):
        """create message by amqp.Message"""
        # Recreate channel if connection lost
        if not self.channel.connection:
            self.channel = self._build_channel()
        message_data = serialize(message_data)
        message = amqp.Message(message_data)
        message.properties["delivery_mode"] = self.delivery_mode
        return message

    def send(self, message_data, delivery_mode=None):
        message = self.create_message(message_data)
        self.channel.basic_publish(message,
                                   exchange=self.exchange,
                                   routing_key=self.routing_key)

    def close(self):
        if getattr(self, "channel") and self.channel.is_open:
            self.channel.close()
