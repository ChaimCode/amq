import uuid
from amq import serialization


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
    warn_if_exists = False
    auto_ack = False
    no_ack = False
    _closed = True

    def __init__(self, connection, queue=None, exchange=None, routing_key=None,
                 **kwargs):
        self.connection = connection
        self.backend_cls = kwargs.get("backend_cls", None)
        self.backend = self.connection.create_backend()

        # Binding.
        self.queue = queue or self.queue
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.callbacks = []

        # Options
        self.durable = kwargs.get("durable", self.durable)
        self.exclusive = kwargs.get("exclusive", self.exclusive)
        self.auto_delete = kwargs.get("auto_delete", self.auto_delete)
        self.exchange_type = kwargs.get("exchange_type", self.exchange_type)
        self.warn_if_exists = kwargs.get("warn_if_exists", self.warn_if_exists)
        self.auto_ack = kwargs.get("auto_ack", self.auto_ack)

        # exclusive implies auto-delete.
        if self.exclusive:
            self.auto_delete = True

        self.consumer_tag = self._generate_consumer_tag()
        self._declare_channel()

    def _generate_consumer_tag(self):
        """generate consumer tag with uuid4"""
        return f"{self.__class__.__module__}.{self.__class__.__name__}-{str(uuid.uuid4())}"

    def _declare_channel(self):
        """constructe channel:
            1. declare queue
            2. declare exchange
            3. bind queue and exchange
        """
        if self.queue:
            self.backend.queue_declare(queue=self.queue, durable=self.durable,
                                       exclusive=self.exclusive,
                                       auto_delete=self.auto_delete,
                                       warn_if_exists=self.warn_if_exists)
        if self.exchange:
            self.backend.exchange_declare(exchange=self.exchange,
                                          type=self.exchange_type,
                                          durable=self.durable,
                                          auto_delete=self.auto_delete)
        if self.queue:
            self.backend.queue_bind(queue=self.queue, exchange=self.exchange,
                                    routing_key=self.routing_key)
        self._closed = False
        return self

    def _receive_callback(self, raw_message):
        message = self.backend.message_to_python(raw_message)
        if self.auto_ack and not message.acknowledged:
            message.ack()
        self.receive(message.payload, message)

    def fetch(self, no_ack=None, auto_ack=None, enable_callbacks=False):
        """receive the next message"""
        no_ack = no_ack or self.no_ack
        auto_ack = auto_ack or self.auto_ack
        message = self.backend.get(self.queue, no_ack=no_ack)
        if message:
            if auto_ack and not message.acknowledged:
                message.ack()
            if enable_callbacks:
                self.receive(message.payload, message)
        return message

    def receive(self, message_data, message):
        """depend on register_callback hook"""
        if not self.callbacks:
            raise NotImplementedError("No consumer callbacks registered")
        for callback in self.callbacks:
            callback(message_data, message)

    def register_callback(self, callback):
        """Register a callback function to be triggered by method `receive`.
        """
        self.callbacks.append(callback)

    def discard_all(self, filterfunc=None):
        """Discard all waiting messages.
        Returns the number of messages discarded.
        *WARNING*: All incoming messages will be ignored and not processed.
        """
        if not filterfunc:
            return self.backend.queue_purge(self.queue)

        if self.no_ack or self.auto_ack:
            raise Exception("discard_all: Can't use filter with auto/no-ack.")

        discarded_count = 0
        while True:
            message = self.fetch()
            if message is None:
                return discarded_count

            if filterfunc(message):
                message.ack()
                discarded_count += 1

    def iterconsume(self, limit=None, no_ack=None):
        """Iterator processing new messages as they arrive.
        Every new message will be passed to the callbacks, and the iterator
        returns ``True``. The iterator is infinite unless the ``limit``
        argument is specified or someone closes the consumer.

        :meth:`iterconsume` uses transient requests for messages on the
        server, while :meth:`iterequeue` uses synchronous access. In most
        cases you want :meth:`iterconsume`, but if your environment does not
        support this behaviour you can resort to using :meth:`iterqueue`
        instead.

        Also, :meth:`iterconsume` does not return the message
        at each step, something which :meth:`iterqueue` does.

        :keyword limit: Maximum number of messages to process.

        :raises StopIteration: if limit is set and the message limit has been
        reached.

        """
        self.backend.declare_consumer(queue=self.queue,
                                      no_ack=no_ack or self.no_ack,
                                      callback=self._receive_callback,
                                      consumer_tag=self.consumer_tag)
        self.channel_open = True
        return self.backend.consume(limit=limit)

    def wait(self, limit=None):
        """Go into consume mode.

        Mostly for testing purposes and simple programs, you probably
        want :meth:`iterconsume` or :meth:`iterqueue` instead.

        This runs an infinite loop, processing all incoming messages
        using :meth:`receive` to apply the message to all registered
        callbacks.

        """
        it = self.iterconsume(limit)
        while True:
            next(it)

    def cancel(self):
        """Cancel a running :meth:`iterconsume` session."""
        if self.channel_open:
            try:
                self.backend.cancel(self.consumer_tag)
            except KeyError:
                pass

    def close(self):
        """Close the channel to the queue."""
        self.cancel()
        self.backend.close()
        self._closed = True

    def flow(self, active):
        """This method asks the peer to pause or restart the flow of
        content data.

        This is a simple flow-control mechanism that a
        peer can use to avoid oveflowing its queues or otherwise
        finding itself receiving more messages than it can process.
        Note that this method is not intended for window control.  The
        peer that receives a request to stop sending content should
        finish sending the current content, if any, and then wait
        until it receives the ``flow(active=True)`` restart method.

        """
        self.backend.flow(active)

    def qos(self, prefetch_size=0, prefetch_count=0, apply_global=False):
        """Request specific Quality of Service. It's similar to special priority queue
        """
        return self.backend.qos(prefetch_size, prefetch_count, apply_global)


class Publisher:
    exchange = ""
    routing_key = ""
    # persistent
    delivery_mode = 2
    _closed = True
    exchange_type = "direct"
    durable = True
    auto_delete = False
    serializer = None

    def __init__(self, connection, exchange=None, routing_key=None, **kwargs):
        self.connection = connection
        self.backend = self.connection.create_backend()
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.delivery_mode = kwargs.get("delivery_mode", self.delivery_mode)
        self.exchange_type = kwargs.get("exchange_type", self.exchange_type)
        self.durable = kwargs.get("durable", self.durable)
        self.auto_delete = kwargs.get("auto_delete", self.auto_delete)
        self.serializer = kwargs.get("serializer", self.serializer)
        self._declare_exchange()
        self._closed = False

    def _declare_exchange(self):
        if self.exchange:
            self.backend.exchange_declare(exchange=self.exchange,
                                          type=self.exchange_type,
                                          durable=self.durable,
                                          auto_delete=self.auto_delete)

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    def create_message(self, message_data, delivery_mode=None, priority=None,
                       content_type=None, content_encoding=None,
                       serializer=None):
        """With any data, serialize it and encapsulate it in a AMQP
        message with the proper headers set."""

        delivery_mode = delivery_mode or self.delivery_mode

        if not content_type:
            serializer = serializer or self.serializer
            (content_type, content_encoding, message_data) = serialization.encode(
                message_data, serializer=serializer)
        return self.backend.prepare_message(message_data, delivery_mode,
                                            priority=priority,
                                            content_type=content_type,
                                            content_encoding=content_encoding)

    def send(self, message_data, routing_key=None, delivery_mode=None,
             mandatory=False, immediate=False, priority=0, content_type=None,
             content_encoding=None, serializer=None):
        routing_key = routing_key or self.routing_key
        message = self.create_message(message_data, priority=priority,
                                      delivery_mode=delivery_mode,
                                      content_type=content_type,
                                      content_encoding=content_encoding,
                                      serializer=serializer)
        self.backend.publish(message,
                             exchange=self.exchange,
                             routing_key=routing_key,
                             mandatory=mandatory,
                             immediate=immediate)

    def close(self):
        """Close connection to queue."""
        self.backend.close()
        self._closed = True


class Messaging(object):
    """A combined message publisher and consumer."""
    queue = ""
    exchange = ""
    routing_key = ""
    publisher_cls = Publisher
    consumer_cls = Consumer
    _closed = True

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.exchange = kwargs.get("exchange", self.exchange)
        self.queue = kwargs.get("queue", self.queue)
        self.routing_key = kwargs.get("routing_key", self.routing_key)
        self.publisher = self.publisher_cls(connection,
                                            exchange=self.exchange,
                                            routing_key=self.routing_key,
                                            )
        self.consumer = self.consumer_cls(connection,
                                          queue=self.queue,
                                          exchange=self.exchange,
                                          routing_key=self.routing_key)
        self.consumer.register_callback(self.receive)
        self.callbacks = []
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    def register_callback(self, callback):
        """See :meth:`Consumer.register_callback`"""
        self.callbacks.append(callback)

    def receive(self, message_data, message):
        """See :meth:`Consumer.receive`"""
        if not self.callbacks:
            raise NotImplementedError("No consumer callbacks registered")
        for callback in self.callbacks:
            callback(message_data, message)

    def send(self, message_data, delivery_mode=None):
        """See :meth:`Publisher.send`"""
        self.publisher.send(message_data, delivery_mode=delivery_mode)

    def fetch(self, **kwargs):
        """See :meth:`Consumer.fetch`"""
        return self.consumer.fetch(**kwargs)

    def close(self):
        """Close any open channels."""
        self.consumer.close()
        self.publisher.close()
        self._closed = True


class ConsumerSet(object):
    """Receive messages from multiple consumers.
    """
    auto_ack = False

    def __init__(self, connection, from_dict=None, consumers=None,
                 callbacks=None, **options):
        self.connection = connection
        self.options = options
        self.from_dict = from_dict or {}
        self.consumers = consumers or []
        self.callbacks = callbacks or []
        self._open_channels = []

        self.backend = self.connection.create_backend()

        self.auto_ack = options.get("auto_ack", self.auto_ack)

        [self.add_consumer_from_dict(queue_name, **queue_options)
         for queue_name, queue_options in self.from_dict.items()]

    def _receive_callback(self, raw_message):
        """Internal method used when a message is received in consume mode."""
        message = self.backend.message_to_python(raw_message)
        if self.auto_ack and not message.acknowledged:
            message.ack()
        self.receive(message.decode(), message)

    def add_consumer_from_dict(self, queue, **options):
        """Add another consumer from dictionary configuration."""
        consumer = Consumer(self.connection, queue=queue,
                            backend=self.backend, **options)
        self.consumers.append(consumer)

    def add_consumer(self, consumer):
        """Add another consumer from a :class:`Consumer` instance."""
        consumer.backend = self.backend
        self.consumers.append(consumer)

    def register_callback(self, callback):
        """Register new callback to be called when a message is received.
        See :meth:`Consumer.register_callback`"""
        self.callbacks.append(callback)

    def receive(self, message_data, message):
        """What to do when a message is received.
        See :meth:`Consumer.receive`."""
        if not self.callbacks:
            raise NotImplementedError("No consumer callbacks registered")
        for callback in self.callbacks:
            callback(message_data, message)

    def _declare_consumer(self, consumer, nowait=False):
        """Declare consumer so messages can be received from it using
        :meth:`iterconsume`."""
        # Use the ConsumerSet's consumer by default, but if the
        # child consumer has a callback, honor it.
        callback = consumer.callbacks and \
            consumer._receive_callback or self._receive_callback
        self.backend.declare_consumer(queue=consumer.queue,
                                      no_ack=consumer.no_ack,
                                      nowait=nowait,
                                      callback=callback,
                                      consumer_tag=consumer.consumer_tag)
        self._open_channels.append(consumer.consumer_tag)

    def iterconsume(self, limit=None):
        """Cycle between all consumers in consume mode.
        See :meth:`Consumer.iterconsume`.
        """
        head = self.consumers[:-1]
        tail = self.consumers[-1]
        [self._declare_consumer(consumer, nowait=True)
         for consumer in head]
        self._declare_consumer(tail, nowait=False)

        return self.backend.consume(limit=limit)

    def discard_all(self):
        """Discard all messages. Does not support filtering.
        See :meth:`Consumer.discard_all`."""
        return sum([consumer.discard_all()
                    for consumer in self.consumers])

    def flow(self, active):
        """This method asks the peer to pause or restart the flow of
        content data.
        See :meth:`Consumer.flow`.
        """
        self.backend.flow(active)

    def qos(self, prefetch_size=0, prefetch_count=0, apply_global=False):
        """Request specific Quality of Service.
        See :meth:`Consumer.cos`.
        """
        self.backend.qos(prefetch_size, prefetch_count, apply_global)

    def cancel(self):
        """Cancel a running :meth:`iterconsume` session."""
        for consumer_tag in self._open_channels:
            try:
                self.backend.cancel(consumer_tag)
            except KeyError:
                pass
        self._open_channels = []

    def close(self):
        """Close all consumers."""
        self.cancel()
        for consumer in self.consumers:
            consumer.close()
