# coding:utf8
class Producer:
    def __init__(self, connection, topic=None, queue=None):
        self.connection = connection
        self._topic = topic
        self._queue = queue
        self.transport = self.connection.create_transport(topic=self._topic, queue=self._queue)

    def publish(self, msg_data, msg_tags=[], routing_key=""):
        """发布消息"""
        msg = self.transport.create_message(msg_data)
        return self.transport.publish(msg, msg_tags=msg_tags, routing_key=routing_key)
