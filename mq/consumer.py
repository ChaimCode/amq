# coding:utf8
from functools import partial


class Consumer:
    def __init__(self, connection, topic=None, queue=None):
        self.connection = connection
        self._topic = topic
        self._queue = queue
        self.transport = self.connection.create_transport(topic=self._topic, queue=self._queue)
        self.callbacks = []

    def register_callback(self, callback):
        """注册回调函数"""
        self.callbacks.append(callback)

    def consume(self, batch=1, wait_seconds=1, auto_ack=False):
        """消费消息, 封装消息确认与消息回调"""
        msg_list = self.transport.consume(batch, wait_seconds)
        res_list = []
        if self.callbacks:
            for msg in msg_list:
                msg.ack = partial(self.transport.ack, msg.receiptHandle)
                if auto_ack:
                    msg.ack()
                for func in self.callbacks:
                    # 将单引号替换为双引号, 用于标准的json.loads
                    res_list.append(func(msg.msgBody.replace("'", '"'), msg))
        return res_list
