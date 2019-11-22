# coding:utf8
import time
import json
import logging
from datetime import  datetime, date
from exts.cmq.queue import Message
from exts.cmq.account import Account
from exts.cmq.cmq_tool import CMQLogger
from exts.cmq.cmq_exception import CMQServerException


class Transport:
    def __init__(self, connection, topic=None, queue=None, debug=False):
        self._connection = connection
        self._topic = topic
        self._queue = queue
        self.logger = CMQLogger.get_logger()
        self.debug = debug

    @property
    def connection(self):
        """真实建立连接"""
        return Account(self._connection.host,
                       self._connection.userid,
                       self._connection.password,
                       debug=self.debug)

    def _json_default(self, obj):
        """ 默认的json格式化函数 """
        if isinstance(obj, (datetime, date)):
            return time.mktime(obj.timetuple()) + obj.microsecond * 0.000001
        raise TypeError('Type %s not serialzable' % type(obj))

    def create_message(self, msg_data):
        """创建消息"""
        if isinstance(msg_data, dict):
            msg_data = json.dumps(msg_data, default=self._json_default)
        message = Message(msg_data)
        return message

    @property
    def client(self):
        """队列/主题操作client"""
        if getattr(self, '_topic'):
            return self.connection.get_topic(self._topic)
        else:
            return self.connection.get_queue(self._queue)

    def publish(self, message, msg_tags=[], routing_key=""):
        """发布消息"""
        if self._topic:
            ret_msg = self.client.publish_message(message, msg_tags=msg_tags, routing_key=routing_key)
        else:
            ret_msg = self.client.send_message(message)
        msg_id = ret_msg.msgId
        return msg_id

    def consume(self, batch=1, wait_seconds=1):
        """消费消息"""
        try:
            return self.client.batch_receive_message(batch, wait_seconds)
        except CMQServerException as e:
            # 忽略无消息时候的报错
            if e.code == 7000:
                logging.debug(e)
            return []

    def ack(self, msg_id):
        self.client.delete_message(msg_id)
