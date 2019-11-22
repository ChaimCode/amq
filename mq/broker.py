# coding:utf8
from .transports import get_transport_cls


class BrokerConnection:

    def __init__(self, hostname=None, userid=None, password=None, port=None, transport_cls=None):
        self.hostname = hostname
        self.userid = userid
        self.password = password
        self.port = port
        self.transport_cls = transport_cls
        self._connection = None

    @property
    def host(self):
        if self.port:
            return ":".join([self.hostname, str(self.port)])
        return self.hostname

    @property
    def connection(self):
        """获取连接"""
        if not getattr(self, '_connection'):
            self._connection = self.create_transport().connection
        return self._connection

    def create_transport(self, topic=None, queue=None):
        """创建转换类实例"""
        transport_cls = self.transport_cls
        if not transport_cls or isinstance(transport_cls, str):
            transport_cls = get_transport_cls(transport_cls)
        return transport_cls(connection=self, topic=topic, queue=queue)
