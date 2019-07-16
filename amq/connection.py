from functools import partial
from amqplib import client_0_8 as amqp


class AMQPConnection:
    virtual_host = '/'
    port = 5672
    insist = False

    def __init__(self, hostname, userid, password,
                 virtual_host=None, port=None, **kwargs):
        """init connection params"""
        self.host = hostname
        self.userid = userid
        self.password = password
        self.virtual_host = virtual_host or self.virtual_host
        self.port = port or self.port
        self.insist = kwargs.get("insist", self.insist)
        # set connection class to self
        self.connect()

    def connect(self):
        """lazy connect to rabbitmq by amqp"""
        self.connection = partial(amqp.Connection, 
                                  host=self.host,
                                  userid=self.userid,
                                  password=self.password,
                                  virtual_host=self.virtual_host,
                                  insist=self.insist)

    def close(self):
        """colse zhe connection"""
        if getattr(self, 'connection'):
            self.connection.colse()


BrokerConnection = AMQPConnection
