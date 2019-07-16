from amqplib import client_0_8 as amqp

# seconds
DEFAULT_CONNECT_TIMEOUT = 5


class AMQPConnection:
    virtual_host = '/'
    port = 5672
    insist = False
    connect_timeout = DEFAULT_CONNECT_TIMEOUT
    ssl = False
    _closed = True

    @property
    def host(self):
        """The host as a hostname/port pair separated by colon."""
        return ":".join([self.hostname, str(self.port)])

    def __init__(self, hostname, userid, password,
                 virtual_host=None, port=None, **kwargs):
        """init connection params"""
        self.hostname = hostname
        self.userid = userid
        self.password = password
        self.virtual_host = virtual_host or self.virtual_host
        self.port = port or self.port
        self.insist = kwargs.get("insist", self.insist)
        self.connect_timeout = kwargs.get(
            "connect_timeout", self.connect_timeout)
        self.ssl = kwargs.get("ssl", self.ssl)
        self.connection = None

        # set connection class to self
        self.connect()
    
    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    def connect(self):
        """lazy connect to rabbitmq by amqp"""
        self.connection = amqp.Connection(host=self.host,
                                          userid=self.userid,
                                          password=self.password,
                                          virtual_host=self.virtual_host,
                                          insist=self.insist,
                                          ssl=self.ssl,
                                          connect_timeout=self.connect_timeout)
        self._closed = False
        return self.connection

    def close(self):
        """colse zhe connection"""
        if self.connection:
            self.connection.close()
        self._closed = True


class DummyConnection(object):
    """A connection class that does nothing, for non-networked backends."""
    _closed = True

    def __init__(self, *args, **kwargs):
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    def connect(self):
        """Doesn't do anything. Just for API compatibility."""
        pass

    def close(self):
        """Doesn't do anything. Just for API compatibility."""
        self._closed = True

    @property
    def host(self):
        """Always empty string."""
        return ""


BrokerConnection = AMQPConnection
