import socket
from amqplib import client_0_8 as amqp
from amqplib.client_0_8.connection import AMQPConnectionException
from amq.backends import get_backend_cls

# seconds
DEFAULT_CONNECT_TIMEOUT = 5


class BrokerConnection:
    virtual_host = '/'
    port = None
    insist = False
    connect_timeout = DEFAULT_CONNECT_TIMEOUT
    ssl = False
    _closed = True
    backend_cls = None

    ConnectionException = AMQPConnectionException

    @property
    def host(self):
        """The host as a hostname/port pair separated by colon."""
        return ":".join([self.hostname, str(self.port)])

    def __init__(self, hostname=None, userid=None, password=None,
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
        self.backend_cls = kwargs.get("backend_cls", None)
        self._closed = None
        self._connection = None

    @property
    def connection(self):
        if self._closed is True:
            return
        if not self._connection:
            self._connection = self._establish_connection()
            self._closed = False
        return self._connection

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    def _establish_connection(self):
        return self.create_backend().establish_connection()

    def get_backend_cls(self):
        """Get the currently used backend class."""
        backend_cls = self.backend_cls
        if not backend_cls or isinstance(backend_cls, str):
            backend_cls = get_backend_cls(backend_cls)
        return backend_cls

    def create_backend(self):
        """Create a new instance of the current backend in
        :attr:`backend_cls`."""
        backend_cls = self.get_backend_cls()
        return backend_cls(connection=self)

    def get_channel(self):
        """Request a new AMQP channel."""
        return self.connection.channel()

    def connect(self):
        """lazy connect to rabbitmq by amqp"""
        self._closed = False
        return self.connection

    def close(self):
        """Close the currently open connection."""
        try:
            if self._connection:
                backend = self.create_backend()
                backend.close_connection(self._connection)
        except socket.error:
            pass
        self._closed = True


AMQPConnection = BrokerConnection
