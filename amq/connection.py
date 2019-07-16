import socket
from collections import deque
from copy import copy
from queue import Queue, Empty as QueueEmpty
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
                 virtual_host=None, port=None, pool=None, **kwargs):
        """init connection params"""
        self.hostname = hostname
        self.userid = userid
        self.password = password
        self.virtual_host = virtual_host or self.virtual_host
        self.port = port or self.port
        self.insist = kwargs.get("insist", self.insist)
        self.pool = pool
        self.connect_timeout = kwargs.get(
            "connect_timeout", self.connect_timeout)
        self.ssl = kwargs.get("ssl", self.ssl)
        self.backend_cls = kwargs.get("backend_cls", None)
        self._closed = None
        self._connection = None

    def __copy__(self):
        return self.__class__(self.hostname, self.userid, self.password,
                              self.virtual_host, self.port,
                              insist=self.insist,
                              connect_timeout=self.connect_timeout,
                              ssl=self.ssl,
                              backend_cls=self.backend_cls,
                              pool=self.pool)

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

    def drain_events(self, **kwargs):
        return self.connection.drain_events(**kwargs)

    def close(self):
        """Close the currently open connection."""
        try:
            if self._connection:
                backend = self.create_backend()
                backend.close_connection(self._connection)
        except socket.error:
            pass
        self._closed = True

    def release(self):
        if not self.pool:
            raise NotImplementedError(
                "Trying to release connection not part of a pool")
        self.pool.release(self)


class ConnectionLimitExceeded(Exception):
    """The maximum number of pool connections has been exceeded."""


class ConnectionPool(object):
    """connection pool by copy instance attributes"""

    def __init__(self, source_connection, min=2, max=None, preload=True):
        self.source_connection = source_connection
        self.min = min
        self.max = max
        self.preload = preload
        self.source_connection.pool = self

        self._connections = Queue()
        self._dirty = deque()

        self._connections.put(self.source_connection)
        for i in range(min - 1):
            self._connections.put_nowait(self._new_connection())

    def acquire(self, block=False, timeout=None, connect_timeout=None):
        try:
            conn = self._connections.get(block=block, timeout=timeout)
        except QueueEmpty:
            conn = self._new_connection()
        self._dirty.append(conn)
        if connect_timeout is not None:
            conn.connect_timeout = connect_timeout
        return conn

    def release(self, connection):
        self._dirty.remove(connection)
        self._connections.put_nowait(connection)

    def _new_connection(self):
        if len(self._dirty) >= self.max:
            raise ConnectionLimitExceeded(self.max)
        return copy(self.source_connection)


AMQPConnection = BrokerConnection
