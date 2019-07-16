"""amq.backends"""
import sys
from amq.utils import rpartition

DEFAULT_BACKEND = "amq.backends.pyamqplib.Backend"

BACKEND_ALIASES = {
    "amqp": "amq.backends.pyamqplib.Backend",
    "amqplib": "amq.backends.pyamqplib.Backend",
    "stomp": "amq.backends.pystomp.Backend",
    "stompy": "amq.backends.pystomp.Backend",
    "memory": "amq.backends.queue.Backend",
    "mem": "amq.backends.queue.Backend",
}

_backend_cache = {}


def resolve_backend(backend=None):
    backend = BACKEND_ALIASES.get(backend, backend)
    backend_module_name, _, backend_cls_name = rpartition(backend, ".")
    return backend_module_name, backend_cls_name


def _get_backend_cls(backend=None):
    backend_module_name, backend_cls_name = resolve_backend(backend)
    __import__(backend_module_name)
    backend_module = sys.modules[backend_module_name]
    return getattr(backend_module, backend_cls_name)


def get_backend_cls(backend=None):
    """Get backend class by name.
    If the name does not include "``.``" (is not fully qualified),
    ``"amq.backends."`` will be prepended to the name. e.g.
    ``"pyqueue"`` becomes ``"amq.backends.pyqueue"``.
    """
    backend = backend or DEFAULT_BACKEND
    if backend not in _backend_cache:
        _backend_cache[backend] = _get_backend_cls(backend)
    return _backend_cache[backend]
