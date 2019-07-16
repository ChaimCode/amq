"""amq.backends"""
import sys

DEFAULT_BACKEND = "pyamqplib"
BACKEND_ALIASES = {
    "amqp": "pyamqplib",
    "amqplib": "pyamqplib",
    "stomp": "pystomp",
    "stompy": "pystomp",
    "memory": "queue",
    "mem": "queue",
}


def get_backend_cls(backend):
    """Get backend class by name.
    If the name does not include "``.``" (is not fully qualified),
    ``"amq.backends."`` will be prepended to the name. e.g.
    ``"pyqueue"`` becomes ``"amq.backends.pyqueue"``.
    """
    if not backend:
        backend = DEFAULT_BACKEND
    
    # usually backend = DEFAULT_BACKEND
    if backend.find(".") == -1:
        alias_to = BACKEND_ALIASES.get(backend.lower(), None)
        backend = f"amq.backends.{alias_to or backend}"
    __import__(backend)
    backend_module = sys.modules[backend]
    return getattr(backend_module, "Backend")
