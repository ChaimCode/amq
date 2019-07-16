"""amq.backends"""
import sys
from functools import partial

DEFAULT_BACKEND = "pyamqplib"


def get_backend_cls(backend):
    """Get backend class by name.
    If the name does not include "``.``" (is not fully qualified),
    ``"amq.backends."`` will be prepended to the name. e.g.
    ``"pyqueue"`` becomes ``"amq.backends.pyqueue"``.
    """
    # usually backend = DEFAULT_BACKEND
    if backend.find(".") == -1:
        backend = f"amq.backends.{backend}"
    __import__(backend)
    backend_module = sys.modules[backend]
    return getattr(backend_module, "Backend")


"""
.. function:: get_default_backend_cls()
    Get the default backend class.
    Default is ``DEFAULT_BACKEND``.
"""
get_default_backend_cls = partial(get_backend_cls, DEFAULT_BACKEND)


"""
.. class:: DefaultBackend
    The default backend class.
    This is the class specified in ``DEFAULT_BACKEND``.
"""
DefaultBackend = get_default_backend_cls()
