# coding:utf8
import sys

# 默认转换类
DEFAULT_TRANSPORT = "exts.mq.transports.CMQ.Transport"

# 转换类别称
TRANSPORT_ALIASES = {
    "cmq": "exts.mq.transports.CMQ.Transport",
}

_transport_cache = {}

def resolve_transport(transport=None):
    """反解transport_module与transport_cls"""
    transport = TRANSPORT_ALIASES.get(transport, transport)
    transport_module_name, _, transport_cls_name = transport.rpartition(".")
    return transport_module_name, transport_cls_name


def get_transport_cls(transport=None):
    """获取transport_cls类"""
    transport = transport or DEFAULT_TRANSPORT
    if transport not in _transport_cache:
        transport_module_name, transport_cls_name = resolve_transport(transport)
        __import__(transport_module_name)
        transport_module = sys.modules[transport_module_name]
        _transport_cache[transport] = getattr(transport_module, transport_cls_name)
    return _transport_cache[transport]
