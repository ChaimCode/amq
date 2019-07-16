def _compat_rl_partition(S, sep, direction=None):
    if direction is None:
        direction = S.split
    items = direction(sep, 1)
    if len(items) == 1:
        return items[0], sep, ''
    return items[0], sep, items[1]


def _compat_partition(S, sep):
    """``partition(S, sep) -> (head, sep, tail)``
    Search for the separator ``sep`` in ``S``, and return the part before
    it, the separator itself, and the part after it. If the separator is not
    found, return ``S`` and two empty strings.
    """
    return _compat_rl_partition(S, sep, direction=S.split)


def _compat_rpartition(S, sep):
    """``rpartition(S, sep) -> (tail, sep, head)``
    Search for the separator ``sep`` in ``S``, starting at the end of ``S``,
    and return the part before it, the separator itself, and the part
    after it. If the separator is not found, return two empty
    strings and ``S``.
    """
    return _compat_rl_partition(S, sep, direction=S.rsplit)


def partition(S, sep):
    if hasattr(S, 'partition'):
        return S.partition(sep)
    else:
        return _compat_partition(S, sep)


def rpartition(S, sep):
    if hasattr(S, 'rpartition'):
        return S.rpartition(sep)
    else:
        return _compat_rpartition(S, sep)
