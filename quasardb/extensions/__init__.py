from .pinned_writer import extend_pinned_writer


__all__ = []


def extend_module(m):
    m.PinnedWriter = extend_pinned_writer(m.PinnedWriter)
