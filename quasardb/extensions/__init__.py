from .writer import extend_writer


__all__ = []


def extend_module(m):
    m.Writer = extend_writer(m.Writer)
