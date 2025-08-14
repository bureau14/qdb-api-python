from typing import Any

from .writer import extend_writer

__all__: list[Any] = []


def extend_module(m: Any) -> None:
    extend_writer(m.Writer)
