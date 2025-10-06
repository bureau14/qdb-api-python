from typing import Any, List

from .writer import extend_writer

__all__: List[Any] = []


def extend_module(m: Any) -> None:
    extend_writer(m.Writer)
