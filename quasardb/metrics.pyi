"""
Keep track of low-level performance metrics
"""

from __future__ import annotations

import typing

__all__ = ["Measure", "clear", "totals"]

class Measure:
    """
    Track all metrics within a block of code
    """

    def __enter__(self) -> Measure: ...
    def __exit__(
        self, arg0: typing.Any, arg1: typing.Any, arg2: typing.Any
    ) -> None: ...
    def __init__(self) -> None: ...
    def get(self) -> dict[str, int]: ...

def clear() -> None: ...
def totals() -> dict[str, int]: ...
