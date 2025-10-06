"""
Keep track of low-level performance metrics
"""

from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

__all__ = ["Measure", "clear", "totals"]

class Measure:
    """
    Track all metrics within a block of code
    """

    def __enter__(self) -> Measure: ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None: ...
    def __init__(self) -> None: ...
    def get(self) -> dict[str, int]: ...

def clear() -> None: ...
def totals() -> dict[str, int]: ...
