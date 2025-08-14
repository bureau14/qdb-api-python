from __future__ import annotations

from types import TracebackType
from typing import Any, Iterator, Optional, Type

class Reader:
    def __enter__(self) -> Reader: ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None: ...
    def __iter__(self) -> Iterator[dict[str, Any]]: ...
    def get_batch_size(self) -> int: ...
