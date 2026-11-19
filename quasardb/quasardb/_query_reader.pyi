from __future__ import annotations

from types import TracebackType
from typing import Dict, Iterator, Optional, Type
from ..typing import MaskedArrayAny

class QueryReader:
    def __enter__(self) -> QueryReader: ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None: ...
    def __iter__(self) -> Iterator[Dict[str, MaskedArrayAny]]: ...
    def get_batch_size(self) -> int: ...
