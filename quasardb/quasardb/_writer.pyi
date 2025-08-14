from __future__ import annotations

from typing import Any, Iterable

from ._table import Table

class WriterData:
    def __init__(self) -> None: ...
    def append(
        self, table: Table, index: Iterable[Any], column_data: Iterable[Any]
    ) -> None: ...
    def empty(self) -> bool: ...

class WriterPushMode:
    Transactional: WriterPushMode  # value = <WriterPushMode.Transactional: 0>
    Truncate: WriterPushMode  # value = <WriterPushMode.Truncate: 1>
    Fast: WriterPushMode  # value = <WriterPushMode.Fast: 2>
    Async: WriterPushMode  # value = <WriterPushMode.Async: 3>
    __members__: dict[str, WriterPushMode]
    def __and__(self, other: Any) -> Any: ...
    def __eq__(self, other: Any) -> bool: ...
    def __ge__(self, other: Any) -> bool: ...
    def __getstate__(self) -> int: ...
    def __gt__(self, other: Any) -> bool: ...
    def __hash__(self) -> int: ...
    def __index__(self) -> int: ...
    def __init__(self, value: int) -> None: ...
    def __int__(self) -> int: ...
    def __invert__(self) -> Any: ...
    def __le__(self, other: Any) -> bool: ...
    def __lt__(self, other: Any) -> bool: ...
    def __ne__(self, other: Any) -> bool: ...
    def __or__(self, other: Any) -> Any: ...
    def __rand__(self, other: Any) -> Any: ...
    def __repr__(self) -> str: ...
    def __ror__(self, other: Any) -> Any: ...
    def __rxor__(self, other: Any) -> Any: ...
    def __setstate__(self, state: int) -> None: ...
    def __str__(self) -> str: ...
    def __xor__(self, other: Any) -> Any: ...
    @property
    def name(self) -> str: ...
    @property
    def value(self) -> int: ...

class Writer:
    def push(
        self,
        data: WriterData,
        write_through: bool,
        push_mode: WriterPushMode,
        deduplication_mode: str,
        deduplicate: str,
        retries: int,
        range: tuple[Any, ...],
        **kwargs: Any,
    ) -> None: ...
    def push_fast(
        self,
        data: WriterData,
        write_through: bool,
        push_mode: WriterPushMode,
        deduplication_mode: str,
        deduplicate: str,
        retries: int,
        range: tuple[Any, ...],
        **kwargs: Any,
    ) -> None:
        """Deprecated: Use `writer.push()` instead."""

    def push_async(
        self,
        data: WriterData,
        write_through: bool,
        push_mode: WriterPushMode,
        deduplication_mode: str,
        deduplicate: str,
        retries: int,
        range: tuple[Any, ...],
        **kwargs: Any,
    ) -> None:
        """Deprecated: Use `writer.push()` instead."""

    def push_truncate(
        self,
        data: WriterData,
        write_through: bool,
        push_mode: WriterPushMode,
        deduplication_mode: str,
        deduplicate: str,
        retries: int,
        range: tuple[Any, ...],
        **kwargs: Any,
    ) -> None:
        """Deprecated: Use `writer.push()` instead3."""

    def start_row(self, table: Any, x: Any) -> None:
        """Legacy function"""

    def set_double(self, idx: Any, value: Any) -> Any:
        """Legacy function"""

    def set_int64(self, idx: Any, value: Any) -> Any:
        """Legacy function"""

    def set_string(self, idx: Any, value: Any) -> Any:
        """Legacy function"""

    def set_blob(self, idx: Any, value: Any) -> Any:
        """Legacy function"""

    def set_timestamp(self, idx: Any, value: Any) -> Any:
        """Legacy function"""
