from __future__ import annotations

from ._table import Table

class WriterData:
    def __init__(self) -> None: ...
    def append(
        self, table: Table, index: list[object], column_data: list[list[object]]
    ) -> None: ...
    def empty(self) -> bool: ...

class WriterPushMode:
    Transactional: WriterPushMode  # value = <WriterPushMode.Transactional: 0>
    Truncate: WriterPushMode  # value = <WriterPushMode.Truncate: 1>
    Fast: WriterPushMode  # value = <WriterPushMode.Fast: 2>
    Async: WriterPushMode  # value = <WriterPushMode.Async: 3>
    __members__: dict[str, WriterPushMode]
    def __and__(self, other: object) -> object: ...
    def __eq__(self, other: object) -> bool: ...
    def __ge__(self, other: object) -> bool: ...
    def __getstate__(self) -> int: ...
    def __gt__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __index__(self) -> int: ...
    def __init__(self, value: int) -> None: ...
    def __int__(self) -> int: ...
    def __invert__(self) -> object: ...
    def __le__(self, other: object) -> bool: ...
    def __lt__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __or__(self, other: object) -> object: ...
    def __rand__(self, other: object) -> object: ...
    def __repr__(self) -> str: ...
    def __ror__(self, other: object) -> object: ...
    def __rxor__(self, other: object) -> object: ...
    def __setstate__(self, state: int) -> None: ...
    def __str__(self) -> str: ...
    def __xor__(self, other: object) -> object: ...
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
        range: tuple[object, ...],
        **kwargs,
    ) -> None: ...
    def push_fast(
        self,
        data: WriterData,
        write_through: bool,
        push_mode: WriterPushMode,
        deduplication_mode: str,
        deduplicate: str,
        retries: int,
        range: tuple[object, ...],
        **kwargs,
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
        range: tuple[object, ...],
        **kwargs,
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
        range: tuple[object, ...],
        **kwargs,
    ) -> None:
        """Deprecated: Use `writer.push()` instead3."""

    def start_row(self, table: object, x: object) -> None:
        """Legacy function"""

    def set_double(self, idx: object, value: object) -> object:
        """Legacy function"""

    def set_int64(self, idx: object, value: object) -> object:
        """Legacy function"""

    def set_string(self, idx: object, value: object) -> object:
        """Legacy function"""

    def set_blob(self, idx: object, value: object) -> object:
        """Legacy function"""

    def set_timestamp(self, idx: object, value: object) -> object:
        """Legacy function"""
