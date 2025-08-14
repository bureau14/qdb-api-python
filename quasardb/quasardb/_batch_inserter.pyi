from typing import Any

class TimeSeriesBatch:
    def push(self) -> None:
        """
        Regular batch push
        """

    def push_async(self) -> None:
        """
        Asynchronous batch push that buffers data inside the QuasarDB daemon
        """

    def push_fast(self) -> None:
        """
        Fast, in-place batch push that is efficient when doing lots of small, incremental pushes.
        """

    def push_truncate(self, **kwargs: Any) -> None:
        """
        Before inserting data, truncates any existing data. This is useful when you want your insertions to be idempotent, e.g. in case of a retry.
        """

    def set_blob(self, index: int, blob: bytes) -> None: ...
    def set_double(self, index: int, double: float) -> None: ...
    def set_int64(self, index: int, int64: int) -> None: ...
    def set_string(self, index: int, string: str) -> None: ...
    def set_timestamp(self, index: int, timestamp: Any) -> None: ...
    def start_row(self, ts: Any) -> None:
        """
        Calling this function marks the beginning of processing a new row.
        """
