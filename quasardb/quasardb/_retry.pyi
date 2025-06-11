import datetime

class RetryOptions:
    delay: datetime.timedelta
    exponent: int
    jitter: float
    retries_left: int
    def __init__(
        self,
        retries: int = 3,
        delay: datetime.timedelta = ...,
        exponent: int = 2,
        jitter: float = 0.1,
    ) -> None: ...
    def has_next(self) -> bool: ...
    def next(self) -> RetryOptions: ...
