class BatchColumnInfo:
    column: str
    elements_count_hint: int
    timeseries: str
    def __init__(self, ts_name: str, col_name: str, size_hint: int = 0) -> None: ...
