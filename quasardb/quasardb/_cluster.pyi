from __future__ import annotations

import datetime

from .._cluster import Cluster
from ._options import Options
from ._query_reader import QueryReader

class NativeCluster(Cluster):
    def __init__(
        self,
        uri: str,
        user_name: str = ...,
        user_private_key: str = ...,
        cluster_public_key: str = ...,
        *,
        user_security_file: str = ...,
        cluster_public_key_file: str = ...,
        timeout: datetime.timedelta = ...,
        do_version_check: bool = ...,
        enable_encryption: bool = ...,
        compression_mode: Options.Compression = ...,
        client_max_parallelism: int = ...,
    ) -> None: ...
    def __enter__(self) -> NativeCluster: ...
    def stream_query(self, query: str, *, batch_size: int = 0) -> QueryReader: ...
