"""Standard-library HTTP transport and incremental Arrow IPC query conversion."""

from __future__ import annotations

import datetime
import http.client
import io
import json
import logging
import math
import operator
import time
import weakref
from types import TracebackType
from typing import Dict, Iterator, Optional, Type
from urllib.parse import urlsplit

import numpy as np

try:
    import pyarrow as pa
except ImportError as exc:
    raise ImportError(
        "REST connections require PyArrow; install it with pip install 'quasardb[rest]'"
    ) from exc

from ._cluster import Cluster, QueryStream
from .quasardb import (
    Error,
    InvalidArgumentError,
    InvalidHandleError,
    InvalidQueryError,
    Options,
    TryAgainError,
    UninitializedError,
)
from .typing import MaskedArrayAny

logger = logging.getLogger(__name__)


class _Response(io.RawIOBase):
    """Keep the socket and HTTP response under the Arrow reader's lifetime."""

    def __init__(self, connection, response):
        super().__init__()
        self.connection = connection
        self.response = response

    def readable(self):
        return True

    def read(self, size=-1):
        if self.closed:
            raise ValueError("read from a closed REST response")
        data = self.response.read(size)
        # Bounded HTTP reads suppress IncompleteRead, and Arrow accepts EOF at a
        # batch boundary. Enforce HTTP framing so a dropped stream cannot look complete.
        if size >= 0 and len(data) < size and self.response.length not in (None, 0):
            raise http.client.IncompleteRead(data, self.response.length)
        return data

    def close(self):
        if not self.closed:
            try:
                self.response.close()
            finally:
                self.connection.close()
                super().close()


def _credentials(user_name, user_private_key, user_security_file):
    if not all(
        isinstance(x, str) for x in (user_name, user_private_key, user_security_file)
    ):
        raise TypeError("REST credentials and security file must be strings")
    if user_security_file:
        if user_name or user_private_key:
            raise InvalidArgumentError(
                "Provide either user credentials or user_security_file"
            )
        try:
            with open(user_security_file, encoding="utf-8") as source:
                data = json.load(source)
            user_name = data["username"]
            user_private_key = data["secret_key"]
        except (OSError, ValueError, KeyError, TypeError):
            raise InvalidArgumentError(
                "Unable to read a valid user security file"
            ) from None
        if not isinstance(user_name, str) or not isinstance(user_private_key, str):
            raise InvalidArgumentError(
                "Security file username and secret_key must be strings"
            )
        if not user_name or not user_private_key:
            raise InvalidArgumentError(
                "Security file username and secret_key must be nonempty"
            )
    if bool(user_name) != bool(user_private_key):
        raise InvalidArgumentError("Provide both user_name and user_private_key")
    return {"username": user_name, "secret_key": user_private_key} if user_name else {}


class RestCluster(Cluster):
    """REST query connection. Native-only options are accepted and ignored."""

    def __init__(
        self,
        uri: str,
        user_name: str = "",
        user_private_key: str = "",
        cluster_public_key: str = "",
        *,
        user_security_file: str = "",
        cluster_public_key_file: str = "",
        timeout: datetime.timedelta = datetime.timedelta(minutes=1),
        do_version_check: bool = False,
        enable_encryption: bool = False,
        compression_mode: Options.Compression = Options.Compression.Balanced,
        client_max_parallelism: int = 0,
    ) -> None:
        if not isinstance(uri, str):
            raise TypeError("uri must be a string")
        try:
            endpoint = urlsplit(uri)
            port = endpoint.port
            if (
                endpoint.scheme not in ("http", "https")
                or not endpoint.hostname
                or endpoint.username is not None
                or endpoint.password is not None
                or endpoint.query
                or endpoint.fragment
            ):
                raise ValueError
        except ValueError:
            raise InvalidArgumentError(
                "Expected an HTTP(S) URI without credentials or query"
            ) from None
        seconds = (
            timeout.total_seconds()
            if isinstance(timeout, datetime.timedelta)
            else float(timeout)
        )
        if not math.isfinite(seconds) or seconds <= 0:
            raise InvalidArgumentError("timeout must be positive and finite")
        self._credentials = _credentials(
            user_name, user_private_key, user_security_file
        )
        self._uri = uri
        self._endpoint = endpoint
        self._port = port
        self._timeout = seconds
        self._access_token = ""
        self._refresh_token = ""
        self._open = True
        self._streams = weakref.WeakSet()
        try:
            self._login()
        except BaseException:
            self.close()
            raise
        logger.debug(
            "REST connection opened host=%s tls=%s",
            endpoint.hostname,
            endpoint.scheme == "https",
        )

    def _check_open(self):
        if not self._open:
            raise InvalidHandleError("REST connection is closed")

    def _request(self, path, body, content_type="application/json", token=""):
        self._check_open()
        # Per-request sockets avoid a custom HTTP pool. quasardb.pool reuses this
        # logical connection and its authentication, with TCP/TLS setup per request.
        # HTTPSConnection uses the standard certificate and hostname verification.
        connection_type = (
            http.client.HTTPSConnection
            if self._endpoint.scheme == "https"
            else http.client.HTTPConnection
        )
        connection = connection_type(
            self._endpoint.hostname, self._port, timeout=self._timeout
        )
        headers = {"Content-Type": content_type, "Accept-Encoding": "identity"}
        if path == "/query":
            headers["Accept"] = "application/vnd.apache.arrow.stream"
        if token:
            headers["Authorization"] = "Bearer " + token
        target = self._endpoint.path.rstrip("/") + "/api/v2" + path
        started = time.monotonic()
        try:
            connection.request("POST", target, body=body, headers=headers)
            response = connection.getresponse()
            logger.debug(
                "REST request path=%s status=%s duration=%.3fs request_id=%s",
                path,
                response.status,
                time.monotonic() - started,
                response.getheader("X-Request-ID", ""),
            )
            return _Response(connection, response)
        except (OSError, http.client.HTTPException, ValueError) as exc:
            connection.close()
            logger.debug(
                "REST request failed path=%s duration=%.3fs error=%s",
                path,
                time.monotonic() - started,
                type(exc).__name__,
            )
            raise Error("REST HTTP request failed") from exc

    def _error(self, response, *, query=False):
        status = response.response.status
        body = b""
        cause = None
        detail = ""
        try:
            body = response.read(16384)
        except (OSError, http.client.HTTPException) as exc:
            cause = exc
            if query and isinstance(exc, http.client.IncompleteRead):
                body = exc.partial[:16384]
        finally:
            response.close()
        try:
            data = json.loads(body)
            detail = data.get("detail", "") if isinstance(data, dict) else ""
            if not isinstance(detail, str):
                detail = ""
        except ValueError:
            # Query proxies may return plain text or truncated JSON. Authentication
            # bodies can contain tokens, so only queries expose this bounded fallback.
            if query:
                detail = body.decode("utf-8", errors="replace").strip()
        # Backend diagnostics pass through unchanged and may include CREATE SECRET
        # SQL containing the user key. Filter known secrets before exposing details.
        for secret in (
            self._credentials.get("secret_key"),
            self._access_token,
            self._refresh_token,
        ):
            if secret:
                detail = detail.replace(secret, "[redacted]")
        message = "REST HTTP {}".format(status)
        if detail:
            message += ": " + detail
        error_type = (
            TryAgainError
            if status == 503
            else InvalidQueryError if query and status == 400 else Error
        )
        error = error_type(message)
        # Auth recovery can handle a refusal without raising; terminal callers still
        # need the original transport failure when reading that error response failed.
        error.__cause__ = cause
        return error

    def _grant(self, path, payload):
        response = self._request(path, json.dumps(payload).encode("utf-8"))
        if response.response.status != 200:
            # A refused refresh is the only authentication error that triggers login.
            refused = path == "/auth/refresh" and response.response.status == 401
            error = self._error(response)
            if refused:
                logger.debug("REST token refresh refused; logging in")
                return False
            raise error
        try:
            data = json.loads(response.read(65536))
            access = data["access_token"]
            refresh = data["refresh_token"]
            if not all(
                isinstance(t, str) and t and all(33 <= ord(c) <= 126 for c in t)
                for t in (access, refresh)
            ):
                raise ValueError
        except (OSError, http.client.HTTPException) as exc:
            raise Error("Unable to read REST authentication response") from exc
        except (ValueError, KeyError, TypeError) as exc:
            # JSON parse errors retain the authentication body, which can contain
            # tokens. Preserve only the exception type in this sensitive path.
            raise Error(
                "Malformed REST authentication response ({})".format(type(exc).__name__)
            ) from None
        finally:
            response.close()
        self._access_token, self._refresh_token = access, refresh
        return True

    def _login(self):
        logger.debug("REST login started")
        self._grant("/auth/login", self._credentials)

    def _query_response(self, query):
        # Recover authentication before returning a body; a partially read query is
        # never resubmitted, since SQL may have side effects or already yielded rows.
        for attempt in range(2):
            response = self._request(
                "/query", query.encode("utf-8"), "text/plain", self._access_token
            )
            if response.response.status == 200:
                content_type = (
                    response.response.getheader("Content-Type", "")
                    .split(";")[0]
                    .strip()
                )
                if content_type != "application/vnd.apache.arrow.stream":
                    response.close()
                    raise Error("REST query did not return an Arrow IPC stream")
                return response
            if response.response.status != 401 or attempt == 1:
                raise self._error(response, query=True)
            response.close()
            logger.debug("REST query authentication expired; refreshing token")
            if not self._grant("/auth/refresh", {"refresh_token": self._refresh_token}):
                self._login()

    def close(self) -> None:
        if not self._open:
            return
        self._open = False
        try:
            for stream in list(self._streams):
                stream.close()
        finally:
            self._credentials.clear()
            self._access_token = ""
            self._refresh_token = ""
            logger.debug("REST connection closed")

    def __enter__(self) -> RestCluster:
        self._check_open()
        return self

    def is_open(self) -> bool:
        return self._open

    def uri(self) -> str:
        return self._uri

    def stream_query(self, query: str, *, batch_size: int = 0) -> RestQueryReader:
        """Read Arrow batches incrementally; batch_size is a maximum, 0 keeps wire batches.

        Slices retain their source batch buffers. Wire batch size sets the minimum
        memory footprint, and retaining yielded arrays keeps those buffers alive.
        """
        self._check_open()
        if not isinstance(query, str):
            raise TypeError("query must be a string")
        batch_size = operator.index(batch_size)
        if batch_size < 0 or batch_size > np.iinfo(np.uintp).max:
            raise TypeError("batch_size must be a non-negative size_t integer")
        return RestQueryReader(self, query, batch_size)


def _masked_array(array) -> MaskedArrayAny:
    # Preserve Arrow's validity independently of NaN, NaT and integer sentinels.
    # 1. Decode dictionary values, retaining their null bitmap.
    # 2. Ask Arrow for a slice-aware null mask, without treating NaN as null.
    # 3. View numeric buffers or copy variable-width values into NumPy arrays.
    # 4. Attach the mask without copying numeric buffers; NumPy owns an Arrow reference.
    if pa.types.is_dictionary(array.type):
        array = array.dictionary_decode()
    count = len(array)
    mask = array.is_null(nan_is_null=False).to_numpy(zero_copy_only=False)
    dtype = array.type
    if (
        pa.types.is_int64(dtype)
        or pa.types.is_float64(dtype)
        or pa.types.is_timestamp(dtype)
    ):
        numpy_dtype = np.dtype("float64" if pa.types.is_float64(dtype) else "int64")
        if pa.types.is_timestamp(dtype):
            numpy_dtype = np.dtype("datetime64[{}]".format(dtype.unit))
        values = array.buffers()[1]
        if count:
            data = np.frombuffer(
                values, dtype=numpy_dtype, count=count, offset=array.offset * 8
            )
        else:
            data = np.empty(0, dtype=numpy_dtype)
        if pa.types.is_timestamp(dtype) and dtype.unit != "ns":
            # s/ms/us timestamps (including DuckDB TIMESTAMP) need a copying scale
            # conversion for the public datetime64[ns] dtype; ns stays a view.
            data = data.astype("datetime64[ns]")
    elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        data = np.array(["" if x is None else x for x in array.to_pylist()], dtype="U")
    elif pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        data = np.array([b"" if x is None else x for x in array.to_pylist()], dtype="O")
    elif pa.types.is_null(dtype):
        data = np.zeros(count, dtype="float64")
    else:
        raise Error("Unsupported REST Arrow column type: {}".format(dtype))
    return np.ma.array(data, mask=mask, copy=False)


class RestQueryReader(QueryStream):
    def __init__(self, cluster: RestCluster, query: str, batch_size: int) -> None:
        self._cluster = cluster
        self._query = query
        self._batch_size = batch_size
        self._response = None
        self._reader = None
        self._batch = None
        self._offset = 0
        self._entered = False
        self._done = False
        self._started = None
        self._wire_batches = 0
        self._yielded_batches = 0
        self._yielded_rows = 0

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def get_batch_size(self) -> int:
        return self._batch_size

    def __enter__(self) -> RestQueryReader:
        if self._entered:
            raise InvalidArgumentError("REST query stream is already open")
        self._cluster._check_open()
        self._started = time.monotonic()
        self._wire_batches = self._yielded_batches = self._yielded_rows = 0
        try:
            self._response = self._cluster._query_response(self._query)
            self._reader = pa.ipc.open_stream(self._response)
        except (
            OSError,
            http.client.HTTPException,
            pa.ArrowException,
            ValueError,
        ) as exc:
            self.close()
            raise Error("Unable to open REST Arrow stream") from exc
        except BaseException:
            self.close()
            raise
        self._entered = True
        self._done = False
        self._cluster._streams.add(self)
        logger.debug("REST query stream opened batch_size=%s", self._batch_size)
        return self

    def __iter__(self) -> Iterator[Dict[str, MaskedArrayAny]]:
        if not self._entered:
            raise UninitializedError("Open the REST query stream in a with block")
        return self

    def __next__(self) -> Dict[str, MaskedArrayAny]:
        if not self._entered:
            raise UninitializedError("Open the REST query stream in a with block")
        if self._done:
            raise StopIteration
        try:
            while self._batch is None or self._offset == self._batch.num_rows:
                self._batch = self._reader.read_next_batch()
                self._wire_batches += 1
                self._offset = 0
                if self._batch.num_rows:
                    break
            size = self._batch.num_rows - self._offset
            if self._batch_size:
                size = min(size, self._batch_size)
            # batch_size caps conversion size; smaller wire batches are never combined.
            # Slicing shares buffers and cannot reduce memory held by a received batch.
            # Retained arrays therefore retain that entire batch's buffers after close.
            batch = self._batch.slice(self._offset, size)
            self._offset += size
            result = {
                name: _masked_array(column)
                for name, column in zip(batch.schema.names, batch.columns)
            }
            if self._offset == self._batch.num_rows:
                self._batch = None
                self._offset = 0
            self._yielded_batches += 1
            self._yielded_rows += size
            return result
        except StopIteration:
            self._done = True
            self._release()
            raise
        except (
            OSError,
            http.client.HTTPException,
            pa.ArrowException,
            ValueError,
        ) as exc:
            self.close()
            raise Error("REST Arrow stream is invalid or interrupted") from exc
        except BaseException:
            self.close()
            raise

    def _release(self):
        self._batch = None
        try:
            if self._reader is not None:
                self._reader.close()
        finally:
            self._reader = None
            if self._response is not None:
                self._response.close()
                self._response = None
            self._cluster._streams.discard(self)
            if self._started is not None:
                logger.debug(
                    "REST query stream closed wire_batches=%s yielded_batches=%s rows=%s duration=%.3fs",
                    self._wire_batches,
                    self._yielded_batches,
                    self._yielded_rows,
                    time.monotonic() - self._started,
                )
                self._started = None

    def close(self) -> None:
        self._entered = False
        self._done = True
        self._release()
