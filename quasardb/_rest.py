"""Standard-library HTTP transport and incremental Arrow IPC query conversion."""

from __future__ import annotations

import datetime
import http.client
import io
import json
import logging
import math
import operator
import threading
import weakref
from contextlib import ExitStack
from types import TracebackType
from typing import Dict, Iterator, Optional, Type
from urllib.parse import urlsplit

import numpy as np

from . import PyArrowRequired
from ._cluster import Cluster

try:
    import pyarrow as pa
except ImportError as exc:
    raise PyArrowRequired(
        "REST connections require PyArrow; install it with pip install 'quasardb[rest]'"
    ) from exc

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

_MAX_ERROR_DETAIL_BYTES = 16 * 1024
_MAX_AUTH_RESPONSE_BYTES = 64 * 1024


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

    _default_query_batch_size = 0

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
        # Readers own their sockets. Track live readers for close() without
        # extending their lifetime when the caller drops the last reference.
        self._streams = weakref.WeakSet()
        self._state_lock = threading.Lock()
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
        try:
            connection.request("POST", target, body=body, headers=headers)
            response = connection.getresponse()
            logger.debug(
                "REST request path=%s status=%s request_id=%s",
                path,
                response.status,
                response.getheader("X-Request-ID", ""),
            )
            return _Response(connection, response)
        except (OSError, http.client.HTTPException, ValueError) as exc:
            connection.close()
            logger.debug(
                "REST request failed path=%s error=%s", path, type(exc).__name__
            )
            raise Error("REST HTTP request failed") from exc

    def _raise_http_error(self, response, *, query=False):
        status = response.response.status
        body = b""
        cause = None
        detail = ""
        try:
            body = response.read(_MAX_ERROR_DETAIL_BYTES)
        except http.client.IncompleteRead as exc:
            # The exception holds the raw response. Only query diagnostics may
            # use partial text, after filtering it below.
            if query:
                body = exc.partial[:_MAX_ERROR_DETAIL_BYTES]
        except (OSError, http.client.HTTPException) as exc:
            cause = exc
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
        # SQL containing the user key. close() clears the values used for redaction.
        with self._state_lock:
            self._check_open()
            secrets = (
                self._credentials.get("secret_key"),
                self._access_token,
                self._refresh_token,
            )
        for secret in secrets:
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
        raise error_type(message) from cause

    def _grant(self, path, payload):
        response = self._request(path, json.dumps(payload).encode("utf-8"))
        if response.response.status != 200:
            # A refused refresh is the only authentication error that triggers login.
            if path == "/auth/refresh" and response.response.status == 401:
                response.close()
                logger.debug("REST token refresh refused; logging in")
                return False
            self._raise_http_error(response)
        failure = None
        try:
            body = response.read(_MAX_AUTH_RESPONSE_BYTES + 1)
            if len(body) > _MAX_AUTH_RESPONSE_BYTES:
                raise Error("REST authentication response exceeds the size limit")
            data = json.loads(body)
            access = data["access_token"]
            refresh = data["refresh_token"]
            # Bearer tokens travel in HTTP headers as printable ASCII without whitespace.
            if not all(
                isinstance(t, str) and t and all(33 <= ord(c) <= 126 for c in t)
                for t in (access, refresh)
            ):
                raise ValueError
        except (OSError, http.client.HTTPException):
            failure = "Unable to read REST authentication response"
        except (ValueError, KeyError, TypeError) as exc:
            failure = "Malformed REST authentication response ({})".format(
                type(exc).__name__
            )
        finally:
            response.close()
        if failure:
            # JSON and framing exceptions keep the response bytes. Raising outside
            # the handler keeps those tokens out of the exception chain.
            raise Error(failure)
        with self._state_lock:
            self._check_open()
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
                    .lower()
                )
                if content_type != "application/vnd.apache.arrow.stream":
                    response.close()
                    raise Error("REST query did not return an Arrow IPC stream")
                return response
            if response.response.status != 401 or attempt == 1:
                self._raise_http_error(response, query=True)
            response.close()
            logger.debug("REST query authentication expired; refreshing token")
            if not self._grant("/auth/refresh", {"refresh_token": self._refresh_token}):
                self._login()

    def close(self) -> None:
        # Mark the handle closed before taking the snapshot so in-flight grants
        # and reader opens cannot publish new state after close.
        with self._state_lock:
            if not self._open and not self._streams:
                return
            self._open = False
            self._credentials.clear()
            self._access_token = ""
            self._refresh_token = ""
            streams = list(self._streams)
        # Reader cleanup takes the state lock to unregister after finishing I/O.
        # Attempt every close even when one transport reports a cleanup error.
        with ExitStack() as cleanup:
            for stream in streams:
                cleanup.callback(stream.close)
        logger.debug("REST connection closed")

    def __enter__(self) -> RestCluster:
        self._check_open()
        return self

    def is_open(self) -> bool:
        return self._open

    def uri(self) -> str:
        return self._uri

    def stream_query(self, query: str, *, batch_size: int = 0) -> RestQueryReader:
        """Yield each nonempty server batch. A positive batch_size logs a warning."""
        self._check_open()
        if not isinstance(query, str):
            raise TypeError("query must be a string")
        batch_size = operator.index(batch_size)
        if batch_size < 0 or batch_size > np.iinfo(np.uintp).max:
            raise TypeError("batch_size must be a non-negative size_t integer")
        if batch_size:
            logger.warning(
                "REST ignores batch_size=%d and yields server batches", batch_size
            )
        return RestQueryReader(self, query)


def _masked_array(array) -> MaskedArrayAny:
    # Preserve Arrow's validity independently of NaN, NaT and integer sentinels.
    # 1. Decode dictionary values, retaining their null bitmap.
    # 2. Ask Arrow for a slice-aware null mask, without treating NaN as null.
    # 3. View numeric buffers or copy variable-width values into NumPy arrays.
    # 4. Attach the mask without copying numeric buffers; NumPy owns an Arrow reference.
    if pa.types.is_dictionary(array.type):
        array = array.dictionary_decode()
    if pa.types.is_timestamp(array.type) and array.type.unit != "ns":
        # Arrow scales only valid timestamps and checks the datetime64[ns] range.
        # A null slot can contain arbitrary bits that overflow a raw NumPy cast.
        array = array.cast(pa.timestamp("ns", tz=array.type.tz), safe=True)
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
            numpy_dtype = np.dtype("datetime64[ns]")
        values = array.buffers()[1]
        if count:
            data = np.frombuffer(
                values, dtype=numpy_dtype, count=count, offset=array.offset * 8
            )
        else:
            data = np.empty(0, dtype=numpy_dtype)
    elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        data = np.array(["" if x is None else x for x in array.to_pylist()], dtype="U")
    elif pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        data = np.array([b"" if x is None else x for x in array.to_pylist()], dtype="O")
    elif pa.types.is_null(dtype):
        # An untyped NULL expression has no physical dtype. Use the native fallback.
        data = np.zeros(count, dtype="float64")
    else:
        raise Error("Unsupported REST Arrow column type: {}".format(dtype))
    return np.ma.array(data, mask=mask, copy=False)


class RestQueryReader:
    def __init__(self, cluster: RestCluster, query: str) -> None:
        self._cluster = cluster
        self._query = query
        # PyArrow releases the GIL while reading. cluster.close() on another thread
        # must wait until reading and conversion finish before closing the Arrow
        # reader and HTTP response. Open and read errors call self.close() while
        # holding this lock, so cleanup on the same thread must be reentrant.
        self._io_lock = threading.RLock()
        self._response = None
        self._reader = None
        self._entered = False
        self._done = False

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def get_batch_size(self) -> int:
        """Zero indicates that the REST server chooses the batch size."""
        return 0

    def __enter__(self) -> RestQueryReader:
        with self._io_lock:
            return self._open()

    def _open(self) -> RestQueryReader:
        if self._entered:
            raise InvalidArgumentError("REST query stream is already open")
        self._cluster._check_open()
        try:
            self._response = self._cluster._query_response(self._query)
            self._reader = pa.ipc.open_stream(self._response)
            # close() may have run while reading the Arrow schema.
            with self._cluster._state_lock:
                self._cluster._check_open()
                self._entered = True
                self._done = False
                self._cluster._streams.add(self)
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
        logger.debug("REST query stream opened")
        return self

    def __iter__(self) -> Iterator[Dict[str, MaskedArrayAny]]:
        if not self._entered:
            raise UninitializedError("Open the REST query stream in a with block")
        return self

    def __next__(self) -> Dict[str, MaskedArrayAny]:
        with self._io_lock:
            return self._next_batch()

    def _next_batch(self) -> Dict[str, MaskedArrayAny]:
        if not self._entered:
            raise UninitializedError("Open the REST query stream in a with block")
        if self._done:
            raise StopIteration
        try:
            batch = self._reader.read_next_batch()
            while not batch.num_rows:
                batch = self._reader.read_next_batch()
            return {
                name: _masked_array(column)
                for name, column in zip(batch.schema.names, batch.columns)
            }
        except StopIteration:
            # Release the socket at EOF. The iterator stays exhausted until context exit.
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
        try:
            if self._reader is not None:
                self._reader.close()
        finally:
            self._reader = None
            if self._response is not None:
                self._response.close()
                self._response = None
            # A failed close keeps the response registered for a subsequent close().
            with self._cluster._state_lock:
                self._cluster._streams.discard(self)

    def close(self) -> None:
        with self._io_lock:
            self._entered = False
            self._done = True
            self._release()
