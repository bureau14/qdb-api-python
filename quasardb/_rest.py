"""Standard-library HTTP transport and incremental Arrow IPC query conversion."""

from __future__ import annotations

import datetime
import http.client
import io
import json
import logging
import operator
import ssl
import weakref
from functools import partial
from types import TracebackType
from typing import Callable, Dict, Iterator, Optional, Tuple, Type
from urllib.parse import urlsplit

import numpy as np

from ._cluster import Cluster

try:
    import pyarrow as pa  # type: ignore[import-untyped]
except ImportError as exc:
    raise ImportError(
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


class _Response(io.RawIOBase):
    """Expose the REST server's HTTP response body as a readable RawIOBase stream.

    A short read with bytes outstanding in Content-Length signals a truncated
    response. Closing the stream releases both the response and its connection.
    """

    def __init__(
        self, connection: http.client.HTTPConnection, response: http.client.HTTPResponse
    ) -> None:
        super().__init__()
        self.connection = connection
        self._response = response
        self._require_arrow_eos = False
        self.status = response.status
        self.headers = response.headers

    def readable(self) -> bool:
        return True

    def open_arrow_stream(self) -> pa.RecordBatchReader:
        # Without HTTP framing, only Arrow's end marker proves the query finished.
        self._require_arrow_eos = (
            self._response.length is None and not self._response.chunked
        )
        return pa.ipc.open_stream(self)

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("read from a closed REST response")
        # HTTPResponse.read() honors Content-Length on a persistent connection.
        # Passing -1 explicitly can wait for the peer to close the socket.
        if size < 0:
            return self._response.read()
        data = self._response.read(size)
        # PyArrow stops on the end marker without requesting another message.
        # An empty read while it still expects bytes means this result is incomplete.
        if self._require_arrow_eos and size > 0 and not data:
            raise http.client.IncompleteRead(b"")
        # Bounded HTTP reads suppress IncompleteRead, and Arrow accepts EOF at a
        # batch boundary. Enforce HTTP framing so a dropped stream cannot look complete.
        if len(data) < size and self._response.length not in (None, 0):
            raise http.client.IncompleteRead(data, self._response.length)
        return data

    def close(self) -> None:
        if not self.closed:
            try:
                self._response.close()
            finally:
                self.connection.close()
                super().close()


def _credentials(
    user_name: str, user_private_key: str, user_security_file: str
) -> Dict[str, str]:
    """Build the REST login payload from a name and key, a file, or anonymous access."""
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


def _validate_bearer_token(token: object) -> None:
    """Require a nonempty token that can follow 'Bearer ' in the HTTP header."""
    if (
        not isinstance(token, str)
        or not token
        or not all(33 <= ord(c) <= 126 for c in token)
    ):
        raise ValueError


def _read_tokens(response: _Response) -> Tuple[str, str]:
    try:
        body = response.read()
        data = json.loads(body)
        access = data["access_token"]
        refresh = data["refresh_token"]
        _validate_bearer_token(access)
        # The refresh token travels in a JSON body and may contain Unicode.
        if not isinstance(refresh, str) or not refresh:
            raise ValueError
        return access, refresh
    except (OSError, http.client.HTTPException):
        failure = "Unable to read REST authentication response"
    except (ValueError, KeyError, TypeError) as exc:
        failure = "Malformed REST authentication response ({})".format(
            type(exc).__name__
        )
    # JSON and framing exceptions keep the response bytes. Raising outside
    # the handler keeps those tokens out of the exception chain.
    raise Error(failure)


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
        verify: bool = True,
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
        if enable_encryption and endpoint.scheme != "https":
            raise InvalidArgumentError("enable_encryption=True requires an HTTPS URI")
        if not isinstance(timeout, datetime.timedelta):
            raise TypeError("timeout must be a datetime.timedelta")
        seconds = timeout.total_seconds()
        if seconds <= 0:
            raise InvalidArgumentError("timeout must be positive")
        self._credentials = _credentials(
            user_name, user_private_key, user_security_file
        )
        self._uri = uri
        if not isinstance(verify, bool):
            raise TypeError("verify must be a bool")
        self._new_connection: Callable[[], http.client.HTTPConnection]
        if endpoint.scheme == "https":
            context = None
            if not verify:
                # A REST server can generate a self-signed certificate. Limit this
                # opt-out to the HTTPS connections opened by this cluster.
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            self._new_connection = partial(
                http.client.HTTPSConnection,
                endpoint.hostname,
                port,
                timeout=seconds,
                context=context,
            )
        else:
            self._new_connection = partial(
                http.client.HTTPConnection, endpoint.hostname, port, timeout=seconds
            )
        self._base_path = endpoint.path.rstrip("/") + "/api/v2"
        self._access_token = ""
        self._refresh_token = ""
        self._open = True
        # A partially consumed reader can still hold an HTTP response and socket.
        # Track open readers so cluster.close() releases those resources even if
        # the caller stops iterating early. A WeakSet lets the caller drop its
        # last reference to a reader without the cluster keeping that reader alive.
        self._streams: weakref.WeakSet[RestQueryReader] = weakref.WeakSet()
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

    def _check_open(self) -> None:
        if not self._open:
            raise InvalidHandleError("REST connection is closed")

    def _request(self, path: str, body: bytes, headers: Dict[str, str]) -> _Response:
        """Send one POST and transfer the connection and response to the caller."""
        try:
            connection = self._new_connection()
            connection.request(
                "POST",
                self._base_path + path,
                body=body,
                headers={"Accept-Encoding": "identity", **headers},
            )
            response = connection.getresponse()
            logger.debug(
                "REST request path=%s status=%s request_id=%s",
                path,
                response.status,
                response.getheader("X-Request-ID", ""),
            )
            return _Response(connection, response)
        except http.client.InvalidURL:
            raise InvalidArgumentError("Invalid REST endpoint")
        except http.client.BadStatusLine:
            # A malformed status line can carry arbitrary server response text.
            raise Error("Invalid REST HTTP response")
        except (OSError, http.client.HTTPException, ValueError) as exc:
            logger.debug(
                "REST request failed path=%s error=%s", path, type(exc).__name__
            )
            raise Error("REST HTTP request failed") from exc

    def _raise_http_error(self, response: _Response, *, query: bool = False) -> None:
        """Read the error response, redact credentials and raise the API exception."""
        status = response.status
        body = b""
        cause = None
        detail = ""
        try:
            body = response.read()
        except http.client.IncompleteRead:
            # A truncated credential cannot be redacted by matching its full value.
            # Keep the HTTP status and discard the partial body and exception.
            pass
        except (OSError, http.client.HTTPException) as exc:
            cause = exc
        try:
            data = json.loads(body)
            detail = data.get("detail", "") if isinstance(data, dict) else ""
            if not isinstance(detail, str):
                detail = ""
        except ValueError:
            # Query proxies may return plain text. A malformed JSON object can
            # end partway through a credential even when HTTP framing is complete.
            if query and not body.lstrip().startswith(b"{"):
                detail = body.decode("utf-8", errors="replace").strip()
        # A backend error can echo the CREATE SECRET statement containing the user's
        # private key. Redact it and the session tokens from the diagnostic.
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
        # A 503 indicates an unavailable service on any endpoint. A 400 maps
        # to InvalidQueryError for queries and to the generic Error for authentication.
        error_type = (
            TryAgainError
            if status == 503
            else InvalidQueryError if query and status == 400 else Error
        )
        raise error_type(message) from cause

    def _grant(self, path: str, payload: Dict[str, str]) -> bool:
        """Store the access and refresh tokens returned by login or refresh.

        Return False when refresh receives a 401 so the caller can log in again.
        """
        body = json.dumps(payload).encode("utf-8")
        with self._request(
            path, body, {"Content-Type": "application/json"}
        ) as response:
            # A refused refresh is the only authentication error that triggers login.
            if path == "/auth/refresh" and response.status == 401:
                logger.debug("REST token refresh refused; logging in")
                return False
            if response.status != 200:
                self._raise_http_error(response)
            tokens = _read_tokens(response)
        self._access_token, self._refresh_token = tokens
        return True

    def _login(self) -> None:
        logger.debug("REST login started")
        self._grant("/auth/login", self._credentials)

    def _post_query(self, query: str) -> _Response:
        """Submit the query and retry once if the server rejects the access token."""
        body = query.encode("utf-8")

        def post() -> _Response:
            return self._request(
                "/query",
                body,
                {
                    "Content-Type": "text/plain",
                    "Accept": "application/vnd.apache.arrow.stream",
                    "Authorization": "Bearer " + self._access_token,
                },
            )

        response = post()
        if response.status != 401:
            return response
        # An interrupted request may already have taken effect. Only a 401 is replayed.
        response.close()
        logger.debug("REST authentication expired; refreshing token")
        if not self._grant("/auth/refresh", {"refresh_token": self._refresh_token}):
            self._login()
        return post()

    def close(self) -> None:
        if not self._open and not self._streams:
            return
        self._open = False
        self._credentials.clear()
        self._access_token = ""
        self._refresh_token = ""
        # Closing a reader removes it from the weak set.
        for stream in list(self._streams):
            stream.close()
        logger.debug("REST connection closed")

    def __enter__(self) -> RestCluster:
        self._check_open()
        return self

    def is_open(self) -> bool:
        return self._open

    def uri(self) -> str:
        return self._uri

    def stream_query(self, query: str, *, batch_size: int = 0) -> RestQueryReader:
        """Enter the reader in a with block and iterate over nonempty server batches.

        Each iterator yields the remaining batches in order and stays exhausted
        at EOF. The REST reader is itself the iterator, so a second loop inside
        the same context continues from the current position.

        Arrow validity determines the mask. Valid NaN and INT64_MIN values are
        unmasked, and typed all-null columns retain their Arrow-derived dtype.
        The native reader masks those sentinel values and uses float64 when an
        entire result column is null.
        """
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


def _masked_array(array: pa.Array) -> MaskedArrayAny:
    """Convert an Arrow column to a NumPy masked array, masking only null rows."""
    if pa.types.is_dictionary(array.type):
        # A valid dictionary index can point to a null value.
        array = array.dictionary_decode()
    if pa.types.is_timestamp(array.type) and array.type.unit != "ns":
        # Arrow scales only valid timestamps and checks the datetime64[ns] range.
        # A null slot can contain arbitrary bits that overflow a raw NumPy cast.
        array = array.cast(pa.timestamp("ns", tz=array.type.tz), safe=True)
    count = len(array)
    # NumPy needs one boolean per row, so expand Arrow's bit-packed null mask.
    # A valid NaN stays unmasked.
    mask = array.is_null(nan_is_null=False).to_numpy(zero_copy_only=False)
    dtype = array.type
    if (
        pa.types.is_int64(dtype)
        or pa.types.is_float64(dtype)
        or pa.types.is_timestamp(dtype)
    ):
        if pa.types.is_timestamp(dtype):
            numpy_dtype = np.dtype("datetime64[ns]")
        else:
            numpy_dtype = np.dtype("float64" if pa.types.is_float64(dtype) else "int64")
        if count:
            # A slice shares its original values buffer. Arrow counts the offset
            # in rows and NumPy counts it in bytes.
            data = np.frombuffer(
                array.buffers()[1],
                dtype=numpy_dtype,
                count=count,
                offset=array.offset * numpy_dtype.itemsize,
            )
        else:
            # An empty column can have no values buffer to view.
            data = np.empty(0, dtype=numpy_dtype)
    elif dtype in (pa.string(), pa.large_string(), pa.binary(), pa.large_binary()):
        is_text = dtype in (pa.string(), pa.large_string())
        # Arrow materializes Python strings or bytes in an object array. Fill
        # its null slots before converting text so None cannot become "None".
        data = array.to_numpy(zero_copy_only=False, writable=True)
        data[mask] = "" if is_text else b""
        if is_text:
            data = data.astype("U")
    elif pa.types.is_null(dtype):
        # An untyped NULL expression has no physical dtype. Use the native fallback.
        data = np.zeros(count, dtype="float64")
    else:
        raise Error("Unsupported REST Arrow column type: {}".format(dtype))
    return np.ma.array(data, mask=mask, copy=False)


def _read_nonempty_batch(reader: pa.RecordBatchReader) -> pa.RecordBatch:
    while True:
        batch = reader.read_next_batch()
        if batch.num_rows:
            return batch


def _batch_to_masked_arrays(batch: pa.RecordBatch) -> Dict[str, MaskedArrayAny]:
    return {
        name: _masked_array(column)
        for name, column in zip(batch.schema.names, batch.columns)
    }


class RestQueryReader:
    def __init__(self, cluster: RestCluster, query: str) -> None:
        self._cluster = cluster
        self._query = query
        self._response: Optional[_Response] = None
        self._reader: Optional[pa.RecordBatchReader] = None
        self._entered = False

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def get_batch_size(self) -> int:
        """Match the native reader interface. Zero means the REST server sizes batches."""
        return 0

    def __enter__(self) -> RestQueryReader:
        if self._entered:
            raise InvalidArgumentError("REST query stream is already open")
        self._cluster._check_open()
        try:
            self._response = self._cluster._post_query(self._query)
            if self._response.status != 200:
                self._cluster._raise_http_error(self._response, query=True)
            self._reader = self._response.open_arrow_stream()
            self._entered = True
            self._cluster._streams.add(self)
            logger.debug("REST query stream opened")
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
        return self

    def __iter__(self) -> Iterator[Dict[str, MaskedArrayAny]]:
        if not self._entered:
            raise UninitializedError("Open the REST query stream in a with block")
        return self

    def __next__(self) -> Dict[str, MaskedArrayAny]:
        if not self._entered:
            raise UninitializedError("Open the REST query stream in a with block")
        if self._reader is None:
            raise StopIteration
        try:
            batch = _read_nonempty_batch(self._reader)
            return _batch_to_masked_arrays(batch)
        except StopIteration:
            # Release the socket at EOF. The iterator stays exhausted until context exit.
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

    def _release(self) -> None:
        try:
            if self._reader is not None:
                self._reader.close()
        finally:
            self._reader = None
            if self._response is not None:
                self._response.close()
                self._response = None
            # A failed close keeps the response registered for a subsequent close().
            self._cluster._streams.discard(self)

    def close(self) -> None:
        self._entered = False
        self._release()
