"""Pooled HTTPX transport and incremental Arrow IPC query conversion."""

from __future__ import annotations

import datetime
import io
import json
import logging
import operator
import ssl
from contextlib import closing, contextmanager
from types import TracebackType
from typing import Dict, Generator, Iterator, Optional, Type
from urllib.parse import urlsplit

import numpy as np

try:
    import httpx
    import pyarrow as pa  # type: ignore[import-untyped]
except ImportError as exc:
    raise ImportError(
        "REST connections require HTTPX and PyArrow; install it with pip install 'quasardb[rest]'"
    ) from exc

from .quasardb import (
    Error,
    InvalidArgumentError,
    InvalidHandleError,
    InvalidQueryError,
    Options,
    TryAgainError,
)
from .typing import MaskedArrayAny

logger = logging.getLogger(__name__)


class _ArrowInput(io.RawIOBase):
    """Supply the sized reads that PyArrow's IPC stream reader requests.

    HTTPX yields chunks of arbitrary size. This adapter saves unused bytes from
    each chunk, and the standard library's io.BufferedReader combines short reads
    until PyArrow has the number of bytes it requested.

    If PyArrow exposes the C++ ipc::StreamDecoder, HTTPX chunks could be fed
    directly to that decoder. See https://github.com/apache/arrow/issues/37390.
    """

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = chunks
        self._pending = memoryview(b"")

    def readable(self) -> bool:
        return True

    def readinto(self, target: bytearray) -> int:
        """Copy up to len(target) bytes from one chunk and return the count copied."""
        if not target:
            return 0
        while not self._pending:
            chunk = next(self._chunks, None)
            if chunk is None:
                # A successful REST query ends with an explicit Arrow marker.
                # PyArrow accepts plain EOF between batches, so returning zero
                # here could silently deliver only part of a failed query.
                raise EOFError("Missing REST Arrow end-of-stream marker")
            self._pending = memoryview(chunk)
        # Slicing a memoryview keeps the unread suffix without copying its bytes.
        count = min(len(target), len(self._pending))
        target[:count] = self._pending[:count]
        self._pending = self._pending[count:]
        return count


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


class RestCluster:
    """REST query connection. Native-only options are accepted and ignored."""

    # REST uses the server's batches. Zero keeps the NumPy adapter's default
    # query from warning about a batch size the user never requested.
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
            # Accessing port validates its numeric value before opening the pool.
            _ = endpoint.port
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
        try:
            base_url = httpx.URL(
                endpoint._replace(path=endpoint.path.rstrip("/") + "/api/v2/").geturl()
            )
        except (httpx.InvalidURL, UnicodeError):
            raise InvalidArgumentError("Invalid REST endpoint") from None
        self._client = httpx.Client(
            base_url=base_url,
            timeout=seconds,
            trust_env=False,
            verify=(
                ssl.create_default_context()
                if verify and endpoint.scheme == "https"
                else verify
            ),
            headers={"Accept-Encoding": "identity"},
        )
        self._access_token = ""
        self._refresh_token = ""
        self._open = True
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

    def _request(
        self, path: str, body: bytes, headers: Dict[str, str]
    ) -> httpx.Response:
        """Send the POST and transfer the streaming response to the caller."""
        try:
            request = self._client.build_request(
                "POST", path.lstrip("/"), content=body, headers=headers
            )
            response = self._client.send(request, stream=True)
        except httpx.InvalidURL:
            failure = InvalidArgumentError("Invalid REST endpoint")
        except (httpx.HTTPError, ValueError) as exc:
            logger.debug(
                "REST request failed path=%s error=%s", path, type(exc).__name__
            )
            if isinstance(exc, httpx.ConnectError):
                raise Error("REST HTTP request failed") from exc
            # Protocol errors can quote raw response bytes. Raise outside the
            # handler to keep those bytes out of the exception chain.
            failure = Error("REST HTTP request failed")
        else:
            logger.debug("REST request path=%s status=%s", path, response.status_code)
            return response
        raise failure

    def _raise_http_error(
        self, response: httpx.Response, *, query: bool = False
    ) -> None:
        """Read the error response, redact credentials and raise the API exception."""
        status = response.status_code
        message = "REST HTTP {}".format(status)
        try:
            problem = json.loads(response.read())
        except (OSError, httpx.HTTPError, ValueError):
            # Partial bodies and parse exceptions can contain credentials.
            problem = None
        detail = problem.get("detail") if isinstance(problem, dict) else None
        if isinstance(detail, str) and detail:
            # Backend diagnostics can echo a private key or session token.
            for secret in (
                self._credentials.get("secret_key"),
                self._access_token,
                self._refresh_token,
            ):
                if secret:
                    detail = detail.replace(secret, "[redacted]")
            message += ": " + detail
        if status == 503:
            raise TryAgainError(message)
        if query and status == 400:
            raise InvalidQueryError(message)
        raise Error(message)

    def _grant(self, path: str, payload: Dict[str, str]) -> bool:
        """Store the access and refresh tokens returned by login or refresh.

        Return False when refresh receives a 401 so the caller can log in again.
        """
        body = json.dumps(payload).encode("utf-8")
        with closing(
            self._request(path, body, {"Content-Type": "application/json"})
        ) as response:
            if response.status_code != 200:
                # A refused refresh is the only authentication error that triggers login.
                if path == "/auth/refresh" and response.status_code == 401:
                    logger.debug("REST token refresh refused; logging in")
                    return False
                self._raise_http_error(response)

            try:
                tokens = json.loads(response.read())
                self._access_token, self._refresh_token = (
                    tokens["access_token"],
                    tokens["refresh_token"],
                )
                return True
            except (OSError, httpx.HTTPError, ValueError, KeyError, TypeError):
                # Decoder exceptions can retain the response containing both tokens.
                pass
            raise Error("Unable to read REST authentication response")

    def _login(self) -> None:
        logger.debug("REST login started")
        self._grant("/auth/login", self._credentials)

    def _post_query(self, query: str) -> httpx.Response:
        """Submit the query and retry once if the server rejects the access token."""
        body = query.encode("utf-8")

        def post() -> httpx.Response:
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
        if response.status_code != 401:
            return response
        # An interrupted request may already have taken effect. Only a 401 is replayed.
        response.close()
        logger.debug("REST authentication expired; refreshing token")
        if not self._grant("/auth/refresh", {"refresh_token": self._refresh_token}):
            self._login()
        return post()

    def close(self) -> None:
        if not self._open:
            return
        self._open = False
        self._credentials.clear()
        self._access_token = ""
        self._refresh_token = ""
        self._client.close()
        logger.debug("REST connection closed")

    def __enter__(self) -> RestCluster:
        self._check_open()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def is_open(self) -> bool:
        return self._open

    def uri(self) -> str:
        return self._uri

    @contextmanager
    def stream_query(
        self, query: str, *, batch_size: int = 0
    ) -> Iterator[Iterator[Dict[str, MaskedArrayAny]]]:
        """Enter the reader in a with block and iterate over nonempty server batches.

        Exit the stream's with-block before closing the cluster. The block closes
        the HTTP response and Arrow reader. Iteration resumes from the current
        position and stays exhausted at EOF.

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
            # TODO: Remove this warning once the REST API honors positive batch sizes.
            logger.warning(
                "REST ignores batch_size=%d and yields server batches", batch_size
            )

        with closing(self._post_query(query)) as response:
            if response.status_code != 200:
                self._raise_http_error(response, query=True)
            chunks = response.iter_raw()
            with io.BufferedReader(_ArrowInput(chunks)) as source:
                with pa.ipc.open_stream(source) as reader, closing(
                    _stream_batches(reader, response, chunks)
                ) as batches:
                    logger.debug("REST query stream opened")
                    yield batches


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
        # A slice shares its original values buffer. Arrow counts the offset
        # in rows and NumPy counts it in bytes.
        data = np.frombuffer(
            array.buffers()[1],
            dtype=numpy_dtype,
            count=count,
            offset=array.offset * numpy_dtype.itemsize,
        )
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


def _batch_to_masked_arrays(batch: pa.RecordBatch) -> Dict[str, MaskedArrayAny]:
    return {
        name: _masked_array(column)
        for name, column in zip(batch.schema.names, batch.columns)
    }


def _stream_batches(
    reader: pa.RecordBatchReader,
    response: httpx.Response,
    chunks: Iterator[bytes],
) -> Generator[Dict[str, MaskedArrayAny], None, None]:
    try:
        for batch in reader:
            if batch.num_rows:
                yield _batch_to_masked_arrays(batch)
        # Arrow stops at its marker. Finish an explicitly framed HTTP body
        # so HTTPX checks its terminator and can reuse the socket.
        headers = response.headers
        if (
            "content-length" in headers
            or "chunked" in headers.get("transfer-encoding", "").lower()
        ):
            for _ in chunks:
                pass
    finally:
        # A caller may catch a read error and stay inside the stream context.
        response.close()
