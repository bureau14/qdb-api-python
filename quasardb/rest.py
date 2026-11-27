"""Pooled HTTPX transport and incremental Arrow IPC query conversion."""

from __future__ import annotations

import datetime
import io
import json
import logging
import ssl
from contextlib import contextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Dict, Iterator, Optional, Type
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

if TYPE_CHECKING:
    from typing_extensions import Buffer

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

    def readinto(self, target: Buffer) -> int:
        """Copy up to len(target) bytes from one chunk and return the count copied."""
        view = memoryview(target).cast("B")
        if not view:
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
        count = min(len(view), len(self._pending))
        view[:count] = self._pending[:count]
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
    if not user_security_file:
        if bool(user_name) != bool(user_private_key):
            raise InvalidArgumentError("Provide both user_name and user_private_key")
        return (
            {"username": user_name, "secret_key": user_private_key} if user_name else {}
        )

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
    if not all(
        isinstance(value, str) and value for value in (user_name, user_private_key)
    ):
        raise InvalidArgumentError("Security file credentials must be nonempty strings")
    return {"username": user_name, "secret_key": user_private_key}


def _rest_base_url(uri: str, enable_encryption: bool) -> httpx.URL:
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
    try:
        return httpx.URL(
            endpoint._replace(path=endpoint.path.rstrip("/") + "/api/v2/").geturl()
        )
    except (httpx.InvalidURL, UnicodeError):
        raise InvalidArgumentError("Invalid REST endpoint") from None


class RestCluster:
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
        base_url = _rest_base_url(uri, enable_encryption)
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
        self._client = httpx.Client(
            base_url=base_url,
            timeout=seconds,
            trust_env=False,
            verify=(
                ssl.create_default_context()
                if verify and base_url.scheme == "https"
                else verify
            ),
            headers={"Accept-Encoding": "identity"},
        )
        self._access_token = ""
        self._refresh_token = ""
        try:
            self._authenticate()
        except BaseException:
            self.close()
            raise
        logger.debug(
            "REST connection opened host=%s tls=%s",
            base_url.host,
            base_url.scheme == "https",
        )

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

    def _authenticate(self, *, refresh: bool = False) -> None:
        """Refresh on request, falling back to login only when refresh is refused."""
        if refresh:
            response = self._client.post(
                "auth/refresh", json={"refresh_token": self._refresh_token}
            )
            if response.status_code == 401:
                response = self._client.post("auth/login", json=self._credentials)
        else:
            response = self._client.post("auth/login", json=self._credentials)
        if response.status_code != 200:
            self._raise_http_error(response)

        try:
            tokens = response.json()
            access, renewal = tokens["access_token"], tokens["refresh_token"]
            if (
                isinstance(access, str)
                and access
                and isinstance(renewal, str)
                and renewal
            ):
                self._access_token, self._refresh_token = access, renewal
                return
        except (ValueError, KeyError, TypeError):
            # Decoder exceptions can retain the response containing both tokens.
            pass
        raise Error("Unable to read REST authentication response")

    @contextmanager
    def _post_query(self, query: str) -> Iterator[httpx.Response]:
        """Close the first unauthorized response before refreshing and retrying once."""
        body = query.encode("utf-8")
        for attempt in range(2):
            headers = {
                "Content-Type": "text/plain",
                "Accept": "application/vnd.apache.arrow.stream",
                "Authorization": "Bearer " + self._access_token,
            }
            with self._client.stream(
                "POST", "query", content=body, headers=headers
            ) as response:
                if response.status_code == 200:
                    yield response
                    return
                if response.status_code != 401 or attempt:
                    self._raise_http_error(response, query=True)
            # An interrupted query may already have taken effect. Only a 401 is replayed.
            self._authenticate(refresh=True)

    def close(self) -> None:
        if self._client.is_closed:
            return
        self._credentials.clear()
        self._access_token = ""
        self._refresh_token = ""
        self._client.close()
        logger.debug("REST connection closed")

    def __enter__(self) -> RestCluster:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def is_open(self) -> bool:
        return not self._client.is_closed

    def uri(self) -> str:
        return self._uri

    @contextmanager
    def stream_query(
        self, query: str, *, batch_size: int = 0
    ) -> Iterator[Iterator[Dict[str, MaskedArrayAny]]]:
        """Enter the reader in a with block and iterate over nonempty server batches.

        Consume batches inside the with-block, which owns the HTTP response and
        Arrow reader. Yielded arrays remain valid after the block exits.

        Arrow validity determines the mask. Valid NaN and INT64_MIN values are
        unmasked, and typed all-null columns retain their Arrow-derived dtype.
        The native reader masks those sentinel values and uses float64 when an
        entire result column is null.
        """
        if not self.is_open():
            raise InvalidHandleError("REST connection is closed")
        with self._post_query(query) as response:
            chunks = response.iter_raw()
            source = io.BufferedReader(_ArrowInput(chunks))
            with source, pa.ipc.open_stream(source) as reader:
                logger.debug("REST query stream opened")
                yield _stream_batches(reader, response, chunks)


_FIXED_WIDTH_DTYPES = {pa.int64(): np.dtype("int64"), pa.float64(): np.dtype("float64")}


def _masked_array(array: pa.Array) -> MaskedArrayAny:
    """Convert an Arrow column to a NumPy masked array, masking only null rows."""
    if pa.types.is_dictionary(array.type):
        # A valid dictionary index can point to a null value.
        array = array.dictionary_decode()
    dtype = array.type
    # NumPy needs one boolean per row, so expand Arrow's bit-packed null mask.
    # A valid NaN stays unmasked.
    mask = array.is_null(nan_is_null=False).to_numpy(zero_copy_only=False)
    numpy_dtype: Optional[np.dtype]
    if pa.types.is_timestamp(dtype):
        if dtype.unit != "ns":
            # Scale only valid timestamps. Null slots can contain arbitrary bits.
            array = array.cast(pa.timestamp("ns", tz=dtype.tz), safe=True)
        numpy_dtype = np.dtype("datetime64[ns]")
    else:
        numpy_dtype = _FIXED_WIDTH_DTYPES.get(dtype)
    if numpy_dtype is not None:
        # A slice shares its original values buffer. Arrow counts the offset
        # in rows and NumPy counts it in bytes.
        data = np.frombuffer(
            array.buffers()[1],
            dtype=numpy_dtype,
            count=len(array),
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
        data = np.zeros(len(array), dtype="float64")
    else:
        raise Error("Unsupported REST Arrow column type: {}".format(dtype))
    return np.ma.array(data, mask=mask, copy=False)


def _stream_batches(
    reader: pa.RecordBatchReader,
    response: httpx.Response,
    chunks: Iterator[bytes],
) -> Iterator[Dict[str, MaskedArrayAny]]:
    for batch in reader:
        if batch.num_rows:
            yield {
                name: _masked_array(column)
                for name, column in zip(batch.schema.names, batch.columns)
            }
    # Arrow stops at its marker. Finish an explicitly framed HTTP body
    # so HTTPX checks its terminator and can reuse the socket.
    headers = response.headers
    if (
        "content-length" in headers
        or "chunked" in headers.get("transfer-encoding", "").lower()
    ):
        for _ in chunks:
            pass
