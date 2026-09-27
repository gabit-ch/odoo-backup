"""Client for the Odoo endpoints the backup service uses.

All endpoints are ``auth="none"`` routes, so no Odoo user session is needed:

* ``POST /web/database/backup`` (database manager form, Odoo 17 and 19): the
  backup itself.  Success is a close-delimited ``application/octet-stream``
  body; EVERY error (wrong master password, unknown database, disabled
  database manager, pg_dump failure) is an HTTP 200 ``text/html`` database
  manager page whose ``alert-danger`` block says ``Database backup error: ...``.
* ``POST /web/webclient/version_info`` (JSON-RPC, web module, not deprecated):
  the server version, for ``server_serie`` in the file name.  Fallback:
  XML-RPC ``/xmlrpc/2/common`` ``version()``.
* ``POST /jsonrpc`` ``db.migrate_databases(master_pwd, [])``: master password
  check without side effects.
* ``POST /web/database/list`` (JSON-RPC): the database list.

The master password is sent only in request bodies.  It never appears in log
messages or exception texts: requests never puts form data into messages,
server supplied texts are redacted, and wrapped exceptions are raised
``from None`` so that no request object (whose body holds the password) is
rendered in tracebacks.
"""

import hashlib
import http.client
import itertools
import json
import logging
import os
import pathlib
import re
import time
import urllib.parse
import xml.parsers.expat
import xmlrpc.client
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NoReturn

import requests

from .verify import (
    HEAD_SIZE,
    StreamVerifier,
    VerificationResult,
    check_magic,
    extract_html_error,
    looks_like_html,
    verify_zip_file,
)

logger = logging.getLogger(__name__)

BACKUP_PATH = "/web/database/backup"
VERSION_INFO_PATH = "/web/webclient/version_info"
JSONRPC_PATH = "/jsonrpc"
DATABASE_LIST_PATH = "/web/database/list"
XMLRPC_COMMON_PATH = "/xmlrpc/2/common"
AUTH_PROBE_SUFFIX = "-odoo-backup-auth-probe"

CHUNK_SIZE = 8 * 1024 * 1024
ERROR_BODY_LIMIT = 64 * 1024
JSON_BODY_LIMIT = 4 * 1024 * 1024
PROGRESS_BYTES = 100 * 1024 * 1024
PROGRESS_SECONDS = 15.0

# HTTP statuses meaning "this endpoint is not available here" (removed route,
# rpc module not loaded, blocked by a reverse proxy) rather than "Odoo is broken".
_UNAVAILABLE_STATUSES = frozenset({403, 404, 405, 410, 501})
# server_serie becomes part of the file name odoo{serie}-{db}-{timestamp}.{fmt},
# which retention parses: it must not contain '-' or '/'.
_SERIE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9.~_+]*")
_ACCESS_DENIED_HINT = (
    "check ODOO_MASTER_PWD; Odoo also answers Access Denied when its database manager is disabled "
    "(list_db = False)"
)


class OdooError(Exception):
    """Odoo is unreachable or answered in an unexpected way."""


class OdooAuthError(OdooError):
    """Odoo rejected the master password."""


class OdooBackupError(OdooError):
    """The backup request failed; nothing usable was downloaded."""


class _EndpointUnavailable(OdooError):
    """The endpoint does not exist on this server (HTTP 404 and similar)."""


class _RpcError(OdooError):
    """A JSON-RPC ``error`` answer; ``name`` is the Odoo exception class path."""

    def __init__(self, message: str, *, code: object, name: str) -> None:
        super().__init__(message)
        self.code = code
        self.name = name

    @property
    def access_denied(self) -> bool:
        return self.name.rsplit(".", 1)[-1] == "AccessDenied"

    @property
    def endpoint_missing(self) -> bool:
        """Route or db-service method does not exist (NotFound / KeyError "Method not found")."""
        return self.code == 404 or self.name.endswith("NotFound") or self.name.rsplit(".", 1)[-1] == "KeyError"


@dataclass
class DownloadResult:
    path: pathlib.Path
    size: int
    sha256: str
    seconds: float
    verification: VerificationResult


class _TimeoutTransport(xmlrpc.client.Transport):
    """XML-RPC transport with a socket timeout (the stdlib default waits forever)."""

    def __init__(self, timeout: float) -> None:
        super().__init__()
        self._timeout = timeout

    def make_connection(self, host):
        connection = super().make_connection(host)
        connection.timeout = self._timeout  # used by connect() and all later socket operations
        return connection


class _TimeoutSafeTransport(xmlrpc.client.SafeTransport):
    """HTTPS variant of :class:`_TimeoutTransport`."""

    def __init__(self, timeout: float) -> None:
        super().__init__()
        self._timeout = timeout

    def make_connection(self, host):
        connection = super().make_connection(host)
        connection.timeout = self._timeout
        return connection


class _ProgressLogger:
    """Logs download progress every PROGRESS_BYTES bytes or PROGRESS_SECONDS seconds."""

    def __init__(self, label: str) -> None:
        self._label = label
        self._every_bytes = PROGRESS_BYTES
        self._every_seconds = PROGRESS_SECONDS
        self._start = self._last = time.monotonic()
        self._next_bytes = self._every_bytes

    def update(self, total: int) -> None:
        now = time.monotonic()
        if total < self._next_bytes and now - self._last < self._every_seconds:
            return
        logger.info(
            "%s: %d bytes (%.2f GiB) received, %.1f MB/s",
            self._label, total, total / 2**30, _mb_per_second(total, now - self._start),
        )
        self._last = now
        while self._next_bytes <= total:
            self._next_bytes += self._every_bytes


class OdooClient:
    """Talks to one Odoo server about one database; see the module docstring.

    ``timeout`` bounds connecting and every JSON-RPC/XML-RPC call;
    ``read_timeout`` is the socket read timeout of the backup download (Odoo
    builds the whole archive before it sends the first byte, so it must cover
    the dump time of the database).  Paths are resolved against the server
    root of ``url`` (a path in ``url``, e.g. ``/odoo``, is ignored).
    """

    def __init__(
        self,
        url: str,
        master_password: str,
        db_name: str,
        timeout: float,
        read_timeout: float,
        session: requests.Session | None = None,
    ) -> None:
        parts = urllib.parse.urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError("the Odoo URL must be an absolute http:// or https:// URL")
        self._url = url
        self._https = parts.scheme == "https"
        host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname  # IPv6 literal
        netloc = host if parts.port is None else f"{host}:{parts.port}"
        self.display_url = urllib.parse.urlunsplit((parts.scheme, netloc, "", "", ""))  # without credentials
        self._password = master_password
        self.db_name = db_name
        self._timeout = timeout
        self._read_timeout = read_timeout
        self._owns_session = session is None
        self._session = session if session is not None else requests.Session()
        self._request_ids = itertools.count(1)

    def __repr__(self) -> str:
        return f"OdooClient(url={self.display_url!r}, db_name={self.db_name!r})"

    def close(self) -> None:
        """Close the HTTP session if this client created it."""
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> "OdooClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Version, master password, database
    # ------------------------------------------------------------------

    def server_serie(self) -> str:
        """Return Odoo's ``server_serie`` (e.g. ``"19.0"``, ``"saas~18.3"``)."""
        try:
            info = self._call_jsonrpc(VERSION_INFO_PATH, {}, what="version check")
            serie = info.get("server_serie") if isinstance(info, dict) else None
            if not serie:
                raise OdooError(f"version check: {VERSION_INFO_PATH} returned no server_serie")
        except OdooError as exc:
            logger.warning(
                "Reading the Odoo version from %s failed (%s); falling back to XML-RPC %s",
                VERSION_INFO_PATH, exc, XMLRPC_COMMON_PATH,
            )
            serie = self._xmlrpc_server_serie()
        if not isinstance(serie, str) or not _SERIE_RE.fullmatch(serie):
            raise OdooError(f"version check: Odoo reported an unusable server_serie {serie!r}")
        return serie

    def check_master_password(self) -> None:
        """Verify the master password without any side effect on the server.

        ``db.migrate_databases`` is the cheapest db-service method that runs
        ``check_super(master_pwd)`` (and requires the database manager,
        ``list_db``): with an EMPTY database list it migrates nothing and
        returns True (Odoo 17 and 19).  ``/jsonrpc`` is deprecated in Odoo 19
        and announced for removal in Odoo 22; when it is unavailable (404) the
        password is checked with a backup request for a database that does
        not exist (see :meth:`_probe_master_password`).

        Raises :class:`OdooAuthError` for a wrong password and
        :class:`OdooError` for everything else (including a disabled database
        manager, which also answers "Access Denied").
        """
        params = {"service": "db", "method": "migrate_databases", "args": [self._password, []]}
        failure: OdooError | None = None
        try:
            result = self._call_jsonrpc(JSONRPC_PATH, params, what="master password check")
        except (_EndpointUnavailable, _RpcError) as exc:
            failure = exc
        # Handled outside the except block: no chained tracebacks in the logs.
        if isinstance(failure, _EndpointUnavailable) or (isinstance(failure, _RpcError) and failure.endpoint_missing):
            logger.info("%s; checking the master password with a backup probe instead", failure)
            self._probe_master_password()
            return
        if isinstance(failure, _RpcError) and failure.access_denied:
            self._raise_access_denied(f"{JSONRPC_PATH} db.migrate_databases")
        if failure is not None:
            raise failure
        if result is not True:
            raise OdooError(f"master password check: unexpected answer {result!r} from db.migrate_databases")
        logger.info("Odoo accepted the master password")

    def check_database(self) -> None:
        """Raise :class:`OdooError` unless ``db_name`` is in Odoo's database list."""
        try:
            databases = self._call_jsonrpc(DATABASE_LIST_PATH, {}, what="database check")
        except _RpcError as exc:
            if exc.access_denied:
                raise OdooError(
                    "database check: Odoo refuses to list databases: its database manager is disabled "
                    "(list_db = False), which also disables /web/database/backup; set list_db = True"
                ) from None
            raise
        if not isinstance(databases, list) or not all(isinstance(name, str) for name in databases):
            raise OdooError(f"database check: unexpected answer from {DATABASE_LIST_PATH}")
        if self.db_name not in databases:
            visible = ", ".join(sorted(databases)[:20]) or "none"
            raise OdooError(
                f"database check: database {self.db_name!r} is not in Odoo's database list "
                f"(visible: {visible}); check ODOO_DB_NAME and Odoo's dbfilter"
            )
        logger.info("Odoo lists database %r", self.db_name)

    # ------------------------------------------------------------------
    # Backup download
    # ------------------------------------------------------------------

    def download_backup(
        self,
        fmt: str,
        dest: pathlib.Path,
        with_filestore: bool,
        progress: Callable[[int], None] | None = None,
    ) -> DownloadResult:
        """Download a backup to ``dest`` (created exclusively, mode 0600) and verify it.

        ``with_filestore=False`` requests a database-only backup (form field
        ``filestore=false``; honoured by Odoo 19 and web_backup, silently
        ignored by Odoo 17).  ``progress`` is called with the number of bytes
        received after every chunk.  On ANY failure ``dest`` is removed (if
        this call created it) and the exception propagates: requests errors
        and unusable answers as :class:`OdooBackupError`, broken archives as
        :class:`~odoo_backup.verify.ArchiveVerificationError`.  Verification
        notes are logged as warnings here.
        """
        verifier = StreamVerifier(fmt)  # ValueError for an unsupported format
        dest = pathlib.Path(dest)
        kind = "full" if with_filestore else "database-only"
        data = {"master_pwd": self._password, "name": self.db_name, "backup_format": fmt}
        if not with_filestore:
            data["filestore"] = "false"

        # Created before the request: a local problem (directory missing, file
        # exists) must not cost a complete dump on the Odoo side.
        fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
        try:
            try:
                size, sha256, seconds = self._stream_backup(fd, data, verifier, dest, kind, fmt, progress)
            finally:
                os.close(fd)
            verification = verifier.finish()
            if fmt == "zip":
                verification = verify_zip_file(dest)
        except BaseException:
            _remove_quietly(dest)
            raise

        members = "n/a" if verification.members is None else str(verification.members)
        filestore = {True: "yes", False: "no", None: "unknown"}[verification.has_filestore]
        logger.info(
            "Downloaded %s %s backup %s: %d bytes (%.2f GiB) in %.1f s (%.1f MB/s), sha256=%s, "
            "members=%s, filestore=%s",
            kind, fmt, dest.name, size, size / 2**30, seconds, _mb_per_second(size, seconds), sha256,
            members, filestore,
        )
        for note in verification.notes:
            logger.warning("Backup verification: %s", note)
        if not with_filestore and verification.has_filestore:
            logger.warning(
                "Odoo ignored filestore=false (web_backup too old?): the database-only backup %s "
                "contains the filestore",
                dest.name,
            )
        return DownloadResult(path=dest, size=size, sha256=sha256, seconds=seconds, verification=verification)

    def _stream_backup(
        self,
        fd: int,
        data: dict[str, str],
        verifier: StreamVerifier,
        dest: pathlib.Path,
        kind: str,
        fmt: str,
        progress: Callable[[int], None] | None,
    ) -> tuple[int, str, float]:
        logger.info(
            "Requesting a %s %s backup of database %r from %s", kind, fmt, self.db_name, self.display_url
        )
        started = time.monotonic()
        response = self._post(
            BACKUP_PATH,
            what="backup download",
            error=OdooBackupError,
            data=data,
            stream=True,
            timeout=(self._timeout, self._read_timeout),
        )
        digest = hashlib.sha256()
        size = 0
        with response:
            self._check_backup_response(response)
            expected_size = self._expected_size(response)
            logger.info(
                "Odoo started sending the backup after %.1f s; writing it to %s",
                time.monotonic() - started, dest,
            )
            reporter = _ProgressLogger(f"Downloading {dest.name}")
            try:
                for chunk in self._checked_chunks(response, fmt):
                    verifier.feed(chunk)
                    digest.update(chunk)
                    _write_all(fd, chunk)
                    size += len(chunk)
                    reporter.update(size)
                    if progress is not None:
                        progress(size)
            except requests.RequestException as exc:
                raise OdooBackupError(
                    f"backup download interrupted after {size} bytes: {self._describe(exc)}"
                ) from None
        if expected_size is not None and size != expected_size:
            raise OdooBackupError(
                f"backup download incomplete: received {size} of {expected_size} bytes (Content-Length)"
            )
        return size, digest.hexdigest(), time.monotonic() - started

    def _check_backup_response(self, response: requests.Response) -> None:
        """Reject everything that is not a backup stream (before any byte is written)."""
        status = response.status_code
        if status != 200:
            body = _read_limited(response, ERROR_BODY_LIMIT)
            if 300 <= status < 400:
                location = self._redact(response.headers.get("Location", "?"))
                raise OdooBackupError(
                    f"Odoo redirected the backup request (HTTP {status} to {location}); redirects are "
                    "not followed, set ODOO_URL to the final address"
                )
            raise OdooBackupError(
                f"Odoo answered the backup request with HTTP {status}: {self._redact(extract_html_error(body))}"
            )
        media_type = _media_type(response)
        if media_type == "text/html":
            raise OdooBackupError(self._html_error_message(_read_limited(response, ERROR_BODY_LIMIT)))
        if media_type != "application/octet-stream":
            body = _read_limited(response, ERROR_BODY_LIMIT)
            raise OdooBackupError(
                f"Odoo answered the backup request with Content-Type {media_type or '(none)'!r} instead of "
                f"application/octet-stream: {self._redact(extract_html_error(body))}"
            )
        disposition = response.headers.get("Content-Disposition", "")
        if not disposition.strip().lower().startswith("attachment"):
            logger.warning("The backup response has no 'Content-Disposition: attachment' header; continuing")

    def _checked_chunks(self, response: requests.Response, fmt: str):
        """Yield the body in CHUNK_SIZE chunks after checking its first bytes.

        An HTML page sent as octet-stream is still an Odoo error page; the
        magic check names the format that arrived instead (e.g. a raw pg_dump
        when web_backup is missing).
        """
        head = bytearray()
        pending: list[bytes] = []
        checked = False
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            if not chunk:
                continue
            if checked:
                yield chunk
                continue
            pending.append(chunk)
            head += chunk[:HEAD_SIZE - len(head)]
            if len(head) < HEAD_SIZE:
                continue
            self._check_backup_head(bytes(head), pending, fmt)
            checked = True
            yield from pending
            pending.clear()
        if not checked:  # body shorter than HEAD_SIZE
            self._check_backup_head(bytes(head), pending, fmt)
            yield from pending

    def _check_backup_head(self, head: bytes, pending: list[bytes], fmt: str) -> None:
        if looks_like_html(head):
            raise OdooBackupError(self._html_error_message(b"".join(pending)[:ERROR_BODY_LIMIT]))
        check_magic(fmt, head)

    def _html_error_message(self, body: bytes) -> str:
        text = self._redact(extract_html_error(body))
        message = f"Odoo returned an HTML page instead of a backup: {text}"
        if "Access Denied" in text:
            message += f" ({_ACCESS_DENIED_HINT})"
        elif "is not known" in text:
            message += " (check ODOO_DB_NAME)"
        return message

    @staticmethod
    def _expected_size(response: requests.Response) -> int | None:
        """Content-Length, if present and comparable with the decoded body size."""
        raw = response.headers.get("Content-Length")
        encoding = response.headers.get("Content-Encoding", "").strip().lower()
        if raw is None or encoding not in ("", "identity"):
            return None
        try:
            return int(raw)
        except ValueError:
            raise OdooBackupError(f"Odoo sent an invalid Content-Length {raw!r}") from None

    # ------------------------------------------------------------------
    # Transport helpers
    # ------------------------------------------------------------------

    def _endpoint(self, path: str) -> str:
        return urllib.parse.urljoin(self._url, path)

    def _redact(self, text: str) -> str:
        return text.replace(self._password, "***") if self._password else text

    def _describe(self, exc: BaseException) -> str:
        return self._redact(f"{type(exc).__name__}: {exc}")

    def _post(self, path: str, *, what: str, error: type[OdooError] = OdooError, **kwargs: Any) -> requests.Response:
        """POST without following redirects (the body may hold the master password)."""
        try:
            return self._session.post(self._endpoint(path), allow_redirects=False, **kwargs)
        except requests.RequestException as exc:
            raise error(f"{what}: cannot reach Odoo at {self.display_url}: {self._describe(exc)}") from None

    def _call_jsonrpc(self, path: str, params: dict[str, Any], *, what: str) -> Any:
        payload = {"jsonrpc": "2.0", "method": "call", "params": params, "id": next(self._request_ids)}
        response = self._post(path, what=what, json=payload, stream=True, timeout=(self._timeout, self._timeout))
        with response:
            status = response.status_code
            location = response.headers.get("Location", "?")
            body = _read_limited(response, JSON_BODY_LIMIT)
        if status in _UNAVAILABLE_STATUSES:
            raise _EndpointUnavailable(f"{what}: {path} is not available on {self.display_url} (HTTP {status})")
        if 300 <= status < 400:
            raise OdooError(
                f"{what}: Odoo redirected {path} (HTTP {status} to {self._redact(location)}); "
                "set ODOO_URL to the final address"
            )
        if status != 200:
            raise OdooError(f"{what}: {path} answered HTTP {status}: {self._redact(extract_html_error(body))}")
        try:
            answer = json.loads(body)
        except ValueError:
            raise OdooError(
                f"{what}: {path} did not answer with JSON-RPC: {self._redact(extract_html_error(body))}"
            ) from None
        if not isinstance(answer, dict):
            raise OdooError(f"{what}: {path} answered with an unexpected JSON document")
        if answer.get("error") is not None:
            raise self._rpc_error(what, path, answer["error"])
        if "result" not in answer:
            raise OdooError(f"{what}: {path} answered without a JSON-RPC result")
        return answer["result"]

    def _rpc_error(self, what: str, path: str, error: object) -> _RpcError:
        error = error if isinstance(error, dict) else {}
        data = error.get("data") if isinstance(error.get("data"), dict) else {}
        name = str(data.get("name") or "")
        message = str(data.get("message") or error.get("message") or "unknown error")
        text = self._redact(" ".join(message.split()))[:500]  # never the "debug" traceback
        return _RpcError(f"{what}: {path} answered {name or 'an error'}: {text}", code=error.get("code"), name=name)

    def _xmlrpc_server_serie(self) -> Any:
        transport = (_TimeoutSafeTransport if self._https else _TimeoutTransport)(self._timeout)
        what = f"version check via XML-RPC {XMLRPC_COMMON_PATH}"
        try:
            with xmlrpc.client.ServerProxy(self._endpoint(XMLRPC_COMMON_PATH), transport=transport) as proxy:
                info = proxy.version()
        except xmlrpc.client.ProtocolError as exc:
            raise OdooError(f"{what}: HTTP {exc.errcode} {exc.errmsg}") from None
        except xmlrpc.client.Fault as exc:
            raise OdooError(f"{what}: fault {exc.faultCode}: {self._redact(str(exc.faultString))[:500]}") from None
        except (xmlrpc.client.Error, OSError, http.client.HTTPException, xml.parsers.expat.ExpatError) as exc:
            raise OdooError(f"{what}: {self._describe(exc)}") from None
        if not isinstance(info, dict) or not info.get("server_serie"):
            raise OdooError(f"{what}: the answer contains no server_serie")
        return info["server_serie"]

    # ------------------------------------------------------------------
    # Master password probe (fallback when /jsonrpc is unavailable)
    # ------------------------------------------------------------------

    def _probe_master_password(self) -> None:
        """Check the password with a backup request for a database that does not exist.

        Odoo's backup controller runs ``check_super(master_pwd)`` before it
        looks the database up, so the error page says either "Access Denied"
        (wrong password, or database manager disabled) or "Database '...' is
        not known" (password accepted).  Like every backup request, this
        changes Odoo's master password to ours if it is still the insecure
        default "admin".  The ``dump`` format without filestore keeps the
        request cheap should the probe database exist after all.
        """
        probe = f"{self.db_name}{AUTH_PROBE_SUFFIX}"
        data = {"master_pwd": self._password, "name": probe, "backup_format": "dump", "filestore": "false"}
        response = self._post(
            BACKUP_PATH, what="master password probe", data=data, stream=True, timeout=(self._timeout, self._timeout)
        )
        with response:
            status = response.status_code
            if status == 200 and _media_type(response) == "application/octet-stream":
                raise OdooError(
                    f"master password probe: Odoo started a backup of {probe!r}, a database that should not exist"
                )
            body = _read_limited(response, ERROR_BODY_LIMIT)
        text = self._redact(extract_html_error(body))
        if status != 200:
            raise OdooError(f"master password probe: {BACKUP_PATH} answered HTTP {status}: {text}")
        if "Access Denied" in text:
            self._raise_access_denied(f"{BACKUP_PATH} (probe)")
        if "is not known" in text:
            logger.info("Odoo accepted the master password (backup probe)")
            return
        raise OdooError(f"master password probe: unexpected answer from Odoo: {text}")

    def _raise_access_denied(self, source: str) -> NoReturn:
        """Tell a wrong password from a disabled database manager (both answer AccessDenied)."""
        if self._database_manager_disabled():
            raise OdooError(
                f"{source} answered Access Denied because Odoo's database manager is disabled "
                "(list_db = False): the master password cannot be verified and /web/database/backup "
                "refuses every backup; set list_db = True"
            ) from None
        raise OdooAuthError(
            f"Odoo rejected the master password ({source} answered Access Denied): check ODOO_MASTER_PWD"
        ) from None

    def _database_manager_disabled(self) -> bool:
        try:
            self._call_jsonrpc(DATABASE_LIST_PATH, {}, what="database manager check")
        except _RpcError as exc:
            return exc.access_denied
        except OdooError:
            return False
        return False


def _media_type(response: requests.Response) -> str:
    return response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()


def _read_limited(response: requests.Response, limit: int) -> bytes:
    """Read at most ``limit`` body bytes; a broken connection yields what arrived."""
    buffer = bytearray()
    try:
        for chunk in response.iter_content(chunk_size=64 * 1024):
            buffer += chunk
            if len(buffer) >= limit:
                break
    except requests.RequestException:
        pass
    return bytes(buffer[:limit])


def _write_all(fd: int, data: bytes) -> None:
    """Unbuffered write; os.write() may write less than asked."""
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _remove_quietly(path: pathlib.Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        logger.warning("Could not remove the incomplete backup file %s: %s", path, exc)


def _mb_per_second(size: int, seconds: float) -> float:
    return size / seconds / 1e6 if seconds > 0 else 0.0
