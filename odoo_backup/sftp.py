"""SFTP access for odoo-backup: host key pinning, authentication and verified uploads.

The module wraps paramiko (5.x) and deliberately owns the full connection setup instead of
using ``paramiko.SSHClient``:

* the TCP connection is opened with ``socket.create_connection`` so ``SFTP_PORT`` is honoured
  (the 1.x code used ``Transport(host, port)``, which silently ignored the port);
* the server host key is checked right after the key exchange and BEFORE any authentication
  request is sent, so a spoofed server never receives the password or a key signature;
* uploads go to ``<name>.upload`` first and are renamed only after the server confirmed every
  write request and the remote size matches, so a visible ``<name>`` is always complete.

Every failure is reported as :class:`SFTPError` (or a subclass). Messages never contain the
password or the key passphrase.
"""

import base64
import binascii
import dataclasses
import hashlib
import itertools
import logging
import os
import posixpath
import secrets
import socket
import stat
import struct
import threading
import time
from collections.abc import Callable, Sequence

import paramiko
from paramiko.sftp import CMD_EXTENDED, CMD_EXTENDED_REPLY, CMD_STATUS

logger = logging.getLogger(__name__)

#: Suffix of a file that is still being uploaded. Must stay equal to
#: ``odoo_backup.retention.PARTIAL_SUFFIX`` (retention treats such files as partials).
PARTIAL_SUFFIX = ".upload"

#: Size of the blocks read from the local backup file per ``write()`` call.
UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024
#: Write request size used when the server does not announce its limits (paramiko's default).
DEFAULT_REQUEST_SIZE = 32768
#: Upper bound for a single SFTP write request. OpenSSH's maximum SFTP packet is 256 KiB
#: including the request header; 261120 (= 256 KiB - 1 KiB) is what OpenSSH itself uses,
#: 262144 already breaks OpenSSH servers.
MAX_REQUEST_SIZE_CAP = 261120
#: Interval of SSH keepalive packets (seconds).
KEEPALIVE_SECONDS = 30
#: Upload retries wait ``RETRY_BACKOFF_SECONDS * attempt`` seconds.
RETRY_BACKOFF_SECONDS = 5.0
#: Progress is logged every ``PROGRESS_LOG_BYTES`` or ``PROGRESS_LOG_SECONDS``, whichever first.
PROGRESS_LOG_BYTES = 100 * 1024 * 1024
PROGRESS_LOG_SECONDS = 15.0

_GIB = 1024**3
_PROBE_PAYLOAD = b"odoo-backup write probe\n"

#: SSH host key algorithms that present a host key of a given public key type. paramiko 5 does
#: not offer the SHA-1 "ssh-rsa" algorithm; RSA host keys are negotiated as rsa-sha2-*.
_HOST_KEY_ALGORITHMS = {
    "ssh-rsa": ("rsa-sha2-512", "rsa-sha2-256", "ssh-rsa"),
}

# Errors raised by paramiko / the socket layer for a failed remote operation. paramiko reports
# SFTP status errors as OSError (FileNotFoundError, PermissionError, plain IOError), a dropped
# connection as SSHException or EOFError, protocol violations as paramiko.sftp.SFTPError and
# timeouts as TimeoutError (an OSError).
_REMOTE_ERRORS = (OSError, EOFError, paramiko.SSHException, paramiko.sftp.SFTPError)


class SFTPError(Exception):
    """An SFTP operation failed (connect, handshake, host key, authentication, file operation)."""


class HostKeyMismatchError(SFTPError):
    """The server presented a host key that matches none of the pinned ``SFTP_HOST_KEY`` entries.

    Raised before any authentication request was sent.
    """


class SFTPAuthenticationError(SFTPError):
    """The server rejected every configured authentication method."""


class UploadError(SFTPError):
    """An upload failed. ``retryable`` is False for errors another attempt cannot fix."""

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


@dataclasses.dataclass(frozen=True)
class RemoteFile:
    """A regular file in a remote directory listing."""

    name: str
    size: int
    mtime: int | None


@dataclasses.dataclass(frozen=True)
class UploadResult:
    """Outcome of :meth:`SFTPConnection.upload`.

    ``seconds`` covers the whole upload call including failed attempts and back-off.
    ``request_size`` is the SFTP write request size used by the successful attempt.
    """

    remote_path: str
    size: int
    seconds: float
    remote_mtime: int | None
    request_size: int


# --------------------------------------------------------------------------------------------
# Host key helpers
# --------------------------------------------------------------------------------------------


def fingerprint(key: paramiko.PKey) -> str:
    """Return the OpenSSH SHA256 fingerprint of a public key: ``SHA256:<base64 without padding>``.

    This is the format printed by ``ssh-keygen -lf`` and ``ssh -v``.
    """
    digest = hashlib.sha256(key.asbytes()).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii").rstrip("=")


def host_key_matches(key: paramiko.PKey, entries: Sequence[str]) -> bool:
    """Return True if ``key`` matches at least one pinned host key entry.

    Supported entry formats (surrounding whitespace is ignored):

    * ``SHA256:<base64>`` - an OpenSSH fingerprint (padding optional, prefix case-insensitive);
    * ``<keytype> <base64> [comment]`` - an OpenSSH public key line;
    * ``<hosts> <keytype> <base64> [comment]`` - a known_hosts line, e.g. from ``ssh-keyscan``
      (``[host]:23 ssh-ed25519 AAAA...``); the host field is not interpreted because the
      connection target is configured separately.

    A public key matches when its decoded key blob equals the server key's blob exactly.
    known_hosts marker lines (``@revoked``, ``@cert-authority``) and comments never match.
    """
    blob = key.asbytes()
    observed = fingerprint(key)[len("SHA256:") :]
    return any(_entry_matches(entry, blob, observed) for entry in entries)


def _entry_matches(entry: str, blob: bytes, observed_fingerprint: str) -> bool:
    text = entry.strip()
    if not text or text.startswith(("#", "@")):
        return False
    if text[:7].upper() == "SHA256:":
        return text[7:].rstrip("=") == observed_fingerprint
    public_key = _entry_public_key(text)
    return public_key is not None and public_key[1] == blob


def _entry_public_key(text: str) -> tuple[str, bytes] | None:
    """Key type and blob of a public key / known_hosts entry; None for anything else.

    The first "<keytype> <base64 blob of that key type>" pair of the line is the key; any tokens
    before it are a known_hosts host field, any tokens after it are a comment.
    """
    if not text or text.startswith(("#", "@")) or text[:7].upper() == "SHA256:":
        return None
    tokens = text.split()
    for key_type, data in itertools.pairwise(tokens):
        candidate = _decode_key_blob(key_type, data)
        if candidate is not None:
            return key_type, candidate
    return None


def pinned_key_types(entries: Sequence[str]) -> tuple[str, ...]:
    """Key types of the pinned public key entries in order (fingerprints name no key type)."""
    types = (_entry_public_key(entry.strip()) for entry in entries)
    return tuple(dict.fromkeys(public_key[0] for public_key in types if public_key is not None))


def _decode_key_blob(key_type: str, data: str) -> bytes | None:
    """Decode an OpenSSH public key blob; None unless it is valid base64 of ``key_type``."""
    try:
        decoded = base64.b64decode(data, validate=True)
    except binascii.Error, ValueError:
        return None
    name = key_type.encode("ascii", "replace")
    header = struct.pack(">I", len(name)) + name
    return decoded if decoded.startswith(header) else None


# --------------------------------------------------------------------------------------------
# Connection
# --------------------------------------------------------------------------------------------


class SFTPConnection:
    """One SSH/SFTP session to the backup server.

    Use it as a context manager (``__enter__`` connects, ``__exit__`` closes). Remote operations
    reconnect automatically when the session was closed or lost, so a single instance can be
    reused across a dropped connection.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str | None = None,
        key_file: str | os.PathLike[str] | None = None,
        key_passphrase: str | None = None,
        host_keys: Sequence[str] = (),
        ciphers: Sequence[str] = (),
        timeout: float = 60.0,
        max_request_size: int | None = None,
        *,
        sleep: Callable[[float], object] | None = None,
    ) -> None:
        """Store the connection parameters; nothing is opened before :meth:`connect`.

        ``host_keys`` pins the server key (see :func:`host_key_matches`); when empty, every
        connect logs a WARNING with the observed fingerprint. ``ciphers`` is the preferred
        cipher order (unsupported names are ignored with a WARNING). ``timeout`` bounds the TCP
        connect, the SSH handshake, the authentication, opening the SFTP session and every SFTP
        round-trip. ``max_request_size`` overrides the
        negotiated write request size. ``sleep`` replaces ``time.sleep`` for the upload back-off
        (tests).
        """
        if password is None and key_file is None:
            raise ValueError("SFTPConnection needs a password or a private key file")
        self.host = host
        self.port = int(port)
        self.user = user
        self._password = password
        self._key_file = key_file
        self._key_passphrase = key_passphrase
        self.host_keys = tuple(host_keys)
        self.ciphers = tuple(ciphers)
        self.timeout = float(timeout)
        self.max_request_size = max_request_size
        self._sleep = sleep

        self._sock: socket.socket | None = None
        self._transport: paramiko.Transport | None = None
        self._sftp: paramiko.SFTPClient | None = None
        self._pkey: paramiko.PKey | None = None
        self._request_size: int | None = None

        #: SHA256 fingerprint and key type of the server host key (set by :meth:`connect`).
        self.server_fingerprint = ""
        self.server_key_type = ""
        #: Negotiated client-to-server cipher of the current session (set by :meth:`connect`).
        self.cipher = ""

    def __repr__(self) -> str:
        return f"SFTPConnection({self.user}@{self.host}:{self.port})"

    def __enter__(self) -> SFTPConnection:
        self.connect()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def connected(self) -> bool:
        return self._sftp is not None and self._transport is not None and self._transport.is_active()

    # -- connection setup ------------------------------------------------------------------

    def connect(self) -> None:
        """Open a new session (closing any existing one).

        Order: TCP connect -> SSH key exchange -> host key verification -> authentication
        (private key first, then password) -> SFTP subsystem. On any failure everything opened
        so far is closed and :class:`SFTPError` (or a subclass) is raised.
        """
        self.close()
        pkey = self._load_private_key()
        try:
            self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        except OSError as exc:
            raise SFTPError(f"cannot connect to SFTP server {self.address}: {_describe(exc)}") from exc
        try:
            transport = self._transport = paramiko.Transport(self._sock)
            transport.auth_timeout = self.timeout  # paramiko's default is a fixed 30 s
            self._apply_cipher_preference(transport)
            self._apply_host_key_preference(transport)
            try:
                transport.start_client(timeout=self.timeout)
                # Raises SSHException if the key exchange did not finish within the timeout.
                server_key = transport.get_remote_server_key()
            except _REMOTE_ERRORS as exc:
                raise SFTPError(f"SSH handshake with {self.address} failed or timed out: {_describe(exc)}") from exc
            self._verify_host_key(server_key)
            self._authenticate(transport, pkey)
            sftp = self._open_sftp_session(transport)
            self._sftp = sftp
            transport.set_keepalive(KEEPALIVE_SECONDS)
            self.cipher = transport.local_cipher
        except BaseException:
            self.close()
            raise
        logger.info(
            "Connected to SFTP server %s as %s (host key %s %s, cipher %s)",
            self.address,
            self.user,
            self.server_key_type,
            self.server_fingerprint,
            self.cipher,
        )

    def close(self) -> None:
        """Close the SFTP session, the SSH transport and the socket. Never raises."""
        sftp, transport, sock = self._sftp, self._transport, self._sock
        self._sftp = self._transport = self._sock = None
        self._request_size = None
        # Transport.close() returns early for a transport whose handshake failed, so the socket
        # is closed explicitly as well.
        for resource in (sftp, transport, sock):
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as exc:  # teardown must never mask the original error
                logger.debug("Ignoring error while closing %r: %s", resource, exc)

    def _load_private_key(self) -> paramiko.PKey | None:
        if self._key_file is None:
            return None
        if self._pkey is None:
            # paramiko 5 passes the password straight to cryptography, which requires bytes.
            password = self._key_passphrase.encode("utf-8") if self._key_passphrase is not None else None
            try:
                try:
                    self._pkey = paramiko.PKey.from_path(self._key_file, password=password)
                except TypeError as exc:
                    # cryptography rejects a passphrase for an unencrypted key; OpenSSH ignores it.
                    if password is None or "not encrypted" not in str(exc):
                        raise
                    logger.warning(
                        "SFTP_PRIVATE_KEY_PASSPHRASE is set but %s is not encrypted; ignoring the passphrase",
                        self._key_file,
                    )
                    self._pkey = paramiko.PKey.from_path(self._key_file, password=None)
            except OSError as exc:
                message = f"cannot read SFTP private key file {self._key_file}: {_describe(exc)}"
                raise SFTPError(message) from exc
            except Exception as exc:
                hint = (
                    "wrong or missing SFTP_PRIVATE_KEY_PASSPHRASE?"
                    if isinstance(exc, (TypeError, ValueError, paramiko.PasswordRequiredException))
                    else "unsupported key format?"
                )
                raise SFTPError(
                    f"cannot load SFTP private key file {self._key_file} ({hint}): "
                    f"{type(exc).__name__}: {_describe(exc)}"
                ) from exc
        return self._pkey

    def _apply_cipher_preference(self, transport: paramiko.Transport) -> None:
        """Put the configured ciphers first, followed by paramiko's remaining defaults."""
        if not self.ciphers:
            return
        options = transport.get_security_options()
        available = tuple(options.ciphers)
        preferred = tuple(dict.fromkeys(name for name in self.ciphers if name in available))
        unsupported = [name for name in self.ciphers if name not in available]
        if unsupported:
            logger.warning("Ignoring SFTP ciphers not supported by paramiko: %s", ", ".join(unsupported))
        options.ciphers = preferred + tuple(name for name in available if name not in preferred)

    def _apply_host_key_preference(self, transport: paramiko.Transport) -> None:
        """Offer the host key algorithms of the pinned public keys first.

        The server presents the host key of the first algorithm in the client's list that it
        supports. paramiko prefers Ed25519, so a server with several host keys (OpenSSH) would
        present its Ed25519 key even when only its RSA key is pinned, and every connection would
        fail as a mismatch. Like OpenSSH with known_hosts, the pinned key types go first; the
        other algorithms stay available (a key they bring still has to match a pinned entry).
        Fingerprint entries name no key type and leave the order unchanged.
        """
        key_types = pinned_key_types(self.host_keys)
        if not key_types:
            return
        options = transport.get_security_options()
        available = tuple(options.key_types)
        wanted = (
            algorithm
            for key_type in key_types
            for algorithm in _HOST_KEY_ALGORITHMS.get(key_type, (key_type,))
            if algorithm in available
        )
        preferred = tuple(dict.fromkeys(wanted))
        if preferred:
            options.key_types = preferred + tuple(name for name in available if name not in preferred)

    def _open_sftp_session(self, transport: paramiko.Transport) -> paramiko.SFTPClient:
        """Open the ``sftp`` subsystem with every step bounded by the timeout.

        ``paramiko.SFTPClient.from_transport()`` waits up to an hour for the session channel,
        forever for the answer to the subsystem request and forever for the server's SFTP
        VERSION packet, so a server that authenticates but whose SFTP server stalls (e.g. a hung
        storage backend) would block the caller indefinitely. Here the channel gets the timeout
        before the version handshake, and a timer closes the transport if the subsystem request
        stays unanswered (closing it wakes paramiko's wait with an exception).
        """
        fired = threading.Event()

        def give_up() -> None:
            fired.set()
            transport.close()

        timer = threading.Timer(self.timeout, give_up)
        timer.name = "odoo-backup-sftp-open"
        timer.daemon = True
        try:
            channel = transport.open_session(timeout=self.timeout)
            # Bounds the version handshake and every later SFTP request/response round-trip.
            channel.settimeout(self.timeout)
            timer.start()
            try:
                channel.invoke_subsystem("sftp")
            finally:
                timer.cancel()
                timer.join()
            return paramiko.SFTPClient(channel)
        except _REMOTE_ERRORS as exc:
            if fired.is_set() or isinstance(exc, TimeoutError):
                reason = f"the SFTP server did not answer within {self.timeout:g} s"
            else:
                reason = _describe(exc)
            raise SFTPError(f"cannot open an SFTP session on {self.address}: {reason}") from exc

    def _verify_host_key(self, server_key: paramiko.PKey) -> None:
        self.server_fingerprint = fingerprint(server_key)
        self.server_key_type = server_key.get_name()
        if self.host_keys:
            if not host_key_matches(server_key, self.host_keys):
                hint = ""
                if len(pinned_key_types(self.host_keys)) < len(self.host_keys):
                    hint = (
                        f" (a server with several host keys presents its {self.server_key_type} key to "
                        "this client: pin that fingerprint, or the ssh-keyscan public key lines)"
                    )
                raise HostKeyMismatchError(
                    f"SFTP host key mismatch for {self.address}: the server presented "
                    f"{self.server_key_type} {self.server_fingerprint}, which matches none of the "
                    f"{len(self.host_keys)} SFTP_HOST_KEY entries; refusing to authenticate{hint}"
                )
            logger.debug("SFTP host key of %s verified: %s", self.address, self.server_fingerprint)
        else:
            logger.warning(
                "SFTP host key of %s is NOT verified because SFTP_HOST_KEY is not set; the server "
                "presented %s %s (pin it with SFTP_HOST_KEY=%s)",
                self.address,
                self.server_key_type,
                self.server_fingerprint,
                self.server_fingerprint,
            )

    def _authenticate(self, transport: paramiko.Transport, pkey: paramiko.PKey | None) -> None:
        """Try the private key first, then the password; raise if neither is accepted.

        A method returning a non-empty "further methods required" list leaves the transport
        unauthenticated, so the next method is tried (this also covers publickey+password).
        """
        attempts: list[tuple[str, Callable[[], object]]] = []
        if pkey is not None:
            attempts.append(("publickey", lambda: transport.auth_publickey(self.user, pkey)))
        if self._password is not None:
            attempts.append(("password", lambda: transport.auth_password(self.user, self._password)))
        tried = []
        for method, authenticate in attempts:
            tried.append(method)
            try:
                authenticate()
            except paramiko.AuthenticationException as exc:  # includes BadAuthenticationType
                logger.debug("SFTP %s authentication for %s rejected: %s", method, self.user, type(exc).__name__)
            except _REMOTE_ERRORS as exc:
                raise SFTPError(
                    f"SFTP authentication with {self.address} failed with a connection error: {_describe(exc)}"
                ) from exc
            if transport.is_authenticated():
                return
        raise SFTPAuthenticationError(
            f"SFTP authentication failed for user {self.user!r} on {self.address} (tried: {', '.join(tried)})"
        )

    def _client(self) -> paramiko.SFTPClient:
        """Return the SFTP client, (re)connecting if the session is closed or was lost."""
        if not self.connected:
            if self._sftp is not None:
                logger.warning("SFTP connection to %s was lost; reconnecting", self.address)
            self.connect()
        client = self._sftp
        if client is None:  # connect() either opens the SFTP session or raises
            raise SFTPError(f"SFTP connection to {self.address} is not open")
        return client

    # -- remote file operations ------------------------------------------------------------

    def list_files(self, directory: str) -> list[RemoteFile]:
        """List the files of ``directory`` sorted by name.

        Directories and symlinks are skipped. Entries without a mode (servers that omit
        permissions) or without file type bits are treated as files.
        """
        try:
            entries = self._client().listdir_attr(directory)
        except _REMOTE_ERRORS as exc:
            raise SFTPError(f"cannot list SFTP directory {directory!r}: {_describe(exc)}") from exc
        files = []
        for attr in entries:
            mode = attr.st_mode
            if mode is not None and (stat.S_ISDIR(mode) or stat.S_ISLNK(mode)):
                continue
            files.append(
                RemoteFile(
                    name=attr.filename,
                    size=attr.st_size if attr.st_size is not None else 0,
                    mtime=attr.st_mtime,
                )
            )
        files.sort(key=lambda remote_file: remote_file.name)
        return files

    def realpath(self, path: str) -> str:
        """Return the server's canonical absolute form of ``path`` (SSH_FXP_REALPATH).

        The server resolves relative paths against the login directory, ``.``/``..``, doubled
        slashes and (OpenSSH) symlinks, so two spellings of one directory compare equal.
        """
        try:
            resolved = self._client().normalize(path)
        except _REMOTE_ERRORS as exc:
            raise SFTPError(f"cannot resolve SFTP path {path!r}: {_describe(exc)}") from exc
        resolved = posixpath.normpath(resolved)
        return "/" + resolved.lstrip("/") if resolved.startswith("//") else resolved

    def exists(self, path: str) -> bool:
        """Return True if ``path`` exists on the server (files and directories)."""
        try:
            self._client().stat(path)
        except FileNotFoundError:
            return False
        except _REMOTE_ERRORS as exc:
            raise SFTPError(f"cannot stat {path!r}: {_describe(exc)}") from exc
        return True

    def ensure_dir(self, directory: str) -> None:
        """Create ``directory`` and its missing parents (``mkdir -p``); existing ones are fine."""
        path = posixpath.normpath(directory)
        if path.startswith("//"):
            path = "/" + path.lstrip("/")
        if path in ("/", "."):
            return
        sftp = self._client()
        try:
            if self._is_dir(sftp, path):
                return
            current = "/" if path.startswith("/") else ""
            for part in path.strip("/").split("/"):
                current = posixpath.join(current, part) if current else part
                if self._is_dir(sftp, current):
                    continue
                try:
                    sftp.mkdir(current)
                except OSError:
                    # Lost a race with another client, or a server that reports "exists" as a
                    # generic failure: only an actual directory is acceptable.
                    if not self._is_dir(sftp, current):
                        raise
                else:
                    logger.info("Created SFTP directory %s", current)
        except _REMOTE_ERRORS as exc:
            raise SFTPError(f"cannot create SFTP directory {directory!r}: {_describe(exc)}") from exc

    @staticmethod
    def _is_dir(sftp: paramiko.SFTPClient, path: str) -> bool:
        try:
            attr = sftp.stat(path)
        except FileNotFoundError:
            return False
        mode = attr.st_mode
        if mode is None or stat.S_IFMT(mode) == 0:
            return True  # the server does not report file types; assume the directory is fine
        if not stat.S_ISDIR(mode):
            raise SFTPError(f"SFTP path {path!r} exists but is not a directory")
        return True

    def remove(self, path: str) -> bool:
        """Delete a remote file. Return False if it did not exist (already gone)."""
        try:
            self._client().remove(path)
        except FileNotFoundError:
            return False
        except _REMOTE_ERRORS as exc:
            raise SFTPError(f"cannot remove {path!r}: {_describe(exc)}") from exc
        return True

    def write_probe(self, directory: str) -> None:
        """Prove that ``directory`` is writable: write a small hidden file, stat it, delete it."""
        name = f".odoo-backup-check-{os.getpid()}-{secrets.token_hex(4)}"
        path = posixpath.join(directory, name)
        sftp = self._client()
        created = False
        try:
            with sftp.open(path, "wb") as probe:
                created = True
                probe.write(_PROBE_PAYLOAD)
            size = sftp.stat(path).st_size
            if size != len(_PROBE_PAYLOAD):
                raise SFTPError(f"SFTP write probe {path!r} has {size} bytes instead of {len(_PROBE_PAYLOAD)}")
            sftp.remove(path)
            created = False
        except _REMOTE_ERRORS as exc:
            raise SFTPError(f"SFTP directory {directory!r} is not writable: {_describe(exc)}") from exc
        finally:
            if created:
                self._remove_quietly(path)

    def _remove_quietly(self, path: str) -> None:
        try:
            self.remove(path)
        except SFTPError as exc:
            logger.warning("Could not remove %s: %s", path, exc)

    # -- upload ----------------------------------------------------------------------------

    def upload(
        self,
        local_path: str | os.PathLike[str],
        directory: str,
        name: str,
        attempts: int = 3,
        progress: Callable[[int], object] | None = None,
    ) -> UploadResult:
        """Upload ``local_path`` as ``directory/name`` without ever exposing a partial file.

        Each attempt (re)connects if needed, writes ``name + ".upload"`` from offset 0 (no
        append-resume: a resumed file could silently combine two different uploads), waits for
        the server's confirmation of every pipelined write, compares the remote size and renames
        it to ``name``. Failed attempts are retried after ``5 s * attempt``; after the last one the
        partial file is removed (best effort) and :class:`UploadError` is raised.

        An existing ``directory/name`` is never overwritten (UploadError, not retried).
        HostKeyMismatchError and SFTPAuthenticationError are raised immediately (not retried).
        ``progress`` is called with the number of bytes written so far in the current attempt.
        """
        if attempts < 1:
            raise ValueError("attempts must be >= 1")
        if not name or "/" in name:
            raise ValueError(f"invalid remote file name {name!r}")
        tmp_path = posixpath.join(directory, name + PARTIAL_SUFFIX)
        final_path = posixpath.join(directory, name)
        try:
            local_size = os.stat(local_path).st_size
        except OSError as exc:
            message = f"cannot read local backup file {local_path}: {_describe(exc)}"
            raise UploadError(message, retryable=False) from exc

        started = time.monotonic()
        state = _UploadState()
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                result = self._upload_attempt(
                    local_path, local_size, tmp_path, final_path, attempt, attempts, progress, state
                )
            except HostKeyMismatchError, SFTPAuthenticationError:
                raise
            except UploadError as exc:
                if not exc.retryable:
                    self._remove_partial_after_failure(tmp_path)
                    raise
                last_error = exc
            except Exception as exc:
                last_error = exc
            else:
                result = dataclasses.replace(result, seconds=time.monotonic() - started)
                logger.info(
                    "Uploaded %s (%d bytes) to %s in %.1f s (%.1f MB/s, write requests of %d bytes, attempt %d/%d)",
                    name,
                    local_size,
                    self.address,
                    result.seconds,
                    _mb_per_second(local_size, result.seconds),
                    result.request_size,
                    attempt,
                    attempts,
                )
                return result
            logger.warning(
                "SFTP upload of %s failed (attempt %d/%d): %s", name, attempt, attempts, _describe(last_error)
            )
            self.close()
            if attempt < attempts:
                delay = RETRY_BACKOFF_SECONDS * attempt
                logger.info("Retrying the upload of %s in %.0f s", name, delay)
                (self._sleep or time.sleep)(delay)

        self._remove_partial_after_failure(tmp_path)
        raise UploadError(
            f"upload of {name} to {self.address}:{directory} failed after {attempts} attempt(s): "
            f"{_describe(last_error)}"
        ) from last_error

    def _upload_attempt(
        self,
        local_path: str | os.PathLike[str],
        local_size: int,
        tmp_path: str,
        final_path: str,
        attempt: int,
        attempts: int,
        progress: Callable[[int], object] | None,
        state: _UploadState,
    ) -> UploadResult:
        """One upload attempt; ``UploadResult.seconds`` is the duration of this attempt."""
        sftp = self._client()
        existing = self._stat_or_none(sftp, final_path)
        if existing is not None:
            if state.rename_sent and existing.st_size == local_size:
                # The previous attempt's rename was executed but its answer got lost.
                logger.info("%s was completed by the rename of the previous attempt", final_path)
                return UploadResult(final_path, local_size, 0.0, existing.st_mtime, state.request_size)
            message = f"remote file {final_path} already exists; refusing to overwrite it"
            raise UploadError(message, retryable=False)

        request_size = state.request_size = self._negotiated_request_size(sftp)
        label = posixpath.basename(final_path)
        logger.info(
            "Uploading %s (%.2f GiB) to %s:%s (attempt %d/%d, write requests of %d bytes)",
            label,
            local_size / _GIB,
            self.address,
            tmp_path,
            attempt,
            attempts,
            request_size,
        )
        reporter = _ProgressReporter(label, local_size, attempt, attempts)
        sent = 0
        with open(local_path, "rb", buffering=0) as local:
            remote = sftp.open(tmp_path, "wb")
            try:
                remote.set_pipelined(True)
                remote.MAX_REQUEST_SIZE = request_size  # per instance, see paramiko SFTPFile._write
                while chunk := local.read(UPLOAD_CHUNK_SIZE):
                    remote.write(chunk)
                    sent += len(chunk)
                    reporter.update(sent)
                    if progress is not None:
                        progress(sent)
                _drain_pipelined_writes(remote, tmp_path)
            except BaseException:
                # Tear the session down first: closing the handle on a hung server would block
                # for another full timeout, on a closed channel it returns immediately.
                self.close()
                _close_quietly(remote)
                raise
            remote.close()

        if sent != local_size:
            message = f"local file changed during the upload: read {sent} bytes, expected {local_size}"
            raise UploadError(message, retryable=False)
        uploaded = sftp.stat(tmp_path).st_size
        if uploaded != local_size:
            raise UploadError(
                f"size mismatch after upload of {tmp_path}: remote {uploaded} bytes, local {local_size} bytes"
            )

        state.rename_sent = True
        self._rename(sftp, tmp_path, final_path)
        final = sftp.stat(final_path)
        if final.st_size != local_size:
            self._remove_quietly(final_path)  # never leave an incomplete file under the final name
            raise UploadError(
                f"size mismatch after renaming to {final_path}: remote {final.st_size} bytes, local {local_size} bytes"
            )
        return UploadResult(final_path, local_size, reporter.elapsed(), final.st_mtime, request_size)

    @staticmethod
    def _stat_or_none(sftp: paramiko.SFTPClient, path: str) -> paramiko.SFTPAttributes | None:
        try:
            return sftp.stat(path)
        except FileNotFoundError:
            return None

    @staticmethod
    def _rename(sftp: paramiko.SFTPClient, source: str, target: str) -> None:
        """Rename with the atomic ``posix-rename@openssh.com`` extension, else plain SFTP rename.

        The caller has checked that ``target`` does not exist (posix-rename would overwrite it).
        """
        try:
            sftp.posix_rename(source, target)
        except OSError as exc:
            if isinstance(exc, TimeoutError) or not _is_unsupported(exc):
                raise
            logger.debug("Server does not support posix-rename; using plain SFTP rename")
            sftp.rename(source, target)

    def _negotiated_request_size(self, sftp: paramiko.SFTPClient) -> int:
        """Return the write request size for this session (cached until the session closes)."""
        if self._request_size is None:
            if self.max_request_size is not None:
                self._request_size = max(1, min(int(self.max_request_size), MAX_REQUEST_SIZE_CAP))
            else:
                self._request_size = _query_write_limit(sftp)
        return self._request_size

    def _remove_partial_after_failure(self, tmp_path: str) -> None:
        """Best effort: a leftover partial is also removed by the next run's stale-partial cleanup."""
        try:
            if self.remove(tmp_path):
                logger.info("Removed partial upload %s", tmp_path)
        except SFTPError as exc:
            logger.warning("Could not remove partial upload %s: %s", tmp_path, exc)


@dataclasses.dataclass
class _UploadState:
    """State carried across the attempts of one upload."""

    rename_sent: bool = False
    request_size: int = DEFAULT_REQUEST_SIZE


class _ProgressReporter:
    """Logs upload progress every PROGRESS_LOG_BYTES or PROGRESS_LOG_SECONDS."""

    def __init__(self, label: str, total: int, attempt: int, attempts: int) -> None:
        self._label = label
        self._total = total
        self._attempt = attempt
        self._attempts = attempts
        self._start = self._last_log = time.monotonic()
        self._next_bytes = PROGRESS_LOG_BYTES

    def elapsed(self) -> float:
        return time.monotonic() - self._start

    def update(self, done: int) -> None:
        now = time.monotonic()
        if done < self._next_bytes and now - self._last_log < PROGRESS_LOG_SECONDS:
            return
        percent = 100.0 * done / self._total if self._total else 100.0
        logger.info(
            "Uploading %s: %.1f%% (%.2f of %.2f GiB) at %.1f MB/s (attempt %d/%d)",
            self._label,
            percent,
            done / _GIB,
            self._total / _GIB,
            _mb_per_second(done, now - self._start),
            self._attempt,
            self._attempts,
        )
        self._last_log = now
        while self._next_bytes <= done:
            self._next_bytes += PROGRESS_LOG_BYTES


def _drain_pipelined_writes(remote: paramiko.SFTPFile, path: str) -> None:
    """Wait for the server's answer to every outstanding pipelined write; raise on any error.

    With ``set_pipelined(True)`` paramiko sends WRITE requests without waiting for their status
    and registers them with ``fileobj=type(None)``. It only collects them opportunistically
    inside later ``write()`` calls. ``SFTPFile.close()`` does NOT surface the errors of the
    still-outstanding writes: while waiting for the CLOSE answer it reads and silently discards
    them. A failed write near the end of the file therefore went unnoticed, and a failed write
    followed by a successful one leaves a hole with the correct total size - the size check
    cannot catch that. This drain is the only reliable detection.

    ``SFTPClient._read_response(req)`` turns an error status into an OSError (paramiko's
    ``_convert_status``); SFTP servers answer requests in order, so waiting for each request
    number in turn consumes exactly these answers.
    """
    pending = getattr(remote, "_reqs", None)
    if pending is None:
        message = "unsupported paramiko version: SFTPFile has no pending request queue"
        raise UploadError(message, retryable=False)
    while pending:
        request = pending.popleft()
        try:
            response_type, _message = remote.sftp._read_response(request)
        except TimeoutError:
            raise  # no answer at all is a connection problem, not a rejected write
        except OSError as exc:
            raise UploadError(f"the SFTP server rejected a write to {path}: {_describe(exc)}") from exc
        if response_type != CMD_STATUS:
            raise UploadError(f"unexpected SFTP response type {response_type} to a write to {path}")


def _query_write_limit(sftp: paramiko.SFTPClient) -> int:
    """Ask the server for its write limit via the ``limits@openssh.com`` extension.

    The reply carries four uint64: max packet length, max read length, max write length and max
    open handles (0 = no stated limit). The result is capped at MAX_REQUEST_SIZE_CAP. Servers
    without the extension (e.g. ProFTPD mod_sftp on Hetzner port 22) answer with an error status,
    which paramiko raises; any failure falls back to DEFAULT_REQUEST_SIZE.
    """
    try:
        response_type, message = sftp._request(CMD_EXTENDED, "limits@openssh.com")
        if response_type != CMD_EXTENDED_REPLY:
            raise paramiko.sftp.SFTPError(f"unexpected response type {response_type}")
        max_packet, _max_read, max_write, _max_handles = (message.get_int64() for _ in range(4))
    except Exception as exc:  # any failure only means "no usable limits"
        logger.debug(
            "limits@openssh.com unavailable (%s); using %d byte write requests",
            _describe(exc),
            DEFAULT_REQUEST_SIZE,
        )
        return DEFAULT_REQUEST_SIZE
    if max_write <= 0:
        return DEFAULT_REQUEST_SIZE
    size = min(max_write, MAX_REQUEST_SIZE_CAP)
    if max_packet > 1024:
        size = min(size, max_packet - 1024)  # leave room for the WRITE request header
    return max(size, 1)


def _close_quietly(remote: paramiko.SFTPFile) -> None:
    try:
        remote.close()
    except Exception as exc:
        logger.debug("Ignoring error while closing a remote file: %s", _describe(exc))


def _is_unsupported(exc: OSError) -> bool:
    text = str(exc).lower()
    return "unsupported" in text or "not supported" in text


def _describe(exc: BaseException | None) -> str:
    if exc is None:
        return "unknown error"
    return str(exc) or type(exc).__name__


def _mb_per_second(size: int, seconds: float) -> float:
    return size / 1_000_000 / seconds if seconds > 0 else 0.0
