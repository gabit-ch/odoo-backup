"""In-process SSH/SFTP server for tests, built on paramiko's server side.

The server listens on 127.0.0.1 (random port), serves a local directory as the SFTP root, uses a
freshly generated Ed25519 host key and supports password and public key authentication::

    with SFTPStubServer(root_dir) as stub:
        connection = SFTPConnection(**stub.connection_kwargs(timeout=5.0))
        env = stub.env()                 # SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASSWORD, SFTP_HOST_KEY
        stub.auth_attempts               # every authentication request the server received

Failure injection knobs are plain attributes that may be changed at any time:

``fail_write_at`` / ``fail_write_span``
    WRITE requests with ``fail_write_at <= offset < fail_write_at + fail_write_span`` answer
    SSH_FX_FAILURE and are not written (span None = to the end of the file).
``fail_rename``
    RENAME and posix-rename answer SSH_FX_FAILURE.
``posix_rename_supported``
    False answers posix-rename@openssh.com with SSH_FX_OP_UNSUPPORTED (like old servers).
``limits``
    None answers limits@openssh.com with SSH_FX_OP_UNSUPPORTED (like ProFTPD mod_sftp);
    a 4-tuple (max packet, max read, max write, max open handles) is sent as the reply.
``drop_after_bytes`` / ``drop_limit``
    Close the connection once a connection has received that many WRITE payload bytes (the
    triggering write is stored, its answer is never sent); at most ``drop_limit`` connections
    are dropped this way (None = every connection).
``drop_after_rename``
    One-shot: perform the next rename, then close the connection before answering.
``omit_permissions``
    Send attributes without permissions (``st_mode`` None on the client).
``stall_sftp``
    ``"request"``: the server never answers the ``sftp`` subsystem request; ``"version"``: it
    accepts the subsystem but never sends the SFTP VERSION packet (a hung sftp-server). Both
    stall until ``stop()``.

``extra_host_keys`` adds further host keys (a server with e.g. Ed25519 and RSA keys); the
client's algorithm preference decides which one the server presents. REALPATH resolves like a
real server: relative to the root, ``..`` and doubled slashes collapsed, symlinks followed.

The password is never recorded; ``auth_attempts`` only stores method, user name and outcome.
"""

import hmac
import io
import logging
import os
import pathlib
import posixpath
import secrets
import socket
import threading
from collections.abc import Sequence
from dataclasses import dataclass

import paramiko
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from paramiko.sftp import CMD_EXTENDED, CMD_EXTENDED_REPLY

HOST = "127.0.0.1"

#: Server-side paramiko transports log here instead of "paramiko.transport". The NullHandler keeps
#: expected server noise (e.g. "Connection reset by peer" when a client closes its socket) out of
#: the unittest output via logging's last-resort handler; records still propagate to the root
#: logger, so a test can capture them with ``assertLogs()``.
SERVER_LOG_CHANNEL = "tests.sftp_stub.server"
logging.getLogger(SERVER_LOG_CHANNEL).addHandler(logging.NullHandler())


def generate_ed25519_key() -> paramiko.Ed25519Key:
    """Return a new Ed25519 private key as a paramiko key (paramiko cannot generate Ed25519)."""
    pem = Ed25519PrivateKey.generate().private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH, serialization.NoEncryption()
    )
    return paramiko.Ed25519Key(file_obj=io.StringIO(pem.decode("ascii")))


def write_private_key(path: str | os.PathLike[str], passphrase: str | None = None) -> paramiko.Ed25519Key:
    """Generate an Ed25519 key, store it OpenSSH-formatted (mode 0600, optionally encrypted) at
    ``path`` and return it as a paramiko key (usable in ``SFTPStubServer(authorized_keys=...)``)."""
    private = Ed25519PrivateKey.generate()
    encryption = (
        serialization.BestAvailableEncryption(passphrase.encode("utf-8"))
        if passphrase is not None
        else serialization.NoEncryption()
    )
    data = private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH, encryption)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
    unencrypted = private.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH, serialization.NoEncryption()
    )
    return paramiko.Ed25519Key(file_obj=io.StringIO(unencrypted.decode("ascii")))


@dataclass(frozen=True)
class AuthAttempt:
    method: str  # "none", "password", "publickey" or "keyboard-interactive"
    username: str
    accepted: bool


@dataclass(frozen=True)
class OpenRequest:
    connection: int  # 1-based number of the SSH connection
    path: str  # remote path as requested by the client
    flags: int  # os.O_* flags


@dataclass(frozen=True)
class WriteRequest:
    connection: int
    path: str
    offset: int
    length: int
    failed: bool  # answered with SSH_FX_FAILURE by fail_write_at


class SFTPStubServer:
    """SSH + SFTP server on 127.0.0.1:<random port> serving ``root`` (see module docstring)."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        username: str = "odoo-backup",
        password: str | None = None,
        authorized_keys: Sequence[paramiko.PKey] = (),
        host_key: paramiko.PKey | None = None,
        extra_host_keys: Sequence[paramiko.PKey] = (),
        auth_methods: Sequence[str] = ("publickey", "password"),
    ) -> None:
        """``password`` defaults to a random throwaway value (``stub.password``)."""
        self.root = pathlib.Path(root)
        self.username = username
        self.password = password if password is not None else secrets.token_urlsafe(16)
        self.authorized_keys = list(authorized_keys)
        self.host_key = host_key if host_key is not None else generate_ed25519_key()
        self.extra_host_keys = list(extra_host_keys)
        self.auth_methods = tuple(auth_methods)

        # Failure injection knobs (see module docstring).
        self.fail_write_at: int | None = None
        self.fail_write_span: int | None = None
        self.fail_rename = False
        self.posix_rename_supported = True
        self.limits: tuple[int, int, int, int] | None = None
        self.drop_after_bytes: int | None = None
        self.drop_limit: int | None = 1
        self.drop_after_rename = False
        self.omit_permissions = False
        self.stall_sftp: str | None = None

        self._lock = threading.Lock()
        self._auth_attempts: list[AuthAttempt] = []
        self._opens: list[OpenRequest] = []
        self._writes: list[WriteRequest] = []
        self._connections: list[_StubConnection] = []
        self._open_files: list[io.IOBase] = []
        self._drops = 0
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stopping = threading.Event()

    # -- lifecycle -------------------------------------------------------------------------

    def __enter__(self) -> "SFTPStubServer":
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    def start(self) -> "SFTPStubServer":
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind((HOST, 0))
        listener.listen(16)
        listener.settimeout(0.1)  # lets the accept loop notice stop()
        self._listener = listener
        self._thread = threading.Thread(target=self._serve, name="sftp-stub-accept", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stopping.set()
        if self._thread is not None:
            self._thread.join(5)
        if self._listener is not None:
            self._listener.close()
        for connection in self.connections:
            connection.close()
            if connection.transport.is_alive():
                connection.transport.join(5)
        # paramiko's SFTP server closes left-over handles in finish_subsystem(), but not reliably
        # after an aborted session (seen as "ResourceWarning: unclosed file" after failed writes).
        with self._lock:
            files, self._open_files = self._open_files, []
        for fileobj in files:
            try:
                fileobj.close()
            except (OSError, ValueError):
                pass

    def disconnect_all(self) -> None:
        """Close every open connection from the server side (simulates a network drop)."""
        for connection in self.connections:
            connection.close()

    # -- facts for clients -----------------------------------------------------------------

    @property
    def host(self) -> str:
        return HOST

    @property
    def port(self) -> int:
        assert self._listener is not None, "start() the stub first"
        return self._listener.getsockname()[1]

    @property
    def fingerprint(self) -> str:
        """``SHA256:<base64>`` fingerprint of the host key (OpenSSH format)."""
        return self.host_key.fingerprint

    @property
    def public_key_line(self) -> str:
        """``ssh-ed25519 AAAA...`` public host key line."""
        return f"{self.host_key.get_name()} {self.host_key.get_base64()}"

    @property
    def known_hosts_line(self) -> str:
        """known_hosts / ssh-keyscan style line: ``[127.0.0.1]:<port> ssh-ed25519 AAAA...``."""
        return f"[{self.host}]:{self.port} {self.public_key_line}"

    def connection_kwargs(self, **overrides: object) -> dict[str, object]:
        """Keyword arguments for ``SFTPConnection`` with password auth and a pinned host key."""
        kwargs: dict[str, object] = {
            "host": self.host,
            "port": self.port,
            "user": self.username,
            "password": self.password,
            "host_keys": (self.fingerprint,),
            "timeout": 10.0,
        }
        kwargs.update(overrides)
        return kwargs

    def env(self) -> dict[str, str]:
        """Environment variables pointing the backup service at this stub (host key pinned)."""
        return {
            "SFTP_HOST": self.host,
            "SFTP_PORT": str(self.port),
            "SFTP_USER": self.username,
            "SFTP_PASSWORD": self.password,
            "SFTP_HOST_KEY": self.fingerprint,
        }

    def local_path(self, remote_path: str) -> pathlib.Path:
        """Map a remote path (absolute or relative to the SFTP root) to the local file system."""
        relative = posixpath.normpath("/" + remote_path).lstrip("/")
        return self.root / relative if relative else self.root

    # -- observations ----------------------------------------------------------------------

    @property
    def auth_attempts(self) -> list[AuthAttempt]:
        with self._lock:
            return list(self._auth_attempts)

    @property
    def opens(self) -> list[OpenRequest]:
        with self._lock:
            return list(self._opens)

    @property
    def writes(self) -> list[WriteRequest]:
        with self._lock:
            return list(self._writes)

    @property
    def connections(self) -> list["_StubConnection"]:
        with self._lock:
            return list(self._connections)

    @property
    def connection_count(self) -> int:
        with self._lock:
            return len(self._connections)

    @property
    def drops(self) -> int:
        with self._lock:
            return self._drops

    # -- internals -------------------------------------------------------------------------

    def _serve(self) -> None:
        assert self._listener is not None
        while not self._stopping.is_set():
            try:
                sock, _address = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self._start_connection(sock)

    def _start_connection(self, sock: socket.socket) -> None:
        with self._lock:
            connection = _StubConnection(self, len(self._connections) + 1, sock)
            self._connections.append(connection)
        transport = connection.transport
        for key in (self.host_key, *self.extra_host_keys):
            transport.add_server_key(key)
        if self.stall_sftp == "version":
            transport.set_subsystem_handler("sftp", _StalledSubsystem, self._stopping)
        else:
            transport.set_subsystem_handler("sftp", _StubSFTPServer, _StubSFTPInterface, connection)
        try:
            transport.start_server(event=threading.Event(), server=_StubServerInterface(self))
        except (paramiko.SSHException, OSError, EOFError):
            connection.close()

    def _record_auth(self, method: str, username: str, accepted: bool) -> None:
        with self._lock:
            self._auth_attempts.append(AuthAttempt(method, username, accepted))

    def _record_open(self, connection: "_StubConnection", path: str, flags: int) -> None:
        with self._lock:
            self._opens.append(OpenRequest(connection.number, path, flags))

    def _record_write(
        self, connection: "_StubConnection", path: str, offset: int, length: int, failed: bool
    ) -> None:
        with self._lock:
            self._writes.append(WriteRequest(connection.number, path, offset, length, failed))

    def _write_fails(self, offset: int) -> bool:
        start = self.fail_write_at
        if start is None or offset < start:
            return False
        span = self.fail_write_span
        return span is None or offset < start + span

    def _consume_drop(self) -> bool:
        with self._lock:
            if self.drop_limit is not None and self._drops >= self.drop_limit:
                return False
            self._drops += 1
            return True

    def _consume_rename_drop(self) -> bool:
        with self._lock:
            if not self.drop_after_rename:
                return False
            self.drop_after_rename = False
            self._drops += 1
            return True


class _StubConnection:
    """One accepted SSH connection."""

    def __init__(self, stub: SFTPStubServer, number: int, sock: socket.socket) -> None:
        self.stub = stub
        self.number = number
        self.transport = paramiko.Transport(sock)
        self.transport.set_log_channel(SERVER_LOG_CHANNEL)
        self.bytes_written = 0
        self.dropped = False

    def account_write(self, length: int) -> bool:
        """Count WRITE payload; return True if the connection must be dropped now."""
        self.bytes_written += length
        limit = self.stub.drop_after_bytes
        if limit is None or self.dropped or self.bytes_written < limit:
            return False
        return self.stub._consume_drop()

    def drop(self) -> None:
        self.dropped = True
        self.close()

    def close(self) -> None:
        # Transport.close() returns early when the handshake never completed; close the socket too.
        self.transport.close()
        self.transport.sock.close()


class _StubServerInterface(paramiko.ServerInterface):
    def __init__(self, stub: SFTPStubServer) -> None:
        self._stub = stub

    def get_allowed_auths(self, username: str) -> str:
        return ",".join(self._stub.auth_methods)

    def check_auth_none(self, username: str) -> int:
        self._stub._record_auth("none", username, False)
        return paramiko.AUTH_FAILED

    def check_auth_password(self, username: str, password: str) -> int:
        stub = self._stub
        accepted = (
            "password" in stub.auth_methods
            and _same(username, stub.username)
            and _same(password, stub.password)
        )
        stub._record_auth("password", username, accepted)
        return paramiko.AUTH_SUCCESSFUL if accepted else paramiko.AUTH_FAILED

    def check_auth_publickey(self, username: str, key: paramiko.PKey) -> int:
        stub = self._stub
        accepted = (
            "publickey" in stub.auth_methods
            and _same(username, stub.username)
            and any(key.asbytes() == authorized.asbytes() for authorized in stub.authorized_keys)
        )
        stub._record_auth("publickey", username, accepted)
        return paramiko.AUTH_SUCCESSFUL if accepted else paramiko.AUTH_FAILED

    def check_auth_interactive(self, username: str, submethods: str) -> int:
        self._stub._record_auth("keyboard-interactive", username, False)
        return paramiko.AUTH_FAILED

    def check_channel_request(self, kind: str, chanid: int) -> int:
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_channel_subsystem_request(self, channel: paramiko.Channel, name: str) -> bool:
        if self._stub.stall_sftp == "request":
            self._stub._stopping.wait()  # blocks this connection's transport thread until stop()
            return False
        return super().check_channel_subsystem_request(channel, name)


class _StalledSubsystem(paramiko.SubsystemHandler):
    """Accepts the ``sftp`` subsystem but never sends the SFTP VERSION packet."""

    def __init__(self, channel: paramiko.Channel, name: str, server: paramiko.ServerInterface,
                 stopping: threading.Event) -> None:
        super().__init__(channel, name, server)
        self._stopping = stopping

    def start_subsystem(self, name: str, transport: paramiko.Transport, channel: paramiko.Channel) -> None:
        self._stopping.wait()


class _StubSFTPServer(paramiko.SFTPServer):
    """paramiko's SFTP server plus the ``limits@openssh.com`` extension (reply or unsupported)."""

    def _process(self, t: int, request_number: int, msg: paramiko.Message) -> None:
        if t == CMD_EXTENDED:
            if msg.get_text() == "limits@openssh.com":
                limits = self.server.limits()
                if limits is None:
                    self._send_status(request_number, paramiko.SFTP_OP_UNSUPPORTED)
                    return
                reply = paramiko.Message()
                reply.add_int(request_number)
                for value in limits:
                    reply.add_int64(value)
                self._send_packet(CMD_EXTENDED_REPLY, reply)
                return
            msg.rewind()
            msg.get_int()  # back to the position after the request id, as the base class expects
        super()._process(t, request_number, msg)


class _StubHandle(paramiko.SFTPHandle):
    def __init__(self, connection: _StubConnection, path: str, fileobj: io.BufferedIOBase, flags: int) -> None:
        super().__init__(flags)
        self._connection = connection
        self._path = path
        self.readfile = fileobj
        self.writefile = fileobj

    def write(self, offset: int, data: bytes) -> int:
        stub = self._connection.stub
        failed = stub._write_fails(offset)
        stub._record_write(self._connection, self._path, offset, len(data), failed)
        if failed:
            return paramiko.SFTP_FAILURE
        result = super().write(offset, data)
        if self._connection.account_write(len(data)):
            self._connection.drop()  # the data is stored, the answer is never sent
        return result

    def stat(self) -> paramiko.SFTPAttributes | int:
        try:
            return paramiko.SFTPAttributes.from_stat(os.fstat(self.writefile.fileno()))
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    def chattr(self, attr: paramiko.SFTPAttributes) -> int:
        return paramiko.SFTP_OK


class _StubSFTPInterface(paramiko.SFTPServerInterface):
    def __init__(
        self, server: paramiko.ServerInterface, connection: _StubConnection, *args: object, **kwargs: object
    ) -> None:
        super().__init__(server, *args, **kwargs)
        self._connection = connection
        self._stub = connection.stub

    def limits(self) -> tuple[int, int, int, int] | None:
        return self._stub.limits

    def canonicalize(self, path: str) -> str:
        """REALPATH like OpenSSH: symlinks resolved, the root acts as the chroot."""
        root = os.path.realpath(self._stub.root)
        local = os.path.realpath(self._stub.local_path(path))
        relative = os.path.relpath(local, root)
        if relative == "." or relative.startswith(".."):
            return "/"
        return "/" + relative.replace(os.sep, "/")

    def _attributes(self, st: os.stat_result, filename: str | None = None) -> paramiko.SFTPAttributes:
        attr = paramiko.SFTPAttributes.from_stat(st, filename)
        if self._stub.omit_permissions:
            attr.st_mode = None
        return attr

    def list_folder(self, path: str) -> list[paramiko.SFTPAttributes] | int:
        folder = self._stub.local_path(path)
        try:
            return [self._attributes(os.lstat(folder / name), name) for name in sorted(os.listdir(folder))]
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    def stat(self, path: str) -> paramiko.SFTPAttributes | int:
        try:
            return self._attributes(os.stat(self._stub.local_path(path)))
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    def lstat(self, path: str) -> paramiko.SFTPAttributes | int:
        try:
            return self._attributes(os.lstat(self._stub.local_path(path)))
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    def open(self, path: str, flags: int, attr: paramiko.SFTPAttributes) -> paramiko.SFTPHandle | int:
        self._stub._record_open(self._connection, path, flags)
        try:
            fd = os.open(self._stub.local_path(path), flags, 0o644)
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)
        access = flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
        if access == os.O_WRONLY:
            mode = "ab" if flags & os.O_APPEND else "wb"
        elif access == os.O_RDWR:
            mode = "a+b" if flags & os.O_APPEND else "r+b"
        else:
            mode = "rb"
        fileobj = os.fdopen(fd, mode)
        with self._stub._lock:
            self._stub._open_files.append(fileobj)  # closed by stop() at the latest
        return _StubHandle(self._connection, path, fileobj, flags)

    def remove(self, path: str) -> int:
        try:
            os.remove(self._stub.local_path(path))
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)
        return paramiko.SFTP_OK

    def rename(self, oldpath: str, newpath: str) -> int:
        # SFTP v3 RENAME: fails if the target exists.
        if self._stub.fail_rename or self._stub.local_path(newpath).exists():
            return paramiko.SFTP_FAILURE
        return self._rename(oldpath, newpath)

    def posix_rename(self, oldpath: str, newpath: str) -> int:
        if not self._stub.posix_rename_supported:
            return paramiko.SFTP_OP_UNSUPPORTED
        if self._stub.fail_rename:
            return paramiko.SFTP_FAILURE
        return self._rename(oldpath, newpath)

    def _rename(self, oldpath: str, newpath: str) -> int:
        try:
            os.replace(self._stub.local_path(oldpath), self._stub.local_path(newpath))
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)
        if self._stub._consume_rename_drop():
            self._connection.drop()  # renamed, but the client never gets the answer
        return paramiko.SFTP_OK

    def mkdir(self, path: str, attr: paramiko.SFTPAttributes) -> int:
        try:
            os.mkdir(self._stub.local_path(path), 0o755)
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)
        return paramiko.SFTP_OK

    def rmdir(self, path: str) -> int:
        try:
            os.rmdir(self._stub.local_path(path))
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)
        return paramiko.SFTP_OK

    def chattr(self, path: str, attr: paramiko.SFTPAttributes) -> int:
        return paramiko.SFTP_OK


def _same(given: str, expected: str) -> bool:
    return hmac.compare_digest(given.encode("utf-8"), expected.encode("utf-8"))
