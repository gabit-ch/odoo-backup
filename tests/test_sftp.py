"""Tests for odoo_backup.sftp against the in-process paramiko SFTP stub (tests/sftp_stub.py)."""

import base64
import hashlib
import logging
import os
import pathlib
import random
import socket
import tempfile
import threading
import time
import unittest

import paramiko

from odoo_backup import sftp
from odoo_backup.sftp import (
    DEFAULT_REQUEST_SIZE,
    MAX_REQUEST_SIZE_CAP,
    PARTIAL_SUFFIX,
    HostKeyMismatchError,
    RemoteFile,
    SFTPAuthenticationError,
    SFTPConnection,
    SFTPError,
    UploadError,
    fingerprint,
    host_key_matches,
    pinned_key_types,
)
from tests.sftp_stub import AuthAttempt, SFTPStubServer, generate_ed25519_key, write_private_key

# Public host key of a Hetzner Storage Box on port 22 (ProFTPD mod_sftp), recorded with
# ssh-keyscan; the README documents its fingerprint. Public data, no secret.
HETZNER_PORT22_KEY = (
    "ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEA5EB5p/5Hp3hGW1oHok+PIOH9Pbn7cnUiGmUEBrCVjnAw+HrKyN8bYVV0dIG"
    "llswYXwkG/+bgiBlE6IVIBAq+JwVWu1Sss3KarHY3OvFJUXZoZyRRg/Gc/+LRCE7lyKpwWQ70dbelGRyyJFH36eNv6ySXoU"
    "YtGkwlU5IVaHPApOxe4LHPZa/qhSRbPo2hwoh0orCtgejRebNtW5nlx00DNFgsvn8Svz2cIYLxsPVzKgUxs8Zxsxgn+Q/Uv"
    "R7uq4AbAhyBMLxv7DjJ1pc7PJocuTno2Rw9uMZi1gkjbnmiOh6TTXIEWbnroyIhwc8555uto9melEUmWNQ+C+PwAK+MPw=="
)
HETZNER_PORT22_FINGERPRINT = "SHA256:EMlfI8GsRIfpVkoW1H2u0zYVpFGKkIMKHFZIRkf2ioI"

OPENSSH_LIMITS = (262144, 261120, 261120, 64)  # what OpenSSH 9.x answers to limits@openssh.com
LOGGER = "odoo_backup.sftp"
REQ = DEFAULT_REQUEST_SIZE


def _sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _public_line(key: paramiko.PKey) -> str:
    return f"{key.get_name()} {key.get_base64()}"


class HostKeyHelperTests(unittest.TestCase):
    """fingerprint() and host_key_matches() are pure functions."""

    def setUp(self) -> None:
        self.key = generate_ed25519_key()
        self.other = generate_ed25519_key()

    def test_fingerprint_of_recorded_hetzner_key_matches_documented_value(self) -> None:
        key = paramiko.RSAKey(data=base64.b64decode(HETZNER_PORT22_KEY.split()[1]))
        self.assertEqual(fingerprint(key), HETZNER_PORT22_FINGERPRINT)

    def test_fingerprint_format(self) -> None:
        value = fingerprint(self.key)
        self.assertTrue(value.startswith("SHA256:"))
        self.assertNotIn("=", value)
        self.assertEqual(value, self.key.fingerprint)  # paramiko's own OpenSSH-style fingerprint

    def test_fingerprint_entries(self) -> None:
        fp = fingerprint(self.key)
        for entry in (fp, fp + "=", "sha256:" + fp[7:], f"  {fp}\n"):
            with self.subTest(entry=entry):
                self.assertTrue(host_key_matches(self.key, [entry]))
        self.assertFalse(host_key_matches(self.key, [fingerprint(self.other)]))
        self.assertFalse(host_key_matches(self.key, ["SHA256:" + fp[8:]]))  # one char missing

    def test_public_key_and_known_hosts_entries(self) -> None:
        line = _public_line(self.key)
        entries = (
            line,
            line + " backup@storagebox",
            "u380201.your-storagebox.de " + line,
            "[u380201.your-storagebox.de]:23 " + line + " comment with spaces",
            "|1|c2FsdA==|aGFzaA== " + line,  # hashed known_hosts host field
        )
        for entry in entries:
            with self.subTest(entry=entry):
                self.assertTrue(host_key_matches(self.key, [entry]))

    def test_rejected_entries(self) -> None:
        line = _public_line(self.key)
        blob = line.split()[1]
        entries = (
            "",
            "   ",
            "# " + line,
            "@revoked * " + line,
            "@cert-authority * " + line,
            _public_line(self.other),
            "ssh-rsa " + blob,  # key type does not match the blob
            "ssh-ed25519 " + blob[:-4],  # truncated / invalid base64
            "ssh-ed25519 not*base64",
            "SHA256:",
        )
        for entry in entries:
            with self.subTest(entry=entry):
                self.assertFalse(host_key_matches(self.key, [entry]))
        self.assertFalse(host_key_matches(self.key, []))

    def test_any_entry_may_match(self) -> None:
        entries = [fingerprint(self.other), _public_line(self.other), fingerprint(self.key)]
        self.assertTrue(host_key_matches(self.key, entries))

    def test_pinned_key_types(self) -> None:
        entries = [
            fingerprint(self.key),
            "u1.your-storagebox.de " + HETZNER_PORT22_KEY,
            "[u1.your-storagebox.de]:23 " + _public_line(self.key) + " comment",
            _public_line(self.other),
            "# " + _public_line(self.key),
            "ssh-rsa " + _public_line(self.key).split()[1],  # blob of another type: no key
        ]
        self.assertEqual(pinned_key_types(entries), ("ssh-rsa", "ssh-ed25519"))
        self.assertEqual(pinned_key_types([fingerprint(self.key)]), ())

    def test_recorded_hetzner_known_hosts_line(self) -> None:
        key = paramiko.RSAKey(data=base64.b64decode(HETZNER_PORT22_KEY.split()[1]))
        self.assertTrue(host_key_matches(key, ["u380201.your-storagebox.de " + HETZNER_PORT22_KEY]))
        self.assertTrue(host_key_matches(key, [HETZNER_PORT22_FINGERPRINT]))
        self.assertFalse(host_key_matches(self.key, [HETZNER_PORT22_KEY]))


class StubTestCase(unittest.TestCase):
    """Starts a stub server over a temporary root for every test."""

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = pathlib.Path(tmp.name)
        self.root = self.tmp / "server"
        self.root.mkdir()
        self.sleeps: list[float] = []
        self.stub = self.start_stub()

    def start_stub(self, **kwargs: object) -> SFTPStubServer:
        stub = SFTPStubServer(self.root, **kwargs).start()
        self.addCleanup(stub.stop)
        return stub

    def connection(self, stub: SFTPStubServer | None = None, **overrides: object) -> SFTPConnection:
        stub = stub or self.stub
        kwargs = stub.connection_kwargs(timeout=5.0, sleep=self.sleeps.append)
        kwargs.update(overrides)
        conn = SFTPConnection(**kwargs)
        self.addCleanup(conn.close)
        return conn

    def local_file(self, size: int, name: str = "local.tar.gz") -> pathlib.Path:
        path = self.tmp / name
        path.write_bytes(random.Random(size).randbytes(size))
        return path


class ConnectTests(StubTestCase):
    def test_password_auth_and_session_facts(self) -> None:
        conn = self.connection()
        conn.connect()
        self.assertTrue(conn.connected)
        self.assertEqual(self.stub.auth_attempts, [AuthAttempt("password", self.stub.username, True)])
        self.assertEqual(conn.server_fingerprint, self.stub.fingerprint)
        self.assertEqual(conn.server_key_type, "ssh-ed25519")
        self.assertEqual(conn.cipher, "aes128-ctr")  # paramiko's default preference

    def test_context_manager_connects_and_closes(self) -> None:
        with self.connection() as conn:
            self.assertTrue(conn.connected)
            self.assertEqual(conn.list_files("/"), [])
        self.assertFalse(conn.connected)
        conn.close()  # idempotent

    def test_configured_cipher_preference(self) -> None:
        default_config = (
            "aes128-gcm@openssh.com", "aes256-gcm@openssh.com", "aes128-ctr", "aes256-ctr", "aes192-ctr",
        )
        for ciphers, expected in ((default_config, "aes128-gcm@openssh.com"), (("aes256-ctr",), "aes256-ctr")):
            with self.subTest(ciphers=ciphers), self.connection(ciphers=ciphers) as conn:
                self.assertEqual(conn.cipher, expected)

    def test_unsupported_cipher_names_are_ignored_with_warning(self) -> None:
        conn = self.connection(ciphers=("chacha20-poly1305@openssh.com", "aes192-ctr"))
        with self.assertLogs(LOGGER, logging.WARNING) as logs:
            conn.connect()
        self.assertIn("chacha20-poly1305@openssh.com", "\n".join(logs.output))
        self.assertEqual(conn.cipher, "aes192-ctr")

    def test_pinned_entry_formats_are_accepted_without_warning(self) -> None:
        entries = (
            (self.stub.fingerprint,),
            (self.stub.public_key_line,),
            (self.stub.known_hosts_line,),
            (fingerprint(generate_ed25519_key()), self.stub.public_key_line + " comment"),
        )
        for host_keys in entries:
            with self.subTest(host_keys=host_keys):
                conn = self.connection(host_keys=host_keys)
                with self.assertNoLogs(LOGGER, logging.WARNING):
                    conn.connect()
                conn.close()

    def test_strict_host_key_mismatch_aborts_before_any_authentication(self) -> None:
        other = generate_ed25519_key()
        pins = ((fingerprint(other),), (_public_line(other),), (f"[127.0.0.1]:22 {_public_line(other)}",))
        for host_keys in pins:
            with self.subTest(host_keys=host_keys):
                conn = self.connection(host_keys=host_keys)
                with self.assertRaises(HostKeyMismatchError) as caught:
                    conn.connect()
                self.assertIsInstance(caught.exception, SFTPError)
                self.assertIn(self.stub.fingerprint, str(caught.exception))
                self.assertFalse(conn.connected)
        self.assertEqual(self.stub.connection_count, 3)  # the server was reached every time ...
        self.assertEqual(self.stub.auth_attempts, [])  # ... but never received an auth request

    def test_unpinned_host_key_logs_warning_with_fingerprint_on_every_connect(self) -> None:
        conn = self.connection(host_keys=())
        for _ in range(2):
            with self.assertLogs(LOGGER, logging.WARNING) as logs:
                conn.connect()
            output = "\n".join(logs.output)
            self.assertIn("NOT verified", output)
            self.assertIn(self.stub.fingerprint, output)
            self.assertIn("ssh-ed25519", output)
        self.assertTrue(conn.connected)

    def test_private_key_auth(self) -> None:
        key_file = self.tmp / "id_ed25519"
        key = write_private_key(key_file)
        stub = self.start_stub(authorized_keys=[key], auth_methods=("publickey",))
        with self.connection(stub, password=None, key_file=str(key_file)) as conn:
            self.assertTrue(conn.connected)
        self.assertEqual(stub.auth_attempts, [AuthAttempt("publickey", stub.username, True)])

    def test_encrypted_private_key_with_passphrase(self) -> None:
        key_file = self.tmp / "id_ed25519"
        key = write_private_key(key_file, passphrase="pässphrase with spaces")
        stub = self.start_stub(authorized_keys=[key], auth_methods=("publickey",))
        conn = self.connection(stub, password=None, key_file=key_file, key_passphrase="pässphrase with spaces")
        with conn:
            self.assertTrue(conn.connected)

    def test_wrong_passphrase_fails_before_connecting_without_leaking_it(self) -> None:
        key_file = self.tmp / "id_ed25519"
        write_private_key(key_file, passphrase="right-passphrase")
        for passphrase in ("WRONG-passphrase-42", None):
            with self.subTest(passphrase=passphrase):
                conn = self.connection(password=None, key_file=key_file, key_passphrase=passphrase)
                with self.assertRaises(SFTPError) as caught:
                    conn.connect()
                self.assertIn("SFTP_PRIVATE_KEY_PASSPHRASE", str(caught.exception))
                self.assertNotIn("WRONG-passphrase-42", str(caught.exception))
        self.assertEqual(self.stub.connection_count, 0)

    def test_passphrase_for_unencrypted_key_is_ignored_like_openssh(self) -> None:
        key_file = self.tmp / "id_ed25519"
        key = write_private_key(key_file)
        stub = self.start_stub(authorized_keys=[key], auth_methods=("publickey",))
        conn = self.connection(stub, password=None, key_file=key_file, key_passphrase="unused")
        with self.assertLogs(LOGGER, logging.WARNING) as logs:
            conn.connect()
        self.assertIn("not encrypted", "\n".join(logs.output))
        self.assertTrue(conn.connected)

    def test_missing_key_file(self) -> None:
        conn = self.connection(password=None, key_file=self.tmp / "missing")
        with self.assertRaisesRegex(SFTPError, "cannot read SFTP private key file"):
            conn.connect()

    def test_rejected_key_falls_back_to_password(self) -> None:
        key_file = self.tmp / "id_ed25519"
        write_private_key(key_file)  # not authorized on the stub
        with self.connection(key_file=key_file) as conn:
            self.assertTrue(conn.connected)
        self.assertEqual(
            self.stub.auth_attempts,
            [
                AuthAttempt("publickey", self.stub.username, False),
                AuthAttempt("password", self.stub.username, True),
            ],
        )

    def test_wrong_password_raises_authentication_error_without_secret(self) -> None:
        secret = "WRONG-secret-" + os.urandom(4).hex()
        conn = self.connection(password=secret)
        with self.assertLogs(level=logging.DEBUG) as logs:  # root logger: includes paramiko
            with self.assertRaises(SFTPAuthenticationError) as caught:
                conn.connect()
            logging.getLogger(LOGGER).debug("end of capture")
        self.assertIsInstance(caught.exception, SFTPError)
        self.assertIn("authentication failed", str(caught.exception))
        self.assertNotIn(secret, str(caught.exception))
        self.assertNotIn(secret, repr(conn))
        self.assertFalse(any(secret in line for line in logs.output))
        self.assertEqual(self.stub.auth_attempts, [AuthAttempt("password", self.stub.username, False)])
        self.assertFalse(conn.connected)

    def test_connection_refused(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        conn = self.connection(port=port)
        with self.assertRaisesRegex(SFTPError, "cannot connect to SFTP server"):
            conn.connect()

    def test_non_ssh_server_fails_the_handshake(self) -> None:
        listener = socket.create_server(("127.0.0.1", 0))
        self.addCleanup(listener.close)

        def answer_garbage() -> None:
            client, _ = listener.accept()
            with client:
                client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")

        thread = threading.Thread(target=answer_garbage, daemon=True)
        thread.start()
        conn = self.connection(port=listener.getsockname()[1])
        # paramiko's client transport thread logs the banner error (with traceback) itself.
        with self.assertLogs("paramiko.transport", logging.ERROR):
            with self.assertRaisesRegex(SFTPError, "SSH handshake"):
                conn.connect()
        thread.join(5)

    def test_reconnects_after_the_server_dropped_the_session(self) -> None:
        conn = self.connection()
        conn.connect()
        self.stub.disconnect_all()
        deadline = time.monotonic() + 5
        while conn.connected and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertFalse(conn.connected)
        with self.assertLogs(LOGGER, logging.WARNING):
            self.assertEqual(conn.list_files("/"), [])
        self.assertEqual(self.stub.connection_count, 2)

    def test_password_or_key_is_required(self) -> None:
        with self.assertRaises(ValueError):
            SFTPConnection("127.0.0.1", 22, "user")

    def test_stalled_sftp_subsystem_times_out(self) -> None:
        """Regression: SFTPClient.from_transport() waited forever for a stalled SFTP server."""
        for stall in ("request", "version"):
            with self.subTest(stall=stall):
                self.stub.stall_sftp = stall
                conn = self.connection(timeout=1.0)
                started = time.monotonic()
                with self.assertRaises(SFTPError) as caught:
                    conn.connect()
                self.assertLess(time.monotonic() - started, 8)
                self.assertRegex(
                    str(caught.exception),
                    r"^cannot open an SFTP session on 127\.0\.0\.1:\d+: the SFTP server did not answer within 1 s$",
                )
                self.assertFalse(conn.connected)
        self.assertEqual([a.accepted for a in self.stub.auth_attempts], [True, True])  # auth itself worked


class MultipleHostKeyTests(StubTestCase):
    """A server with an Ed25519 and an RSA host key (OpenSSH, e.g. Hetzner port 23)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.rsa_key = paramiko.RSAKey.generate(2048)

    def setUp(self) -> None:
        super().setUp()
        self.multi = self.start_stub(extra_host_keys=[self.rsa_key])

    def test_pinned_rsa_public_key_is_negotiated(self) -> None:
        rsa_line = _public_line(self.rsa_key)
        for host_keys in ((rsa_line,), (f"[127.0.0.1]:{self.multi.port} {rsa_line}",),
                          (fingerprint(generate_ed25519_key()), rsa_line)):
            with self.subTest(host_keys=host_keys), self.connection(self.multi, host_keys=host_keys) as conn:
                self.assertEqual(conn.server_key_type, "ssh-rsa")
                self.assertEqual(conn.server_fingerprint, fingerprint(self.rsa_key))

    def test_without_public_keys_the_default_preference_applies(self) -> None:
        with self.connection(self.multi, host_keys=(self.multi.fingerprint,)) as conn:
            self.assertEqual(conn.server_key_type, "ssh-ed25519")
        with self.connection(self.multi, host_keys=(self.multi.public_key_line, _public_line(self.rsa_key))) as conn:
            self.assertEqual(conn.server_key_type, "ssh-ed25519")  # the first pinned type wins

    def test_rsa_fingerprint_alone_fails_with_a_hint(self) -> None:
        conn = self.connection(self.multi, host_keys=(fingerprint(self.rsa_key),))
        with self.assertRaises(HostKeyMismatchError) as caught:
            conn.connect()
        self.assertIn("presents its ssh-ed25519 key to this client: pin that fingerprint", str(caught.exception))
        self.assertEqual(self.multi.auth_attempts, [])


class RealpathTests(StubTestCase):
    def test_spellings_of_one_directory_resolve_to_one_path(self) -> None:
        (self.root / "backups" / "db-only").mkdir(parents=True)
        (self.root / "alias").symlink_to(self.root / "backups", target_is_directory=True)
        with self.connection() as conn:
            for spelling in ("backups", "/backups", "//backups/", "./backups/db-only/..", "/alias"):
                with self.subTest(spelling=spelling):
                    self.assertEqual(conn.realpath(spelling), "/backups")
            self.assertEqual(conn.realpath("backups/db-only"), "/backups/db-only")
            self.assertEqual(conn.realpath("."), "/")


class FileOperationTests(StubTestCase):
    def test_list_files_skips_directories_and_symlinks(self) -> None:
        (self.root / "b.tar.gz").write_bytes(b"12345")
        (self.root / "a.zip").write_bytes(b"1")
        (self.root / "sub").mkdir()
        (self.root / "link").symlink_to(self.root / "a.zip")
        with self.connection() as conn:
            files = conn.list_files("/")
        self.assertEqual([(f.name, f.size) for f in files], [("a.zip", 1), ("b.tar.gz", 5)])
        self.assertIsInstance(files[0], RemoteFile)
        self.assertAlmostEqual(files[0].mtime, time.time(), delta=60)

    def test_list_files_treats_entries_without_mode_as_files(self) -> None:
        (self.root / "a.zip").write_bytes(b"1")
        (self.root / "sub").mkdir()
        self.stub.omit_permissions = True
        with self.connection() as conn:
            self.assertEqual([f.name for f in conn.list_files("")], ["a.zip", "sub"])

    def test_list_missing_directory_raises(self) -> None:
        with self.connection() as conn, self.assertRaisesRegex(SFTPError, "cannot list"):
            conn.list_files("/missing")

    def test_ensure_dir_creates_nested_directories_idempotently(self) -> None:
        with self.connection() as conn:
            conn.ensure_dir("backups/db-only/deeper")
            conn.ensure_dir("backups/db-only/deeper/")
            conn.ensure_dir("/absolute//nested/")
            conn.ensure_dir("/")
            conn.ensure_dir(".")
        self.assertTrue((self.root / "backups" / "db-only" / "deeper").is_dir())
        self.assertTrue((self.root / "absolute" / "nested").is_dir())

    def test_ensure_dir_refuses_a_file_in_the_way(self) -> None:
        (self.root / "backups").write_bytes(b"not a directory")
        with self.connection() as conn, self.assertRaisesRegex(SFTPError, "not a directory"):
            conn.ensure_dir("backups/db-only")

    def test_ensure_dir_without_server_file_types(self) -> None:
        self.stub.omit_permissions = True
        with self.connection() as conn:
            conn.ensure_dir("x/y")
            conn.ensure_dir("x/y")
        self.assertTrue((self.root / "x" / "y").is_dir())

    def test_remove_and_exists(self) -> None:
        (self.root / "old.zip").write_bytes(b"x")
        with self.connection() as conn:
            self.assertTrue(conn.exists("/old.zip"))
            self.assertTrue(conn.remove("/old.zip"))
            self.assertFalse(conn.exists("/old.zip"))
            self.assertFalse(conn.remove("/old.zip"))
            self.assertFalse(conn.remove("missing-dir/x.zip"))

    def test_remove_error_other_than_missing_raises(self) -> None:
        (self.root / "dir").mkdir()
        with self.connection() as conn, self.assertRaisesRegex(SFTPError, "cannot remove"):
            conn.remove("/dir")

    def test_write_probe_leaves_nothing_behind(self) -> None:
        (self.root / "backups").mkdir()
        with self.connection() as conn:
            conn.write_probe("/backups")
        self.assertEqual(list((self.root / "backups").iterdir()), [])
        [probe] = self.stub.opens
        self.assertRegex(probe.path, rf"^/backups/\.odoo-backup-check-{os.getpid()}-[0-9a-f]{{8}}$")

    def test_write_probe_reports_unwritable_directory_and_cleans_up(self) -> None:
        (self.root / "backups").mkdir()
        self.stub.fail_write_at = 0
        with self.connection() as conn, self.assertRaisesRegex(SFTPError, "not writable"):
            conn.write_probe("/backups")
        self.assertEqual(list((self.root / "backups").iterdir()), [])

    def test_write_probe_missing_directory(self) -> None:
        with self.connection() as conn, self.assertRaisesRegex(SFTPError, "not writable"):
            conn.write_probe("/missing")


class UploadTests(StubTestCase):
    def upload(
        self, local: pathlib.Path, conn: SFTPConnection | None = None, **kwargs: object
    ) -> sftp.UploadResult:
        conn = conn or self.connection()
        kwargs.setdefault("attempts", 3)
        return conn.upload(local, "/backups", "odoo19.0-master-20260927-010000.tar.gz", **kwargs)

    def setUp(self) -> None:
        super().setUp()
        (self.root / "backups").mkdir()
        self.final = self.root / "backups" / "odoo19.0-master-20260927-010000.tar.gz"
        self.partial = self.final.with_name(self.final.name + PARTIAL_SUFFIX)

    def backup_writes(self) -> list:
        return [w for w in self.stub.writes if w.path.endswith(PARTIAL_SUFFIX)]

    def assert_attempt_warnings(self, records: list[logging.LogRecord], attempts: int, text: str) -> None:
        """One WARNING per failed attempt, numbered 1..attempts, each containing ``text``."""
        failures = [r.getMessage() for r in records if "failed (attempt" in r.getMessage()]
        self.assertEqual(len(failures), attempts, failures)
        for number, message in enumerate(failures, start=1):
            self.assertIn(f"(attempt {number}/", message)
            self.assertIn(text, message)

    def test_upload_success_with_openssh_limits(self) -> None:
        self.stub.limits = OPENSSH_LIMITS
        local = self.local_file(9 * 1024 * 1024 + 7)  # two 8 MiB reads, unaligned tail
        seen: list[int] = []
        result = self.upload(local, progress=seen.append)
        self.assertEqual(_sha256(self.final), _sha256(local))
        self.assertFalse(self.partial.exists())
        self.assertEqual(result.remote_path, "/backups/" + self.final.name)
        self.assertEqual(result.size, local.stat().st_size)
        self.assertEqual(result.request_size, MAX_REQUEST_SIZE_CAP)  # > 32768 when limits@ is supported
        self.assertEqual(max(w.length for w in self.backup_writes()), MAX_REQUEST_SIZE_CAP)
        self.assertAlmostEqual(result.remote_mtime, time.time(), delta=60)
        self.assertGreaterEqual(result.seconds, 0)
        self.assertEqual(seen, [8 * 1024 * 1024, local.stat().st_size])
        self.assertEqual(self.sleeps, [])
        [tmp_open] = [o for o in self.stub.opens if o.path.endswith(PARTIAL_SUFFIX)]
        self.assertTrue(tmp_open.flags & os.O_TRUNC)
        self.assertFalse(tmp_open.flags & os.O_APPEND)

    def test_request_size_falls_back_to_32768_without_limits(self) -> None:
        self.stub.limits = None  # ProFTPD mod_sftp (Hetzner port 22) has no limits@openssh.com
        local = self.local_file(1024 * 1024 + 1)
        result = self.upload(local)
        self.assertEqual(result.request_size, DEFAULT_REQUEST_SIZE)
        self.assertEqual(max(w.length for w in self.backup_writes()), DEFAULT_REQUEST_SIZE)
        self.assertEqual(_sha256(self.final), _sha256(local))

    def test_request_size_follows_server_limits_and_cap(self) -> None:
        cases = (
            ((1 << 22, 1 << 22, 1 << 22, 0), MAX_REQUEST_SIZE_CAP),  # capped at 261120
            ((0, 0, 65536, 0), 65536),  # server write limit below the cap
            ((65536, 65536, 1 << 20, 0), 65536 - 1024),  # packet limit leaves room for the header
            ((0, 0, 0, 0), DEFAULT_REQUEST_SIZE),  # no stated limit
        )
        for limits, expected in cases:
            with self.subTest(limits=limits):
                self.stub.limits = limits
                self.final.unlink(missing_ok=True)
                self.assertEqual(self.upload(self.local_file(300_000)).request_size, expected)

    def test_request_size_override(self) -> None:
        self.stub.limits = OPENSSH_LIMITS
        result = self.upload(self.local_file(300_000), conn=self.connection(max_request_size=65536))
        self.assertEqual(result.request_size, 65536)
        self.assertEqual(max(w.length for w in self.backup_writes()), 65536)

    def test_empty_file(self) -> None:
        result = self.upload(self.local_file(0))
        self.assertEqual(result.size, 0)
        self.assertEqual(self.final.read_bytes(), b"")

    def test_existing_final_file_is_never_overwritten(self) -> None:
        self.final.write_bytes(b"previous backup")
        with self.assertRaisesRegex(UploadError, "already exists") as caught:
            self.upload(self.local_file(1000))
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(self.final.read_bytes(), b"previous backup")
        self.assertFalse(self.partial.exists())
        self.assertEqual(self.sleeps, [])  # not retried
        self.assertEqual(self.backup_writes(), [])

    def test_tail_write_failure_is_detected(self) -> None:
        # 97 write requests of 32 KiB; the last seven fail. paramiko only collects pipelined
        # answers inside write() once more than 100 are outstanding, so all failures are answers
        # that SFTPFile.close() would silently discard.
        local = self.local_file(96 * REQ + 12345)
        self.stub.fail_write_at = 90 * REQ
        with self.assertLogs(LOGGER, logging.WARNING) as logs:
            with self.assertRaisesRegex(UploadError, "rejected a write") as caught:
                self.upload(local)
        self.assert_attempt_warnings(logs.records, 3, "rejected a write")
        self.assertIn("after 3 attempt(s)", str(caught.exception))
        self.assertEqual(self.sleeps, [5.0, 10.0])
        self.assertFalse(self.final.exists())
        self.assertFalse(self.partial.exists())  # best-effort cleanup after the last attempt
        self.assertEqual(len({w.connection for w in self.backup_writes()}), 3)

    def test_failed_write_hidden_by_a_complete_file_size_is_detected(self) -> None:
        # Exactly one pipelined write in the tail fails, the following (last) write succeeds, so
        # the remote file reaches the full size with a hole: only draining the pipelined answers
        # can detect it. Without the drain this upload "succeeds" with a corrupt file.
        size = 96 * REQ + 12345
        local = self.local_file(size)
        self.stub.fail_write_at = 95 * REQ
        self.stub.fail_write_span = REQ
        with self.assertLogs(LOGGER, logging.WARNING) as logs:
            with self.assertRaisesRegex(UploadError, "rejected a write"):
                self.upload(local, attempts=1)
        self.assert_attempt_warnings(logs.records, 1, "rejected a write")
        self.assertFalse(self.final.exists())
        self.assertFalse(self.partial.exists())
        writes = self.backup_writes()
        self.assertLessEqual(len(writes), 100)  # the premise: no opportunistic collection by paramiko
        self.assertEqual([w.offset for w in writes if w.failed], [95 * REQ])
        self.assertEqual(max(w.offset + w.length for w in writes if not w.failed), size)

    def test_connection_drop_mid_upload_restarts_from_zero(self) -> None:
        local = self.local_file(9 * 1024 * 1024 + 7)
        self.stub.drop_after_bytes = 4 * 1024 * 1024
        conn = self.connection()
        # Root logger: paramiko's client transport may also log the reset connection as ERROR.
        with self.assertLogs(level=logging.WARNING) as logs:
            result = self.upload(local, conn=conn)
        self.assert_attempt_warnings(logs.records, 1, "")
        self.assertEqual(self.stub.drops, 1)
        self.assertEqual(_sha256(self.final), _sha256(local))
        self.assertEqual(result.size, local.stat().st_size)
        self.assertEqual(self.sleeps, [5.0])
        tmp_opens = [o for o in self.stub.opens if o.path.endswith(PARTIAL_SUFFIX)]
        self.assertEqual([o.connection for o in tmp_opens], [1, 2])
        for tmp_open in tmp_opens:  # restart from 0: truncate, never append
            self.assertTrue(tmp_open.flags & os.O_TRUNC)
            self.assertFalse(tmp_open.flags & os.O_APPEND)
        second = [w for w in self.backup_writes() if w.connection == 2]
        self.assertEqual(second[0].offset, 0)
        self.assertEqual(sum(w.length for w in second), local.stat().st_size)

    def test_rename_falls_back_when_posix_rename_is_unsupported(self) -> None:
        self.stub.posix_rename_supported = False
        local = self.local_file(100_000)
        self.upload(local)
        self.assertEqual(_sha256(self.final), _sha256(local))
        self.assertFalse(self.partial.exists())

    def test_rename_failure_fails_the_upload_and_removes_the_partial_file(self) -> None:
        self.stub.fail_rename = True
        with self.assertLogs(LOGGER, logging.WARNING) as logs:
            with self.assertRaises(UploadError):
                self.upload(self.local_file(100_000))
        self.assert_attempt_warnings(logs.records, 3, "Failure")
        self.assertEqual(self.sleeps, [5.0, 10.0])
        self.assertFalse(self.final.exists())
        self.assertFalse(self.partial.exists())

    def test_lost_rename_answer_is_recognised_on_retry(self) -> None:
        local = self.local_file(100_000)
        self.stub.drop_after_rename = True
        with self.assertLogs(LOGGER, logging.INFO) as logs:
            self.upload(local)
        self.assert_attempt_warnings(logs.records, 1, "")
        self.assertTrue(any("completed by the rename of the previous attempt" in line for line in logs.output))
        self.assertEqual(_sha256(self.final), _sha256(local))
        self.assertEqual(self.sleeps, [5.0])
        self.assertEqual({w.connection for w in self.backup_writes()}, {1})  # not uploaded twice

    def test_upload_into_missing_directory_fails_after_all_attempts(self) -> None:
        conn = self.connection()
        with self.assertLogs(LOGGER, logging.WARNING) as logs:
            with self.assertRaisesRegex(UploadError, "after 2 attempt"):
                conn.upload(self.local_file(10), "/missing", "x.zip", attempts=2)
        self.assert_attempt_warnings(logs.records, 2, "No such file")
        self.assertEqual(self.sleeps, [5.0])

    def test_missing_local_file(self) -> None:
        with self.assertRaises(UploadError) as caught:
            self.upload(self.tmp / "missing.tar.gz")
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(self.stub.connection_count, 0)

    def test_invalid_arguments(self) -> None:
        conn = self.connection()
        with self.assertRaises(ValueError):
            conn.upload(self.local_file(1), "/backups", "a/b.zip")
        with self.assertRaises(ValueError):
            conn.upload(self.local_file(1), "/backups", "b.zip", attempts=0)

    def test_host_key_mismatch_during_upload_is_not_retried(self) -> None:
        conn = self.connection(host_keys=(fingerprint(generate_ed25519_key()),))
        with self.assertRaises(HostKeyMismatchError):
            self.upload(self.local_file(10), conn=conn)
        self.assertEqual(self.sleeps, [])
        self.assertEqual(self.stub.auth_attempts, [])

    def test_password_never_appears_in_logs_or_errors(self) -> None:
        self.stub.fail_rename = True
        with self.assertLogs(level=logging.DEBUG) as logs:
            with self.assertRaises(UploadError) as caught:
                self.upload(self.local_file(100_000))
        self.assertNotIn(self.stub.password, str(caught.exception))
        self.assertFalse(any(self.stub.password in line for line in logs.output))

    def test_partial_suffix_matches_retention(self) -> None:
        try:
            from odoo_backup import retention
        except ImportError:  # part A not present yet
            self.skipTest("odoo_backup.retention is not available")
        self.assertEqual(PARTIAL_SUFFIX, retention.PARTIAL_SUFFIX)


if __name__ == "__main__":
    unittest.main()
