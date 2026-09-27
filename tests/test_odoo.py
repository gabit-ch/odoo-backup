import dataclasses
import hashlib
import json
import logging
import pathlib
import socket
import stat
import tempfile
import time
import traceback
import unittest
import urllib.parse
from unittest import mock

from odoo_backup import odoo as odoo_module
from odoo_backup.odoo import DownloadResult, OdooAuthError, OdooBackupError, OdooClient, OdooError
from odoo_backup.verify import ArchiveVerificationError
from tests.http_stub import (
    BACKUP_PATH,
    DATABASE_LIST_PATH,
    JSONRPC_PATH,
    VERSION_INFO_PATH,
    XMLRPC_COMMON_PATH,
    OdooHTTPStub,
    StubResponse,
    backup_response,
    build_backup,
    html_response,
    jsonrpc_error,
    jsonrpc_result,
    not_found,
    odoo_error_page,
    xmlrpc_result,
)

PASSWORD = "S3cr3t-Master+Pwd/19!"
WRONG_PASSWORD = "Wr0ng-Guess+Pwd/17?"
SECRETS = (PASSWORD, WRONG_PASSWORD)


def secret_variants() -> list[str]:
    """The secrets as they could appear in logs: raw, form-encoded, JSON-encoded."""
    variants = []
    for secret in SECRETS:
        variants += [secret, urllib.parse.quote_plus(secret), json.dumps(secret)[1:-1]]
    return variants


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class OdooStubTestCase(unittest.TestCase):
    """Starts an Odoo stub, captures ALL log records (DEBUG, incl. urllib3) and checks them for secrets."""

    def setUp(self) -> None:
        self.stub = OdooHTTPStub(master_password=PASSWORD, databases=["master", "other"])
        self.stub.start()
        self.addCleanup(self.stub.stop)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = pathlib.Path(tmp.name)

        self.capture = _CaptureHandler()
        root = logging.getLogger()
        previous_level = root.level
        root.addHandler(self.capture)
        root.setLevel(logging.DEBUG)
        self.addCleanup(root.setLevel, previous_level)
        self.addCleanup(root.removeHandler, self.capture)
        self.addCleanup(self.assert_no_secret_logged)  # runs after the test's own cleanups

    def client(
        self,
        *,
        password: str = PASSWORD,
        db: str = "master",
        url: str | None = None,
        timeout: float = 5.0,
        read_timeout: float = 5.0,
    ) -> OdooClient:
        client = OdooClient(url or self.stub.url, password, db, timeout, read_timeout)
        self.addCleanup(client.close)
        return client

    # assertions ---------------------------------------------------------------

    def assert_no_secret_logged(self) -> None:
        formatter = logging.Formatter()
        for record in self.capture.records:
            text = record.getMessage()
            if record.exc_info:
                text += formatter.formatException(record.exc_info)
            for secret in secret_variants():
                self.assertNotIn(secret, text, f"secret in log record {record.name}: {text[:200]}")

    def assert_no_secret(self, exc: BaseException) -> None:
        texts = [str(exc), repr(exc), "".join(traceback.format_exception(exc))]
        for text in texts:
            for secret in secret_variants():
                self.assertNotIn(secret, text)

    def logged(self, level: int, fragment: str) -> list[str]:
        return [
            record.getMessage()
            for record in self.capture.records
            if record.levelno == level and fragment in record.getMessage()
        ]

    def assert_tmp_empty(self) -> None:
        self.assertEqual(list(self.tmp.iterdir()), [])


# --------------------------------------------------------------------------
# server_serie
# --------------------------------------------------------------------------


class ServerSerieTests(OdooStubTestCase):
    def test_version_info(self):
        self.assertEqual(self.client().server_serie(), "19.0")
        [request] = self.stub.requests_to(VERSION_INFO_PATH)
        self.assertEqual(request.headers["content-type"], "application/json")
        self.assertEqual(request.json["method"], "call")
        self.assertEqual(request.json["params"], {})
        self.assertEqual(self.stub.requests_to(XMLRPC_COMMON_PATH), [])

    def test_saas_serie(self):
        self.stub.server_serie = "saas~18.3"
        self.assertEqual(self.client().server_serie(), "saas~18.3")

    def test_fallback_to_xmlrpc(self):
        self.stub.set_handler(VERSION_INFO_PATH, not_found)
        self.stub.server_serie = "17.0"
        self.assertEqual(self.client().server_serie(), "17.0")
        self.assertEqual(len(self.stub.requests_to(XMLRPC_COMMON_PATH)), 1)
        self.assertTrue(self.logged(logging.WARNING, "falling back to XML-RPC"))

    def test_both_endpoints_fail(self):
        self.stub.set_handler(VERSION_INFO_PATH, not_found)
        self.stub.set_handler(XMLRPC_COMMON_PATH, not_found)
        with self.assertRaisesRegex(OdooError, "XML-RPC.*HTTP 404"):
            self.client().server_serie()

    def test_xmlrpc_uses_the_timeout(self):
        self.stub.set_handler(VERSION_INFO_PATH, not_found)
        self.stub.set_handler(XMLRPC_COMMON_PATH, lambda req: dataclasses.replace(xmlrpc_result({}), delay=3))
        started = time.monotonic()
        with self.assertRaisesRegex(OdooError, "XML-RPC.*(timed out|TimeoutError)"):
            self.client(timeout=0.5).server_serie()
        self.assertLess(time.monotonic() - started, 2.5)

    def test_unusable_serie(self):
        for serie in ("19.0-custom", "19/0", "", None):
            with self.subTest(serie=serie):
                self.stub.set_handler(VERSION_INFO_PATH, lambda req, s=serie: jsonrpc_result({"server_serie": s}))
                self.stub.set_handler(XMLRPC_COMMON_PATH, lambda req, s=serie: xmlrpc_result({"server_serie": s}))
                with self.assertRaises(OdooError):
                    self.client().server_serie()


# --------------------------------------------------------------------------
# master password
# --------------------------------------------------------------------------


class MasterPasswordTests(OdooStubTestCase):
    def test_correct_password(self):
        self.client().check_master_password()
        [request] = self.stub.requests_to(JSONRPC_PATH)
        self.assertEqual(
            request.json["params"], {"service": "db", "method": "migrate_databases", "args": [PASSWORD, []]}
        )
        self.assertEqual(self.stub.requests_to(BACKUP_PATH), [])

    def test_wrong_password(self):
        with self.assertRaises(OdooAuthError) as ctx:
            self.client(password=WRONG_PASSWORD).check_master_password()
        self.assertIn("ODOO_MASTER_PWD", str(ctx.exception))
        self.assert_no_secret(ctx.exception)

    def test_disabled_database_manager_is_not_reported_as_a_wrong_password(self):
        self.stub.list_db = False
        with self.assertRaises(OdooError) as ctx:
            self.client().check_master_password()
        self.assertNotIsInstance(ctx.exception, OdooAuthError)
        self.assertIn("list_db", str(ctx.exception))

    def test_backup_probe_when_jsonrpc_is_gone(self):
        self.stub.set_handler(JSONRPC_PATH, not_found)
        self.client().check_master_password()
        [probe] = self.stub.requests_to(BACKUP_PATH)
        self.assertEqual(
            probe.form,
            {
                "master_pwd": PASSWORD,
                "name": "master-odoo-backup-auth-probe",
                "backup_format": "dump",
                "filestore": "false",
            },
        )
        self.assertTrue(self.logged(logging.INFO, "backup probe"))

    def test_backup_probe_when_the_db_method_is_gone(self):
        self.stub.set_handler(JSONRPC_PATH, lambda req: jsonrpc_error("builtins.KeyError", "'Method not found'"))
        self.client().check_master_password()
        self.assertEqual(len(self.stub.requests_to(BACKUP_PATH)), 1)

    def test_backup_probe_wrong_password(self):
        self.stub.set_handler(JSONRPC_PATH, not_found)
        with self.assertRaises(OdooAuthError) as ctx:
            self.client(password=WRONG_PASSWORD).check_master_password()
        self.assert_no_secret(ctx.exception)

    def test_backup_probe_with_disabled_database_manager(self):
        self.stub.set_handler(JSONRPC_PATH, not_found)
        self.stub.list_db = False
        with self.assertRaises(OdooError) as ctx:
            self.client().check_master_password()
        self.assertNotIsInstance(ctx.exception, OdooAuthError)
        self.assertIn("list_db", str(ctx.exception))

    def test_backup_probe_unexpected_answers(self):
        self.stub.set_handler(JSONRPC_PATH, not_found)
        answers = {
            "other error page": html_response(odoo_error_page("Database backup error: pg_dump failed")),
            "backup stream": backup_response(build_backup("dump")),
            "server error": html_response("<html><body><h1>500 Internal Server Error</h1></body></html>", 500),
        }
        for label, answer in answers.items():
            with self.subTest(answer=label):
                self.stub.set_handler(BACKUP_PATH, lambda req, a=answer: a)
                with self.assertRaises(OdooError) as ctx:
                    self.client().check_master_password()
                self.assertNotIsInstance(ctx.exception, OdooAuthError)

    def test_other_rpc_errors_and_bad_answers(self):
        answers = {
            "server error": jsonrpc_error("psycopg2.OperationalError", "connection to server failed"),
            "html instead of json": html_response("<html><body>Login</body></html>"),
            "json without result": StubResponse(200, b'{"jsonrpc": "2.0"}', [("Content-Type", "application/json")]),
            "unexpected result": jsonrpc_result(False),
            "redirect": StubResponse(302, b"", [("Location", "https://odoo.example.com/jsonrpc")]),
            "bad gateway": html_response("<html><body>502 Bad Gateway</body></html>", 502),
        }
        for label, answer in answers.items():
            with self.subTest(answer=label):
                self.stub.set_handler(JSONRPC_PATH, lambda req, a=answer: a)
                with self.assertRaises(OdooError) as ctx:
                    self.client().check_master_password()
                self.assertNotIsInstance(ctx.exception, OdooAuthError)
                self.assert_no_secret(ctx.exception)
        self.assertEqual(self.stub.requests_to(BACKUP_PATH), [])

    def test_unreachable_odoo(self):
        with self.assertRaisesRegex(OdooError, "cannot reach Odoo") as ctx:
            self.client(url=f"http://127.0.0.1:{_closed_port()}").check_master_password()
        self.assert_no_secret(ctx.exception)


# --------------------------------------------------------------------------
# database check
# --------------------------------------------------------------------------


class DatabaseTests(OdooStubTestCase):
    def test_database_is_listed(self):
        self.client().check_database()
        self.assertEqual(len(self.stub.requests_to(DATABASE_LIST_PATH)), 1)

    def test_unknown_database(self):
        with self.assertRaisesRegex(OdooError, r"'nope' is not in Odoo's database list \(visible: master, other\)"):
            self.client(db="nope").check_database()

    def test_disabled_database_manager(self):
        self.stub.list_db = False
        with self.assertRaisesRegex(OdooError, "list_db"):
            self.client().check_database()

    def test_timeout(self):
        self.stub.set_handler(DATABASE_LIST_PATH, lambda req: dataclasses.replace(jsonrpc_result([]), delay=2))
        started = time.monotonic()
        with self.assertRaisesRegex(OdooError, "cannot reach Odoo"):
            self.client(timeout=0.3).check_database()
        self.assertLess(time.monotonic() - started, 1.8)


# --------------------------------------------------------------------------
# backup download
# --------------------------------------------------------------------------


class DownloadSuccessTests(OdooStubTestCase):
    def download(self, fmt: str = "tar.gz", with_filestore: bool = True, **kwargs) -> DownloadResult:
        dest = self.tmp / f"odoo19.0-master-20260927-010000.{fmt}.part"
        return self.client().download_backup(fmt, dest, with_filestore, **kwargs)

    def test_close_delimited_full_backup(self):
        body = build_backup("tar.gz")
        result = self.download("tar.gz")
        self.assertEqual(result.size, len(body))
        self.assertEqual(result.sha256, hashlib.sha256(body).hexdigest())
        self.assertEqual(result.path.read_bytes(), body)
        self.assertEqual(stat.S_IMODE(result.path.stat().st_mode), 0o600)
        self.assertGreater(result.seconds, 0)
        self.assertEqual(result.verification.members, 8)
        self.assertTrue(result.verification.has_filestore)
        [request] = self.stub.requests_to(BACKUP_PATH)
        self.assertEqual(request.form, {"master_pwd": PASSWORD, "name": "master", "backup_format": "tar.gz"})
        [final] = self.logged(logging.INFO, "Downloaded full tar.gz backup")
        self.assertIn(f"sha256={result.sha256}", final)
        self.assertIn("members=8, filestore=yes", final)

    def test_with_content_length(self):
        self.stub.backup_overrides = {"content_length": True}
        self.assertEqual(self.download("tar.zst").size, len(build_backup("tar.zst")))

    def test_database_only_sends_filestore_false(self):
        result = self.download("tar.gz", with_filestore=False)
        self.assertFalse(result.verification.has_filestore)
        self.assertEqual(result.verification.members, 2)
        [request] = self.stub.requests_to(BACKUP_PATH)
        self.assertEqual(request.form["filestore"], "false")
        self.assertFalse(self.logged(logging.WARNING, "ignored filestore=false"))

    def test_full_backup_sends_no_filestore_field(self):
        self.download("zip", with_filestore=True)
        [request] = self.stub.requests_to(BACKUP_PATH)
        self.assertNotIn("filestore", request.form)

    def test_ignored_filestore_false_is_a_warning(self):
        self.stub.honour_filestore = False  # Odoo 17 / an old web_backup
        result = self.download("tar.gz", with_filestore=False)
        self.assertTrue(result.verification.has_filestore)
        self.assertTrue(self.logged(logging.WARNING, "Odoo ignored filestore=false (web_backup too old?)"))

    def test_every_format(self):
        for fmt in ("zip", "dump", "tar", "tar.gz", "tar.bz2", "tar.xz", "tar.zst"):
            with self.subTest(fmt=fmt):
                result = self.download(fmt)
                self.assertEqual(result.size, len(build_backup(fmt)))
                if fmt == "zip":
                    self.assertEqual(result.verification.members, 8)  # verify_zip_file ran
                    self.assertTrue(result.verification.has_filestore)
        self.assertEqual(len(self.logged(logging.WARNING, "dump format: truncation cannot be detected")), 1)

    def test_missing_content_disposition_is_only_a_warning(self):
        body = build_backup("tar")
        octet_stream_only = StubResponse(200, body, [("Content-Type", "application/octet-stream")])
        self.stub.set_handler(BACKUP_PATH, lambda req: octet_stream_only)
        self.assertEqual(self.download("tar").size, len(body))
        self.assertTrue(self.logged(logging.WARNING, "Content-Disposition"))

    def test_progress_callback_and_log(self):
        seen: list[int] = []
        with (
            mock.patch.object(odoo_module, "CHUNK_SIZE", 4096),
            mock.patch.object(odoo_module, "PROGRESS_BYTES", 10_000),
        ):
            result = self.download("tar", progress=seen.append)
        self.assertEqual(seen, sorted(seen))
        self.assertEqual(seen[-1], result.size)
        self.assertGreater(len(seen), 5)
        progress_lines = self.logged(logging.INFO, "received,")
        self.assertGreaterEqual(len(progress_lines), result.size // 10_000)


class DownloadFailureTests(OdooStubTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.dest = self.tmp / "odoo19.0-master-20260927-010000.tar.gz.part"

    def assert_download_fails(
        self,
        exc_type: type[BaseException],
        *,
        client: OdooClient | None = None,
        fmt: str = "tar.gz",
        with_filestore: bool = True,
    ) -> BaseException:
        client = client or self.client()
        with self.assertRaises(exc_type) as ctx:
            client.download_backup(fmt, self.dest, with_filestore)
        self.assertFalse(self.dest.exists(), "the incomplete backup file must be removed")
        self.assert_tmp_empty()
        self.assert_no_secret(ctx.exception)
        return ctx.exception

    def test_wrong_master_password_html_page(self):
        exc = self.assert_download_fails(OdooBackupError, client=self.client(password=WRONG_PASSWORD))
        self.assertIn("Odoo returned an HTML page instead of a backup: Database backup error: Access Denied", str(exc))
        self.assertIn("check ODOO_MASTER_PWD", str(exc))

    def test_unknown_database_html_page(self):
        exc = self.assert_download_fails(OdooBackupError, client=self.client(db="nope"))
        self.assertIn("Database backup error: Database 'nope' is not known", str(exc))
        self.assertIn("check ODOO_DB_NAME", str(exc))

    def test_disabled_database_manager_html_page(self):
        self.stub.list_db = False
        exc = self.assert_download_fails(OdooBackupError)
        self.assertIn("The database manager has been disabled by the administrator", str(exc))

    def test_html_page_sent_as_octet_stream(self):
        page = odoo_error_page("Database backup error: Command `pg_dump` failed").encode()
        self.stub.set_handler(BACKUP_PATH, lambda req: backup_response(page))
        exc = self.assert_download_fails(OdooBackupError)
        self.assertIn("HTML page instead of a backup: Database backup error: Command `pg_dump` failed", str(exc))

    def test_truncated_close_delimited_tar_gz(self):
        size = len(build_backup("tar.gz"))
        for cut in (size // 2, size - 3):  # in the middle, inside the gzip trailer
            with self.subTest(cut=cut):
                self.stub.backup_overrides = {"truncate_after": cut}
                exc = self.assert_download_fails(ArchiveVerificationError)
                self.assertIn("truncated", str(exc))

    def test_truncated_zip(self):
        self.stub.backup_overrides = {"truncate_after": len(build_backup("zip")) - 10}
        self.assert_download_fails(ArchiveVerificationError, fmt="zip")

    def test_content_length_mismatch(self):
        self.stub.backup_overrides = {"content_length": len(build_backup("tar.gz")) + 1000}
        exc = self.assert_download_fails(OdooBackupError)
        self.assertRegex(str(exc), "interrupted|incomplete")

    def test_empty_body(self):
        self.stub.backup_body = lambda fmt, with_filestore: b""
        self.assertIn("empty", str(self.assert_download_fails(ArchiveVerificationError)))

    def test_raw_dump_when_web_backup_is_missing(self):
        self.stub.backup_body = lambda fmt, with_filestore: build_backup("dump")
        self.assertIn("web_backup", str(self.assert_download_fails(ArchiveVerificationError)))

    def test_corrupt_archive_aborts_the_download(self):
        body = bytearray(build_backup("tar.gz", seed=3))
        body[len(body) // 2] ^= 0xFF
        self.stub.backup_body = lambda fmt, with_filestore: bytes(body)
        self.assert_download_fails(ArchiveVerificationError)

    def test_error_status(self):
        page = "<html><head><title>502 Bad Gateway</title></head><body><h1>502 Bad Gateway</h1></body></html>"
        self.stub.set_handler(BACKUP_PATH, lambda req: html_response(page, 502))
        exc = self.assert_download_fails(OdooBackupError)
        self.assertIn("HTTP 502: 502 Bad Gateway", str(exc))

    def test_redirect_is_not_followed(self):
        redirect = StubResponse(307, b"", [("Location", self.stub.url + "/elsewhere")])
        self.stub.set_handler(BACKUP_PATH, lambda req: redirect)
        exc = self.assert_download_fails(OdooBackupError)
        self.assertIn("redirected", str(exc))
        self.assertEqual(self.stub.requests_to("/elsewhere"), [])

    def test_unexpected_content_type(self):
        self.stub.set_handler(BACKUP_PATH, lambda req: jsonrpc_error("odoo.exceptions.UserError", "nope"))
        exc = self.assert_download_fails(OdooBackupError)
        self.assertIn("Content-Type 'application/json'", str(exc))

    def test_unreachable_odoo(self):
        client = self.client(url=f"http://127.0.0.1:{_closed_port()}")
        self.assertIn("cannot reach Odoo", str(self.assert_download_fails(OdooBackupError, client=client)))

    def test_read_timeout(self):
        self.stub.backup_overrides = {"delay": 2}
        client = self.client(read_timeout=0.3)
        self.assert_download_fails(OdooBackupError, client=client)

    def test_interrupt_removes_the_file(self):
        def interrupt(size: int) -> None:
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self.client().download_backup("tar.gz", self.dest, True, progress=interrupt)
        self.assertFalse(self.dest.exists())

    def test_existing_destination_is_left_alone(self):
        self.dest.write_bytes(b"keep me")
        with self.assertRaises(FileExistsError):
            self.client().download_backup("tar.gz", self.dest, True)
        self.assertEqual(self.dest.read_bytes(), b"keep me")
        self.assertEqual(self.stub.requests_to(BACKUP_PATH), [])

    def test_unsupported_format(self):
        with self.assertRaises(ValueError):
            self.client().download_backup("rar", self.dest, True)
        self.assertEqual(self.stub.requests_to(BACKUP_PATH), [])


# --------------------------------------------------------------------------
# client details
# --------------------------------------------------------------------------


class ClientTests(OdooStubTestCase):
    def test_url_credentials_and_password_are_not_shown(self):
        client = self.client(url=f"http://admin:basic-auth-pw@127.0.0.1:{self.stub.port}/odoo/")
        self.assertEqual(client.display_url, f"http://127.0.0.1:{self.stub.port}")
        self.assertNotIn("basic-auth-pw", repr(client))
        self.assertNotIn(PASSWORD, repr(client))

    def test_path_of_the_url_is_ignored(self):
        self.client(url=self.stub.url + "/odoo/action-123").check_database()
        self.assertEqual(len(self.stub.requests_to(DATABASE_LIST_PATH)), 1)

    def test_invalid_url(self):
        for url in ("odoo.example.com", "ftp://odoo.example.com", "http://"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                OdooClient(url, PASSWORD, "master", 5, 5)


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


if __name__ == "__main__":
    unittest.main()
