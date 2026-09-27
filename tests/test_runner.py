"""End-to-end tests of one backup run against the Odoo HTTP stub and the in-process SFTP server."""

import datetime
import functools
import hashlib
import io
import json
import logging
import pathlib
import re
import stat
import tarfile
import tempfile
import threading
import time
import types
import unittest
import urllib.parse
from zoneinfo import ZoneInfo

from odoo_backup.config import Config, load_config
from odoo_backup.runner import (
    RunResult,
    Shutdown,
    Watchdog,
    make_odoo_client,
    make_sftp_connection,
    run_backup,
)
from odoo_backup.sftp import SFTPError
from odoo_backup.state import StateStore, health_status
from tests.http_stub import BACKUP_PATH, OdooHTTPStub, StubResponse, build_backup
from tests.sftp_stub import SFTPStubServer, generate_ed25519_key

ZURICH = ZoneInfo("Europe/Zurich")
MASTER = "Runner-Master+Pwd/19!"
WRONG_MASTER = "Wrong-Master+Guess/17?"
HEARTBEAT_TOKEN = "hb-7d1e0c4a-token"
HEARTBEAT_PATH = f"/ping/{HEARTBEAT_TOKEN}"
DB = "master"
LOGGER = "odoo_backup.runner"


def name_at(ts: datetime.datetime, *, db: str = DB, serie: str = "19.0", fmt: str = "tar.gz") -> str:
    return f"odoo{serie}-{db}-{ts:%Y%m%d-%H%M%S}.{fmt}"


def every_two_hours(first: datetime.date, last: datetime.date) -> list[datetime.datetime]:
    """01:00, 03:00, ..., 23:00 of every day from ``first`` to ``last`` (the production slots)."""
    stamps = []
    day = first
    while day <= last:
        stamps += [datetime.datetime.combine(day, datetime.time(hour)) for hour in range(1, 24, 2)]
        day += datetime.timedelta(days=1)
    return stamps


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class RunnerTestCase(unittest.TestCase):
    """Real stubs on 127.0.0.1, a fake clock, and a guard that no secret is ever logged."""

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.base = pathlib.Path(tmp.name)
        self.sftp_root = self.base / "sftp"
        (self.sftp_root / "backups").mkdir(parents=True)
        self.tmp_dir = self.base / "work"
        self.state_dir = self.base / "state"

        self.odoo = OdooHTTPStub(master_password=MASTER, databases=[DB, "other"]).start()
        self.addCleanup(self.odoo.stop)
        self.heartbeat_status = 200
        self.odoo.set_handler(
            HEARTBEAT_PATH, lambda req: StubResponse(self.heartbeat_status, b"OK", content_length=True)
        )
        self.sftp = SFTPStubServer(self.sftp_root).start()
        self.addCleanup(self.sftp.stop)

        self.now = datetime.datetime(2026, 9, 27, 13, 0, 5, tzinfo=ZURICH)
        self.sleeps: list[float] = []
        self.secrets = [MASTER, WRONG_MASTER, self.sftp.password, HEARTBEAT_TOKEN]

        self.capture = _CaptureHandler()
        root = logging.getLogger()
        previous = root.level
        root.addHandler(self.capture)
        root.setLevel(logging.DEBUG)
        self.addCleanup(root.setLevel, previous)
        self.addCleanup(root.removeHandler, self.capture)
        self.addCleanup(self.assert_no_secret_logged)

    # -- configuration and runs -------------------------------------------------------------

    def env(self, **overrides: str | None) -> dict[str, str]:
        env = {
            "ODOO_URL": self.odoo.url,
            "ODOO_MASTER_PWD": MASTER,
            "ODOO_DB_NAME": DB,
            "ODOO_BACKUP_FORMAT": "tar.gz",
            "ODOO_TIMEOUT": "10",
            "ODOO_READ_TIMEOUT": "30",
            **self.sftp.env(),
            "SFTP_PATH": "/backups",
            "SFTP_TIMEOUT": "10",
            "SFTP_UPLOAD_ATTEMPTS": "1",
            "TZ": "Europe/Zurich",
            "BACKUP_TIME": "01:00",
            "BACKUP_EVERY_HOUR": "2",
            "HOURLY_BACKUP_FILESTORE": "false",
            "HOURLY_BACKUP_KEEP": "4",
            "DAILY_BACKUP_KEEP": "30",
            "MONTHLY_BACKUP_KEEP": "12",
            "YEARLY_BACKUP_KEEP": "-1",
            "BACKUP_TMP_DIR": str(self.tmp_dir),
            "BACKUP_STATE_DIR": str(self.state_dir),
            "HEARTBEAT_URL": self.odoo.url + HEARTBEAT_PATH,
        }
        for key, value in overrides.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        return env

    def config(self, **overrides: str | None) -> Config:
        return load_config(self.env(**overrides))

    def run_backup(self, full: bool = True, config: Config | None = None, **kwargs) -> RunResult:
        kwargs.setdefault("clock", lambda: self.now)
        kwargs.setdefault("state", StateStore(self.state_dir))
        kwargs.setdefault("sftp_factory", functools.partial(make_sftp_connection, sleep=self.sleeps.append))
        return run_backup(config or self.config(), full, **kwargs)

    # -- remote files -----------------------------------------------------------------------

    def put(self, name: str, directory: str = "backups", data: bytes = b"old backup") -> None:
        path = self.sftp_root / directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def remote_names(self, directory: str = "backups") -> set[str]:
        path = self.sftp_root / directory
        return {entry.name for entry in path.iterdir() if entry.is_file()} if path.exists() else set()

    # -- log inspection ---------------------------------------------------------------------

    def messages(self, level: int | None = None, logger: str | None = None) -> list[str]:
        return [
            record.getMessage()
            for record in self.capture.records
            if (level is None or record.levelno == level) and (logger is None or record.name == logger)
        ]

    def final_line(self) -> logging.LogRecord:
        finals = [
            record for record in self.capture.records
            if record.name == LOGGER and record.getMessage().startswith(("backup finished:", "backup failed:"))
        ]
        self.assertEqual(len(finals), 1, [record.getMessage() for record in finals])
        return finals[0]

    # -- assertions -------------------------------------------------------------------------

    def assert_no_secret_logged(self) -> None:
        formatter = logging.Formatter()
        variants = [v for s in self.secrets for v in (s, urllib.parse.quote_plus(s), json.dumps(s)[1:-1])]
        for record in self.capture.records:
            text = record.getMessage()
            if record.exc_info:
                text += formatter.formatException(record.exc_info)
            for secret in variants:
                self.assertNotIn(secret, text, f"secret in log record {record.name}: {text[:200]}")

    def assert_tmp_dir_empty(self) -> None:
        if self.tmp_dir.exists():
            self.assertEqual(list(self.tmp_dir.iterdir()), [])

    def assert_no_partial(self) -> None:
        partials = [p for p in self.sftp_root.rglob("*.upload")]
        self.assertEqual(partials, [])

    def state(self) -> dict:
        return json.loads((self.state_dir / "state.json").read_text())

    def heartbeats(self) -> int:
        return len(self.odoo.requests_to(HEARTBEAT_PATH))


class SuccessfulRunTests(RunnerTestCase):
    def test_full_backup(self) -> None:
        result = self.run_backup(full=True)
        expected = name_at(self.now.replace(tzinfo=None))
        self.assertTrue(result.ok, result.error)
        self.assertEqual((result.kind, result.stage, result.file_name), ("full", "retention", expected))
        self.assertEqual(result.remote_path, f"/backups/{expected}")
        uploaded = self.sftp_root / "backups" / expected
        data = uploaded.read_bytes()
        self.assertEqual(hashlib.sha256(data).hexdigest(), result.sha256)
        self.assertEqual(result.size, len(data))
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            self.assertIn("filestore/ab", archive.getnames())
        (request,) = self.odoo.requests_to(BACKUP_PATH)
        self.assertNotIn("filestore", request.form)
        self.assertEqual(request.form["backup_format"], "tar.gz")
        self.assert_tmp_dir_empty()
        self.assert_no_partial()
        self.assertEqual(self.heartbeats(), 1)
        self.assertTrue(result.heartbeat_sent)
        self.assertEqual(result.retention, "deleted 0/failed 0")
        self.assertRegex(
            self.final_line().getMessage(),
            rf"^backup finished: run_id=[0-9a-f]{{8}} kind=full file={re.escape(expected)} bytes={len(data)} "
            r"download_s=\d+\.\d upload_s=\d+\.\d retention=deleted 0/failed 0 total_s=\d+\.\d$",
        )
        self.assertEqual(self.final_line().levelno, logging.INFO)

    def test_database_only_backup_goes_to_the_db_only_directory(self) -> None:
        result = self.run_backup(full=False)
        expected = name_at(self.now.replace(tzinfo=None))
        self.assertTrue(result.ok, result.error)
        self.assertEqual(result.kind, "database-only")
        self.assertEqual(result.remote_path, f"/backups/db-only/{expected}")
        self.assertEqual(self.remote_names("backups/db-only"), {expected})
        self.assertEqual(self.remote_names("backups"), set())
        (request,) = self.odoo.requests_to(BACKUP_PATH)
        self.assertEqual(request.form["filestore"], "false")
        data = (self.sftp_root / "backups" / "db-only" / expected).read_bytes()
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            self.assertEqual(sorted(archive.getnames()), ["manifest.json", "sql.dump"])
        self.assertIn("kind=database-only", self.final_line().getMessage())

    def test_every_format_end_to_end(self) -> None:
        for index, fmt in enumerate(("zip", "dump", "tar", "tar.bz2", "tar.xz", "tar.zst")):
            with self.subTest(fmt=fmt):
                self.now = datetime.datetime(2026, 9, 27, 13, index, 5, tzinfo=ZURICH)
                result = self.run_backup(config=self.config(ODOO_BACKUP_FORMAT=fmt))
                self.assertTrue(result.ok, result.error)
                uploaded = self.sftp_root / "backups" / name_at(self.now.replace(tzinfo=None), fmt=fmt)
                self.assertEqual(hashlib.sha256(uploaded.read_bytes()).hexdigest(), result.sha256)
                self.assert_tmp_dir_empty()
        self.assertIn("Backup verification: dump format: truncation cannot be detected", self.messages(logging.WARNING))

    def test_state_file_after_success_and_failure(self) -> None:
        result = self.run_backup()
        state = self.state()
        self.assertEqual(state["last_success_file"], result.file_name)
        self.assertEqual(state["last_success_at"], "2026-09-27T11:00:05+00:00")
        self.assertIsNone(state["last_error"])
        self.assertEqual(stat.S_IMODE((self.state_dir / "state.json").stat().st_mode), 0o600)

        self.now += datetime.timedelta(hours=2)
        failed = self.run_backup(config=self.config(ODOO_MASTER_PWD=WRONG_MASTER))
        self.assertFalse(failed.ok)
        state = self.state()
        self.assertEqual(state["last_success_file"], result.file_name)  # unchanged
        self.assertEqual(state["last_failure_at"], "2026-09-27T13:00:05+00:00")
        self.assertTrue(state["last_error"].startswith("full backup failed at stage download: Odoo returned an HTML"))
        text = (self.state_dir / "state.json").read_text()
        for secret in self.secrets:
            self.assertNotIn(secret, text)

    def test_heartbeat_failure_does_not_fail_the_run(self) -> None:
        self.heartbeat_status = 503
        result = self.run_backup()
        self.assertTrue(result.ok)
        self.assertFalse(result.heartbeat_sent)
        self.assertIn("Heartbeat ping answered HTTP 503", self.messages(logging.WARNING))

    def test_no_heartbeat_configured(self) -> None:
        result = self.run_backup(config=self.config(HEARTBEAT_URL=None))
        self.assertTrue(result.ok)
        self.assertEqual(self.heartbeats(), 0)


class FailedRunTests(RunnerTestCase):
    """A failed run uploads nothing, deletes nothing, leaves no temp file and sends no heartbeat."""

    def setUp(self) -> None:
        super().setUp()
        # Two dense days: a successful run would delete e.g. the 03:00 backup of 2026-09-25.
        self.history = {name_at(ts) for ts in every_two_hours(datetime.date(2026, 9, 25), datetime.date(2026, 9, 26))}
        for name in self.history:
            self.put(name)

    def assert_nothing_changed(self, result: RunResult, stage: str) -> None:
        self.assertFalse(result.ok)
        self.assertEqual(result.stage, stage)
        self.assertEqual(self.remote_names(), self.history)
        self.assert_tmp_dir_empty()
        self.assert_no_partial()
        self.assertEqual(self.heartbeats(), 0)
        self.assertEqual(result.retention, "not run")
        final = self.final_line()
        self.assertEqual(final.levelno, logging.ERROR)
        self.assertIn(f"stage={stage} ", final.getMessage())
        self.assertEqual(len(self.messages(logging.ERROR, LOGGER)), 1)
        self.assertIn(f"at stage {stage}", self.state()["last_error"])

    def test_html_error_page(self) -> None:
        result = self.run_backup(config=self.config(ODOO_MASTER_PWD=WRONG_MASTER))
        self.assert_nothing_changed(result, "download")
        self.assertIn(
            "Odoo returned an HTML page instead of a backup: Database backup error: Access Denied", result.error
        )
        self.assertIn("check ODOO_MASTER_PWD", result.error)
        self.assertIn(result.error, self.final_line().getMessage())

    def test_unknown_database(self) -> None:
        self.odoo.databases = ["other"]
        result = self.run_backup()
        self.assert_nothing_changed(result, "download")
        self.assertIn("is not known", result.error)

    def test_truncated_archive(self) -> None:
        self.odoo.backup_overrides = {"truncate_after": 3000}
        result = self.run_backup()
        self.assert_nothing_changed(result, "download")
        self.assertRegex(result.error, "(?i)truncat|incomplete|end of")

    def test_content_length_mismatch(self) -> None:
        self.odoo.backup_overrides = {"content_length": 10**7}
        result = self.run_backup()
        self.assert_nothing_changed(result, "download")

    def test_upload_failure_skips_retention(self) -> None:
        self.sftp.fail_write_at = 0
        result = self.run_backup()
        self.assert_nothing_changed(result, "upload")
        self.assertIn("rejected a write", result.error)

    def test_host_key_mismatch_stops_before_odoo_and_before_any_authentication(self) -> None:
        other = generate_ed25519_key().fingerprint
        result = self.run_backup(config=self.config(SFTP_HOST_KEY=other))
        self.assert_nothing_changed(result, "sftp-preflight")
        self.assertIn("host key mismatch", result.error)
        self.assertEqual(self.sftp.auth_attempts, [])
        self.assertEqual(self.odoo.requests, [])

    def test_disk_preflight(self) -> None:
        newest = sorted(self.history)[-1]
        self.put(newest, data=b"x" * 5000)
        self.history.add(newest)
        result = self.run_backup(disk_usage=lambda path: types.SimpleNamespace(free=5000))
        self.assert_nothing_changed(result, "disk-preflight")
        self.assertIn("not enough free space in BACKUP_TMP_DIR", result.error)
        self.assertEqual(self.odoo.requests_to(BACKUP_PATH), [])

    def test_db_only_path_aliasing_sftp_path_fails_before_the_download(self) -> None:
        """Regression: SFTP_PATH=/backups + DB_ONLY_BACKUP_PATH=backups deleted almost every full backup."""
        (self.sftp_root / "alias").symlink_to(self.sftp_root / "backups", target_is_directory=True)
        for spelling in ("backups", "//backups", "./backups/", "/alias"):
            config = self.config(DB_ONLY_BACKUP_PATH=spelling)  # accepted: the spellings differ
            for full in (True, False):
                with self.subTest(spelling=spelling, full=full):
                    self.capture.records.clear()
                    result = self.run_backup(full=full, config=config)
                    self.assert_nothing_changed(result, "sftp-preflight")
                    self.assertIn(
                        f"SFTP_PATH '/backups' and DB_ONLY_BACKUP_PATH {spelling!r} are the same directory on the "
                        "SFTP server (/backups)", result.error,
                    )
        self.assertEqual(self.odoo.requests, [])

    def test_a_second_concurrent_run_is_refused(self) -> None:
        with StateStore(self.state_dir).run_lock():
            result = self.run_backup()
        self.assert_nothing_changed(result, "lock")
        self.assertIn("another backup run is in progress", result.error)
        self.assertEqual(self.odoo.requests, [])

    def test_unexpected_error_is_logged_once_with_its_traceback(self) -> None:
        def broken_factory(config, **kwargs):
            raise TypeError("boom")

        result = self.run_backup(odoo_factory=broken_factory)
        self.assert_nothing_changed(result, "odoo")
        self.assertEqual(result.error, "TypeError: boom")
        self.assertIsNotNone(self.final_line().exc_info)

    def test_clock_skew_skips_retention(self) -> None:
        result = self.run_backup(wall_time=lambda: time.time() + 7200)
        self.assertFalse(result.ok)
        self.assertEqual(result.stage, "clock-check")
        self.assertIn("clock skew", result.error)
        self.assertEqual(self.remote_names(), self.history | {result.file_name})  # uploaded, nothing deleted
        self.assertEqual(self.heartbeats(), 0)
        self.assert_tmp_dir_empty()

    def test_disabled_retention_uploads_but_fails_the_run(self) -> None:
        with self.assertLogs("odoo_backup.config", logging.WARNING):
            config = self.config(DAILY_BACKUP_KEEP="-5")
        self.assertIsNone(config.retention)
        result = self.run_backup(config=config)
        self.assertFalse(result.ok)
        self.assertEqual((result.stage, result.retention), ("retention", "disabled"))
        self.assertIn("retention disabled: DAILY_BACKUP_KEEP must be an integer >= 0", result.error)
        self.assertEqual(self.remote_names(), self.history | {result.file_name})
        self.assertEqual(self.heartbeats(), 0)

    def test_shutdown_during_the_download_is_recorded_and_cleaned_up(self) -> None:
        def interrupting_factory(config, **kwargs):
            client = make_odoo_client(config, **kwargs)
            original = client.download_backup

            def download_backup(fmt, dest, with_filestore, progress=None):
                def interrupt(size: int) -> None:
                    raise Shutdown("received SIGTERM")

                return original(fmt, dest, with_filestore, progress=interrupt)

            client.download_backup = download_backup
            return client

        with self.assertRaises(Shutdown):
            self.run_backup(odoo_factory=interrupting_factory)
        self.assertEqual(self.remote_names(), self.history)
        self.assert_tmp_dir_empty()
        self.assertEqual(self.heartbeats(), 0)
        self.assertIn("stage=download error=interrupted (received SIGTERM)", self.final_line().getMessage())
        self.assertIn("interrupted (received SIGTERM)", self.state()["last_error"])


class RetentionRunTests(RunnerTestCase):
    def test_retention_deletes_old_backups_in_hourly_mode(self) -> None:
        """Regression: 1.x never deleted anything in hourly mode (exhausted generator)."""
        stamps = every_two_hours(datetime.date(2026, 7, 29), datetime.date(2026, 9, 26))
        self.assertEqual(len(stamps), 720)
        for ts in stamps:
            self.put(name_at(ts))
        foreign = {name_at(stamps[0], db="other"), name_at(stamps[-1], db="other"), "notes.txt"}
        for name in foreign:
            self.put(name)

        result = self.run_backup()

        self.assertTrue(result.ok, result.error)
        new = name_at(self.now.replace(tzinfo=None))
        hourly = {name_at(datetime.datetime(2026, 9, 26, hour)) for hour in (19, 21, 23)}
        daily = {name_at(datetime.datetime(2026, 9, 26, 1) - datetime.timedelta(days=n)) for n in range(29)}
        monthly_yearly = {name_at(datetime.datetime(2026, 8, 1, 1)), name_at(datetime.datetime(2026, 7, 29, 1))}
        expected = {new} | hourly | daily | monthly_yearly | foreign
        self.assertEqual(len(expected - foreign), 35)
        self.assertEqual(self.remote_names(), expected)
        self.assertEqual((result.deleted, result.delete_failed), (686, 0))
        self.assertEqual(result.retention, "deleted 686/failed 0")
        self.assertIn(
            "Retention for /backups (full backups; last 4, daily 30, monthly 12, yearly all (anchor 01:00:00)): "
            "keep 35 [hourly 4, daily 30, monthly 3, yearly 1], delete 686, ignored 3, future 0",
            self.messages(logging.INFO),
        )
        self.assertIn("Retention: 3 file(s) in /backups are not backups of database 'master' and are ignored",
                      self.messages(logging.INFO))
        self.assertEqual(len([m for m in self.messages(logging.INFO) if m.startswith("Retention: deleted ")]), 686)
        self.assertIn("retention=deleted 686/failed 0", self.final_line().getMessage())

        # A second run on the next slot is a fixed point apart from the rotation of the newest backups.
        self.now += datetime.timedelta(hours=2)
        second = self.run_backup()
        self.assertTrue(second.ok, second.error)
        self.assertEqual(second.deleted, 1)  # 2026-09-26 19:00 drops out of the newest four
        self.assertNotIn(name_at(datetime.datetime(2026, 9, 26, 19)), self.remote_names())

    def test_dry_run_deletes_nothing(self) -> None:
        history = {name_at(ts) for ts in every_two_hours(datetime.date(2026, 9, 25), datetime.date(2026, 9, 26))}
        for name in history:
            self.put(name)
        result = self.run_backup(config=self.config(RETENTION_DRY_RUN="true"))
        self.assertTrue(result.ok, result.error)
        self.assertEqual(self.remote_names(), history | {result.file_name})
        self.assertEqual(result.retention, "dry-run/would-delete 19")
        (warning,) = [m for m in self.messages(logging.WARNING) if m.startswith("RETENTION_DRY_RUN")]
        self.assertTrue(warning.startswith("RETENTION_DRY_RUN: would delete 19 backup(s) from /backups: "
                                           "odoo19.0-master-20260925-030000.tar.gz, "))
        self.assertEqual(self.heartbeats(), 1)

    def test_dry_run_lists_at_most_fifty_names(self) -> None:
        for ts in every_two_hours(datetime.date(2026, 9, 1), datetime.date(2026, 9, 26)):
            self.put(name_at(ts))
        result = self.run_backup(config=self.config(RETENTION_DRY_RUN="true"))
        self.assertTrue(result.ok)
        (warning,) = [m for m in self.messages(logging.WARNING) if m.startswith("RETENTION_DRY_RUN")]
        would_delete = 26 * 12 + 1 - (26 + 1 + 3)  # 26 dailies, the new file, 3 more of the newest four
        self.assertIn(f"would delete {would_delete} backup(s)", warning)
        self.assertTrue(warning.endswith(f" and {would_delete - 50} more"))

    def test_stale_partials_of_this_database_are_removed(self) -> None:
        stale = name_at(datetime.datetime(2026, 9, 26, 1)) + ".upload"
        foreign = name_at(datetime.datetime(2026, 9, 26, 1), db="other") + ".upload"
        newer = name_at(datetime.datetime(2026, 9, 27, 14)) + ".upload"  # after this run's start
        for name in (stale, foreign, newer):
            self.put(name)
        result = self.run_backup()
        self.assertTrue(result.ok, result.error)
        self.assertEqual(self.remote_names(), {result.file_name, foreign, newer})
        self.assertIn(
            f"Removing the stale partial upload /backups/{stale} left by an earlier run", self.messages(logging.WARNING)
        )

    def test_dry_run_keeps_stale_partials(self) -> None:
        stale = name_at(datetime.datetime(2026, 9, 26, 1)) + ".upload"
        self.put(stale)
        result = self.run_backup(config=self.config(RETENTION_DRY_RUN="true"))
        self.assertTrue(result.ok, result.error)
        self.assertEqual(self.remote_names(), {result.file_name, stale})
        self.assertIn(f"RETENTION_DRY_RUN: would remove the stale partial upload /backups/{stale}",
                      self.messages(logging.WARNING))

    def test_future_dated_backups_are_kept_with_a_warning(self) -> None:
        future = name_at(datetime.datetime(2026, 10, 5, 1))
        self.put(future)
        result = self.run_backup()
        self.assertTrue(result.ok, result.error)
        self.assertIn(future, self.remote_names())
        self.assertTrue(any("dated more than a day in the future" in m and future in m
                            for m in self.messages(logging.WARNING)))

    def test_database_only_directory_keeps_the_newest_hourly_keep(self) -> None:
        db_only = [name_at(datetime.datetime(2026, 9, 26, hour)) for hour in range(3, 24, 2)]
        for name in db_only:
            self.put(name, directory="backups/db-only")
        self.put(name_at(datetime.datetime(2026, 9, 26, 1)))  # the day's full backup
        result = self.run_backup(full=False)
        self.assertTrue(result.ok, result.error)
        expected = {result.file_name} | set(db_only[-3:])
        self.assertEqual(self.remote_names("backups/db-only"), expected)
        self.assertEqual(self.remote_names(), {name_at(datetime.datetime(2026, 9, 26, 1))})
        self.assertEqual(result.deleted, len(db_only) - 3)
        self.assertIn(
            "Retention for /backups/db-only (database-only backups; last 4, daily 0, monthly 0, yearly 0 "
            "(anchor 01:00:00)): keep 4 [hourly 4, daily 0, monthly 0, yearly 0], delete 8, ignored 0, future 0",
            self.messages(logging.INFO),
        )

    def test_hourly_backup_keep_0_keeps_only_the_newest_database_only_backup(self) -> None:
        """HOURLY_BACKUP_KEEP=0 must not let the database-only backups of every slot pile up."""
        db_only = [name_at(datetime.datetime(2026, 9, 26, hour)) for hour in range(3, 24, 2)]
        for name in db_only:
            self.put(name, directory="backups/db-only")
        result = self.run_backup(full=False, config=self.config(HOURLY_BACKUP_KEEP="0"))
        self.assertTrue(result.ok, result.error)
        self.assertEqual(self.remote_names("backups/db-only"), {result.file_name})
        self.assertEqual(result.deleted, len(db_only))

    def test_daily_mode_never_deletes_database_only_backups(self) -> None:
        db_only = {name_at(datetime.datetime(2026, 9, 20 + n, 13)) for n in range(5)}
        for name in db_only:
            self.put(name, directory="backups/db-only")
        result = self.run_backup(config=self.config(BACKUP_EVERY_HOUR=None, HOURLY_BACKUP_FILESTORE=None))
        self.assertTrue(result.ok, result.error)
        self.assertEqual(self.remote_names("backups/db-only"), db_only)
        self.assertTrue(any(m.startswith("Retention for /backups/db-only (database-only backups) skipped: ")
                            for m in self.messages(logging.INFO)))

    def _failing_remove_factory(self, fragment: str):
        def factory(config, **kwargs):
            connection = make_sftp_connection(config, sleep=self.sleeps.append, **kwargs)
            real_remove = connection.remove

            def remove(path: str) -> bool:
                if fragment in path:
                    raise SFTPError(f"cannot remove {path!r}: Permission denied")
                return real_remove(path)

            connection.remove = remove
            return connection

        return factory

    def test_failed_deletions_fail_the_run_and_abort_after_five_in_a_row(self) -> None:
        for ts in every_two_hours(datetime.date(2026, 9, 25), datetime.date(2026, 9, 26)):
            self.put(name_at(ts))
        result = self.run_backup(sftp_factory=self._failing_remove_factory("-20260925-"))
        self.assertFalse(result.ok)
        self.assertEqual(result.stage, "retention")
        self.assertEqual((result.deleted, result.delete_failed), (0, 5))
        self.assertEqual(result.error, "retention: 5 deletion(s) failed (aborted after repeated failures)")
        self.assertIn("Retention in /backups aborted after 5 failed deletions in a row (14 not attempted)",
                      self.messages(logging.WARNING))
        self.assertEqual(self.heartbeats(), 0)

    def test_a_single_failed_deletion_does_not_stop_the_others(self) -> None:
        for ts in every_two_hours(datetime.date(2026, 9, 25), datetime.date(2026, 9, 26)):
            self.put(name_at(ts))
        result = self.run_backup(sftp_factory=self._failing_remove_factory("-20260925-030000"))
        self.assertFalse(result.ok)
        self.assertEqual((result.deleted, result.delete_failed), (18, 1))
        self.assertEqual(result.retention, "deleted 18/failed 1")

    def test_file_deleted_meanwhile_counts_as_already_gone(self) -> None:
        for ts in every_two_hours(datetime.date(2026, 9, 25), datetime.date(2026, 9, 26)):
            self.put(name_at(ts))
        victim = self.sftp_root / "backups" / name_at(datetime.datetime(2026, 9, 25, 3))
        listings: list[str] = []

        def factory(config, **kwargs):
            connection = make_sftp_connection(config, **kwargs)
            real_list = connection.list_files

            def list_files(directory: str):
                listing = real_list(directory)
                listings.append(directory)
                if listings.count("/backups") == 2:  # 1 = preflight, 2 = retention
                    victim.unlink()  # deleted by someone else between listing and deletion
                return listing

            connection.list_files = list_files
            return connection

        result = self.run_backup(sftp_factory=factory)
        self.assertTrue(result.ok, result.error)
        self.assertEqual(result.deleted, 18)
        self.assertIn(f"Retention: /backups/{victim.name} was already gone", self.messages(logging.INFO))

    def test_local_file_is_removed_before_the_retention_starts(self) -> None:
        """The local copy must not occupy BACKUP_TMP_DIR while the retention runs."""
        seen_at_retention: list[list[str]] = []

        def factory(config, **kwargs):
            connection = make_sftp_connection(config, **kwargs)
            real_exists = connection.exists

            def exists(path: str) -> bool:  # plan_directories() starts with exists()
                if self.tmp_dir.exists():  # the sftp-preflight calls exists() before it is created
                    seen_at_retention.append(sorted(p.name for p in self.tmp_dir.iterdir()))
                return real_exists(path)

            connection.exists = exists
            return connection

        result = self.run_backup(sftp_factory=factory)
        self.assertTrue(result.ok, result.error)
        self.assertTrue(seen_at_retention)
        self.assertEqual(seen_at_retention, [[]] * len(seen_at_retention))


class FullBackupMonitoringTests(RunnerTestCase):
    """HOURLY_BACKUP_FILESTORE=false: successful database-only runs must not hide failing full backups."""

    def setUp(self) -> None:
        super().setUp()
        self.full_backups_fail = True

        def body(fmt: str, with_filestore: bool) -> bytes:
            data = build_backup(fmt, with_filestore)
            return data[:2000] if with_filestore and self.full_backups_fail else data  # truncated

        self.odoo.backup_body = body
        self.store = StateStore(self.state_dir)
        self.store.record_start(self.now - datetime.timedelta(hours=1))
        self.cfg = self.config()
        self.assertEqual(self.cfg.full_backup_max_age, datetime.timedelta(hours=36))

    def health(self) -> tuple[bool, str]:
        return health_status(self.store.load(), self.now, self.cfg.healthcheck_max_age, self.cfg.full_backup_max_age)

    def test_heartbeat_and_health_follow_the_full_backups(self) -> None:
        self.assertTrue(self.run_backup(full=False, config=self.cfg).ok)
        self.assertEqual(self.heartbeats(), 1)  # start-up grace period for the first full backup

        # Two days later: every full backup failed, the database-only backups still succeed.
        self.now += datetime.timedelta(hours=40)
        failed = self.run_backup(full=True, config=self.cfg)
        self.assertFalse(failed.ok)
        self.assertEqual(failed.stage, "download")
        db_only = self.run_backup(full=False, config=self.cfg)
        self.assertTrue(db_only.ok, db_only.error)
        self.assertFalse(db_only.heartbeat_sent)
        self.assertEqual(self.heartbeats(), 1)
        self.assertIn(
            "Heartbeat ping not sent: no full backup (database + filestore) since the service started 1d 17h 0m ago",
            self.messages(logging.WARNING),
        )
        ok, message = self.health()
        self.assertFalse(ok)
        self.assertTrue(message.startswith("unhealthy: no full backup (database + filestore) since the service "
                                           "started 1d 17h 0m ago; last failure 0m ago: full backup failed at "
                                           "stage download"), message)

        # The next full backup succeeds: heartbeat and health recover.
        self.full_backups_fail = False
        self.now += datetime.timedelta(minutes=10)
        full = self.run_backup(full=True, config=self.cfg)
        self.assertTrue(full.ok, full.error)
        self.assertTrue(full.heartbeat_sent)
        self.now += datetime.timedelta(hours=2)
        self.assertTrue(self.run_backup(full=False, config=self.cfg).heartbeat_sent)
        state = self.store.load()
        self.assertEqual(state.last_full_success_file, full.file_name)
        ok, message = self.health()
        self.assertTrue(ok, message)
        self.assertIn(f"; last full backup 2h 0m ago ({full.file_name})", message)

    def test_full_backup_older_than_the_limit_stops_the_heartbeat(self) -> None:
        self.full_backups_fail = False
        self.assertTrue(self.run_backup(full=True, config=self.cfg).heartbeat_sent)
        self.full_backups_fail = True
        self.now += datetime.timedelta(hours=37)
        self.assertFalse(self.run_backup(full=True, config=self.cfg).ok)
        self.assertFalse(self.run_backup(full=False, config=self.cfg).heartbeat_sent)
        self.assertIn("Heartbeat ping not sent: last full backup (database + filestore) 1d 13h 0m ago, more than "
                      "the allowed 1d 12h 0m", self.messages(logging.WARNING))
        self.assertFalse(self.health()[0])

    def test_all_full_slots_keep_the_old_heartbeat_rule(self) -> None:
        config = self.config(HOURLY_BACKUP_FILESTORE="true")
        self.assertIsNone(config.full_backup_max_age)
        self.now += datetime.timedelta(days=5)
        self.assertTrue(self.run_backup(full=False, config=config).heartbeat_sent)


class WatchdogTests(unittest.TestCase):
    def test_fires_after_the_limit(self) -> None:
        exits: list[int] = []
        with self.assertLogs(LOGGER, logging.CRITICAL) as logs:
            with Watchdog(0.05, exit_func=exits.append) as watchdog:
                self.assertTrue(watchdog.fired.wait(5))
                deadline = time.monotonic() + 5
                while not exits and time.monotonic() < deadline:
                    time.sleep(0.01)
        self.assertEqual(exits, [3])
        self.assertIn("The backup run did not finish within 0:00:00; terminating the process with exit code 3",
                      logs.output[0])

    def test_leaving_the_block_disarms_it(self) -> None:
        exits: list[int] = []
        with Watchdog(0.2, exit_func=exits.append) as watchdog:
            pass
        self.assertEqual([t for t in threading.enumerate() if t.name == "odoo-backup-watchdog"], [])
        time.sleep(0.3)
        self.assertEqual(exits, [])
        self.assertFalse(watchdog.fired.is_set())

    def test_timer_is_a_daemon_thread(self) -> None:
        with Watchdog(60, exit_func=lambda code: None):
            watchdogs = [t for t in threading.enumerate() if t.name == "odoo-backup-watchdog"]
            self.assertEqual(len(watchdogs), 1)
            self.assertTrue(watchdogs[0].daemon)


if __name__ == "__main__":
    unittest.main()
