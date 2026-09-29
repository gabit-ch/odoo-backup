"""Tests of the command line: --check, --health, --once, --retention-plan, the service loop and signals."""

import contextlib
import datetime
import importlib
import io
import json
import logging
import os
import pathlib
import re
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import warnings

from odoo_backup import __version__, cli
from odoo_backup.cli import (
    ServiceJob,
    SignalHandler,
    StopEvent,
    log_config_summary,
    main,
    prepare_tmp_dir,
    run_startup_checks,
    setup_logging,
    watchdog_recorder,
)
from odoo_backup.config import load_config
from odoo_backup.runner import CheckResult, Shutdown, Watchdog
from odoo_backup.scheduler import Slot, build_slots
from odoo_backup.state import State, StateStore
from tests.http_stub import BACKUP_PATH, OdooHTTPStub
from tests.sftp_stub import SFTPStubServer, generate_ed25519_key

REPO = pathlib.Path(__file__).resolve().parents[1]
BACKUP_PY = REPO / "backup.py"
MASTER = "Cli-Master+Pwd/19!"
WRONG_MASTER = "Cli-Wrong+Guess/17?"
UTC = datetime.UTC


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class CliTestCase(unittest.TestCase):
    """Odoo and SFTP stubs plus a root log capture (keeps the test output clean, checks secrets)."""

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.base = pathlib.Path(tmp.name)
        self.sftp_root = self.base / "sftp"
        (self.sftp_root / "backups").mkdir(parents=True)
        self.tmp_dir = self.base / "work"
        self.state_dir = self.base / "state"
        self.odoo = OdooHTTPStub(master_password=MASTER, databases=["master"]).start()
        self.addCleanup(self.odoo.stop)
        self.sftp = SFTPStubServer(self.sftp_root).start()
        self.addCleanup(self.sftp.stop)
        self.secrets = [MASTER, WRONG_MASTER, self.sftp.password]

        self.capture = _CaptureHandler()
        root = logging.getLogger()
        previous = root.level
        root.addHandler(self.capture)
        root.setLevel(logging.DEBUG)
        self.addCleanup(root.setLevel, previous)
        self.addCleanup(root.removeHandler, self.capture)
        self.addCleanup(self.assert_no_secret_logged)

    def env(self, **overrides: str | None) -> dict[str, str]:
        env = {
            "ODOO_URL": self.odoo.url,
            "ODOO_MASTER_PWD": MASTER,
            "ODOO_DB_NAME": "master",
            "ODOO_BACKUP_FORMAT": "tar.gz",
            "ODOO_TIMEOUT": "10",
            **self.sftp.env(),
            "SFTP_PATH": "/backups",
            "SFTP_TIMEOUT": "10",
            "SFTP_UPLOAD_ATTEMPTS": "1",
            "BACKUP_EVERY_HOUR": "2",
            "BACKUP_TIME": "01:00",
            "HOURLY_BACKUP_FILESTORE": "false",
            "HOURLY_BACKUP_KEEP": "4",
            "TZ": "Europe/Zurich",
            "BACKUP_TMP_DIR": str(self.tmp_dir),
            "BACKUP_STATE_DIR": str(self.state_dir),
        }
        for key, value in overrides.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        return env

    def run_main(self, *argv: str, **overrides: str | None) -> tuple[int, str]:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = main(list(argv), env=self.env(**overrides), configure_logging=False)
        return code, output.getvalue()

    def assert_no_secret_logged(self) -> None:
        formatter = logging.Formatter()
        for record in self.capture.records:
            text = record.getMessage()
            if record.exc_info:
                text += formatter.formatException(record.exc_info)
            for secret in self.secrets:
                self.assertNotIn(secret, text, f"secret in log record {record.name}")

    def messages(self, level: int | None = None) -> list[str]:
        return [r.getMessage() for r in self.capture.records if level is None or r.levelno == level]


# ------------------------------------------------------------------------------------------
# --check
# ------------------------------------------------------------------------------------------


class CheckTests(CliTestCase):
    def test_all_checks_ok(self) -> None:
        code, output = self.run_main("--check")
        lines = output.splitlines()
        self.assertEqual(code, 0, output)
        self.assertEqual(
            [line.split(":", 1)[0] for line in lines],
            [
                "OK config",
                "OK retention",
                "OK odoo",
                "OK master-password",
                "OK database",
                "OK sftp",
                "OK target-dir",
                "OK db-only-dir",
            ],
        )
        self.assertIn(f"OK odoo: Odoo 19.0 at {self.odoo.url}", lines)
        sftp_line = next(line for line in lines if line.startswith("OK sftp:"))
        self.assertIn(f"host key ssh-ed25519 {self.sftp.fingerprint} pinned", sftp_line)
        self.assertIn("OK target-dir: /backups exists and is writable", lines)
        self.assertIn("OK db-only-dir: /backups/db-only created and is writable", lines)
        self.assertTrue((self.sftp_root / "backups" / "db-only").is_dir())
        self.assertEqual([p.name for p in self.sftp_root.rglob("*") if p.is_file()], [])  # probes removed
        self.assertEqual(self.odoo.requests_to(BACKUP_PATH), [])  # no backup was taken
        for line in lines:
            self.assertNotIn(line, self.messages())  # stdout is the report; the line is not logged again
        for secret in self.secrets:
            self.assertNotIn(secret, output)

    def test_unpinned_host_key_is_reported_but_ok(self) -> None:
        code, output = self.run_main("--check", SFTP_HOST_KEY=None)
        self.assertEqual(code, 0, output)
        self.assertIn(f"{self.sftp.fingerprint} NOT pinned (set SFTP_HOST_KEY)", output)

    def test_daily_mode_has_no_db_only_check(self) -> None:
        code, output = self.run_main("--check", BACKUP_EVERY_HOUR=None, HOURLY_BACKUP_FILESTORE=None)
        self.assertEqual(code, 0, output)
        self.assertNotIn("db-only-dir", output)

    def test_wrong_master_password(self) -> None:
        code, output = self.run_main("--check", ODOO_MASTER_PWD=WRONG_MASTER)
        self.assertEqual(code, 1)
        self.assertIn(
            "FAIL master-password: Odoo rejected the master password (/jsonrpc db.migrate_databases answered "
            "Access Denied): check ODOO_MASTER_PWD",
            output.splitlines(),
        )
        self.assertIn("OK database:", output)
        self.assertNotIn(WRONG_MASTER, output)

    def test_host_key_mismatch_fails_without_any_authentication(self) -> None:
        other = generate_ed25519_key().fingerprint
        code, output = self.run_main("--check", SFTP_HOST_KEY=other)
        self.assertEqual(code, 1)
        self.assertRegex(
            output,
            rf"FAIL sftp: SFTP host key mismatch .* presented ssh-ed25519 "
            rf"{re.escape(self.sftp.fingerprint)}",
        )
        self.assertIn("FAIL target-dir: not checked: no SFTP connection", output)
        self.assertIn("FAIL db-only-dir: not checked: no SFTP connection", output)
        self.assertEqual(self.sftp.auth_attempts, [])

    def test_missing_target_directory_is_created_like_a_backup_run_does(self) -> None:
        """A fresh Storage Box: the post-deployment check must not fail before the first backup."""
        code, output = self.run_main("--check", SFTP_PATH="/backups/odoo")
        self.assertEqual(code, 0, output)
        self.assertIn("OK target-dir: /backups/odoo created and is writable", output)
        self.assertIn("OK db-only-dir: /backups/odoo/db-only created and is writable", output)
        self.assertTrue((self.sftp_root / "backups" / "odoo" / "db-only").is_dir())
        code, output = self.run_main("--check", SFTP_PATH="/backups/odoo")
        self.assertIn("OK target-dir: /backups/odoo exists and is writable", output)

    def test_target_path_that_is_a_file(self) -> None:
        (self.sftp_root / "backups" / "file").write_bytes(b"x")
        code, output = self.run_main("--check", SFTP_PATH="/backups/file")
        self.assertEqual(code, 1)
        self.assertIn("FAIL target-dir: SFTP path '/backups/file' exists but is not a directory", output)
        self.assertIn("FAIL db-only-dir: not checked: the target directory is not usable", output)

    def test_db_only_path_naming_the_target_directory_fails(self) -> None:
        for filestore in ("false", "true"):
            with self.subTest(HOURLY_BACKUP_FILESTORE=filestore):
                code, output = self.run_main(
                    "--check", DB_ONLY_BACKUP_PATH="backups", HOURLY_BACKUP_FILESTORE=filestore
                )
                self.assertEqual(code, 1, output)
                self.assertIn(
                    "FAIL db-only-dir: SFTP_PATH '/backups' and DB_ONLY_BACKUP_PATH 'backups' are the same directory "
                    "on the SFTP server (/backups)",
                    output,
                )
        code, output = self.run_main("--check", HOURLY_BACKUP_FILESTORE="true")
        self.assertEqual(code, 0, output)
        self.assertNotIn("db-only-dir", output)  # no database-only slots and no alias: nothing to report

    @unittest.skipIf(os.geteuid() == 0, "root can write into read-only directories")
    def test_read_only_target_directory(self) -> None:
        target = self.sftp_root / "backups"
        target.chmod(0o555)
        self.addCleanup(target.chmod, 0o755)
        code, output = self.run_main("--check")
        self.assertEqual(code, 1)
        self.assertRegex(output, r"FAIL target-dir: SFTP directory '/backups' is not writable: ")
        self.assertIn("FAIL db-only-dir: not checked: the target directory is not usable", output)

    def test_unreachable_odoo_skips_the_dependent_checks(self) -> None:
        with socket_port() as port:
            started = time.monotonic()
            code, output = self.run_main("--check", ODOO_URL=f"http://127.0.0.1:{port}")
        self.assertEqual(code, 1)
        self.assertRegex(output, r"FAIL odoo: version check via XML-RPC /xmlrpc/2/common: ConnectionRefusedError")
        self.assertIn("FAIL master-password: not checked: Odoo is not reachable", output)
        self.assertIn("FAIL database: not checked: Odoo is not reachable", output)
        self.assertIn("OK sftp:", output)
        self.assertLess(time.monotonic() - started, 30)

    def test_invalid_configuration(self) -> None:
        code, output = self.run_main("--check", ODOO_URL=None, SFTP_USER=None)
        self.assertEqual(code, 1)
        self.assertEqual(
            output.splitlines(), ["FAIL config: ODOO_URL is required", "FAIL config: SFTP_USER is required"]
        )

    def test_disabled_retention_fails_the_check(self) -> None:
        code, output = self.run_main("--check", DAILY_BACKUP_KEEP="x")
        self.assertEqual(code, 1)
        self.assertIn("FAIL retention: disabled: DAILY_BACKUP_KEEP must be an integer >= 0, got 'x'", output)


@contextlib.contextmanager
def socket_port():
    """A local TCP port that refuses connections."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    yield port


# ------------------------------------------------------------------------------------------
# --health, --once, --retention-plan, configuration errors
# ------------------------------------------------------------------------------------------


class HealthTests(CliTestCase):
    def test_exit_codes(self) -> None:
        store = StateStore(self.state_dir)
        now = datetime.datetime.now(UTC)
        code, output = self.run_main("--health")
        self.assertEqual(code, 1)
        self.assertEqual(output.strip(), "unhealthy: no state recorded yet (is the backup service running?)")

        store.record_start(now - datetime.timedelta(minutes=30))
        code, output = self.run_main("--health")
        self.assertEqual(code, 0)
        self.assertTrue(output.startswith("healthy: starting"))

        store.record_success(now - datetime.timedelta(hours=1), "odoo19.0-master-20260927-010000.tar.gz")
        code, output = self.run_main("--health")
        self.assertEqual(code, 0)
        self.assertIn("odoo19.0-master-20260927-010000.tar.gz", output)

        five_hours_ago = now - datetime.timedelta(hours=5)
        store.save(
            State(
                started_at=now - datetime.timedelta(days=3),
                last_success_at=five_hours_ago,
                last_full_success_at=five_hours_ago,
            )
        )
        code, output = self.run_main("--health")  # every 2 h => max age 4 h
        self.assertEqual(code, 1)
        self.assertTrue(output.startswith("unhealthy: last successful backup 5h 0m ago"))

        code, output = self.run_main("--health", HEALTHCHECK_MAX_AGE_HOURS="6")
        self.assertEqual(code, 0)

    def test_database_only_successes_do_not_hide_failing_full_backups(self) -> None:
        now = datetime.datetime.now(UTC)
        StateStore(self.state_dir).save(
            State(
                started_at=now - datetime.timedelta(days=10),
                last_success_at=now - datetime.timedelta(hours=1),
                last_full_success_at=now - datetime.timedelta(days=3),
            )
        )
        code, output = self.run_main("--health")  # HOURLY_BACKUP_FILESTORE=false: full backups max 36 h
        self.assertEqual(code, 1)
        self.assertTrue(
            output.startswith(
                "unhealthy: last full backup (database + filestore) 3d 0h 0m ago, more than the allowed 1d 12h 0m"
            ),
            output,
        )
        code, output = self.run_main("--health", HOURLY_BACKUP_FILESTORE="true")
        self.assertEqual(code, 0, output)

    def test_invalid_configuration_is_unhealthy(self) -> None:
        code, output = self.run_main("--health", ODOO_URL=None)
        self.assertEqual(code, 1)
        self.assertEqual(output.strip(), "unhealthy: invalid configuration: ODOO_URL is required")


class OnceTests(CliTestCase):
    def test_full_and_database_only_runs(self) -> None:
        code, _ = self.run_main("--once")
        self.assertEqual(code, 0)
        self.assertEqual(len([p for p in (self.sftp_root / "backups").iterdir() if p.is_file()]), 1)
        code, _ = self.run_main("--once", "--database-only")
        self.assertEqual(code, 0)
        self.assertEqual(len(list((self.sftp_root / "backups" / "db-only").iterdir())), 1)
        self.assertEqual(self.odoo.requests_to(BACKUP_PATH)[-1].form["filestore"], "false")
        state = StateStore(self.state_dir).load()
        self.assertIsNotNone(state.last_success_at)

    def test_failed_run_exits_1(self) -> None:
        code, _ = self.run_main("--once", ODOO_MASTER_PWD=WRONG_MASTER)
        self.assertEqual(code, 1)
        self.assertTrue(StateStore(self.state_dir).load().last_error.startswith("full backup failed at stage download"))

    def test_test_mode_runs_once_instead_of_the_schedule(self) -> None:
        code, _ = self.run_main(TEST_MODE="true")
        self.assertEqual(code, 0)
        self.assertEqual(len([p for p in (self.sftp_root / "backups").iterdir() if p.is_file()]), 1)
        self.assertIn("TEST_MODE is set: running one full backup now instead of the schedule", self.messages())

    def test_invalid_configuration_exits_2(self) -> None:
        for argv in (["--once"], [], ["--retention-plan"]):
            with self.subTest(argv=argv):
                code, _ = self.run_main(*argv, SFTP_HOST=None)
                self.assertEqual(code, 2)
        self.assertIn("Configuration problem: SFTP_HOST is required", self.messages(logging.ERROR))

    def test_database_only_requires_once(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()) as err, self.assertRaises(SystemExit) as caught:
            main(["--database-only"], env=self.env(), configure_logging=False)
        self.assertEqual(caught.exception.code, 2)
        self.assertIn("--database-only requires --once", err.getvalue())


class RetentionPlanTests(CliTestCase):
    def test_prints_the_plan_and_deletes_nothing(self) -> None:
        names = [
            f"odoo19.0-master-202609{day:02d}-{hour:02d}0000.tar.gz" for day in (25, 26) for hour in range(1, 24, 2)
        ]
        for name in names:
            (self.sftp_root / "backups" / name).write_bytes(b"x")
        (self.sftp_root / "backups" / "db-only").mkdir()
        before = sorted(p.name for p in (self.sftp_root / "backups").iterdir())
        code, output = self.run_main("--retention-plan")
        self.assertEqual(code, 0, output)
        lines = output.splitlines()
        # The newest four count from the real "now": all 24 names are older than a day.
        self.assertTrue(
            lines[0].startswith(
                "/backups (full backups; last 4, daily 30, monthly 12, yearly all (anchor 01:00:00)): keep "
            ),
            lines[0],
        )
        self.assertIn("  would delete odoo19.0-master-20260925-030000.tar.gz", lines)
        self.assertNotIn("  would delete odoo19.0-master-20260925-010000.tar.gz", lines)
        self.assertTrue(any(line.startswith("/backups/db-only (database-only backups; last 4") for line in lines))
        self.assertEqual(lines[-1], "Nothing was deleted (retention plan only).")
        self.assertEqual(sorted(p.name for p in (self.sftp_root / "backups").iterdir()), before)

    def test_disabled_retention(self) -> None:
        code, output = self.run_main("--retention-plan", MONTHLY_BACKUP_KEEP="-2")
        self.assertEqual(code, 1)
        self.assertIn("Retention is disabled: MONTHLY_BACKUP_KEEP must be an integer >= 0", output)

    def test_sftp_failure(self) -> None:
        code, output = self.run_main("--retention-plan", SFTP_HOST_KEY=generate_ed25519_key().fingerprint)
        self.assertEqual(code, 1)
        self.assertIn("FAIL sftp: SFTP host key mismatch", output)

    def test_aliased_directories_are_refused(self) -> None:
        (self.sftp_root / "backups" / "odoo19.0-master-20260901-010000.tar.gz").write_bytes(b"x")
        code, output = self.run_main("--retention-plan", DB_ONLY_BACKUP_PATH="backups")
        self.assertEqual(code, 1)
        self.assertEqual(
            output.strip(),
            "FAIL directories: SFTP_PATH '/backups' and DB_ONLY_BACKUP_PATH 'backups' are the same directory on "
            "the SFTP server (/backups); the database-only retention would delete the full backups there. Set "
            "DB_ONLY_BACKUP_PATH to a separate directory",
        )


# ------------------------------------------------------------------------------------------
# Service building blocks
# ------------------------------------------------------------------------------------------


class SignalAndJobTests(unittest.TestCase):
    def test_stop_event(self) -> None:
        stop = StopEvent()
        self.addCleanup(stop.close)
        self.assertFalse(stop.wait(0.01))
        threading.Timer(0.05, stop.set).start()
        started = time.monotonic()
        self.assertTrue(stop.wait(10))
        self.assertLess(time.monotonic() - started, 5)
        self.assertTrue(stop.is_set())
        stop.set()  # idempotent
        self.assertTrue(stop.wait(0))

    def test_signal_while_idle_only_stops(self) -> None:
        stop = StopEvent()
        self.addCleanup(stop.close)
        handler = SignalHandler(stop)
        handler(signal.SIGTERM, None)
        self.assertTrue(stop.is_set())
        self.assertEqual(handler.received, "SIGTERM")

    def test_signal_during_a_run_raises_shutdown_once(self) -> None:
        stop = StopEvent()
        self.addCleanup(stop.close)
        handler = SignalHandler(stop)
        handler.running = True
        with self.assertRaisesRegex(Shutdown, "received SIGINT"):
            handler(signal.SIGINT, None)
        handler(signal.SIGTERM, None)  # a second signal during the clean-up does not raise
        self.assertTrue(stop.is_set())

    def _job(self, run) -> tuple[ServiceJob, SignalHandler]:
        stop = StopEvent()
        self.addCleanup(stop.close)
        signals = SignalHandler(stop)
        config = offline_config()
        return ServiceJob(config, StateStore("/nonexistent"), signals, run=run), signals

    def test_service_job_runs_the_slot_kind_under_a_watchdog(self) -> None:
        calls = []

        def run(config, full, *, state):
            watchdogs = [t for t in threading.enumerate() if t.name == "odoo-backup-watchdog"]
            calls.append((full, signals.running, len(watchdogs)))

        job, signals = self._job(run)
        job(Slot(datetime.time(1), True))
        job(Slot(datetime.time(3), False))
        self.assertEqual(calls, [(True, True, 1), (False, True, 1)])
        self.assertFalse(signals.running)

    def test_service_job_skips_the_run_after_a_stop_request(self) -> None:
        job, signals = self._job(lambda *args, **kwargs: self.fail("must not run"))
        signals.stop.set()
        job(Slot(datetime.time(1), True))
        self.assertFalse(signals.running)

    def test_signal_during_the_job_propagates_as_shutdown(self) -> None:
        def run(config, full, *, state):
            signals(signal.SIGTERM, None)  # as if SIGTERM arrived during the run

        job, signals = self._job(run)
        with self.assertRaises(Shutdown):
            job(Slot(datetime.time(1), True))
        self.assertFalse(signals.running)
        self.assertTrue(signals.stop.is_set())

    def test_startup_checks_that_hang_do_not_block_the_service(self) -> None:
        stop = StopEvent()
        self.addCleanup(stop.close)
        release = threading.Event()
        self.addCleanup(release.set)

        def hanging_checks(config, *, report):
            release.wait(30)
            return []

        started = time.monotonic()
        with self.assertLogs(cli.logger, logging.WARNING) as logs:
            run_startup_checks(offline_config(), stop, checks=hanging_checks, deadline=0.3)
        self.assertLess(time.monotonic() - started, 5)
        self.assertIn("The start-up checks did not finish within 0.3 s; starting the schedule anyway", logs.output[0])

    def test_stop_request_ends_the_startup_checks_at_once(self) -> None:
        stop = StopEvent()
        self.addCleanup(stop.close)
        release = threading.Event()
        self.addCleanup(release.set)

        def hanging_checks(config, *, report):
            release.wait(30)
            return []

        threading.Timer(0.1, stop.set).start()  # as if SIGTERM arrived during the checks
        started = time.monotonic()
        run_startup_checks(offline_config(), stop, checks=hanging_checks, deadline=60)
        self.assertLess(time.monotonic() - started, 5)

    def test_startup_check_failures_are_counted(self) -> None:
        stop = StopEvent()
        self.addCleanup(stop.close)

        def checks(config, *, report):
            results = [CheckResult("odoo", True, "fine"), CheckResult("sftp", False, "down")]
            for result in results:
                report(result)
            return results

        with self.assertLogs(cli.logger, logging.INFO) as logs:
            run_startup_checks(offline_config(), stop, checks=checks)
        self.assertIn("Start-up check: FAIL sftp: down", "\n".join(logs.output))
        self.assertIn("1 start-up check(s) failed; the service keeps running", logs.output[-1])

    def test_watchdog_records_the_hung_run_in_the_state_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = StateStore(tmp)
            exits: list[int] = []
            config = offline_config()
            with (
                self.assertLogs("odoo_backup.runner", logging.CRITICAL),
                Watchdog(0.05, on_timeout=watchdog_recorder(config, store, False), exit_func=exits.append) as dog,
            ):
                self.assertTrue(dog.fired.wait(5))
                deadline = time.monotonic() + 5
                while not exits and time.monotonic() < deadline:
                    time.sleep(0.01)
            self.assertEqual(exits, [3])
            state = store.load()
            self.assertIsNotNone(state.last_failure_at)
            self.assertEqual(
                state.last_error,
                "database-only backup failed: the run did not finish within BACKUP_MAX_RUNTIME_MINUTES (8:00:00); "
                "the watchdog ended the process (exit code 3)",
            )


def offline_config():
    env = {
        "ODOO_URL": "http://odoo.invalid:8069",
        "ODOO_MASTER_PWD": "irrelevant-master",
        "ODOO_DB_NAME": "master",
        "SFTP_HOST": "sftp.invalid",
        "SFTP_USER": "user",
        "SFTP_PASSWORD": "irrelevant-sftp",
        "BACKUP_TMP_DIR": "/nonexistent/odoo-backup-tmp",
        "BACKUP_STATE_DIR": "/nonexistent/odoo-backup-state",
    }
    return load_config(env)


class ServiceHelperTests(CliTestCase):
    def test_prepare_tmp_dir_removes_only_the_contents(self) -> None:
        outside = self.base / "outside.txt"
        outside.write_text("keep me")
        outside_dir = self.base / "outside-dir"
        outside_dir.mkdir()
        (outside_dir / "file").write_text("keep me too")
        self.tmp_dir.mkdir(mode=0o755)
        (self.tmp_dir / "leftover.part").write_bytes(b"x")
        (self.tmp_dir / "sub").mkdir()
        (self.tmp_dir / "sub" / "file").write_bytes(b"x")
        (self.tmp_dir / "link").symlink_to(outside)
        (self.tmp_dir / "dir-link").symlink_to(outside_dir, target_is_directory=True)
        self.assertEqual(prepare_tmp_dir(self.tmp_dir), 4)
        self.assertEqual(list(self.tmp_dir.iterdir()), [])
        self.assertEqual(stat.S_IMODE(self.tmp_dir.stat().st_mode), 0o700)
        self.assertEqual(outside.read_text(), "keep me")
        self.assertEqual((outside_dir / "file").read_text(), "keep me too")  # symlinks are never followed
        self.assertEqual(prepare_tmp_dir(self.base / "new" / "dir"), 0)  # created with its parents

    def test_config_summary_has_no_secrets(self) -> None:
        # A password with "/" is not valid inside a URL: urlsplit() then sees it as part of the path.
        for url in (f"http://admin:{MASTER}@127.0.0.1:{self.odoo.port}", "http://admin:url-secret@odoo:8069/odoo"):
            with self.subTest(url=url):
                self.capture.records.clear()
                config = load_config(self.env(ODOO_URL=url))
                log_config_summary(config, build_slots(config.backup_time, config.backup_every_hour, False))
                text = "\n".join(self.messages())
                self.assertNotIn(MASTER, text)
                self.assertNotIn("url-secret", text)
        self.assertIn("Odoo: http://odoo:8069, database 'master', format tar.gz", text)
        self.assertIn(f"odoo-backup {__version__}", text)
        self.assertIn("Schedule: every 2 h from 01:00:00 (Europe/Zurich): 01:00, 03:00*, 05:00*", text)
        self.assertIn("host key pinned (1 SFTP_HOST_KEY entries), authentication: password", text)
        self.assertIn(
            "Retention: last 4, daily 30, monthly 12, yearly all (anchor 01:00:00); database-only backups: newest 4",
            text,
        )
        self.assertNotIn(self.sftp.password, text)

    def test_setup_logging_keeps_library_loggers_quiet(self) -> None:
        root = logging.getLogger()
        saved_handlers, saved_level = root.handlers[:], root.level
        saved_library = {name: logging.getLogger(name).level for name in ("paramiko", "urllib3")}
        root.handlers = []
        try:
            with warnings.catch_warnings():
                setup_logging({"LOG_LEVEL": "debug"})
            self.assertEqual(root.level, logging.DEBUG)
            self.assertEqual(logging.getLogger("paramiko").level, logging.WARNING)
            self.assertEqual(logging.getLogger("urllib3").level, logging.WARNING)
            self.assertEqual(root.handlers[0].formatter._fmt, "%(asctime)s %(levelname)s %(name)s: %(message)s")
        finally:
            for handler in root.handlers:
                handler.close()
            root.handlers = saved_handlers
            root.setLevel(saved_level)
            for name, level in saved_library.items():
                logging.getLogger(name).setLevel(level)
            logging.captureWarnings(False)

    def test_setup_logging_default_level_yields_to_log_level(self) -> None:
        root = logging.getLogger()
        saved_handlers, saved_level = root.handlers[:], root.level
        saved_library = {name: logging.getLogger(name).level for name in ("paramiko", "urllib3")}
        cases = (
            ({}, logging.WARNING, logging.WARNING),
            ({"LOG_LEVEL": "info"}, logging.WARNING, logging.INFO),
            ({"LOG_LEVEL": "loud"}, logging.WARNING, logging.WARNING),
            ({}, logging.INFO, logging.INFO),
        )
        try:
            for env, default_level, expected in cases:
                with self.subTest(env=env, default_level=default_level):
                    root.handlers = []
                    with warnings.catch_warnings():
                        if env.get("LOG_LEVEL") == "loud":
                            with self.assertLogs("odoo_backup.cli", logging.WARNING) as logs:
                                setup_logging(env, default_level=default_level)
                            self.assertIn("is not a logging level; using WARNING", logs.output[0])
                        else:
                            setup_logging(env, default_level=default_level)
                    self.assertEqual(root.level, expected)
                    for handler in root.handlers:
                        handler.close()
        finally:
            root.handlers = saved_handlers
            root.setLevel(saved_level)
            for name, level in saved_library.items():
                logging.getLogger(name).setLevel(level)
            logging.captureWarnings(False)

    def test_readme_documents_exactly_the_environment_variables(self) -> None:
        readme = REPO / "README.md"
        if not readme.exists():
            self.skipTest("README.md is not part of the image")
        source = (REPO / "odoo_backup" / "config.py").read_text()
        methods = r"raw|required|secret|boolean|integer|optional_integer|positive_number|directory|is_set|http_url"
        names = set(re.findall(rf'reader\.(?:{methods})\("([A-Z][A-Z0-9_]+)"', source))
        names |= {f"{name}_FILE" for name in re.findall(r'reader\.secret\("([A-Z][A-Z0-9_]+)"', source)}
        names.add("LOG_LEVEL")  # read by cli.py
        table = readme.read_text().split("## Environment variables", 1)[1].split("\n## ", 1)[0]
        documented = set(re.findall(r"^\| `([A-Z][A-Z0-9_]+)`", table, re.M))
        documented |= set(re.findall(r"^\| `[A-Z][A-Z0-9_]+` / `([A-Z][A-Z0-9_]+)`", table, re.M))
        self.assertEqual(documented, names)


# ------------------------------------------------------------------------------------------
# Real processes: entry point, import side effects, signals
# ------------------------------------------------------------------------------------------


class _Process:
    """``python backup.py ...`` in a subprocess; stderr lines are collected by a thread."""

    def __init__(self, test: unittest.TestCase, args: list[str], env: dict[str, str]) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, "-X", "dev", "-W", "error::DeprecationWarning", str(BACKUP_PY), *args],
            cwd=REPO,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        test.addCleanup(self._kill)
        self.lines: list[str] = []
        self._cond = threading.Condition()
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self) -> None:
        for line in self.proc.stderr:
            with self._cond:
                self.lines.append(line)
                self._cond.notify_all()

    def wait_for(self, fragment: str, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._cond:
            while not any(fragment in line for line in self.lines):
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self.proc.poll() is not None:
                    return any(fragment in line for line in self.lines)
                self._cond.wait(min(remaining, 0.1))
        return True

    def finish(self, timeout: float) -> int:
        code = self.proc.wait(timeout)
        self._reader.join(5)
        return code

    @property
    def stderr(self) -> str:
        with self._cond:
            return "".join(self.lines)

    def _kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(10)
        self._reader.join(5)
        if self.proc.stderr is not None:
            self.proc.stderr.close()


class ProcessTests(CliTestCase):
    def process_env(self, **overrides: str | None) -> dict[str, str]:
        env = self.env(**overrides)
        env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1", TMPDIR=str(self.base))
        return env

    def run_process(self, args: list[str], env: dict[str, str], timeout: float = 60) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-X", "dev", "-W", "error::DeprecationWarning", str(BACKUP_PY), *args],
            cwd=REPO,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,  # the tests assert the exit code themselves
        )

    def test_help(self) -> None:
        completed = self.run_process(["--help"], {"PYTHONDONTWRITEBYTECODE": "1"})
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("--check", completed.stdout)
        self.assertIn("--retention-plan", completed.stdout)
        self.assertEqual(completed.stderr, "")
        version = self.run_process(["--version"], {"PYTHONDONTWRITEBYTECODE": "1"})
        self.assertEqual(version.stdout.strip(), f"backup.py {__version__}")

    def test_check_against_the_stubs(self) -> None:
        completed = self.run_process(["--check"], self.process_env())
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        lines = completed.stdout.splitlines()
        self.assertEqual(len(lines), 8, completed.stdout)
        self.assertTrue(all(line.startswith("OK ") for line in lines), completed.stdout)
        self.assertNotIn("Traceback", completed.stderr)
        for secret in self.secrets:
            self.assertNotIn(secret, completed.stdout + completed.stderr)
        # Every result once, on stdout only; the report commands log no INFO lines by default.
        self.assert_each_line_once(completed)
        self.assertNotIn(" INFO ", completed.stderr)
        self.assertNotIn("OK ", completed.stderr)

    def test_check_failure_exit_code(self) -> None:
        completed = self.run_process(["--check"], self.process_env(ODOO_MASTER_PWD=WRONG_MASTER))
        self.assertEqual(completed.returncode, 1)
        self.assertIn("FAIL master-password:", completed.stdout)
        self.assertEqual((completed.stdout + completed.stderr).count("FAIL master-password:"), 1)

    def assert_each_line_once(self, completed: subprocess.CompletedProcess) -> None:
        combined = completed.stdout + completed.stderr
        for line in completed.stdout.splitlines():
            self.assertEqual(combined.count(line), 1, f"{line!r} appears more than once:\n{combined}")

    def test_check_with_log_level_info_shows_details_but_each_result_once(self) -> None:
        completed = self.run_process(["--check"], self.process_env(LOG_LEVEL="INFO"))
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertIn("INFO odoo_backup.sftp: Connected to SFTP server", completed.stderr)
        self.assert_each_line_once(completed)
        self.assertNotIn("OK ", completed.stderr)

    def test_check_invalid_configuration_prints_each_problem_once(self) -> None:
        completed = self.run_process(["--check"], self.process_env(SFTP_HOST=None))
        self.assertEqual(completed.returncode, 1, completed.stdout + completed.stderr)
        self.assertIn("FAIL config: SFTP_HOST is required", completed.stdout)
        self.assert_each_line_once(completed)

    def test_retention_plan_prints_the_plan_without_info_logs(self) -> None:
        completed = self.run_process(["--retention-plan"], self.process_env())
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(completed.stdout.splitlines()[-1], "Nothing was deleted (retention plan only).")
        self.assertNotIn(" INFO ", completed.stderr)

    def test_importing_has_no_side_effects(self) -> None:
        sentinel = self.base / "sentinel.txt"
        sentinel.write_text("x")
        (self.base / "odoo-backup").mkdir()
        inner = self.base / "odoo-backup" / "sentinel.txt"
        inner.write_text("x")
        code = (
            "import json, logging, threading\n"
            "import backup, odoo_backup.cli, odoo_backup.runner, odoo_backup.state, odoo_backup.config\n"
            "print(json.dumps({'threads': threading.active_count(), 'handlers': len(logging.getLogger().handlers)}))\n"
        )
        completed = subprocess.run(
            [sys.executable, "-X", "dev", "-W", "error::DeprecationWarning", "-c", code],
            cwd=REPO,
            env={"TMPDIR": str(self.base), "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout), {"threads": 1, "handlers": 0})
        self.assertEqual(completed.stderr, "")
        self.assertTrue(sentinel.exists())
        self.assertTrue(inner.exists())

    def test_importing_in_process_keeps_a_sentinel_in_the_system_temp_dir(self) -> None:
        # No bytecode cache in the repository root (__pycache__/ there is not ours to write).
        previous, sys.dont_write_bytecode = sys.dont_write_bytecode, True
        self.addCleanup(setattr, sys, "dont_write_bytecode", previous)
        with tempfile.NamedTemporaryFile(prefix="odoo-backup-sentinel-", dir=tempfile.gettempdir()) as sentinel:
            module = importlib.import_module("backup")
            importlib.reload(module)
            self.assertTrue(os.path.exists(sentinel.name))
        self.assertIs(module.main, cli.main)

    def test_service_stops_on_sigterm_while_idle(self) -> None:
        self.tmp_dir.mkdir()
        (self.tmp_dir / "leftover.tar.gz.part").write_bytes(b"x" * 100)
        outside = self.base / "outside.txt"
        outside.write_text("keep")
        later = (datetime.datetime.now(UTC) + datetime.timedelta(hours=12)).strftime("%H:%M")
        process = _Process(
            self,
            [],
            self.process_env(TZ="UTC", BACKUP_EVERY_HOUR=None, HOURLY_BACKUP_FILESTORE=None, BACKUP_TIME=later),
        )
        self.assertTrue(process.wait_for("Next backup at", 30), process.stderr)
        process.proc.send_signal(signal.SIGTERM)
        code = process.finish(15)
        stderr = process.stderr
        self.assertEqual(code, 0, stderr)
        self.assertIn("Stopped by SIGTERM", stderr)
        self.assertIn("Start-up check: OK master-password: accepted by Odoo", stderr)
        self.assertIn("Removed 1 leftover entries from BACKUP_TMP_DIR", stderr)
        self.assertNotIn("Traceback", stderr)
        self.assertEqual(list(self.tmp_dir.iterdir()), [])
        self.assertEqual(outside.read_text(), "keep")
        self.assertIsNotNone(StateStore(self.state_dir).load().started_at)
        for secret in self.secrets:
            self.assertNotIn(secret, stderr)

    def test_service_starts_the_schedule_despite_a_stalled_sftp_server(self) -> None:
        """Regression: a server that authenticates but never starts SFTP blocked the start-up forever."""
        for stall in ("request", "version"):
            with self.subTest(stall=stall):
                self.sftp.stall_sftp = stall
                later = (datetime.datetime.now(UTC) + datetime.timedelta(hours=12)).strftime("%H:%M")
                process = _Process(
                    self,
                    [],
                    self.process_env(
                        TZ="UTC",
                        BACKUP_EVERY_HOUR=None,
                        SFTP_TIMEOUT="2",
                        HOURLY_BACKUP_FILESTORE=None,
                        BACKUP_TIME=later,
                    ),
                )
                self.assertTrue(process.wait_for("Next backup at", 30), process.stderr)
                self.assertIn("Start-up check: FAIL sftp: cannot open an SFTP session on 127.0.0.1:", process.stderr)
                self.assertIn("the SFTP server did not answer within 2 s", process.stderr)
                process.proc.send_signal(signal.SIGTERM)
                self.assertEqual(process.finish(15), 0, process.stderr)

    def test_sigterm_during_the_startup_checks_stops_the_service(self) -> None:
        self.sftp.stall_sftp = "request"  # the SFTP check waits CHECK_SFTP_TIMEOUT (20 s)
        later = (datetime.datetime.now(UTC) + datetime.timedelta(hours=12)).strftime("%H:%M")
        process = _Process(
            self,
            [],
            self.process_env(TZ="UTC", BACKUP_EVERY_HOUR=None, HOURLY_BACKUP_FILESTORE=None, BACKUP_TIME=later),
        )
        self.assertTrue(process.wait_for("Start-up check: OK database", 30), process.stderr)
        time.sleep(0.5)
        started = time.monotonic()
        process.proc.send_signal(signal.SIGTERM)
        self.assertEqual(process.finish(15), 0, process.stderr)
        self.assertLess(time.monotonic() - started, 10)
        self.assertIn("Stopped by SIGTERM", process.stderr)
        self.assertNotIn("Next backup at", process.stderr)

    def test_service_sigterm_during_a_run_records_it_and_exits_0(self) -> None:
        self.odoo.backup_overrides = {"delay": 30}  # Odoo "works" on the dump for 30 s
        soon = (datetime.datetime.now(UTC) + datetime.timedelta(seconds=6)).strftime("%H:%M:%S")
        process = _Process(
            self, [], self.process_env(TZ="UTC", BACKUP_EVERY_HOUR=None, HOURLY_BACKUP_FILESTORE=None, BACKUP_TIME=soon)
        )
        self.assertTrue(process.wait_for("Requesting a full tar.gz backup", 40), process.stderr)
        time.sleep(0.3)
        process.proc.send_signal(signal.SIGTERM)
        code = process.finish(15)
        stderr = process.stderr
        self.assertEqual(code, 0, stderr)
        self.assertIn("stage=download error=interrupted (received SIGTERM)", stderr)
        self.assertIn("Stopped by SIGTERM during a backup run (recorded as interrupted)", stderr)
        self.assertEqual(list(self.tmp_dir.iterdir()), [])
        self.assertEqual([p for p in (self.sftp_root / "backups").iterdir() if p.is_file()], [])
        self.assertIn("interrupted (received SIGTERM)", StateStore(self.state_dir).load().last_error)

    def test_once_sigterm_during_the_download_exits_1(self) -> None:
        self.odoo.backup_overrides = {"delay": 30}
        process = _Process(self, ["--once"], self.process_env())
        self.assertTrue(process.wait_for("Requesting a full tar.gz backup", 30), process.stderr)
        time.sleep(0.3)
        process.proc.send_signal(signal.SIGTERM)
        code = process.finish(15)
        stderr = process.stderr
        self.assertEqual(code, 1, stderr)
        self.assertIn("Interrupted by SIGTERM; the run was recorded as failed", stderr)
        self.assertEqual(list(self.tmp_dir.iterdir()), [])
        self.assertIn("interrupted (received SIGTERM)", StateStore(self.state_dir).load().last_error)


if __name__ == "__main__":
    unittest.main()
