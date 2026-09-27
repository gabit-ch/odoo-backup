import datetime
import json
import logging
import pathlib
import socket
import stat
import tempfile
import unittest
from unittest import mock

from odoo_backup import __version__
from odoo_backup import state as state_module
from odoo_backup.state import (
    MAX_ERROR_LENGTH,
    RunLockedError,
    State,
    StateStore,
    full_backup_problem,
    health_status,
    send_heartbeat,
    short_error,
)
from tests.http_stub import OdooHTTPStub, StubResponse

UTC = datetime.UTC
T0 = datetime.datetime(2026, 9, 27, 1, 0, 5, tzinfo=UTC)
HOUR = datetime.timedelta(hours=1)
LOGGER = "odoo_backup.state"


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def capture_all_logs(test: unittest.TestCase) -> _CaptureHandler:
    """Capture every record of every logger at DEBUG for the rest of the test."""
    handler = _CaptureHandler()
    root = logging.getLogger()
    previous = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    test.addCleanup(root.setLevel, previous)
    test.addCleanup(root.removeHandler, handler)
    return handler


class StateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.directory = pathlib.Path(tmp.name) / "state"  # created on the first save
        self.store = StateStore(self.directory)

    def test_missing_file_gives_an_empty_state(self) -> None:
        self.assertEqual(self.store.load(), State())
        self.assertFalse(self.directory.exists())

    def test_round_trip_file_mode_and_no_temporary_leftovers(self) -> None:
        self.store.record_start(T0)
        self.store.record_success(T0 + HOUR, "odoo19.0-master-20260927-020005.tar.gz")
        self.store.record_success(T0 + 3 * HOUR, "odoo19.0-master-20260927-040005.tar.gz", full=False)
        self.store.record_failure(T0 + 2 * HOUR, "upload failed")
        loaded = self.store.load()
        self.assertEqual(
            loaded,
            State(
                started_at=T0,
                last_success_at=T0 + 3 * HOUR,
                last_success_file="odoo19.0-master-20260927-040005.tar.gz",
                last_full_success_at=T0 + HOUR,
                last_full_success_file="odoo19.0-master-20260927-020005.tar.gz",
                last_failure_at=T0 + 2 * HOUR,
                last_error="upload failed",
            ),
        )
        self.assertEqual(stat.S_IMODE(self.store.path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.directory.stat().st_mode), 0o700)
        self.assertEqual(sorted(p.name for p in self.directory.iterdir()), ["state.json"])

    def test_json_layout_uses_utc_iso_timestamps(self) -> None:
        zurich = datetime.timezone(datetime.timedelta(hours=2))
        self.store.record_success(datetime.datetime(2026, 9, 27, 3, 0, 5, tzinfo=zurich), "file.tar.gz")
        data = json.loads(self.store.path.read_text())
        self.assertEqual(
            data,
            {
                "started_at": None,
                "last_success_at": "2026-09-27T01:00:05+00:00",
                "last_success_file": "file.tar.gz",
                "last_full_success_at": "2026-09-27T01:00:05+00:00",
                "last_full_success_file": "file.tar.gz",
                "last_failure_at": None,
                "last_error": None,
            },
        )

    def test_state_files_without_the_full_backup_fields_still_load(self) -> None:
        self.directory.mkdir()
        self.store.path.write_text('{"started_at": "2026-09-27T01:00:05+00:00", "last_success_at": null}')
        self.assertEqual(self.store.load(), State(started_at=T0))

    def test_database_only_success_leaves_the_full_backup_fields_alone(self) -> None:
        self.store.record_success(T0, "full.tar.gz")
        self.store.record_success(T0 + HOUR, "db-only.tar.gz", full=False)
        state = self.store.load()
        self.assertEqual((state.last_success_at, state.last_success_file), (T0 + HOUR, "db-only.tar.gz"))
        self.assertEqual((state.last_full_success_at, state.last_full_success_file), (T0, "full.tar.gz"))

    def test_restarts_do_not_extend_the_start_up_grace_period(self) -> None:
        """Regression: every start overwrote started_at, so hung runs + restarts stayed healthy."""
        self.store.record_start(T0)
        self.store.record_start(T0 + 20 * HOUR)  # e.g. the watchdog ended a hung run, Docker restarted
        self.assertEqual(self.store.load().started_at, T0)
        self.store.record_success(T0 + 21 * HOUR, "db-only.tar.gz", full=False)
        self.store.record_start(T0 + 22 * HOUR)
        self.assertEqual(self.store.load().started_at, T0)  # still no full backup since T0
        self.store.record_success(T0 + 23 * HOUR, "full.tar.gz")
        self.store.record_start(T0 + 24 * HOUR)
        self.assertEqual(self.store.load().started_at, T0 + 24 * HOUR)  # a new period starts

    def test_daily_restarts_without_any_success_turn_unhealthy(self) -> None:
        max_age = datetime.timedelta(hours=36)
        verdicts = []
        for day in range(5):
            start = T0 + day * 24 * HOUR
            self.store.record_start(start)
            verdicts.append(health_status(self.store.load(), start + 23 * HOUR, max_age)[0])
        self.assertEqual(verdicts, [True, False, False, False, False])

    def test_success_and_failure_keep_each_others_fields(self) -> None:
        self.store.record_failure(T0, "first error")
        self.store.record_success(T0 + HOUR, "a.tar.gz")
        state = self.store.load()
        self.assertEqual((state.last_failure_at, state.last_error), (T0, "first error"))
        self.store.record_failure(T0 + 2 * HOUR, "second error")
        state = self.store.load()
        self.assertEqual((state.last_success_at, state.last_success_file), (T0 + HOUR, "a.tar.gz"))
        self.assertEqual(state.last_error, "second error")

    def test_last_error_is_one_short_line(self) -> None:
        self.store.record_failure(T0, "line one\n  line two\t" + "x" * 1000)
        error = self.store.load().last_error
        self.assertEqual(len(error), MAX_ERROR_LENGTH)
        self.assertTrue(error.startswith("line one line two x"))
        self.assertTrue(error.endswith("..."))
        self.assertEqual(short_error("  a \n b "), "a b")

    def test_corrupt_or_malformed_files_are_ignored_with_a_warning(self) -> None:
        self.directory.mkdir()
        for content in ("{not json", "[]", '{"started_at": 5}', '{"started_at": "2026-09-27T01:00:00"}',
                        '{"last_error": 7}', '{"started_at": "yesterday"}'):
            with self.subTest(content=content):
                self.store.path.write_text(content)
                with self.assertLogs(LOGGER, logging.WARNING) as logs:
                    self.assertEqual(self.store.load(), State())
                self.assertIn("Ignoring the invalid state file", logs.output[0])

    def test_failed_save_keeps_the_old_file_and_leaves_no_temporary_file(self) -> None:
        self.store.record_success(T0, "old.tar.gz")
        with mock.patch.object(state_module.os, "replace", side_effect=OSError(28, "No space left on device")):
            with self.assertRaises(OSError):
                self.store.record_success(T0 + HOUR, "new.tar.gz")
        self.assertEqual(self.store.load().last_success_file, "old.tar.gz")
        self.assertEqual(sorted(p.name for p in self.directory.iterdir()), ["state.json"])

    def test_run_lock_is_exclusive_and_released(self) -> None:
        with self.store.run_lock():
            with self.assertRaisesRegex(RunLockedError, "another backup run is in progress"):
                with self.store.run_lock():
                    pass
        with self.store.run_lock():  # released after the block
            pass
        with self.assertRaises(ValueError), self.store.run_lock():
            raise ValueError("boom")
        with self.store.run_lock():  # released after an exception as well
            pass
        self.assertEqual(stat.S_IMODE((self.directory / "run.lock").stat().st_mode), 0o600)


class HealthStatusTests(unittest.TestCase):
    MAX_AGE = datetime.timedelta(hours=4)

    def test_recent_success_is_healthy(self) -> None:
        state = State(started_at=T0 - 48 * HOUR, last_success_at=T0 - 3 * HOUR, last_success_file="a.tar.gz")
        ok, message = health_status(state, T0, self.MAX_AGE)
        self.assertTrue(ok)
        self.assertEqual(message, "healthy: last successful backup 3h 0m ago (a.tar.gz)")

    def test_success_exactly_at_the_limit_is_healthy(self) -> None:
        state = State(last_success_at=T0 - self.MAX_AGE, last_success_file="a.tar.gz")
        self.assertTrue(health_status(state, T0, self.MAX_AGE)[0])

    def test_old_success_is_unhealthy_and_names_the_last_error(self) -> None:
        state = State(
            started_at=T0 - 10 * HOUR,
            last_success_at=T0 - 26 * HOUR,
            last_success_file="a.tar.gz",
            last_failure_at=T0 - HOUR,
            last_error="download failed",
        )
        ok, message = health_status(state, T0, self.MAX_AGE)
        self.assertFalse(ok)
        self.assertEqual(
            message,
            "unhealthy: last successful backup 1d 2h 0m ago, more than the allowed 4h 0m; "
            "last failure 1h 0m ago: download failed",
        )

    def test_old_success_stays_unhealthy_after_a_restart(self) -> None:
        state = State(started_at=T0 - HOUR / 2, last_success_at=T0 - 30 * HOUR, last_success_file="a.tar.gz")
        self.assertFalse(health_status(state, T0, self.MAX_AGE)[0])

    def test_start_up_grace_period_without_any_success(self) -> None:
        ok, message = health_status(State(started_at=T0 - HOUR), T0, self.MAX_AGE)
        self.assertTrue(ok)
        self.assertIn("starting", message)
        ok, message = health_status(State(started_at=T0 - 5 * HOUR), T0, self.MAX_AGE)
        self.assertFalse(ok)
        self.assertEqual(message, "unhealthy: no successful backup since the service started 5h 0m ago")

    def test_no_state_at_all_is_unhealthy(self) -> None:
        ok, message = health_status(State(), T0, self.MAX_AGE)
        self.assertFalse(ok)
        self.assertIn("no state recorded yet", message)


class FullBackupHealthTests(unittest.TestCase):
    """HOURLY_BACKUP_FILESTORE=false: database-only successes must not hide failing full backups."""

    MAX_AGE = datetime.timedelta(hours=4)
    FULL_MAX_AGE = datetime.timedelta(hours=36)

    def status(self, state: State) -> tuple[bool, str]:
        return health_status(state, T0, self.MAX_AGE, self.FULL_MAX_AGE)

    def test_fresh_full_and_database_only_backups_are_healthy(self) -> None:
        state = State(
            started_at=T0 - 48 * HOUR,
            last_success_at=T0 - HOUR, last_success_file="db.tar.gz",
            last_full_success_at=T0 - 12 * HOUR, last_full_success_file="full.tar.gz",
        )
        self.assertEqual(
            self.status(state),
            (True, "healthy: last successful backup 1h 0m ago (db.tar.gz); last full backup 12h 0m ago (full.tar.gz)"),
        )

    def test_overdue_full_backup_is_unhealthy_despite_database_only_successes(self) -> None:
        state = State(
            started_at=T0 - 30 * 24 * HOUR,
            last_success_at=T0 - HOUR, last_success_file="db.tar.gz",
            last_full_success_at=T0 - 60 * HOUR, last_full_success_file="full.tar.gz",
            last_failure_at=T0 - 24 * HOUR, last_error="full backup failed at stage download: truncated",
        )
        self.assertEqual(
            self.status(state),
            (False, "unhealthy: last full backup (database + filestore) 2d 12h 0m ago, more than the allowed "
                    "1d 12h 0m; last failure 1d 0h 0m ago: full backup failed at stage download: truncated"),
        )

    def test_first_full_backup_gets_the_start_up_grace_period(self) -> None:
        state = State(started_at=T0 - 20 * HOUR, last_success_at=T0 - HOUR, last_success_file="db.tar.gz")
        self.assertTrue(self.status(state)[0])
        state = State(started_at=T0 - 40 * HOUR, last_success_at=T0 - HOUR, last_success_file="db.tar.gz")
        self.assertEqual(
            self.status(state),
            (False, "unhealthy: no full backup (database + filestore) since the service started 1d 16h 0m ago"),
        )

    def test_without_a_limit_only_the_last_success_counts(self) -> None:
        state = State(last_success_at=T0 - HOUR, last_success_file="db.tar.gz")
        self.assertTrue(health_status(state, T0, self.MAX_AGE)[0])
        self.assertEqual(full_backup_problem(state, T0, self.FULL_MAX_AGE),
                         "no full backup (database + filestore) recorded yet")

    def test_full_backup_problem(self) -> None:
        self.assertIsNone(full_backup_problem(State(last_full_success_at=T0 - self.FULL_MAX_AGE), T0,
                                              self.FULL_MAX_AGE))
        self.assertIsNone(full_backup_problem(State(started_at=T0 - HOUR), T0, self.FULL_MAX_AGE))


class HeartbeatTests(unittest.TestCase):
    TOKEN = "3f2b9c0e-heartbeat-token"

    def setUp(self) -> None:
        self.stub = OdooHTTPStub().start()
        self.addCleanup(self.stub.stop)
        self.path = f"/ping/{self.TOKEN}"
        self.url = self.stub.url + self.path
        self.capture = capture_all_logs(self)
        self.addCleanup(self.assert_url_never_logged)

    def assert_url_never_logged(self) -> None:
        formatter = logging.Formatter()
        for record in self.capture.records:
            text = record.getMessage()
            if record.exc_info:
                text += formatter.formatException(record.exc_info)
            self.assertNotIn(self.TOKEN, text, f"heartbeat URL in log record {record.name}")

    def test_success_sends_one_get(self) -> None:
        self.stub.set_handler(self.path, lambda req: StubResponse(200, b"OK", content_length=True))
        self.assertTrue(send_heartbeat(self.url))
        requests = self.stub.requests_to(self.path)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].method, "GET")
        self.assertEqual(requests[0].headers["user-agent"], f"odoo-backup/{__version__}")
        self.assertTrue(any(r.getMessage() == "Heartbeat ping sent" for r in self.capture.records))

    def test_http_error_is_a_warning(self) -> None:
        self.stub.set_handler(self.path, lambda req: StubResponse(500, b"down", content_length=True))
        with self.assertLogs(LOGGER, logging.WARNING) as logs:
            self.assertFalse(send_heartbeat(self.url))
        self.assertEqual(logs.output, [f"WARNING:{LOGGER}:Heartbeat ping answered HTTP 500"])

    def test_unknown_path_is_a_warning(self) -> None:
        with self.assertLogs(LOGGER, logging.WARNING) as logs:
            self.assertFalse(send_heartbeat(self.url))  # the stub answers 404
        self.assertIn("HTTP 404", logs.output[0])

    def test_connection_refused_is_a_warning(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        with self.assertLogs(LOGGER, logging.WARNING) as logs:
            self.assertFalse(send_heartbeat(f"http://127.0.0.1:{port}{self.path}"))
        self.assertIn("Heartbeat ping failed: ConnectionRefusedError", logs.output[0])

    def test_timeout_is_a_warning(self) -> None:
        self.stub.set_handler(self.path, lambda req: StubResponse(200, b"OK", content_length=True, delay=1.0))
        with self.assertLogs(LOGGER, logging.WARNING) as logs:
            self.assertFalse(send_heartbeat(self.url, timeout=0.2))
        self.assertIn("timed out", logs.output[0])


if __name__ == "__main__":
    unittest.main()
