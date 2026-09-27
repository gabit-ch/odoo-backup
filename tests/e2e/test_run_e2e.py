"""Unit tests of the end-to-end driver without Docker (only 127.0.0.1 and local subprocesses).

The retention expectation of scenario (f) is an independent re-statement of the README rules for
BACKUP_TIME=00:00; these tests pin it to odoo_backup.retention.plan_retention() on many generated
directories, so a wrong expectation cannot hide a retention defect (or report a false one). The
host key refusal of scenario (e) is checked against the service's own mismatch message.
"""

import contextlib
import datetime
import io
import json
import os
import pathlib
import random
import signal
import socket
import sys
import tempfile
import time
import unittest
import zipfile

from odoo_backup.retention import RetentionPolicy, parse_backup_name, plan_retention
from odoo_backup.sftp import HostKeyMismatchError, SFTPConnection, fingerprint
from tests.e2e import run_e2e
from tests.e2e.run_e2e import DB_NAME, DeadlineExceeded, E2EFailure
from tests.sftp_stub import generate_ed25519_key

MIDNIGHT = datetime.time(0, 0)


def plan(names, now, *, daily, monthly, yearly, protect=()):
    policy = RetentionPolicy(0, daily, monthly, yearly, MIDNIGHT)
    result = plan_retention(names, DB_NAME, policy, now, protect=protect)
    return set(result.keep), {backup.name for backup in result.delete}


class FingerprintTest(unittest.TestCase):
    def test_matches_ssh_keygen(self):
        # ssh-keygen -l -E sha256 of this public key printed the fingerprint below.
        line = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDFXfKpNV0rle+HYO1G/urNDhzfIcHvfBLJd4SWRqEa1 vector\n"
        self.assertEqual(run_e2e.ssh_fingerprint(line), "SHA256:1GyFYnKDXMSw+V/aK33Gda18lB2/QQ7FJ3RMG8+PSlQ")


class BackupNameTest(unittest.TestCase):
    def test_names_follow_the_service_contract(self):
        ts = datetime.datetime(2026, 9, 27, 1, 2, 3)
        for kwargs in ({}, {"serie": "17.0", "fmt": "tar.gz"}, {"serie": "saas~18.3", "fmt": "tar.zst"}):
            name = run_e2e.backup_name("my-db.x", ts, **kwargs)
            ours, theirs = run_e2e.parse_backup_name(name), parse_backup_name(name)
            self.assertIsNotNone(ours)
            self.assertIsNotNone(theirs)
            self.assertEqual((ours.db, ours.ts, ours.partial), (theirs.db, theirs.ts, theirs.partial))
            self.assertTrue(run_e2e.parse_backup_name(name + ".upload").partial)

    def test_rejects_what_the_service_ignores(self):
        for name in ("notes-20250101-010000.txt", "odoo19.0-e2e-20261301-010000.zip", "odoo19.0-e2e-x.zip.bak"):
            self.assertIsNone(run_e2e.parse_backup_name(name))
            self.assertIsNone(parse_backup_name(name))


class RetentionExpectationTest(unittest.TestCase):
    def assert_same_as_service(self, names, now, **kwargs):
        protect = kwargs.pop("protect", ())
        expected = run_e2e.expected_retention(
            names,
            DB_NAME,
            keep_daily=kwargs["daily"],
            keep_monthly=kwargs["monthly"],
            keep_yearly=kwargs["yearly"],
            protect=protect,
        )
        self.assertEqual(expected, plan(names, now, protect=protect, **kwargs))

    def test_matches_plan_retention_on_random_directories(self):
        rng = random.Random(2417)
        for _ in range(300):
            now = datetime.datetime(2026, 1, 1) + datetime.timedelta(minutes=rng.randrange(0, 2 * 365 * 24 * 60))
            names = set()
            for _ in range(rng.randrange(0, 60)):
                ts = now - datetime.timedelta(minutes=rng.randrange(0, 900 * 24 * 60))
                ts = ts.replace(second=rng.choice((0, 0, 30)))
                serie, fmt = rng.choice((("19.0", "zip"), ("17.0", "zip"), ("19.0", "tar.zst")))
                db = rng.choice((DB_NAME, DB_NAME, DB_NAME, "otherdb"))
                names.add(run_e2e.backup_name(db, ts, serie=serie, fmt=fmt) + rng.choice(("", "", "", ".upload")))
            names |= {"notes-20250101-010000.txt"}
            protect = {rng.choice(sorted(names))} if names and rng.random() < 0.5 else set()
            policy = {"daily": rng.randrange(0, 8), "monthly": rng.randrange(0, 4), "yearly": rng.choice((-1, 0, 1, 2))}
            if not any(policy.values()):
                policy["daily"] = 1
            with self.subTest(now=now, names=len(names), **policy):
                self.assert_same_as_service(names, now, protect=protect, **policy)

    def test_same_timestamp_in_two_formats(self):
        ts = datetime.datetime(2026, 9, 1, 1, 0)
        names = {run_e2e.backup_name(DB_NAME, ts), run_e2e.backup_name(DB_NAME, ts, serie="17.0", fmt="tar.gz")}
        names.add(run_e2e.backup_name(DB_NAME, ts + datetime.timedelta(days=40)))
        self.assert_same_as_service(names, ts + datetime.timedelta(days=41), daily=0, monthly=2, yearly=0)

    def test_scenario_f_on_every_day_of_two_years(self):
        # The seed, the dry-run backup and the real backup, for any day the CI might run on.
        start = datetime.datetime(2026, 1, 1, 0, 0, 5)
        for day in range(0, 2 * 366, 3):
            now = start + datetime.timedelta(days=day, hours=day % 24)
            seed = run_e2e.retention_seed(now)
            first = run_e2e.backup_name(DB_NAME, now)
            second = run_e2e.backup_name(DB_NAME, now + datetime.timedelta(seconds=40))
            listing = {*seed.all, first, second}
            with self.subTest(now=now):
                self.assert_same_as_service(listing, now, daily=5, monthly=2, yearly=1, protect={second})
                kept, deleted = run_e2e.expected_retention(
                    listing, DB_NAME, keep_daily=5, keep_monthly=2, keep_yearly=1, protect={second}
                )
                self.assertGreaterEqual(len(deleted), 40)
                self.assertLessEqual({first, second}, kept)
                self.assertFalse(deleted & set(seed.untouchable))
                self.assertNotIn(seed.stale_partial, kept | deleted)

    def test_seed_shape(self):
        seed = run_e2e.retention_seed(datetime.datetime(2026, 9, 27, 14, 0))
        self.assertEqual(len(seed.own), 50)
        dates = sorted(parse_backup_name(name).ts for name in seed.own)
        self.assertEqual((dates[0].date(), dates[-1].date()), (datetime.date(2026, 6, 20), datetime.date(2026, 9, 26)))
        self.assertIn(run_e2e.FOREIGN_FILE, seed.untouchable)
        self.assertTrue(any(name.startswith("odoo17.0-") and name.endswith(".tar.gz") for name in seed.own))
        self.assertTrue(parse_backup_name(seed.stale_partial).partial)


class MultipartTest(unittest.TestCase):
    def test_round_trip(self):
        data = bytes(range(256)) * 10 + b"\r\n--not-a-boundary\r\n"
        content_type, body = run_e2e.multipart_form(
            {"master_pwd": "s3cret", "name": "e2e_restored"}, {"backup_file": ("x.zip", data)}
        )
        self.assertTrue(content_type.startswith("multipart/form-data; boundary="))
        parts = run_e2e.parse_multipart(content_type, body)
        self.assertEqual(parts, {"master_pwd": b"s3cret", "name": b"e2e_restored", "backup_file": data})


class CheckLinesTest(unittest.TestCase):
    def test_parses_ok_and_fail_lines(self):
        stdout = "OK config: database 'e2e'\nFAIL master-password: Odoo rejected it\n\nFAIL target-dir: not checked\n"
        self.assertEqual(
            run_e2e.parse_check_lines(stdout),
            {
                "config": (True, "database 'e2e'"),
                "master-password": (False, "Odoo rejected it"),
                "target-dir": (False, "not checked"),
            },
        )

    def test_rejects_other_lines(self):
        with self.assertRaises(E2EFailure):
            run_e2e.parse_check_lines("OK config: fine\nTraceback (most recent call last):\n")


class HostKeyRefusalTest(unittest.TestCase):
    def setUp(self):
        self.presented, self.pinned = generate_ed25519_key(), generate_ed25519_key()
        # The service's own message for a server key that matches no SFTP_HOST_KEY entry, so the
        # expectation of scenario (e) cannot drift from what the image prints.
        connection = SFTPConnection("sftp", 22, "e2e", password="unused", host_keys=(fingerprint(self.pinned),))
        with self.assertRaises(HostKeyMismatchError) as caught:
            connection._verify_host_key(self.presented)
        self.message = str(caught.exception)

    def test_driver_fingerprint_is_the_services(self):
        line = f"{self.presented.get_name()} {self.presented.get_base64()} comment"
        self.assertEqual(run_e2e.ssh_fingerprint(line), fingerprint(self.presented))

    def test_accepts_the_refusal_in_a_check_line_and_in_a_log(self):
        presented = fingerprint(self.presented)
        run_e2e.expect_host_key_refusal(self.message, presented, "--check")
        log = f"2026-09-27 ERROR backup failed: run_id=x kind=full stage=sftp-preflight error={self.message} file=-"
        run_e2e.expect_host_key_refusal(log, presented, "--once")

    def test_rejects_other_failures_and_other_keys(self):
        presented = fingerprint(self.presented)
        cases = (
            (self.message, fingerprint(self.pinned)),  # the refusal names another presented key
            ("SFTP authentication failed for e2e@sftp:22: Authentication failed.", presented),
            ("SSH handshake with sftp:22 failed or timed out: timed out", presented),
            ("", presented),
        )
        for text, key in cases:
            with self.subTest(text=text), self.assertRaises(E2EFailure):
                run_e2e.expect_host_key_refusal(text, key, "--once")


class DeadlineTest(unittest.TestCase):
    def test_interrupts_a_blocking_call_and_restores_the_signal_state(self):
        before = signal.getsignal(signal.SIGALRM)
        started = time.monotonic()
        with self.assertRaises(DeadlineExceeded), run_e2e.deadline(0.2):
            time.sleep(10)
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))
        self.assertIs(signal.getsignal(signal.SIGALRM), before)

    def test_zero_sets_no_timer(self):
        with run_e2e.deadline(0):
            self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))

    def test_no_timer_is_left_after_a_run_within_the_deadline(self):
        with run_e2e.deadline(30):
            self.assertGreater(signal.getitimer(signal.ITIMER_REAL)[0], 0)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))

    def test_parse_args(self):
        self.assertEqual(run_e2e.parse_args(["--image", "x"]).deadline, run_e2e.DEFAULT_DEADLINE)
        self.assertEqual(run_e2e.parse_args(["--image", "x", "--deadline", "0"]).deadline, 0)
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            run_e2e.parse_args(["--image", "x", "--deadline", "-1"])


class ShellTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = pathlib.Path(tmp.name)
        self.shell = run_e2e.Shell(self.dir, {}, ("s3cret-value",))

    def log(self):
        return (self.dir / "commands.log").read_text()

    def test_logs_command_exit_and_output_redacted(self):
        result = self.shell.run([sys.executable, "-c", "print('out s3cret-value')"])
        self.assertEqual((result.code, result.stdout), (0, "out s3cret-value\n"))
        log = self.log()
        self.assertIn("### [1] ", log)
        self.assertIn("### [1] exit 0 after", log)
        self.assertIn("out ***", log)
        self.assertNotIn("s3cret-value", log)

    def test_a_command_interrupted_by_the_deadline_is_logged_and_killed(self):
        pid_file = self.dir / "child.pid"
        code = f"import os, pathlib, time; pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid())); time.sleep(60)"
        started = time.monotonic()
        with self.assertRaises(DeadlineExceeded), run_e2e.deadline(1.5):
            self.shell.run([sys.executable, "-c", code + "  # s3cret-value"])
        self.assertLess(time.monotonic() - started, 20)
        log = self.log()
        self.assertIn("time.sleep(60)", log)  # the command is in the log although it never finished
        self.assertIn("### [1] interrupted after", log)
        self.assertIn("DeadlineExceeded: the end-to-end run exceeded its deadline of 1.5 s", log)
        self.assertNotIn("s3cret-value", log)
        with self.assertRaises(ProcessLookupError):  # subprocess.run killed and reaped the child
            os.kill(int(pid_file.read_text()), 0)


class OdooAPITimeoutTest(unittest.TestCase):
    """An Odoo that accepts connections but never answers must not block the run."""

    def setUp(self):
        self.server = socket.create_server(("127.0.0.1", 0))  # listens, never accepts or answers
        self.addCleanup(self.server.close)
        self.api = run_e2e.OdooAPI(self.server.getsockname()[1], timeout=0.3)

    # The outer deadline turns a missing timeout into a test error instead of a hang.

    def test_xmlrpc_times_out(self):
        with run_e2e.deadline(10):
            with self.assertRaises(TimeoutError):
                self.api.authenticate(DB_NAME, "admin", "unused")
            with self.assertRaises(TimeoutError):
                self.api.execute(DB_NAME, 2, "unused", "res.users", "read", [2])

    def test_http_times_out(self):
        with run_e2e.deadline(10), self.assertRaises(TimeoutError):
            self.api.jsonrpc("/web/webclient/version_info")

    def test_wait_until_ready_does_not_retry_past_the_deadline(self):
        # wait_until_ready() retries on errors, E2EFailure included, and gives up after 8 s with
        # an E2EFailure; the deadline must end it after 1 s instead. The request outlasts the
        # deadline, so the deadline fires inside the retried call (not in the pause between).
        api = run_e2e.OdooAPI(self.server.getsockname()[1], timeout=5)
        with self.assertRaises(DeadlineExceeded), run_e2e.deadline(1.0):
            api.wait_until_ready(8)


class FakeShell:
    """Records commands; ``answers`` maps the command's first two words after docker to a Result."""

    def __init__(self, answers):
        self.answers = answers
        self.calls = []

    def run(self, args, *, env=None, timeout=None):
        self.calls.append((list(args), env, timeout))
        return self.answers.get(tuple(args[1:3]), run_e2e.Result(0, "", ""))

    def redact(self, text):
        return text.replace("s3cret", "***")


class BackupContainerTest(unittest.TestCase):
    def test_backup_runs_carry_the_label_and_pass_values_by_environment(self):
        shell = FakeShell({})
        run_e2e.BackupImage(shell, "odoo-backup:ci", {"SFTP_PASSWORD": "s3cret"}).run("--check", TZ="UTC")
        ((command, env, _timeout),) = shell.calls
        self.assertEqual(command[command.index("--label") + 1], run_e2e.BACKUP_LABEL)
        self.assertEqual(command[-4:], ["odoo-backup:ci", "python", "backup.py", "--check"])
        self.assertNotIn("s3cret", " ".join(command))
        self.assertEqual((env["SFTP_PASSWORD"], env["TZ"]), ("s3cret", "UTC"))

    def test_leftover_containers_are_logged_redacted_and_removed(self):
        ids = ["a" * 64, "b" * 64]
        shell = FakeShell(
            {
                ("ps", "--all"): run_e2e.Result(0, "\n".join(ids) + "\n", ""),
                ("logs", "--timestamps"): run_e2e.Result(0, "started\n", "hung with s3cret\n"),
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(run_e2e.remove_backup_containers(shell, pathlib.Path(tmp)), 2)
            for container in ids:
                log = (pathlib.Path(tmp) / f"backup-container-{container[:12]}.log").read_text()
                self.assertEqual(log, "started\nhung with ***\n")
        commands = [call[0] for call in shell.calls]
        self.assertIn(f"label={run_e2e.BACKUP_LABEL}", commands[0])
        self.assertEqual([c for c in commands if c[1] == "rm"], [["docker", "rm", "--force", c] for c in ids])
        self.assertTrue(all(call[2] == run_e2e.CLEANUP_TIMEOUT for call in shell.calls))

    def test_nothing_to_remove_when_docker_ps_fails(self):
        shell = FakeShell({("ps", "--all"): run_e2e.Result(1, "", "Cannot connect to the Docker daemon")})
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(run_e2e.remove_backup_containers(shell, pathlib.Path(tmp)), 0)
        self.assertEqual(len(shell.calls), 1)


class ZipAndHelpersTest(unittest.TestCase):
    def test_summarize_zip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "backup.zip"
            with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("dump.sql", "-- dump")
                archive.writestr("manifest.json", json.dumps({"db_name": DB_NAME}))
                archive.writestr("filestore/ab/abcdef", b"x" * 100)
            summary = run_e2e.summarize_zip(path)
            self.assertEqual(summary.manifest, {"db_name": DB_NAME})
            self.assertEqual(summary.filestore, ("filestore/ab/abcdef",))

    def test_summarize_zip_detects_corruption(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr("dump.sql", b"A" * 1000)
        raw = bytearray(buffer.getvalue())
        raw[raw.index(b"AAAA") + 10] ^= 0xFF
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "broken.zip"
            path.write_bytes(bytes(raw))
            with self.assertRaises(E2EFailure):
                run_e2e.summarize_zip(path)

    def test_redact_and_secrets(self):
        secrets = run_e2e.generate_secrets()
        values = secrets.values()
        self.assertEqual(len(set(values)), 5)
        self.assertTrue(all(len(value) >= 32 and ":" not in value for value in values))
        text = f"user:{secrets.sftp_password}:1001 and {secrets.master_password}"
        self.assertEqual(run_e2e.redact(text, values), "user:***:1001 and ***")

    def test_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "a").mkdir()
            (root / "a" / "f.zip").write_bytes(b"123")
            (root / "g").write_bytes(b"")
            self.assertEqual(run_e2e.snapshot(root), {"a/f.zip": 3, "g": 0})


if __name__ == "__main__":
    unittest.main()
