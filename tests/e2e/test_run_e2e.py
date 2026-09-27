"""Unit tests of the pure parts of the end-to-end driver (no Docker, no network).

The retention expectation of scenario (f) is an independent re-statement of the README rules for
BACKUP_TIME=00:00; these tests pin it to odoo_backup.retention.plan_retention() on many generated
directories, so a wrong expectation cannot hide a retention defect (or report a false one).
"""

import datetime
import io
import json
import pathlib
import random
import tempfile
import unittest
import zipfile

from odoo_backup.retention import RetentionPolicy, parse_backup_name, plan_retention
from tests.e2e import run_e2e
from tests.e2e.run_e2e import DB_NAME, E2EFailure

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
