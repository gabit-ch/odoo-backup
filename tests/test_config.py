import base64
import dataclasses
import hashlib
import os
import pathlib
import struct
import tempfile
import unittest
from datetime import time, timedelta
from unittest import mock
from zoneinfo import ZoneInfo

from odoo_backup.config import (
    ALLOWED_FORMATS,
    DEFAULT_SFTP_CIPHERS,
    Config,
    ConfigError,
    load_config,
    parse_bool,
    parse_host_keys,
    read_secret,
)
from odoo_backup.retention import RetentionPolicy

LOGGER = "odoo_backup.config"
MASTER_PWD = "master-secret-4711"
SFTP_PWD = "sftp-secret-0815"
BASE = {
    "ODOO_URL": "https://odoo.example.com",
    "ODOO_MASTER_PWD": MASTER_PWD,
    "ODOO_DB_NAME": "master",
    "SFTP_HOST": "u123456.your-storagebox.de",
    "SFTP_USER": "u123456",
    "SFTP_PASSWORD": SFTP_PWD,
}
# Published host key fingerprints of the Hetzner Storage Box (port 22 RSA, port 23 ed25519).
HETZNER_22 = "SHA256:EMlfI8GsRIfpVkoW1H2u0zYVpFGKkIMKHFZIRkf2ioI"
HETZNER_23 = "SHA256:XqONwb1S0zuj5A1CDxpOSuD2hnAArV1A3wKY7Z3sdgM"


def env_with(**overrides):
    env = dict(BASE)
    for name, value in overrides.items():
        if value is None:
            env.pop(name, None)
        else:
            env[name] = value
    return env


def load(**overrides) -> Config:
    return load_config(env_with(**overrides))


def ssh_string(data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + data


def public_key(key_type="ssh-ed25519", blob_type=None, seed=b"k") -> str:
    """Base64 of a syntactically valid OpenSSH public key blob."""
    blob = ssh_string((blob_type or key_type).encode()) + ssh_string(hashlib.sha256(seed).digest())
    return base64.b64encode(blob).decode()


def fingerprint(b64_key: str) -> str:
    digest = hashlib.sha256(base64.b64decode(b64_key)).digest()
    return "SHA256:" + base64.b64encode(digest).decode().rstrip("=")


class ConfigTestCase(unittest.TestCase):
    def problems(self, **overrides) -> list[str]:
        with self.assertRaises(ConfigError) as ctx:
            load(**overrides)
        return ctx.exception.problems

    def assertProblem(self, fragment: str, **overrides) -> None:
        problems = self.problems(**overrides)
        self.assertTrue(any(fragment in p for p in problems), f"{fragment!r} not in {problems}")

    def temp_file(self, content: str | bytes, name="secret") -> str:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = pathlib.Path(directory.name, name)
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding="utf-8")
        return str(path)


class DefaultsTest(ConfigTestCase):
    def test_minimal_environment_uses_the_documented_defaults(self):
        config = load()
        temp_root = pathlib.Path(tempfile.gettempdir())
        expected = {
            "odoo_url": "https://odoo.example.com",
            "odoo_master_password": MASTER_PWD,
            "odoo_db_name": "master",
            "backup_format": "zip",
            "backup_time": time(2, 0),
            "backup_every_hour": None,
            "hourly_backup_filestore": True,
            "tz_name": "UTC",
            "tz": ZoneInfo("UTC"),
            "retention": RetentionPolicy(keep_last=0, keep_daily=30, keep_monthly=12, keep_yearly=-1, anchor=time(2)),
            "retention_errors": (),
            "retention_dry_run": False,
            "sftp_host": "u123456.your-storagebox.de",
            "sftp_port": 22,
            "sftp_user": "u123456",
            "sftp_password": SFTP_PWD,
            "sftp_private_key_file": None,
            "sftp_private_key_passphrase": None,
            "sftp_path": "/",
            "db_only_path": "/db-only",
            "sftp_host_keys": (),
            "sftp_ciphers": DEFAULT_SFTP_CIPHERS,
            "sftp_timeout": 60.0,
            "sftp_upload_attempts": 3,
            "sftp_max_request_size": None,
            "odoo_timeout": 30.0,
            "odoo_read_timeout": 14400.0,
            "tmp_dir": temp_root / "odoo-backup",
            "state_dir": temp_root / "odoo-backup-state",
            "max_runtime": timedelta(minutes=480),
            "heartbeat_url": None,
            "healthcheck_max_age": timedelta(hours=36),
            "test_mode": False,
        }
        self.assertEqual({f.name for f in dataclasses.fields(Config)}, set(expected))
        for name, value in expected.items():
            with self.subTest(field=name):
                self.assertEqual(getattr(config, name), value)
        self.assertFalse(config.hourly)
        self.assertEqual(config.interval, timedelta(hours=24))

    def test_hourly_and_interval(self):
        config = load(BACKUP_EVERY_HOUR="2")
        self.assertTrue(config.hourly)
        self.assertEqual(config.interval, timedelta(hours=2))

    def test_config_is_frozen(self):
        with self.assertRaises(dataclasses.FrozenInstanceError):
            load().odoo_db_name = "other"

    def test_reads_os_environ_by_default(self):
        with mock.patch.dict(os.environ, env_with(ODOO_DB_NAME="from-environ"), clear=True):
            self.assertEqual(load_config().odoo_db_name, "from-environ")

    def test_empty_values_count_as_unset(self):
        config = load(ODOO_BACKUP_FORMAT="", BACKUP_TIME=" ", SFTP_PORT="", SFTP_PATH="", TZ="", SFTP_HOST_KEY="")
        self.assertEqual(
            (
                config.backup_format,
                config.backup_time,
                config.sftp_port,
                config.sftp_path,
                config.tz_name,
                config.sftp_host_keys,
            ),
            ("zip", time(2), 22, "/", "UTC", ()),
        )


class ProblemsTest(ConfigTestCase):
    def test_every_problem_is_reported_at_once(self):
        with self.assertRaises(ConfigError) as ctx:
            load_config({"ODOO_BACKUP_FORMAT": "rar", "BACKUP_TIME": "25:00", "SFTP_PORT": "0", "TZ": "Mars/Base"})
        problems = ctx.exception.problems
        for fragment in (
            "ODOO_URL is required",
            "ODOO_MASTER_PWD or ODOO_MASTER_PWD_FILE is required",
            "ODOO_DB_NAME is required",
            "SFTP_HOST is required",
            "SFTP_USER is required",
            "SFTP_PASSWORD, SFTP_PASSWORD_FILE or SFTP_PRIVATE_KEY_FILE is required",
            "ODOO_BACKUP_FORMAT",
            "BACKUP_TIME",
            "SFTP_PORT",
            "TZ",
        ):
            self.assertTrue(any(fragment in p for p in problems), f"{fragment!r} not in {problems}")
        self.assertEqual(len(problems), 10)
        self.assertIsInstance(ctx.exception, ValueError)
        self.assertIn("ODOO_URL is required", str(ctx.exception))

    def test_config_error_accepts_a_single_problem(self):
        self.assertEqual(ConfigError("x").problems, ["x"])

    def test_secrets_never_appear_in_problems_repr_or_logs(self):
        heartbeat = "https://hc-ping.example/secret-token-99"
        key_passphrase = "key-passphrase-77"
        with self.assertLogs(LOGGER, "WARNING") as logs, self.assertRaises(ConfigError) as ctx:
            load(
                ODOO_MASTER_PWD_FILE="/nonexistent/master",
                SFTP_PASSWORD_FILE="/nonexistent/sftp",
                HEARTBEAT_URL=heartbeat,
                HEARTBEAT_URL_FILE="/nonexistent/hb",
                SFTP_PRIVATE_KEY_PASSPHRASE=key_passphrase,
                ODOO_URL="htp://admin:url-secret-5@odoo",
                BACKUP_EVERY_HOUR="5",
                HOURLY_BACKUP_KEEP="x",
                SFTP_HOST="host:2222",
            )
        text = "\n".join([str(ctx.exception), *ctx.exception.problems, *logs.output])
        for secret in (MASTER_PWD, SFTP_PWD, heartbeat, "secret-token-99", key_passphrase, "url-secret-5"):
            self.assertNotIn(secret, text)
        config = load(
            HEARTBEAT_URL=heartbeat,
            SFTP_PRIVATE_KEY_PASSPHRASE=key_passphrase,
            SFTP_PRIVATE_KEY_FILE=self.temp_file("KEY"),
        )
        for secret in (MASTER_PWD, SFTP_PWD, heartbeat, key_passphrase):
            self.assertNotIn(secret, repr(config))
            self.assertNotIn(secret, str(config))


class SecretsTest(ConfigTestCase):
    def test_direct_value(self):
        self.assertEqual(read_secret({"X": " pw with spaces "}, "X"), " pw with spaces ")
        self.assertIsNone(read_secret({}, "X"))
        self.assertIsNone(read_secret({"X": "", "X_FILE": ""}, "X"))

    def test_file_value_loses_only_the_trailing_line_break(self):
        for content, expected in (
            ("pw\n", "pw"),
            ("pw\r\n", "pw"),
            (" pw \n", " pw "),
            ("pw", "pw"),
            ("line1\nline2\n", "line1\nline2"),
        ):
            with self.subTest(content=content):
                self.assertEqual(read_secret({"X_FILE": self.temp_file(content)}, "X"), expected)

    def test_both_set_is_a_problem(self):
        with self.assertRaises(ConfigError) as ctx:
            read_secret({"X": "direct-secret", "X_FILE": self.temp_file("file-secret")}, "X")
        self.assertEqual(ctx.exception.problems, ["X and X_FILE are both set; use only one of them"])

    def test_unreadable_or_empty_files_are_problems(self):
        directory = tempfile.mkdtemp()
        self.addCleanup(os.rmdir, directory)
        for path, fragment in (
            ("/nonexistent/secret", "cannot read"),
            (directory, "cannot read"),
            (self.temp_file(b"\xff\xfe"), "cannot read"),
            (self.temp_file("\n"), "is empty"),
        ):
            with self.subTest(path=path), self.assertRaises(ConfigError) as ctx:
                read_secret({"X_FILE": path}, "X")
            self.assertIn(fragment, ctx.exception.problems[0])
            self.assertTrue(ctx.exception.problems[0].startswith("X_FILE: "))

    def test_file_variants_in_load_config(self):
        config = load(
            ODOO_MASTER_PWD=None,
            ODOO_MASTER_PWD_FILE=self.temp_file("m\n"),
            SFTP_PASSWORD=None,
            SFTP_PASSWORD_FILE=self.temp_file("s\n"),
            HEARTBEAT_URL_FILE=self.temp_file("https://hc.example/ping/abc\n"),
        )
        self.assertEqual(
            (config.odoo_master_password, config.sftp_password, config.heartbeat_url),
            ("m", "s", "https://hc.example/ping/abc"),
        )

    def test_unreadable_secret_file_is_reported_once(self):
        problems = self.problems(ODOO_MASTER_PWD=None, ODOO_MASTER_PWD_FILE="/nonexistent/x")
        self.assertEqual(len(problems), 1)
        self.assertIn("ODOO_MASTER_PWD_FILE: cannot read '/nonexistent/x'", problems[0])


class ParseBoolTest(ConfigTestCase):
    def test_accepted_spellings(self):
        for raw in ("true", "TRUE", "True", "1", "yes", "on", " On "):
            self.assertIs(parse_bool(raw, "X"), True)
        for raw in ("false", "FALSE", "0", "no", "off", "Off"):
            self.assertIs(parse_bool(raw, "X"), False)

    def test_anything_else_is_a_problem(self):
        for raw in ("", "maybe", "2", "tru", "y", "enabled"):
            with self.subTest(raw=raw), self.assertRaises(ConfigError) as ctx:
                parse_bool(raw, "FLAG")
            self.assertIn("FLAG must be one of true/false/1/0/yes/no/on/off", ctx.exception.problems[0])

    def test_boolean_variables(self):
        # TEST_MODE used to be true for ANY non-empty value, including "False".
        self.assertIs(load(TEST_MODE="False").test_mode, False)
        self.assertIs(load(TEST_MODE="yes").test_mode, True)
        self.assertIs(load(RETENTION_DRY_RUN="true").retention_dry_run, True)
        self.assertIs(load(BACKUP_EVERY_HOUR="2", HOURLY_BACKUP_FILESTORE="off").hourly_backup_filestore, False)
        for name in ("TEST_MODE", "RETENTION_DRY_RUN", "HOURLY_BACKUP_FILESTORE"):
            with self.subTest(name=name):
                self.assertProblem(f"{name} must be one of", **{name: "sometimes"})


class ScheduleSettingsTest(ConfigTestCase):
    def test_backup_time_formats(self):
        for raw, expected in (
            ("1:00", time(1)),
            ("01:00", time(1)),
            ("23:59:59", time(23, 59, 59)),
            ("00:00", time(0)),
            ("7:05:09", time(7, 5, 9)),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(load(BACKUP_TIME=raw).backup_time, expected)
        for raw in ("24:00", "1:60", "0100", "01:00:60", "1", "01:00 am", "-1:00", "1:5", "001:00"):
            with self.subTest(raw=raw):
                self.assertProblem("BACKUP_TIME", BACKUP_TIME=raw)

    def test_backup_every_hour(self):
        self.assertIsNone(load(BACKUP_EVERY_HOUR="0").backup_every_hour)
        self.assertIsNone(load(BACKUP_EVERY_HOUR="").backup_every_hour)
        self.assertEqual(load(BACKUP_EVERY_HOUR="2").backup_every_hour, 2)
        self.assertEqual(load(BACKUP_EVERY_HOUR="24").backup_every_hour, 24)
        for raw in ("25", "-1", "x", "2.5", "1_0"):
            with self.subTest(raw=raw):
                self.assertProblem("BACKUP_EVERY_HOUR must be an integer between 0 and 24", BACKUP_EVERY_HOUR=raw)

    def test_interval_not_dividing_24_is_a_warning(self):
        with self.assertLogs(LOGGER, "WARNING") as logs:
            self.assertEqual(load(BACKUP_EVERY_HOUR="5").backup_every_hour, 5)
        self.assertEqual(
            logs.records[0].getMessage(),
            "BACKUP_EVERY_HOUR=5 does not divide 24: 4 backups per day, the gap before the first "
            "backup of the next day is 9 h",
        )
        with self.assertNoLogs(LOGGER, "WARNING"):
            load(BACKUP_EVERY_HOUR="3")

    def test_filestore_setting_without_hourly_mode_is_a_warning(self):
        with self.assertLogs(LOGGER, "WARNING") as logs:
            load(HOURLY_BACKUP_FILESTORE="false")
        self.assertIn("HOURLY_BACKUP_FILESTORE=false has no effect", logs.output[0])

    def test_time_zone(self):
        self.assertEqual(load(TZ="Europe/Zurich").tz, ZoneInfo("Europe/Zurich"))
        config = load(TZ=":Europe/Zurich")
        self.assertEqual((config.tz_name, config.tz), ("Europe/Zurich", ZoneInfo("Europe/Zurich")))
        for raw in ("Europe/Zurch", "Mars/Base", "/etc/localtime", "../etc/passwd", ":"):
            with self.subTest(raw=raw):
                self.assertProblem("TZ: unknown time zone", TZ=raw)


class RetentionSettingsTest(ConfigTestCase):
    def test_daily_mode_ignores_hourly_keep(self):
        policy = load(HOURLY_BACKUP_KEEP="12", BACKUP_TIME="01:00").retention
        self.assertEqual(policy, RetentionPolicy(0, 30, 12, -1, time(1)))

    def test_hourly_mode_uses_hourly_keep(self):
        self.assertEqual(load(BACKUP_EVERY_HOUR="2").retention.keep_last, 4)
        config = load(
            BACKUP_EVERY_HOUR="2",
            BACKUP_TIME="01:00",
            HOURLY_BACKUP_KEEP="12",
            DAILY_BACKUP_KEEP="30",
            MONTHLY_BACKUP_KEEP="12",
            YEARLY_BACKUP_KEEP="-1",
        )
        self.assertEqual(config.retention, RetentionPolicy(12, 30, 12, -1, time(1)))

    def test_invalid_values_disable_retention_without_failing(self):
        for name, raw in (
            ("HOURLY_BACKUP_KEEP", "-1"),
            ("DAILY_BACKUP_KEEP", "x"),
            ("MONTHLY_BACKUP_KEEP", "1.5"),
            ("YEARLY_BACKUP_KEEP", "-2"),
        ):
            with self.subTest(name=name), self.assertLogs(LOGGER, "WARNING") as logs:
                config = load(**{name: raw})
            self.assertIsNone(config.retention)
            self.assertEqual(len(config.retention_errors), 1)
            self.assertIn(name, config.retention_errors[0])
            self.assertIn("Retention disabled", logs.output[0])
        with self.assertLogs(LOGGER, "WARNING") as logs:
            config = load(DAILY_BACKUP_KEEP="-1", MONTHLY_BACKUP_KEEP="many")
        self.assertEqual(len(config.retention_errors), 2)
        self.assertEqual(len(logs.records), 2)

    def test_all_effective_values_zero_disable_retention(self):
        with self.assertLogs(LOGGER, "WARNING"):
            config = load(
                HOURLY_BACKUP_KEEP="5", DAILY_BACKUP_KEEP="0", MONTHLY_BACKUP_KEEP="0", YEARLY_BACKUP_KEEP="0"
            )
        self.assertIsNone(config.retention)
        self.assertIn("all effective retention values are 0", config.retention_errors[0])
        hourly = load(
            BACKUP_EVERY_HOUR="2",
            HOURLY_BACKUP_KEEP="5",
            DAILY_BACKUP_KEEP="0",
            MONTHLY_BACKUP_KEEP="0",
            YEARLY_BACKUP_KEEP="0",
        )
        self.assertEqual(hourly.retention, RetentionPolicy(5, 0, 0, 0, time(2)))

    def test_yearly_values(self):
        self.assertEqual(load(YEARLY_BACKUP_KEEP="-1").retention.keep_yearly, -1)
        self.assertEqual(load(YEARLY_BACKUP_KEEP="0").retention.keep_yearly, 0)
        self.assertEqual(load(YEARLY_BACKUP_KEEP="3").retention.keep_yearly, 3)


class OdooSettingsTest(ConfigTestCase):
    def test_url(self):
        self.assertEqual(load(ODOO_URL="https://odoo.example.com/").odoo_url, "https://odoo.example.com")
        self.assertEqual(load(ODOO_URL="http://odoo:8069").odoo_url, "http://odoo:8069")
        for raw in ("odoo.example.com", "ftp://odoo.example.com", "https://", "http://[::1"):
            with self.subTest(raw=raw):
                self.assertProblem("ODOO_URL must be an http:// or https:// URL", ODOO_URL=raw)

    def test_database_name(self):
        for name in ("master", "tz-prod.v2_1", "0db"):
            self.assertEqual(load(ODOO_DB_NAME=name).odoo_db_name, name)
        for name in ("a", "-master", ".master", "mas ter", "master/x", "mästér", "master;drop"):
            with self.subTest(name=name):
                self.assertProblem("is not a valid Odoo database name", ODOO_DB_NAME=name)

    def test_backup_format(self):
        self.assertEqual(load(ODOO_BACKUP_FORMAT="TAR.ZST").backup_format, "tar.zst")
        for fmt in ALLOWED_FORMATS:
            self.assertEqual(load(ODOO_BACKUP_FORMAT=fmt).backup_format, fmt)
        self.assertProblem("ODOO_BACKUP_FORMAT must be one of", ODOO_BACKUP_FORMAT="rar")

    def test_timeouts(self):
        config = load(ODOO_TIMEOUT="2.5", ODOO_READ_TIMEOUT="600", SFTP_TIMEOUT=".5")
        self.assertEqual((config.odoo_timeout, config.odoo_read_timeout, config.sftp_timeout), (2.5, 600.0, 0.5))
        for name in ("ODOO_TIMEOUT", "ODOO_READ_TIMEOUT", "SFTP_TIMEOUT"):
            for raw in ("0", "-1", "abc", "inf", "nan", "1e3"):
                with self.subTest(name=name, raw=raw):
                    self.assertProblem(f"{name} must be a number > 0", **{name: raw})


class SftpSettingsTest(ConfigTestCase):
    def test_port(self):
        self.assertEqual(load(SFTP_PORT="23").sftp_port, 23)
        for raw in ("0", "65536", "x", "22.0"):
            with self.subTest(raw=raw):
                self.assertProblem("SFTP_PORT must be an integer between 1 and 65535", SFTP_PORT=raw)

    def test_legacy_host_with_port(self):
        with self.assertLogs(LOGGER, "WARNING") as logs:
            config = load(SFTP_HOST="backup.example.com:23")
        self.assertEqual((config.sftp_host, config.sftp_port), ("backup.example.com", 23))
        self.assertIn("deprecated", logs.output[0])
        with self.assertLogs(LOGGER, "WARNING"):
            self.assertEqual(load(SFTP_HOST="backup.example.com:23", SFTP_PORT="23").sftp_port, 23)
        self.assertProblem(
            "SFTP_HOST contains port 23 but SFTP_PORT is 22", SFTP_HOST="backup.example.com:23", SFTP_PORT="22"
        )
        for raw in ("backup.example.com:ssh", ":22", "backup.example.com:0", "backup.example.com:99999"):
            with self.subTest(raw=raw):
                self.assertProblem("is not a host name", SFTP_HOST=raw)

    def test_host(self):
        self.assertEqual(load(SFTP_HOST="2001:db8::1").sftp_host, "2001:db8::1")  # IPv6 is not split
        for raw in ("user@backup.example.com", "backup example.com", "sftp://backup.example.com"):
            with self.subTest(raw=raw):
                self.assertProblem("is not a host name", SFTP_HOST=raw)

    def test_private_key_file(self):
        key_file = self.temp_file("-----BEGIN OPENSSH PRIVATE KEY-----\n")
        config = load(SFTP_PASSWORD=None, SFTP_PRIVATE_KEY_FILE=key_file, SFTP_PRIVATE_KEY_PASSPHRASE="pp")
        self.assertEqual(
            (config.sftp_password, config.sftp_private_key_file, config.sftp_private_key_passphrase),
            (None, key_file, "pp"),
        )
        self.assertProblem(
            "SFTP_PRIVATE_KEY_FILE: cannot read '/nonexistent/id_ed25519'",
            SFTP_PRIVATE_KEY_FILE="/nonexistent/id_ed25519",
        )
        self.assertProblem(
            "SFTP_PRIVATE_KEY_PASSPHRASE is set but SFTP_PRIVATE_KEY_FILE is not", SFTP_PRIVATE_KEY_PASSPHRASE="pp"
        )
        self.assertProblem("SFTP_PASSWORD, SFTP_PASSWORD_FILE or SFTP_PRIVATE_KEY_FILE is required", SFTP_PASSWORD=None)

    def test_paths(self):
        config = load(SFTP_PATH="/backups/odoo")
        self.assertEqual((config.sftp_path, config.db_only_path), ("/backups/odoo", "/backups/odoo/db-only"))
        self.assertEqual(load(SFTP_PATH="backups").db_only_path, "backups/db-only")
        self.assertEqual(load(DB_ONLY_BACKUP_PATH="/hourly").db_only_path, "/hourly")
        for sftp_path, db_only in (("/backups/odoo/", "/backups/odoo"), ("/", "/"), ("backups", "./backups")):
            with self.subTest(sftp_path=sftp_path, db_only=db_only):
                self.assertProblem(
                    "DB_ONLY_BACKUP_PATH must differ from SFTP_PATH", SFTP_PATH=sftp_path, DB_ONLY_BACKUP_PATH=db_only
                )

    def test_ciphers(self):
        config = load(SFTP_CIPHERS=" aes256-gcm@openssh.com , aes128-ctr,,aes128-ctr ")
        self.assertEqual(config.sftp_ciphers, ("aes256-gcm@openssh.com", "aes128-ctr"))
        for raw in (",", "aes128-ctr;rm -rf", "aes 128"):
            with self.subTest(raw=raw):
                self.assertProblem("SFTP_CIPHERS must be a comma separated list", SFTP_CIPHERS=raw)

    def test_upload_attempts_and_request_size(self):
        config = load(SFTP_UPLOAD_ATTEMPTS="10", SFTP_MAX_REQUEST_SIZE="261120")
        self.assertEqual((config.sftp_upload_attempts, config.sftp_max_request_size), (10, 261120))
        self.assertEqual(load(SFTP_MAX_REQUEST_SIZE="4096").sftp_max_request_size, 4096)
        for raw in ("0", "11", "x"):
            self.assertProblem("SFTP_UPLOAD_ATTEMPTS must be an integer between 1 and 10", SFTP_UPLOAD_ATTEMPTS=raw)
        for raw in ("4095", "262144", "auto"):
            self.assertProblem(
                "SFTP_MAX_REQUEST_SIZE must be an integer between 4096 and 261120", SFTP_MAX_REQUEST_SIZE=raw
            )


class HostKeyTest(ConfigTestCase):
    ED25519 = public_key("ssh-ed25519", seed=b"a")
    RSA = public_key("ssh-rsa", seed=b"b")
    ECDSA = public_key("ecdsa-sha2-nistp256", seed=b"c")

    def test_hetzner_fingerprints(self):
        self.assertEqual(load(SFTP_HOST_KEY=f"{HETZNER_22},{HETZNER_23}").sftp_host_keys, (HETZNER_22, HETZNER_23))

    def test_fingerprint_normalisation(self):
        fp = fingerprint(self.ED25519)
        self.assertEqual(parse_host_keys(fp + "="), (fp,))
        self.assertEqual(parse_host_keys("sha256:" + fp[7:]), (fp,))

    def test_public_key_forms(self):
        expected = f"ssh-ed25519 {self.ED25519}"
        for raw in (
            expected,
            f"ssh-ed25519 {self.ED25519} root@backup",
            f"[backup.example.com]:23 ssh-ed25519 {self.ED25519}",
            f"backup.example.com,1.2.3.4 ssh-ed25519 {self.ED25519}",
            f"|1|c2FsdA==|aGFzaA== ssh-ed25519 {self.ED25519} comment with spaces",
        ):
            with self.subTest(raw=raw):
                self.assertEqual(parse_host_keys(raw), (expected,))

    def test_separators_comments_and_duplicates(self):
        fp = fingerprint(self.RSA)
        raw = (
            f"# ssh-keyscan -p 23 backup.example.com\n"
            f"backup.example.com ssh-rsa {self.RSA}\r\n"
            f"{fp}, ecdsa-sha2-nistp256 {self.ECDSA} ,\n\n"
            f"ssh-rsa {self.RSA} again"
        )
        self.assertEqual(
            load(SFTP_HOST_KEY=raw).sftp_host_keys, (f"ssh-rsa {self.RSA}", fp, f"ecdsa-sha2-nistp256 {self.ECDSA}")
        )

    def test_invalid_entries_are_all_reported(self):
        cases = {
            "SHA256:tooShort": "is not a valid SHA256 fingerprint",
            "SHA256:" + "!" * 43: "is not a valid SHA256 fingerprint",
            "MD5:16:27:ac:a5:76:28:2d:36:63:1b:56:4d:eb:df:a6:48": "MD5 fingerprints are not supported",
            f"ssh-dss {self.ED25519}": "neither a SHA256 fingerprint nor a public key",
            f"@cert-authority * ssh-ed25519 {self.ED25519}": "known_hosts markers",
            f"ssh-rsa {self.ED25519}": "does not contain a ssh-rsa key",
            f"ecdsa-sha2-nistp384 {self.ECDSA}": "does not contain a ecdsa-sha2-nistp384 key",  # same length
            "ssh-ed25519 not*base64": "is not valid base64",
            "ssh-ed25519 AAAA": "does not contain a ssh-ed25519 key",
            "garbage": "neither a SHA256 fingerprint nor a public key",
            f"ssh-ed25519 {self.ED25519} me@host,laptop": "'laptop' is neither",
        }
        for raw, fragment in cases.items():
            with self.subTest(raw=raw), self.assertRaises(ConfigError) as ctx:
                parse_host_keys(raw)
            self.assertTrue(any(fragment in p for p in ctx.exception.problems), ctx.exception.problems)
        problems = self.problems(SFTP_HOST_KEY=f"garbage\nSHA256:short,{HETZNER_23}")
        self.assertEqual(len(problems), 2)
        self.assertTrue(all(p.startswith("SFTP_HOST_KEY: ") for p in problems))

    def test_no_entry_at_all(self):
        self.assertProblem("SFTP_HOST_KEY is set but contains no host key", SFTP_HOST_KEY=", ,\n# comment")


class LocalSettingsTest(ConfigTestCase):
    def test_local_directories(self):
        base = tempfile.mkdtemp()
        self.addCleanup(os.rmdir, base)
        config = load(BACKUP_TMP_DIR=f"{base}/tmp/", BACKUP_STATE_DIR=f"{base}/state")
        self.assertEqual((config.tmp_dir, config.state_dir), (pathlib.Path(base, "tmp"), pathlib.Path(base, "state")))
        self.assertProblem("BACKUP_TMP_DIR must be an absolute path", BACKUP_TMP_DIR="relative/tmp")
        self.assertProblem("BACKUP_STATE_DIR must be an absolute path", BACKUP_STATE_DIR="state")
        for tmp_dir in ("/", tempfile.gettempdir(), tempfile.gettempdir() + "/"):
            with self.subTest(tmp_dir=tmp_dir):
                self.assertProblem("BACKUP_TMP_DIR must be a dedicated directory", BACKUP_TMP_DIR=tmp_dir)
        for state_dir in (f"{base}/tmp", f"{base}/tmp/state"):
            with self.subTest(state_dir=state_dir):
                self.assertProblem(
                    "BACKUP_STATE_DIR must not be inside BACKUP_TMP_DIR",
                    BACKUP_TMP_DIR=f"{base}/tmp",
                    BACKUP_STATE_DIR=state_dir,
                )

    def test_max_runtime(self):
        self.assertEqual(load(BACKUP_MAX_RUNTIME_MINUTES="10").max_runtime, timedelta(minutes=10))
        self.assertProblem("BACKUP_MAX_RUNTIME_MINUTES must be an integer >= 10", BACKUP_MAX_RUNTIME_MINUTES="9")

    def test_heartbeat_url(self):
        self.assertEqual(load(HEARTBEAT_URL="https://hc.example/ping/x").heartbeat_url, "https://hc.example/ping/x")
        self.assertEqual(load(HEARTBEAT_URL=" https://hc.example/ping/x\n").heartbeat_url, "https://hc.example/ping/x")
        for raw in ("hc.example/ping/x", "ftp://hc.example/x", "https://"):
            with self.subTest(raw=raw):
                problems = self.problems(HEARTBEAT_URL=raw)
                self.assertEqual(problems, ["HEARTBEAT_URL must be an http:// or https:// URL"])

    def test_healthcheck_max_age(self):
        for every, hours in ((None, 36), ("1", 3), ("2", 4), ("6", 9), ("8", 12), ("24", 36)):
            with self.subTest(every=every):
                self.assertEqual(load(BACKUP_EVERY_HOUR=every).healthcheck_max_age, timedelta(hours=hours))
        self.assertEqual(load(HEALTHCHECK_MAX_AGE_HOURS="1.5").healthcheck_max_age, timedelta(hours=1.5))
        for raw in ("0", "-2", "x"):
            self.assertProblem("HEALTHCHECK_MAX_AGE_HOURS must be a number > 0", HEALTHCHECK_MAX_AGE_HOURS=raw)

    def test_full_backup_max_age_only_with_database_only_slots(self):
        self.assertIsNone(load().full_backup_max_age)
        self.assertIsNone(load(BACKUP_EVERY_HOUR="2").full_backup_max_age)
        with self.assertLogs("odoo_backup.config", "WARNING"):  # "... has no effect without BACKUP_EVERY_HOUR"
            self.assertIsNone(load(HOURLY_BACKUP_FILESTORE="false").full_backup_max_age)  # daily mode
        db_only = {"BACKUP_EVERY_HOUR": "2", "HOURLY_BACKUP_FILESTORE": "false"}
        self.assertEqual(load(**db_only).full_backup_max_age, timedelta(hours=36))
        self.assertEqual(load(**db_only, HEALTHCHECK_MAX_AGE_HOURS="6").full_backup_max_age, timedelta(hours=36))
        self.assertEqual(load(**db_only, HEALTHCHECK_MAX_AGE_HOURS="48").full_backup_max_age, timedelta(hours=48))


if __name__ == "__main__":
    unittest.main()
