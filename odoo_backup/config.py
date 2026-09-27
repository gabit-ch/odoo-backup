"""Environment configuration of the backup service.

load_config() validates every variable and collects ALL problems before it fails, so an
operator sees the complete list at once instead of one error per container restart. An
empty variable counts as unset. Secrets can be passed directly or through ``NAME_FILE``
(Docker secrets); they are excluded from repr() and never quoted in problem messages.

Invalid retention values are the one exception to failing fast: backups are more important
than cleanup, so they only disable the deletion of old backups (``retention`` is None and
``retention_errors`` explains why).
"""

import base64
import binascii
import datetime
import logging
import os
import pathlib
import posixpath
import re
import struct
import tempfile
import zoneinfo
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from .retention import RetentionPolicy

logger = logging.getLogger(__name__)

ALLOWED_FORMATS = ("zip", "dump", "tar", "tar.gz", "tar.bz2", "tar.xz", "tar.zst")

# Odoo's own rule (odoo/addons/web/controllers/database.py, DBNAME_PATTERN).
DB_NAME_PATTERN = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]+", re.ASCII)

DEFAULT_SFTP_CIPHERS = (
    "aes128-gcm@openssh.com",
    "aes256-gcm@openssh.com",
    "aes128-ctr",
    "aes256-ctr",
    "aes192-ctr",
)
# Host key types paramiko 5 can verify.
HOST_KEY_TYPES = frozenset({
    "ssh-ed25519",
    "ssh-rsa",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
})
MIN_SFTP_REQUEST_SIZE = 4096
MAX_SFTP_REQUEST_SIZE = 261120  # 262144 breaks OpenSSH

_TRUE = frozenset({"true", "1", "yes", "on"})
_FALSE = frozenset({"false", "0", "no", "off"})
_INT_RE = re.compile(r"[+-]?[0-9]+", re.ASCII)
_NUMBER_RE = re.compile(r"\+?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)", re.ASCII)
_TIME_RE = re.compile(r"([0-9]{1,2}):([0-9]{2})(?::([0-9]{2}))?", re.ASCII)
_CIPHER_RE = re.compile(r"[A-Za-z0-9@._+-]+", re.ASCII)
_FINGERPRINT_PREFIX = "SHA256:"


class ConfigError(ValueError):
    """Carries a list of human readable problems (self.problems)."""

    def __init__(self, problems: list[str] | tuple[str, ...] | str) -> None:
        self.problems = [problems] if isinstance(problems, str) else list(problems)
        super().__init__("invalid configuration: " + "; ".join(self.problems))


@dataclass(frozen=True)
class Config:
    """Validated service configuration (see load_config() for the variables and defaults).

    Timeouts are seconds. ``backup_time`` is a wall-clock time in ``tz``. ``retention`` is None
    when a retention variable is invalid (``retention_errors`` says why); backups then run
    without deleting old ones. ``sftp_host_keys`` holds normalised entries (see
    parse_host_keys()); empty means the host key is not pinned. Secrets are left out of repr().
    """

    odoo_url: str
    odoo_master_password: str = field(repr=False)
    odoo_db_name: str
    backup_format: str
    backup_time: datetime.time
    backup_every_hour: int | None
    hourly_backup_filestore: bool
    tz_name: str
    tz: zoneinfo.ZoneInfo
    retention: RetentionPolicy | None
    retention_errors: tuple[str, ...]
    retention_dry_run: bool
    sftp_host: str
    sftp_port: int
    sftp_user: str
    sftp_password: str | None = field(repr=False)
    sftp_private_key_file: str | None
    sftp_private_key_passphrase: str | None = field(repr=False)
    sftp_path: str
    db_only_path: str
    sftp_host_keys: tuple[str, ...]
    sftp_ciphers: tuple[str, ...]
    sftp_timeout: float
    sftp_upload_attempts: int
    sftp_max_request_size: int | None
    odoo_timeout: float
    odoo_read_timeout: float
    tmp_dir: pathlib.Path
    state_dir: pathlib.Path
    max_runtime: datetime.timedelta
    heartbeat_url: str | None = field(repr=False)
    healthcheck_max_age: datetime.timedelta
    test_mode: bool

    @property
    def hourly(self) -> bool:
        """True when BACKUP_EVERY_HOUR is set (several slots per day)."""
        return self.backup_every_hour is not None

    @property
    def interval(self) -> datetime.timedelta:
        """Nominal time between two backups: BACKUP_EVERY_HOUR hours, or 24 h in daily mode."""
        return datetime.timedelta(hours=self.backup_every_hour or 24)

    @property
    def full_backup_max_age(self) -> datetime.timedelta | None:
        """Maximum age of the last full backup (database + filestore) for --health and the heartbeat.

        Only set with database-only slots (BACKUP_EVERY_HOUR and HOURLY_BACKUP_FILESTORE=false):
        their successes keep the last success fresh, so the one full backup per day needs a limit
        of its own. It is the daily-mode default (36 h), or HEALTHCHECK_MAX_AGE_HOURS if larger.
        """
        if not self.hourly or self.hourly_backup_filestore:
            return None
        return max(self.healthcheck_max_age, _default_healthcheck_age(None))


def parse_bool(raw: str, name: str) -> bool:
    """Parse true/false/1/0/yes/no/on/off (case-insensitive); anything else raises ConfigError."""
    value = raw.strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise ConfigError([f"{name} must be one of true/false/1/0/yes/no/on/off, got {raw!r}"])


def read_secret(env: Mapping[str, str], name: str) -> str | None:
    """Return the secret from ``NAME`` or from the file named by ``NAME_FILE``.

    The file content loses its trailing line break (Docker secrets and ``echo`` add one);
    other whitespace is part of the secret. Raises ConfigError when both variables are set,
    or when the file cannot be read or is empty. The secret itself never appears in a message.
    """
    file_var = f"{name}_FILE"
    direct = env.get(name) or None
    path = (env.get(file_var) or "").strip() or None
    if direct is not None and path is not None:
        raise ConfigError([f"{name} and {file_var} are both set; use only one of them"])
    if path is None:
        return direct
    try:
        content = pathlib.Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        reason = exc.strerror if isinstance(exc, OSError) and exc.strerror else type(exc).__name__
        raise ConfigError([f"{file_var}: cannot read {path!r}: {reason}"]) from None
    secret = content.rstrip("\r\n")
    if not secret:
        raise ConfigError([f"{file_var}: {path!r} is empty"])
    return secret


def parse_backup_time(raw: str) -> datetime.time:
    """Parse H:MM, HH:MM or HH:MM:SS (00:00 .. 23:59:59); raises ValueError otherwise."""
    match = _TIME_RE.fullmatch(raw.strip())
    if match is None:
        raise ValueError(f"expected HH:MM or HH:MM:SS, got {raw!r}")
    hour, minute, second = (int(group or 0) for group in match.groups())
    try:
        return datetime.time(hour, minute, second)
    except ValueError:
        raise ValueError(f"{raw!r} is not a time between 00:00 and 23:59:59") from None


def _shorten(text: str, limit: int = 60) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _parse_fingerprint(entry: str) -> str:
    """Normalise ``SHA256:<base64>`` (padding optional) to OpenSSH's unpadded form."""
    b64 = entry[len(_FINGERPRINT_PREFIX):].rstrip("=")
    try:
        digest = base64.b64decode(b64 + "=" * (-len(b64) % 4), validate=True)
    except binascii.Error:
        digest = b""
    if len(digest) != 32:
        raise ValueError(f"{_shorten(entry)!r} is not a valid SHA256 fingerprint")
    return _FINGERPRINT_PREFIX + b64


def _parse_public_key(entry: str) -> str:
    """Normalise an OpenSSH public key line to ``<keytype> <base64>``.

    Accepts ``<keytype> <base64> [comment]`` with an optional leading known_hosts host field
    (``[host]:23``, ``host1,host2``, hashed ``|1|...``). The key blob must start with its own
    key type, which catches keys pasted with the wrong type.
    """
    tokens = entry.split()
    if tokens and tokens[0].startswith("@"):
        raise ValueError(f"{_shorten(entry)!r}: known_hosts markers such as @cert-authority are not supported")
    if len(tokens) >= 2 and tokens[0] in HOST_KEY_TYPES:
        key_type, b64 = tokens[0], tokens[1]
    elif len(tokens) >= 3 and tokens[1] in HOST_KEY_TYPES:
        key_type, b64 = tokens[1], tokens[2]
    else:
        raise ValueError(
            f"{_shorten(entry)!r} is neither a SHA256 fingerprint nor a public key of a supported type "
            f"({', '.join(sorted(HOST_KEY_TYPES))})"
        )
    try:
        blob = base64.b64decode(b64, validate=True)
    except binascii.Error:
        raise ValueError(f"{_shorten(entry)!r}: the key is not valid base64") from None
    encoded_type = key_type.encode()
    if len(blob) <= 4 + len(encoded_type) or struct.unpack(">I", blob[:4])[0] != len(encoded_type) \
            or blob[4:4 + len(encoded_type)] != encoded_type:
        raise ValueError(f"{_shorten(entry)!r}: the key data does not contain a {key_type} key")
    return f"{key_type} {b64}"


def _looks_like_host_field(piece: str) -> bool:
    """A comma-free fragment without whitespace that is neither a fingerprint nor a key type.

    known_hosts host fields may list several hosts separated by commas
    ("host,1.2.3.4 ssh-ed25519 AAAA..."); such fragments belong to the next entry.
    """
    return (
        not any(ch.isspace() for ch in piece)
        and not piece.upper().startswith(("SHA256:", "MD5:"))
        and piece not in HOST_KEY_TYPES
    )


def parse_host_keys(raw: str) -> tuple[str, ...]:
    """Parse SFTP_HOST_KEY into normalised entries (deduplicated, original order).

    Entries are separated by newlines or commas. Each is a fingerprint ``SHA256:<base64>``
    (as printed by ``ssh-keygen -lf``) or an OpenSSH public key, optionally in known_hosts
    form. Normalised forms: ``SHA256:<base64 without padding>`` and ``<keytype> <base64>``.
    Lines starting with "#" are comments. Raises ConfigError listing every invalid entry.
    """
    entries: list[str] = []
    problems: list[str] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        pending_hosts: list[str] = []
        for piece in (part.strip() for part in line.split(",")):
            if not piece:
                continue
            if _looks_like_host_field(piece):
                pending_hosts.append(piece)
                continue
            entry = ",".join([*pending_hosts, piece])
            pending_hosts = []
            try:
                if entry.upper().startswith("MD5:"):
                    raise ValueError(f"{_shorten(entry)!r}: MD5 fingerprints are not supported, use SHA256")
                if entry.upper().startswith(_FINGERPRINT_PREFIX):
                    entries.append(_parse_fingerprint(entry))
                else:
                    entries.append(_parse_public_key(entry))
            except ValueError as exc:
                problems.append(f"SFTP_HOST_KEY: {exc}")
        problems.extend(
            f"SFTP_HOST_KEY: {_shorten(host)!r} is neither a SHA256 fingerprint nor a public key"
            for host in pending_hosts
        )
    if not entries and not problems:
        problems.append("SFTP_HOST_KEY is set but contains no host key")
    if problems:
        raise ConfigError(problems)
    return tuple(dict.fromkeys(entries))


class _EnvReader:
    """Reads typed values from the environment and collects problems instead of raising.

    After recording a problem a method returns a type-correct fallback, so loading can go on
    and report every problem; the fallbacks never reach a Config because load_config()
    raises as soon as any problem was recorded.
    """

    def __init__(self, env: Mapping[str, str]) -> None:
        self.env = env
        self.problems: list[str] = []

    def raw(self, name: str) -> str | None:
        """Stripped value; an empty value counts as unset."""
        value = self.env.get(name)
        if value is None:
            return None
        return value.strip() or None

    def is_set(self, name: str) -> bool:
        return self.raw(name) is not None

    def required(self, name: str) -> str:
        value = self.raw(name)
        if value is None:
            self.problems.append(f"{name} is required")
            return ""
        return value

    def secret(self, name: str) -> str | None:
        try:
            return read_secret(self.env, name)
        except ConfigError as exc:
            self.problems.extend(exc.problems)
            return None

    def boolean(self, name: str, default: bool) -> bool:
        raw = self.raw(name)
        if raw is None:
            return default
        try:
            return parse_bool(raw, name)
        except ConfigError as exc:
            self.problems.extend(exc.problems)
            return default

    def optional_integer(self, name: str, minimum: int, maximum: int | None = None,
                         problems: list[str] | None = None) -> int | None:
        """Integer within [minimum, maximum], None when unset or invalid.

        A problem goes to ``problems`` (default: self.problems).
        """
        raw = self.raw(name)
        if raw is None:
            return None
        value = int(raw) if _INT_RE.fullmatch(raw) else None
        if value is None or value < minimum or (maximum is not None and value > maximum):
            limits = f"between {minimum} and {maximum}" if maximum is not None else f">= {minimum}"
            (self.problems if problems is None else problems).append(
                f"{name} must be an integer {limits}, got {raw!r}"
            )
            return None
        return value

    def integer(self, name: str, default: int, minimum: int, maximum: int | None = None,
                problems: list[str] | None = None) -> int:
        """Like optional_integer(), but ``default`` when unset or invalid."""
        value = self.optional_integer(name, minimum, maximum, problems)
        return default if value is None else value

    def positive_number(self, name: str, default: float) -> float:
        raw = self.raw(name)
        if raw is None:
            return default
        value = float(raw) if _NUMBER_RE.fullmatch(raw) else 0.0
        if value <= 0:
            self.problems.append(f"{name} must be a number > 0, got {raw!r}")
            return default
        return value

    def http_url(self, name: str, value: str | None) -> str | None:
        """Validate an http(s) URL without quoting it (it may carry credentials or tokens)."""
        if value is None:
            return None
        try:
            parts = urlsplit(value)
            valid = parts.scheme in ("http", "https") and bool(parts.hostname)
        except ValueError:
            valid = False
        if not valid:
            self.problems.append(f"{name} must be an http:// or https:// URL")
            return None
        return value

    def directory(self, name: str, default: pathlib.Path) -> pathlib.Path:
        raw = self.raw(name)
        if raw is None:
            return default
        if not os.path.isabs(raw):
            self.problems.append(f"{name} must be an absolute path, got {raw!r}")
            return default
        return pathlib.Path(os.path.normpath(raw))


def _load_backup_time(reader: _EnvReader) -> datetime.time:
    raw = reader.raw("BACKUP_TIME") or "02:00"
    try:
        return parse_backup_time(raw)
    except ValueError as exc:
        reader.problems.append(f"BACKUP_TIME: {exc}")
        return datetime.time(2, 0)


def _load_every_hour(reader: _EnvReader) -> int | None:
    every = reader.optional_integer("BACKUP_EVERY_HOUR", 0, 24)
    if not every:  # unset, "0" or invalid (already reported)
        return None
    if 24 % every:
        slots = 24 // every
        logger.warning(
            "BACKUP_EVERY_HOUR=%d does not divide 24: %d backups per day, the gap before the "
            "first backup of the next day is %d h",
            every, slots, 24 - every * (slots - 1),
        )
    return every


def _load_retention(reader: _EnvReader, backup_time: datetime.time,
                    every_hour: int | None) -> tuple[RetentionPolicy | None, tuple[str, ...]]:
    """Build the retention policy; problems disable retention instead of failing the config."""
    errors: list[str] = []
    hourly_keep = reader.integer("HOURLY_BACKUP_KEEP", 4, 0, problems=errors)
    daily_keep = reader.integer("DAILY_BACKUP_KEEP", 30, 0, problems=errors)
    monthly_keep = reader.integer("MONTHLY_BACKUP_KEEP", 12, 0, problems=errors)
    yearly_keep = reader.integer("YEARLY_BACKUP_KEEP", -1, -1, problems=errors)
    policy = None
    if not errors:
        keep_last = hourly_keep if every_hour is not None else 0
        if not (keep_last or daily_keep or monthly_keep or yearly_keep):
            errors.append(
                "all effective retention values are 0 (HOURLY_BACKUP_KEEP only counts with "
                "BACKUP_EVERY_HOUR): refusing a policy that keeps nothing"
            )
        else:
            policy = RetentionPolicy(keep_last, daily_keep, monthly_keep, yearly_keep, backup_time)
    for error in errors:
        logger.warning("Retention disabled (backups continue, old backups are not deleted): %s", error)
    return policy, tuple(errors)


def _load_sftp_endpoint(reader: _EnvReader) -> tuple[str, int]:
    """SFTP_HOST and SFTP_PORT, including the deprecated SFTP_HOST=host:port form."""
    host = reader.required("SFTP_HOST")
    port_problems: list[str] = []
    port = reader.integer("SFTP_PORT", 22, 1, 65535, problems=port_problems)
    reader.problems.extend(port_problems)
    if not host:
        return host, port
    if host.count(":") == 1:  # IPv6 literals contain several colons and are left alone
        legacy_host, legacy_port_raw = host.split(":")
        if not legacy_host or not re.fullmatch(r"[0-9]+", legacy_port_raw, re.ASCII) \
                or not 1 <= int(legacy_port_raw) <= 65535:
            reader.problems.append(f"SFTP_HOST: {host!r} is not a host name (set the port with SFTP_PORT)")
            return host, port
        legacy_port = int(legacy_port_raw)
        if reader.is_set("SFTP_PORT") and not port_problems and port != legacy_port:
            reader.problems.append(
                f"SFTP_HOST contains port {legacy_port} but SFTP_PORT is {port}; remove the port from SFTP_HOST"
            )
            return host, port
        logger.warning(
            "SFTP_HOST=%r with a port is deprecated: use SFTP_HOST=%r and SFTP_PORT=%d",
            host, legacy_host, legacy_port,
        )
        return legacy_host, legacy_port
    if any(ch.isspace() or ch in "/@" for ch in host):
        reader.problems.append(f"SFTP_HOST: {host!r} is not a host name")
    return host, port


def _load_sftp_credentials(reader: _EnvReader) -> tuple[str | None, str | None, str | None]:
    password = reader.secret("SFTP_PASSWORD")
    key_file = reader.raw("SFTP_PRIVATE_KEY_FILE")
    passphrase = reader.secret("SFTP_PRIVATE_KEY_PASSPHRASE")
    if key_file is not None:
        try:
            with open(key_file, "rb"):
                pass
        except OSError as exc:
            reader.problems.append(f"SFTP_PRIVATE_KEY_FILE: cannot read {key_file!r}: {exc.strerror or exc}")
    elif passphrase is not None:
        reader.problems.append("SFTP_PRIVATE_KEY_PASSPHRASE is set but SFTP_PRIVATE_KEY_FILE is not")
    if password is None and key_file is None and not reader.is_set("SFTP_PASSWORD_FILE"):
        reader.problems.append("SFTP_PASSWORD, SFTP_PASSWORD_FILE or SFTP_PRIVATE_KEY_FILE is required")
    return password, key_file, passphrase


def _load_paths(reader: _EnvReader, sftp_path: str) -> str:
    db_only = reader.raw("DB_ONLY_BACKUP_PATH") or posixpath.join(sftp_path, "db-only")
    if posixpath.normpath(db_only) == posixpath.normpath(sftp_path):
        reader.problems.append(
            f"DB_ONLY_BACKUP_PATH must differ from SFTP_PATH ({sftp_path!r}): restore tooling picks the "
            "newest archive in SFTP_PATH and must never pick a database-only backup"
        )
    return db_only


def _load_host_keys(reader: _EnvReader) -> tuple[str, ...]:
    raw = reader.raw("SFTP_HOST_KEY")
    if raw is None:
        return ()
    try:
        return parse_host_keys(raw)
    except ConfigError as exc:
        reader.problems.extend(exc.problems)
        return ()


def _load_ciphers(reader: _EnvReader) -> tuple[str, ...]:
    raw = reader.raw("SFTP_CIPHERS")
    if raw is None:
        return DEFAULT_SFTP_CIPHERS
    names = [name.strip() for name in raw.split(",") if name.strip()]
    invalid = [name for name in names if not _CIPHER_RE.fullmatch(name)]
    if not names or invalid:
        reader.problems.append(f"SFTP_CIPHERS must be a comma separated list of cipher names, got {raw!r}")
        return DEFAULT_SFTP_CIPHERS
    return tuple(dict.fromkeys(names))


def _check_local_dirs(reader: _EnvReader, tmp_dir: pathlib.Path, state_dir: pathlib.Path) -> None:
    """The service deletes the CONTENTS of tmp_dir at startup; refuse dangerous choices.

    Symlinks are resolved (read-only) so that e.g. /tmp and /private/tmp on macOS compare equal.
    """
    tmp_real = pathlib.Path(os.path.realpath(tmp_dir))
    system_temp = pathlib.Path(os.path.realpath(tempfile.gettempdir()))
    if tmp_real == pathlib.Path(tmp_real.anchor) or tmp_real == system_temp:
        reader.problems.append(
            f"BACKUP_TMP_DIR must be a dedicated directory, not {str(tmp_dir)!r}: its contents are deleted at startup"
        )
    state_real = pathlib.Path(os.path.realpath(state_dir))
    if state_real == tmp_real or tmp_real in state_real.parents:
        reader.problems.append(
            "BACKUP_STATE_DIR must not be inside BACKUP_TMP_DIR: its contents are deleted at startup"
        )


def _load_timezone(reader: _EnvReader) -> tuple[str, zoneinfo.ZoneInfo]:
    name = (reader.raw("TZ") or "UTC").removeprefix(":")  # glibc accepts TZ=":Europe/Zurich"
    try:
        return name, zoneinfo.ZoneInfo(name)
    except (zoneinfo.ZoneInfoNotFoundError, ValueError, OSError):
        reader.problems.append(f"TZ: unknown time zone {name!r}")
        return name, zoneinfo.ZoneInfo("UTC")


def _default_healthcheck_age(every_hour: int | None) -> datetime.timedelta:
    interval_hours = every_hour or 24
    return datetime.timedelta(hours=interval_hours + max(2, interval_hours // 2))


def load_config(env: Mapping[str, str] | None = None) -> Config:
    """Build the configuration from ``env`` (default os.environ).

    Raises ConfigError with every problem found. Logs WARNINGs for deprecated or unusual
    settings and for retention problems (which do not raise, see the module docstring).
    """
    reader = _EnvReader(os.environ if env is None else env)
    problems = reader.problems

    odoo_url = reader.http_url("ODOO_URL", reader.required("ODOO_URL") or None)
    master_password = reader.secret("ODOO_MASTER_PWD")
    if master_password is None and not reader.is_set("ODOO_MASTER_PWD_FILE"):
        problems.append("ODOO_MASTER_PWD or ODOO_MASTER_PWD_FILE is required")
    db_name = reader.required("ODOO_DB_NAME")
    if db_name and not DB_NAME_PATTERN.fullmatch(db_name):
        problems.append(f"ODOO_DB_NAME: {db_name!r} is not a valid Odoo database name "
                        f"(^{DB_NAME_PATTERN.pattern}$)")
    backup_format = (reader.raw("ODOO_BACKUP_FORMAT") or "zip").lower()
    if backup_format not in ALLOWED_FORMATS:
        problems.append(f"ODOO_BACKUP_FORMAT must be one of {', '.join(ALLOWED_FORMATS)}, got {backup_format!r}")

    backup_time = _load_backup_time(reader)
    every_hour = _load_every_hour(reader)
    hourly_filestore = reader.boolean("HOURLY_BACKUP_FILESTORE", True)
    if every_hour is None and not hourly_filestore:
        logger.warning("HOURLY_BACKUP_FILESTORE=false has no effect without BACKUP_EVERY_HOUR")
    tz_name, tz = _load_timezone(reader)
    retention, retention_errors = _load_retention(reader, backup_time, every_hour)
    dry_run = reader.boolean("RETENTION_DRY_RUN", False)

    sftp_host, sftp_port = _load_sftp_endpoint(reader)
    sftp_user = reader.required("SFTP_USER")
    sftp_password, key_file, passphrase = _load_sftp_credentials(reader)
    sftp_path = reader.raw("SFTP_PATH") or "/"
    db_only_path = _load_paths(reader, sftp_path)
    host_keys = _load_host_keys(reader)
    ciphers = _load_ciphers(reader)
    sftp_timeout = reader.positive_number("SFTP_TIMEOUT", 60.0)
    upload_attempts = reader.integer("SFTP_UPLOAD_ATTEMPTS", 3, 1, 10)
    max_request_size = reader.optional_integer("SFTP_MAX_REQUEST_SIZE", MIN_SFTP_REQUEST_SIZE, MAX_SFTP_REQUEST_SIZE)

    odoo_timeout = reader.positive_number("ODOO_TIMEOUT", 30.0)
    odoo_read_timeout = reader.positive_number("ODOO_READ_TIMEOUT", 14400.0)

    temp_root = pathlib.Path(tempfile.gettempdir())
    tmp_dir = reader.directory("BACKUP_TMP_DIR", temp_root / "odoo-backup")
    state_dir = reader.directory("BACKUP_STATE_DIR", temp_root / "odoo-backup-state")
    _check_local_dirs(reader, tmp_dir, state_dir)
    max_runtime_minutes = reader.integer("BACKUP_MAX_RUNTIME_MINUTES", 480, 10)
    heartbeat_secret = reader.secret("HEARTBEAT_URL")
    heartbeat_url = reader.http_url("HEARTBEAT_URL", heartbeat_secret.strip() if heartbeat_secret else None)
    healthcheck_max_age = _default_healthcheck_age(every_hour)
    if reader.is_set("HEALTHCHECK_MAX_AGE_HOURS"):
        healthcheck_max_age = datetime.timedelta(hours=reader.positive_number("HEALTHCHECK_MAX_AGE_HOURS", 1.0))
    test_mode = reader.boolean("TEST_MODE", False)

    if problems:
        raise ConfigError(problems)
    return Config(
        odoo_url=(odoo_url or "").rstrip("/"),
        odoo_master_password=master_password or "",
        odoo_db_name=db_name,
        backup_format=backup_format,
        backup_time=backup_time,
        backup_every_hour=every_hour,
        hourly_backup_filestore=hourly_filestore,
        tz_name=tz_name,
        tz=tz,
        retention=retention,
        retention_errors=retention_errors,
        retention_dry_run=dry_run,
        sftp_host=sftp_host,
        sftp_port=sftp_port,
        sftp_user=sftp_user,
        sftp_password=sftp_password,
        sftp_private_key_file=key_file,
        sftp_private_key_passphrase=passphrase,
        sftp_path=sftp_path,
        db_only_path=db_only_path,
        sftp_host_keys=host_keys,
        sftp_ciphers=ciphers,
        sftp_timeout=sftp_timeout,
        sftp_upload_attempts=upload_attempts,
        sftp_max_request_size=max_request_size,
        odoo_timeout=odoo_timeout,
        odoo_read_timeout=odoo_read_timeout,
        tmp_dir=tmp_dir,
        state_dir=state_dir,
        max_runtime=datetime.timedelta(minutes=max_runtime_minutes),
        heartbeat_url=heartbeat_url,
        healthcheck_max_age=healthcheck_max_age,
        test_mode=test_mode,
    )
