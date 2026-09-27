"""Run state on disk, the health verdict derived from it, and the heartbeat ping.

The state file ``<BACKUP_STATE_DIR>/state.json`` is the service's only memory across runs and
restarts. It is written atomically (temporary file + ``os.replace``) with mode 0600 and holds
timestamps plus a short error text; it never contains secrets. ``python backup.py --health``
(the Docker HEALTHCHECK) reads it and compares the last success with HEALTHCHECK_MAX_AGE_HOURS.

The run lock (``run.lock`` in the same directory) keeps two backup runs from overlapping, e.g. a
manual ``docker exec ... backup.py --once`` while the scheduled run is uploading: the second run
would otherwise treat the first run's ``.upload`` file as a stale partial and delete it.
"""

import contextlib
import dataclasses
import datetime
import fcntl
import json
import logging
import os
import pathlib
import tempfile
import urllib.error
import urllib.request
from collections.abc import Iterator

from . import __version__

logger = logging.getLogger(__name__)

STATE_FILE_NAME = "state.json"
LOCK_FILE_NAME = "run.lock"
MAX_ERROR_LENGTH = 300
HEARTBEAT_TIMEOUT = 10.0


class RunLockedError(RuntimeError):
    """Another process holds the run lock (a backup run is already in progress)."""


@dataclasses.dataclass
class State:
    """Content of the state file; all timestamps are timezone-aware (stored as ISO 8601 in UTC).

    ``started_at`` anchors the health check's start-up grace period: the first service start
    that has not been followed by a full backup yet. ``last_success_*`` covers every successful
    run, ``last_full_success_*`` only full backups (database + filestore).
    """

    started_at: datetime.datetime | None = None
    last_success_at: datetime.datetime | None = None
    last_success_file: str | None = None
    last_full_success_at: datetime.datetime | None = None
    last_full_success_file: str | None = None
    last_failure_at: datetime.datetime | None = None
    last_error: str | None = None

    def to_json(self) -> dict[str, str | None]:
        return {
            "started_at": _format_time(self.started_at),
            "last_success_at": _format_time(self.last_success_at),
            "last_success_file": self.last_success_file,
            "last_full_success_at": _format_time(self.last_full_success_at),
            "last_full_success_file": self.last_full_success_file,
            "last_failure_at": _format_time(self.last_failure_at),
            "last_error": self.last_error,
        }

    @classmethod
    def from_json(cls, data: object) -> State:
        """Build a State from parsed JSON; raises ValueError for anything malformed."""
        if not isinstance(data, dict):
            raise ValueError("the state file does not contain a JSON object")
        return cls(
            started_at=_parse_time(data.get("started_at"), "started_at"),
            last_success_at=_parse_time(data.get("last_success_at"), "last_success_at"),
            last_success_file=_optional_str(data.get("last_success_file"), "last_success_file"),
            last_full_success_at=_parse_time(data.get("last_full_success_at"), "last_full_success_at"),
            last_full_success_file=_optional_str(data.get("last_full_success_file"), "last_full_success_file"),
            last_failure_at=_parse_time(data.get("last_failure_at"), "last_failure_at"),
            last_error=_optional_str(data.get("last_error"), "last_error"),
        )


def _format_time(value: datetime.datetime | None) -> str | None:
    return None if value is None else value.astimezone(datetime.UTC).isoformat()


def _parse_time(raw: object, name: str) -> datetime.datetime | None:
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError(f"{name} is not a string")
    value = datetime.datetime.fromisoformat(raw)
    if value.tzinfo is None:
        raise ValueError(f"{name} has no time zone")
    return value


def _optional_str(raw: object, name: str) -> str | None:
    if raw is None or isinstance(raw, str):
        return raw
    raise ValueError(f"{name} is not a string")


def short_error(text: str, limit: int = MAX_ERROR_LENGTH) -> str:
    """One line of at most ``limit`` characters (whitespace collapsed) for the state file."""
    line = " ".join(str(text).split())
    return line if len(line) <= limit else line[: limit - 3] + "..."


class StateStore:
    """Reads and writes ``state.json`` in ``state_dir`` and provides the run lock."""

    def __init__(self, state_dir: str | os.PathLike[str]) -> None:
        self.directory = pathlib.Path(state_dir)
        self.path = self.directory / STATE_FILE_NAME

    def __repr__(self) -> str:
        return f"StateStore({str(self.directory)!r})"

    def load(self) -> State:
        """Return the stored state; a missing file gives an empty State.

        A corrupt or unreadable file is logged as a WARNING and treated as empty, so the next
        successful run rewrites it instead of failing forever.
        """
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return State()
        except OSError as exc:
            logger.warning("Cannot read the state file %s: %s", self.path, exc.strerror or exc)
            return State()
        try:
            return State.from_json(json.loads(raw))
        except ValueError as exc:
            logger.warning("Ignoring the invalid state file %s: %s", self.path, exc)
            return State()

    def save(self, state: State) -> None:
        """Write the state atomically with mode 0600 (raises OSError on failure)."""
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd, tmp_name = tempfile.mkstemp(prefix=".state-", suffix=".tmp", dir=self.directory)  # mode 0600
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(state.to_json(), handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_name)
            raise

    def update(self, **changes: object) -> State:
        """Load, apply ``changes`` (State field names) and save; returns the new state."""
        state = dataclasses.replace(self.load(), **changes)
        self.save(state)
        return state

    def record_start(self, now: datetime.datetime) -> State:
        """The service (re)started: anchor the health check's start-up grace period.

        The grace period counts from the first start that has not been followed by a full
        backup yet. A restart does not extend it: otherwise an installation whose every run
        hangs until the watchdog ends the process (and Docker restarts it) would be reported
        healthy forever without a single backup.
        """
        state = self.load()
        full = state.last_full_success_at
        if state.started_at is not None and (full is None or full < state.started_at):
            return state
        return self.update(started_at=now)

    def record_success(self, now: datetime.datetime, file_name: str, *, full: bool = True) -> State:
        """A run succeeded; ``full`` is False for a database-only backup."""
        changes: dict[str, object] = {"last_success_at": now, "last_success_file": file_name}
        if full:
            changes.update(last_full_success_at=now, last_full_success_file=file_name)
        return self.update(**changes)

    def record_failure(self, now: datetime.datetime, error: str) -> State:
        return self.update(last_failure_at=now, last_error=short_error(error))

    @contextlib.contextmanager
    def run_lock(self) -> Iterator[None]:
        """Hold an exclusive, non-blocking lock for one backup run.

        Raises RunLockedError when another process (or another open of the lock file) holds it.
        The kernel releases the lock when the process dies, so a crash never leaves it stale.
        """
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock_path = self.directory / LOCK_FILE_NAME
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RunLockedError(f"another backup run is in progress (lock {lock_path} is held)") from None
            yield
        finally:
            os.close(fd)  # releases the lock


def _format_age(delta: datetime.timedelta) -> str:
    minutes = max(0, int(delta.total_seconds() // 60))
    days, minutes = divmod(minutes, 24 * 60)
    hours, minutes = divmod(minutes, 60)
    if days:
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def full_backup_problem(state: State, now: datetime.datetime, max_age: datetime.timedelta) -> str | None:
    """Why the full backups (database + filestore) are overdue, or None while they are fresh.

    Needed when database-only backups run between the daily full backups: they keep the last
    success fresh even if every full backup fails. Before the first full backup, the start-up
    grace period (``max_age`` from ``started_at``) applies.
    """
    if state.last_full_success_at is not None:
        age = now - state.last_full_success_at
        if age <= max_age:
            return None
        return (
            f"last full backup (database + filestore) {_format_age(age)} ago, more than the allowed "
            f"{_format_age(max_age)}"
        )
    if state.started_at is None:
        return "no full backup (database + filestore) recorded yet"
    running = now - state.started_at
    if running < max_age:
        return None
    return f"no full backup (database + filestore) since the service started {_format_age(running)} ago"


def health_status(
    state: State,
    now: datetime.datetime,
    max_age: datetime.timedelta,
    full_max_age: datetime.timedelta | None = None,
) -> tuple[bool, str]:
    """Return ``(ok, message)`` for the Docker HEALTHCHECK.

    Healthy when the last success is at most ``max_age`` old. Without any success yet the service
    is healthy while it has been running for less than ``max_age`` (start-up grace period).
    ``full_max_age`` (HOURLY_BACKUP_FILESTORE=false) additionally limits the age of the last
    full backup, see full_backup_problem().
    """
    failure = ""
    if state.last_error and state.last_failure_at is not None:
        failure = f"; last failure {_format_age(now - state.last_failure_at)} ago: {state.last_error}"
    if state.last_success_at is not None:
        age = now - state.last_success_at
        if age > max_age:
            return False, (
                f"unhealthy: last successful backup {_format_age(age)} ago, more than the allowed "
                f"{_format_age(max_age)}{failure}"
            )
        message = f"healthy: last successful backup {_format_age(age)} ago ({state.last_success_file})"
    elif state.started_at is None:
        return False, "unhealthy: no state recorded yet (is the backup service running?)"
    elif now - state.started_at < max_age:
        started = _format_age(now - state.started_at)
        message = f"healthy: starting, no successful backup yet (service started {started} ago){failure}"
    else:
        running = _format_age(now - state.started_at)
        return False, f"unhealthy: no successful backup since the service started {running} ago{failure}"
    if full_max_age is None:
        return True, message
    problem = full_backup_problem(state, now, full_max_age)
    if problem is not None:
        return False, f"unhealthy: {problem}{failure}"
    if state.last_full_success_at is not None and state.last_full_success_at != state.last_success_at:
        age = _format_age(now - state.last_full_success_at)
        message += f"; last full backup {age} ago ({state.last_full_success_file})"
    return True, message


def send_heartbeat(url: str, timeout: float = HEARTBEAT_TIMEOUT) -> bool:
    """GET ``url`` (e.g. a healthchecks.io ping URL); returns True on a 2xx answer.

    The URL is a secret (it usually contains the check's token), so it is never logged, and
    urllib is used instead of requests so no library debug logging can print it either.
    Failures are logged as WARNING and never raise.
    """
    # S310: load_config() only accepts http:// and https:// heartbeat URLs (_EnvReader.http_url()).
    request = urllib.request.Request(url, method="GET", headers={"User-Agent": f"odoo-backup/{__version__}"})  # noqa: S310
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            status = response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
    except (OSError, ValueError) as exc:  # URLError, timeouts, TLS errors are OSErrors
        logger.warning("Heartbeat ping failed: %s", _describe_network_error(exc))
        return False
    if not 200 <= status < 300:
        logger.warning("Heartbeat ping answered HTTP %d", status)
        return False
    logger.info("Heartbeat ping sent")
    return True


def _describe_network_error(exc: BaseException) -> str:
    """Name the failure without its message: urllib and ssl messages can contain the URL."""
    reason = exc.reason if isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, BaseException) else exc
    detail = type(reason).__name__
    if isinstance(reason, TimeoutError):
        return f"{detail} (timed out)"
    strerror = getattr(reason, "strerror", None)
    return f"{detail} ({strerror})" if isinstance(strerror, str) and strerror else detail
