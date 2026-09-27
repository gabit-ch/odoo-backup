"""One backup run, the retention of the backup directories and the deployment checks.

:func:`run_backup` executes the stages of one run:

1. ``lock``: take the run lock (a second, overlapping run fails immediately).
2. ``sftp-preflight``: connect (host key), create and list the target directory, make sure
   SFTP_PATH and DB_ONLY_BACKUP_PATH are different directories on the server, delete stale
   ``.upload`` partials of this database left by earlier runs (RETENTION_DRY_RUN only logs
   them), remember the newest backup size.
3. ``disk-preflight``: BACKUP_TMP_DIR needs 110 % of the newest backup's size free.
4. ``odoo``: read ``server_serie`` and build the file name
   ``odoo{serie}-{db}-{YYYYmmdd-HHMMSS}.{fmt}`` from the local wall-clock start time.
5. ``download``: download and verify the backup into BACKUP_TMP_DIR (``<name>.part``).
6. ``upload``: upload over a fresh SFTP connection; the local file is removed afterwards,
   whatever happened.
7. ``clock-check``: the server's mtime of the upload must be within an hour of the local clock,
   otherwise retention (which trusts the local clock) is skipped and the run fails.
8. ``retention``: plan and apply the retention in the full-backup and the database-only
   directory (RETENTION_DRY_RUN only logs the plan).

A run succeeds only when every stage succeeded (a dry-run retention counts as success, a
disabled retention does not). The heartbeat follows a successful run, except while the full
backups are overdue in the HOURLY_BACKUP_FILESTORE=false mode (see
``Config.full_backup_max_age``). Every exception is caught at run level and logged once, in the
final "backup failed" line together with the stage name; the scheduler keeps going. Only
:class:`Shutdown` and KeyboardInterrupt propagate, after the run was recorded as interrupted.
"""

import contextlib
import dataclasses
import datetime
import logging
import os
import pathlib
import posixpath
import secrets
import shutil
import threading
import time
from collections.abc import Callable, Collection, Iterator
from typing import Any

from .config import Config
from .odoo import OdooClient, OdooError
from .retention import RetentionPlan, RetentionPolicy, build_file_name, parse_backup_name, plan_retention
from .sftp import SFTPConnection, SFTPError, UploadResult
from .state import RunLockedError, State, StateStore, full_backup_problem, send_heartbeat
from .verify import ArchiveVerificationError

logger = logging.getLogger(__name__)

#: Free space needed in BACKUP_TMP_DIR relative to the newest backup of the same kind.
DISK_HEADROOM = 1.1
#: Maximum difference between the SFTP server's mtime of the upload and the local clock (seconds).
CLOCK_SKEW_LIMIT = 3600.0
#: Retention stops deleting after this many failed deletions in a row.
MAX_CONSECUTIVE_DELETE_FAILURES = 5
#: RETENTION_DRY_RUN lists at most this many names per directory.
DRY_RUN_LIST_LIMIT = 50
#: Exit code of the process when the watchdog fires.
WATCHDOG_EXIT_CODE = 3
#: Timeouts used by the deployment checks, so ``--check`` stays well below two minutes.
CHECK_ODOO_TIMEOUT = 15.0
CHECK_SFTP_TIMEOUT = 20.0

STAGE_LOCK = "lock"
STAGE_SFTP_PREFLIGHT = "sftp-preflight"
STAGE_DISK_PREFLIGHT = "disk-preflight"
STAGE_ODOO = "odoo"
STAGE_DOWNLOAD = "download"
STAGE_UPLOAD = "upload"
STAGE_CLOCK_CHECK = "clock-check"
STAGE_RETENTION = "retention"
STAGE_STATE = "state"

KIND_FULL = "full"
KIND_DB_ONLY = "database-only"

# Exceptions whose message is self-explanatory; anything else is logged with its traceback.
_EXPECTED_ERRORS = (OdooError, SFTPError, ArchiveVerificationError, RunLockedError, OSError)


class Shutdown(BaseException):
    """Raised by the SIGTERM/SIGINT handler while a backup run is active.

    A BaseException on purpose: it must pass every ``except Exception`` on its way out (for
    example the upload retry loop), so the run stops promptly and its ``finally`` blocks remove
    the temporary files.
    """


class RunError(Exception):
    """A stage failed for a reason the run itself detected (disk space, clock skew, retention)."""


# ------------------------------------------------------------------------------------------
# Factories and helpers
# ------------------------------------------------------------------------------------------


def make_odoo_client(config: Config, *, timeout: float | None = None) -> OdooClient:
    """OdooClient for ``config``; ``timeout`` overrides ODOO_TIMEOUT (used by the checks)."""
    return OdooClient(
        config.odoo_url,
        config.odoo_master_password,
        config.odoo_db_name,
        timeout=config.odoo_timeout if timeout is None else timeout,
        read_timeout=config.odoo_read_timeout,
    )


def make_sftp_connection(
    config: Config, *, timeout: float | None = None, sleep: Callable[[float], object] | None = None
) -> SFTPConnection:
    """Unconnected SFTPConnection for ``config``; ``timeout`` overrides SFTP_TIMEOUT."""
    return SFTPConnection(
        config.sftp_host,
        config.sftp_port,
        config.sftp_user,
        password=config.sftp_password,
        key_file=config.sftp_private_key_file,
        key_passphrase=config.sftp_private_key_passphrase,
        host_keys=config.sftp_host_keys,
        ciphers=config.sftp_ciphers,
        timeout=config.sftp_timeout if timeout is None else timeout,
        max_request_size=config.sftp_max_request_size,
        sleep=sleep,
    )


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def config_secrets(config: Config) -> tuple[str, ...]:
    """Secret values that must never appear in logs or in the state file."""
    values = (
        config.odoo_master_password,
        config.sftp_password,
        config.sftp_private_key_passphrase,
        config.heartbeat_url,
    )
    return tuple(value for value in values if value and len(value) >= 4)


def redact(text: str, config: Config) -> str:
    """Replace every configured secret in ``text`` by ``***`` (defence in depth)."""
    for secret in config_secrets(config):
        text = text.replace(secret, "***")
    return text


def describe_error(exc: BaseException, config: Config) -> str:
    """Short, secret-free description of ``exc`` for log lines and the state file."""
    text = str(exc)
    if not isinstance(exc, (RunError, *_EXPECTED_ERRORS)) or not text:
        text = f"{type(exc).__name__}: {text}" if text else type(exc).__name__
    return redact(text, config)


def _format_bytes(size: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


class Watchdog:
    """Ends the process when a run exceeds its time limit.

    A hung connection must not block every later backup: after ``limit`` the watchdog logs a
    CRITICAL line and calls ``os._exit(exit_code)``, so Docker's restart policy starts a fresh
    service (whose start-up removes the leftover temporary files). Use it as a context manager
    around the run; leaving the block disarms it.
    """

    def __init__(
        self,
        limit: datetime.timedelta | float,
        *,
        what: str = "The backup run",
        exit_code: int = WATCHDOG_EXIT_CODE,
        on_timeout: Callable[[], object] | None = None,
        exit_func: Callable[[int], object] = os._exit,
    ) -> None:
        self.seconds = limit.total_seconds() if isinstance(limit, datetime.timedelta) else float(limit)
        self.what = what
        self.exit_code = exit_code
        self.on_timeout = on_timeout
        self.exit_func = exit_func
        self.fired = threading.Event()
        self._timer: threading.Timer | None = None

    def __enter__(self) -> Watchdog:
        self._timer = threading.Timer(self.seconds, self._fire)
        self._timer.name = "odoo-backup-watchdog"
        self._timer.daemon = True
        self._timer.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        timer = self._timer
        if timer is not None and timer is not threading.current_thread():
            timer.cancel()
            timer.join(5)  # a cancelled timer ends at once; no watchdog thread outlives its block

    def _fire(self) -> None:
        self.fired.set()
        logger.critical(
            "%s did not finish within %s; terminating the process with exit code %d",
            self.what,
            datetime.timedelta(seconds=round(self.seconds)),
            self.exit_code,
        )
        if self.on_timeout is not None:
            with contextlib.suppress(Exception):
                self.on_timeout()
        for handler in logging.getLogger().handlers:
            with contextlib.suppress(Exception):
                handler.flush()
        self.exit_func(self.exit_code)


# ------------------------------------------------------------------------------------------
# Retention of the backup directories
# ------------------------------------------------------------------------------------------


@dataclasses.dataclass
class DirectoryRetention:
    """Retention of one SFTP directory: its plan and what happened when it was applied."""

    directory: str
    kind: str  # KIND_FULL or KIND_DB_ONLY
    policy: RetentionPolicy | None
    plan: RetentionPlan | None = None
    skipped: str | None = None
    deleted: int = 0
    already_gone: int = 0
    failed: int = 0
    aborted: bool = False


def db_only_policy(config: Config) -> RetentionPolicy | None:
    """Retention of DB_ONLY_BACKUP_PATH: the newest HOURLY_BACKUP_KEEP backups, at least one.

    HOURLY_BACKUP_KEEP=0 keeps only the newest database-only backup (the newest backup of a
    directory is always kept); without that floor the database-only backups of every slot would
    pile up until the SFTP server is full and the full backups fail too. Returns None in daily
    mode, where database-only backups only come from ``--once --database-only`` and are never
    deleted, and when the retention is disabled.
    """
    policy = config.retention
    if policy is None or not config.hourly:
        return None
    return RetentionPolicy(max(policy.keep_last, 1), 0, 0, 0, policy.anchor)


def same_directory(config: Config, sftp: SFTPConnection) -> str | None:
    """The server path when SFTP_PATH and DB_ONLY_BACKUP_PATH name the same directory, else None.

    load_config() only compares the spelled paths. A relative and an absolute spelling, a
    doubled slash or a symlink can still name one directory; the database-only retention (the
    newest HOURLY_BACKUP_KEEP only) would then delete the full backups, and database-only files
    would land where restore tooling picks the newest archive. The server resolves both paths
    (SSH_FXP_REALPATH); a directory that does not exist cannot be the other, existing one.
    """
    if not (sftp.exists(config.sftp_path) and sftp.exists(config.db_only_path)):
        return None
    full = sftp.realpath(config.sftp_path)
    return full if sftp.realpath(config.db_only_path) == full else None


def same_directory_error(config: Config, resolved: str) -> str:
    return (
        f"SFTP_PATH {config.sftp_path!r} and DB_ONLY_BACKUP_PATH {config.db_only_path!r} are the same "
        f"directory on the SFTP server ({resolved}); the database-only retention would delete the full "
        "backups there. Set DB_ONLY_BACKUP_PATH to a separate directory"
    )


def check_distinct_directories(config: Config, sftp: SFTPConnection) -> None:
    """Raise RunError when SFTP_PATH and DB_ONLY_BACKUP_PATH are one directory (see same_directory())."""
    resolved = same_directory(config, sftp)
    if resolved is not None:
        raise RunError(same_directory_error(config, resolved))


def plan_directories(
    config: Config, sftp: SFTPConnection, now: datetime.datetime, protect: Collection[str] = ()
) -> list[DirectoryRetention]:
    """List and plan the full-backup and the database-only directory; nothing is deleted.

    ``now`` is the naive local wall-clock time (the time zone of the file names). A directory
    that does not exist is skipped. Raises ValueError when the retention is disabled and
    RunError when both paths name the same server directory.
    """
    if config.retention is None:
        raise ValueError("retention is disabled")
    check_distinct_directories(config, sftp)
    entries = [
        DirectoryRetention(config.sftp_path, KIND_FULL, config.retention),
        DirectoryRetention(config.db_only_path, KIND_DB_ONLY, db_only_policy(config)),
    ]
    for entry in entries:
        if not sftp.exists(entry.directory):
            entry.skipped = "the directory does not exist"
        elif entry.policy is None:
            entry.skipped = (
                "database-only backups are never deleted in daily mode (they only come from --once --database-only)"
            )
        else:
            names = [remote.name for remote in sftp.list_files(entry.directory)]
            entry.plan = plan_retention(names, config.odoo_db_name, entry.policy, now, protect=protect)
    return entries


def log_directory_plan(entry: DirectoryRetention, db_name: str) -> None:
    """Log the plan of one directory: summary INFO, names DEBUG, future files WARNING."""
    plan, policy = entry.plan, entry.policy
    if plan is None or policy is None:  # plan_directories() only plans with a policy
        level = logging.DEBUG if entry.skipped == "the directory does not exist" else logging.INFO
        logger.log(level, "Retention for %s (%s backups) skipped: %s", entry.directory, entry.kind, entry.skipped)
        return
    logger.info(
        "Retention for %s (%s backups; %s): %s",
        entry.directory,
        entry.kind,
        policy.describe(),
        plan.summary(),
    )
    for name, reasons in plan.keep.items():
        logger.debug("Retention: keep %s (%s)", name, ", ".join(sorted(reasons)))
    for backup in plan.delete:
        logger.debug("Retention: delete %s", backup.name)
    if plan.future:
        logger.warning(
            "Retention: %d backup(s) in %s are dated more than a day in the future and are kept "
            "(clock or TZ changed?): %s",
            len(plan.future),
            entry.directory,
            ", ".join(backup.name for backup in plan.future[:10]),
        )
    if plan.ignored:
        logger.info(
            "Retention: %d file(s) in %s are not backups of database %r and are ignored",
            plan.ignored,
            entry.directory,
            db_name,
        )


def apply_retention(sftp: SFTPConnection, entry: DirectoryRetention, *, dry_run: bool) -> None:
    """Delete the planned files oldest first (or only log them with ``dry_run``)."""
    plan = entry.plan
    if plan is None or not plan.delete:
        return
    names = [backup.name for backup in plan.delete]
    if dry_run:
        more = len(names) - DRY_RUN_LIST_LIMIT
        logger.warning(
            "RETENTION_DRY_RUN: would delete %d backup(s) from %s: %s%s",
            len(names),
            entry.directory,
            ", ".join(names[:DRY_RUN_LIST_LIMIT]),
            f" and {more} more" if more > 0 else "",
        )
        return
    consecutive = 0
    for index, name in enumerate(names):
        path = posixpath.join(entry.directory, name)
        try:
            removed = sftp.remove(path)
        except SFTPError as exc:
            entry.failed += 1
            consecutive += 1
            logger.warning("Retention: cannot delete %s: %s", path, exc)
            if consecutive >= MAX_CONSECUTIVE_DELETE_FAILURES:
                entry.aborted = True
                logger.warning(
                    "Retention in %s aborted after %d failed deletions in a row (%d not attempted)",
                    entry.directory,
                    consecutive,
                    len(names) - index - 1,
                )
                return
            continue
        consecutive = 0
        if removed:
            entry.deleted += 1
            logger.info("Retention: deleted %s", path)
        else:
            entry.already_gone += 1
            logger.info("Retention: %s was already gone", path)


# ------------------------------------------------------------------------------------------
# One backup run
# ------------------------------------------------------------------------------------------


@dataclasses.dataclass
class RunResult:
    """Outcome of one run; ``stage`` is the failed stage (or the last one on success)."""

    run_id: str
    kind: str
    started_at: datetime.datetime
    ok: bool = False
    stage: str = STAGE_LOCK
    error: str | None = None
    file_name: str | None = None
    remote_path: str | None = None
    size: int | None = None
    sha256: str | None = None
    download_seconds: float | None = None
    upload_seconds: float | None = None
    retention: str = "not run"
    deleted: int = 0
    delete_failed: int = 0
    heartbeat_sent: bool = False
    total_seconds: float = 0.0
    directories: list[DirectoryRetention] = dataclasses.field(default_factory=list)


class _StageFailed(Exception):
    """Internal: a stage failed and its error is already stored in the RunResult."""


def run_backup(
    config: Config,
    full: bool,
    *,
    now: datetime.datetime | None = None,
    clock: Callable[[], datetime.datetime] | None = None,
    odoo_factory: Callable[..., OdooClient] = make_odoo_client,
    sftp_factory: Callable[..., SFTPConnection] = make_sftp_connection,
    state: StateStore | None = None,
    heartbeat: Callable[[str], bool] = send_heartbeat,
    wall_time: Callable[[], float] = time.time,
    disk_usage: Callable[[pathlib.Path], Any] = shutil.disk_usage,
) -> RunResult:
    """Run one backup (``full``: database + filestore, else database only); see the module docstring.

    ``now`` is the start time (default ``clock()``), ``clock`` returns the current aware time
    (default: the system clock) and is also used for the retention and the state file.
    ``wall_time`` is compared with the server's mtime in the clock check. The factories,
    ``state``, ``heartbeat`` and ``disk_usage`` are injectable for tests.
    """
    run = _BackupRun(
        config,
        full,
        now=now,
        clock=clock or _utc_now,
        odoo_factory=odoo_factory,
        sftp_factory=sftp_factory,
        state=state,
        heartbeat=heartbeat,
        wall_time=wall_time,
        disk_usage=disk_usage,
    )
    return run.execute()


class _BackupRun:
    def __init__(
        self,
        config: Config,
        full: bool,
        *,
        now: datetime.datetime | None,
        clock: Callable[[], datetime.datetime],
        odoo_factory: Callable[..., OdooClient],
        sftp_factory: Callable[..., SFTPConnection],
        state: StateStore | None,
        heartbeat: Callable[[str], bool],
        wall_time: Callable[[], float],
        disk_usage: Callable[[pathlib.Path], Any],
    ) -> None:
        self.config = config
        self.full = full
        self.clock = clock
        self.odoo_factory = odoo_factory
        self.sftp_factory = sftp_factory
        self.state = state
        self.heartbeat = heartbeat
        self.wall_time = wall_time
        self.disk_usage = disk_usage
        start = (now or clock()).astimezone(config.tz)
        self.start = start
        self.target_dir = config.sftp_path if full else config.db_only_path
        self.local_path: pathlib.Path | None = None
        self.unexpected: BaseException | None = None
        self.result = RunResult(
            run_id=secrets.token_hex(4),
            kind=KIND_FULL if full else KIND_DB_ONLY,
            started_at=start,
        )

    # -- orchestration -------------------------------------------------------------------

    def execute(self) -> RunResult:
        started = time.monotonic()
        result, config = self.result, self.config
        logger.info(
            "backup started: run_id=%s kind=%s db=%s format=%s target=%s@%s:%d:%s",
            result.run_id,
            result.kind,
            config.odoo_db_name,
            config.backup_format,
            config.sftp_user,
            config.sftp_host,
            config.sftp_port,
            self.target_dir,
        )
        try:
            try:
                self._stages()
                result.ok = True
            except _StageFailed:
                pass
            finally:
                self._remove_local_file()
        except (Shutdown, KeyboardInterrupt) as exc:
            reason = str(exc) or type(exc).__name__
            result.ok = False
            result.error = f"interrupted ({reason})"
            self._finish(started)
            raise
        self._finish(started)
        return result

    @contextlib.contextmanager
    def _stage(self, name: str) -> Iterator[None]:
        self.result.stage = name
        try:
            yield
        except _StageFailed:
            raise  # an inner stage failed and already recorded its error
        except Exception as exc:
            self.result.error = describe_error(exc, self.config)
            if not isinstance(exc, (RunError, *_EXPECTED_ERRORS)):
                self.unexpected = exc
            raise _StageFailed() from exc

    def _stages(self) -> None:
        with self._stage(STAGE_LOCK), self._run_lock():
            with self._stage(STAGE_SFTP_PREFLIGHT):
                newest_size = self._sftp_preflight()
            with self._stage(STAGE_DISK_PREFLIGHT):
                self._disk_preflight(newest_size)
            with self._stage(STAGE_ODOO):
                odoo = self.odoo_factory(self.config)
            with odoo:
                with self._stage(STAGE_ODOO):
                    file_name = self._prepare_file_name(odoo)
                with self._stage(STAGE_DOWNLOAD):
                    local_path = self._download(odoo, file_name)
            with contextlib.ExitStack() as stack:
                with self._stage(STAGE_UPLOAD):
                    sftp = self.sftp_factory(self.config)
                    stack.enter_context(sftp)  # connects
                    upload = self._upload(sftp, local_path, file_name)
                with self._stage(STAGE_CLOCK_CHECK):
                    self._check_clock(upload)
                with self._stage(STAGE_RETENTION):
                    self._retention(sftp)

    @contextlib.contextmanager
    def _run_lock(self) -> Iterator[None]:
        """Hold the run lock; an unusable state directory only costs the overlap protection."""
        with contextlib.ExitStack() as stack:
            if self.state is not None:
                try:
                    stack.enter_context(self.state.run_lock())
                except RunLockedError:
                    raise
                except OSError as exc:
                    logger.warning(
                        "Cannot take the run lock in %s (%s); continuing without it", self.state.directory, exc
                    )
            yield

    # -- stages ----------------------------------------------------------------------------

    def _sftp_preflight(self) -> int | None:
        """Check the SFTP side before Odoo spends time on a dump; returns the newest backup size."""
        config = self.config
        cutoff = self.start.replace(tzinfo=None)
        with self.sftp_factory(config) as sftp:
            sftp.ensure_dir(self.target_dir)
            # Before Odoo builds a dump: an upload into the other directory and its retention
            # would do the damage described in same_directory().
            check_distinct_directories(config, sftp)
            listing = sftp.list_files(self.target_dir)
            names = [remote.name for remote in listing]
            # Any policy will do: only the stale partials of the plan are used here.
            probe_policy = RetentionPolicy(1, 0, 0, 0, config.backup_time)
            plan = plan_retention(names, config.odoo_db_name, probe_policy, cutoff, partial_cutoff=cutoff)
            for partial in plan.stale_partials:
                path = posixpath.join(self.target_dir, partial.name)
                if config.retention_dry_run:
                    logger.warning("RETENTION_DRY_RUN: would remove the stale partial upload %s", path)
                    continue
                logger.warning("Removing the stale partial upload %s left by an earlier run", path)
                try:
                    sftp.remove(path)
                except SFTPError as exc:
                    logger.warning("Could not remove the stale partial upload %s: %s", path, exc)
        newest = None
        for remote in listing:
            parsed = parse_backup_name(remote.name)
            if parsed is None or parsed.partial or parsed.db != config.odoo_db_name:
                continue
            if newest is None or (parsed.ts, parsed.name) > (newest[0].ts, newest[0].name):
                newest = (parsed, remote.size)
        return None if newest is None else newest[1]

    def _disk_preflight(self, newest_size: int | None) -> None:
        tmp_dir = self.config.tmp_dir
        tmp_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not newest_size:
            logger.info("No earlier %s backup in %s: free space check skipped", self.result.kind, self.target_dir)
            return
        free = self.disk_usage(tmp_dir).free
        needed = int(newest_size * DISK_HEADROOM)
        if free < needed:
            raise RunError(
                f"not enough free space in BACKUP_TMP_DIR {tmp_dir}: {_format_bytes(free)} free, but the newest "
                f"{self.result.kind} backup has {_format_bytes(newest_size)} (need {_format_bytes(needed)})"
            )

    def _prepare_file_name(self, odoo: OdooClient) -> str:
        config = self.config
        serie = odoo.server_serie()
        file_name = build_file_name(serie, config.odoo_db_name, self.start.replace(tzinfo=None), config.backup_format)
        self.result.file_name = file_name
        return file_name

    def _download(self, odoo: OdooClient, file_name: str) -> pathlib.Path:
        local_path = self.config.tmp_dir / (file_name + ".part")
        self.local_path = local_path  # removed by _remove_local_file(), whatever happens
        download = odoo.download_backup(self.config.backup_format, local_path, with_filestore=self.full)
        self.result.size = download.size
        self.result.sha256 = download.sha256
        self.result.download_seconds = download.seconds
        return local_path

    def _upload(self, sftp: SFTPConnection, local_path: pathlib.Path, file_name: str) -> UploadResult:
        try:
            upload = sftp.upload(local_path, self.target_dir, file_name, attempts=self.config.sftp_upload_attempts)
        finally:
            self._remove_local_file()
        self.result.remote_path = upload.remote_path
        self.result.upload_seconds = upload.seconds
        return upload

    def _check_clock(self, upload: UploadResult) -> None:
        if upload.remote_mtime is None:
            logger.info("The SFTP server reports no modification time; clock check skipped")
            return
        skew = upload.remote_mtime - self.wall_time()
        if abs(skew) > CLOCK_SKEW_LIMIT:
            raise RunError(
                f"clock skew: the SFTP server's time differs from the local clock by {skew:+.0f} s "
                f"(limit {CLOCK_SKEW_LIMIT:.0f} s); retention skipped because it relies on the local "
                "clock (check NTP and TZ)"
            )

    def _retention(self, sftp: SFTPConnection) -> None:
        config, result = self.config, self.result
        if config.retention is None:
            result.retention = "disabled"
            raise RunError("retention disabled: " + "; ".join(config.retention_errors))
        now = self.clock().astimezone(config.tz).replace(tzinfo=None)
        protect = (result.file_name,) if result.file_name else ()
        entries = plan_directories(config, sftp, now, protect=protect)
        result.directories = entries
        for entry in entries:
            log_directory_plan(entry, config.odoo_db_name)
            apply_retention(sftp, entry, dry_run=config.retention_dry_run)
        result.deleted = sum(entry.deleted for entry in entries)
        result.delete_failed = sum(entry.failed for entry in entries)
        if config.retention_dry_run:
            would_delete = sum(len(entry.plan.delete) for entry in entries if entry.plan is not None)
            result.retention = f"dry-run/would-delete {would_delete}"
        else:
            result.retention = f"deleted {result.deleted}/failed {result.delete_failed}"
        if result.delete_failed:
            aborted = " (aborted after repeated failures)" if any(entry.aborted for entry in entries) else ""
            raise RunError(f"retention: {result.delete_failed} deletion(s) failed{aborted}")

    # -- clean-up and reporting ------------------------------------------------------------

    def _remove_local_file(self) -> None:
        path = self.local_path
        if path is None:
            return
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("Could not remove the local backup file %s: %s", path, exc)

    def _finish(self, started: float) -> None:
        result, config = self.result, self.config
        result.total_seconds = time.monotonic() - started
        state = self._record_state()
        if result.ok and config.heartbeat_url:
            overdue = self._full_backup_overdue(state)
            if overdue is None:
                result.heartbeat_sent = self.heartbeat(config.heartbeat_url)
            else:
                logger.warning("Heartbeat ping not sent: %s", overdue)
        if result.ok:
            logger.info(
                "backup finished: run_id=%s kind=%s file=%s bytes=%d download_s=%.1f upload_s=%.1f "
                "retention=%s total_s=%.1f",
                result.run_id,
                result.kind,
                result.file_name,
                result.size or 0,
                result.download_seconds or 0.0,
                result.upload_seconds or 0.0,
                result.retention,
                result.total_seconds,
            )
        else:
            logger.error(
                "backup failed: run_id=%s kind=%s stage=%s error=%s file=%s total_s=%.1f",
                result.run_id,
                result.kind,
                result.stage,
                result.error,
                result.file_name or "-",
                result.total_seconds,
                exc_info=self.unexpected,
            )

    def _record_state(self) -> State | None:
        """Record the outcome in the state file; returns the new state (None without one)."""
        if self.state is None:
            return None
        result = self.result
        now = self.clock()
        try:
            if result.ok and result.file_name is not None:  # a successful run always has a file name
                return self.state.record_success(now, result.file_name, full=self.full)
            return self.state.record_failure(
                now, f"{result.kind} backup failed at stage {result.stage}: {result.error}"
            )
        except OSError as exc:
            logger.error("Cannot update the state file %s: %s", self.state.path, exc)
            return None

    def _full_backup_overdue(self, state: State | None) -> str | None:
        """Why the heartbeat must wait for a full backup, or None.

        With database-only slots (HOURLY_BACKUP_FILESTORE=false) a successful database-only run
        must not report "backups are fine" while the daily full backup (database + filestore,
        the one restore tooling uses) keeps failing: the dead-man's switch has to go off.
        """
        max_age = self.config.full_backup_max_age
        if max_age is None or state is None:
            return None
        return full_backup_problem(state, self.clock(), max_age)


# ------------------------------------------------------------------------------------------
# Deployment checks (--check and service start-up)
# ------------------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class CheckResult:
    component: str
    ok: bool
    detail: str

    def line(self) -> str:
        return f"{'OK' if self.ok else 'FAIL'} {self.component}: {self.detail}"


def run_checks(
    config: Config,
    *,
    odoo_factory: Callable[..., OdooClient] = make_odoo_client,
    sftp_factory: Callable[..., SFTPConnection] = make_sftp_connection,
    report: Callable[[CheckResult], object] | None = None,
) -> list[CheckResult]:
    """Verify everything a backup run needs, without taking a backup.

    Checks the retention settings, Odoo (version, master password, database), the SFTP
    connection (host key, authentication) and the writability of the target directory (and of
    the database-only directory when HOURLY_BACKUP_FILESTORE=false); missing directories are
    created, as a backup run would. Fails when SFTP_PATH and DB_ONLY_BACKUP_PATH name the same
    directory on the server. ``report`` is called with
    every result as soon as it is known. Dependent checks are reported as failed without a
    network round-trip when their prerequisite failed, which keeps the worst case short.
    """
    results: list[CheckResult] = []

    def add(component: str, ok: bool, detail: str) -> None:
        check = CheckResult(component, ok, redact(detail, config))
        results.append(check)
        if report is not None:
            report(check)

    def failure(exc: BaseException) -> str:
        return describe_error(exc, config)

    if config.retention is None:
        add("retention", False, "disabled: " + "; ".join(config.retention_errors))
    else:
        dry_run = " (RETENTION_DRY_RUN: nothing is deleted)" if config.retention_dry_run else ""
        add("retention", True, config.retention.describe() + dry_run)
    _check_odoo(config, odoo_factory, add, failure)
    _check_sftp(config, sftp_factory, add, failure)
    return results


def _check_odoo(
    config: Config,
    odoo_factory: Callable[..., OdooClient],
    add: Callable[[str, bool, str], None],
    failure: Callable[[BaseException], str],
) -> None:
    try:
        odoo = odoo_factory(config, timeout=min(config.odoo_timeout, CHECK_ODOO_TIMEOUT))
    except Exception as exc:
        add("odoo", False, failure(exc))
        return
    with odoo:
        try:
            serie = odoo.server_serie()
        except Exception as exc:
            add("odoo", False, failure(exc))
            add("master-password", False, "not checked: Odoo is not reachable")
            add("database", False, "not checked: Odoo is not reachable")
            return
        add("odoo", True, f"Odoo {serie} at {odoo.display_url}")
        try:
            odoo.check_master_password()
        except Exception as exc:
            add("master-password", False, failure(exc))
        else:
            add("master-password", True, "accepted by Odoo")
        try:
            odoo.check_database()
        except Exception as exc:
            add("database", False, failure(exc))
        else:
            add("database", True, f"database {config.odoo_db_name!r} exists")


def _check_sftp(
    config: Config,
    sftp_factory: Callable[..., SFTPConnection],
    add: Callable[[str, bool, str], None],
    failure: Callable[[BaseException], str],
) -> None:
    check_db_only = config.hourly and not config.hourly_backup_filestore
    try:
        sftp = sftp_factory(config, timeout=min(config.sftp_timeout, CHECK_SFTP_TIMEOUT))
        sftp.connect()
    except Exception as exc:
        add("sftp", False, failure(exc))
        add("target-dir", False, "not checked: no SFTP connection")
        if check_db_only:
            add("db-only-dir", False, "not checked: no SFTP connection")
        return
    try:
        pinned = "pinned" if config.sftp_host_keys else "NOT pinned (set SFTP_HOST_KEY)"
        add(
            "sftp",
            True,
            f"{config.sftp_user}@{config.sftp_host}:{config.sftp_port}, host key {sftp.server_key_type} "
            f"{sftp.server_fingerprint} {pinned}, cipher {sftp.cipher}",
        )
        target_ok = False
        try:
            # Like every backup run, which creates SFTP_PATH when it is missing (a fresh
            # Storage Box); "created" in the line points at a mistyped path.
            existed = sftp.exists(config.sftp_path)
            sftp.ensure_dir(config.sftp_path)
            sftp.write_probe(config.sftp_path)
            add("target-dir", True, f"{config.sftp_path} {'exists' if existed else 'created'} and is writable")
            target_ok = True
        except Exception as exc:
            add("target-dir", False, failure(exc))
        if check_db_only and not target_ok:
            add("db-only-dir", False, "not checked: the target directory is not usable")
        elif check_db_only:
            try:
                existed = sftp.exists(config.db_only_path)
                sftp.ensure_dir(config.db_only_path)
                resolved = same_directory(config, sftp)
                if resolved is not None:
                    raise RunError(same_directory_error(config, resolved))
                sftp.write_probe(config.db_only_path)
            except Exception as exc:
                add("db-only-dir", False, failure(exc))
            else:
                state = "exists" if existed else "created"
                add("db-only-dir", True, f"{config.db_only_path} {state} and is writable")
        elif target_ok:
            # No database-only slots, but the retention still plans an existing DB_ONLY_BACKUP_PATH.
            try:
                resolved = same_directory(config, sftp)
            except Exception as exc:
                add("db-only-dir", False, failure(exc))
            else:
                if resolved is not None:
                    add("db-only-dir", False, same_directory_error(config, resolved))
    finally:
        sftp.close()


__all__ = [
    "CheckResult",
    "DirectoryRetention",
    "RunError",
    "RunResult",
    "Shutdown",
    "Watchdog",
    "apply_retention",
    "check_distinct_directories",
    "db_only_policy",
    "describe_error",
    "log_directory_plan",
    "make_odoo_client",
    "make_sftp_connection",
    "plan_directories",
    "redact",
    "run_backup",
    "run_checks",
    "same_directory",
]
