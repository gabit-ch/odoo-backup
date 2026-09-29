"""Command line entry point: the scheduled service and the one-shot modes.

::

    python backup.py                              # service: backups at the configured slots
    python backup.py --once [--database-only]     # one backup now
    python backup.py --check                      # post-deployment verification
    python backup.py --retention-plan             # what the retention would keep and delete
    python backup.py --health                     # Docker HEALTHCHECK

Exit codes: 0 success; 1 failed run, failed check or unhealthy; 2 invalid configuration (service,
``--once`` and ``--retention-plan``); 3 the watchdog ended a run that exceeded
BACKUP_MAX_RUNTIME_MINUTES (service and ``--once``). ``--check`` and ``--health`` only ever
return 0 or 1.

The configuration comes from environment variables (see config.py and the README).
"""

import argparse
import contextlib
import datetime
import logging
import os
import pathlib
import platform
import select
import shutil
import signal
import sys
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterator, Mapping, Sequence

import paramiko

from . import __version__
from .config import Config, ConfigError, load_config
from .runner import (
    WATCHDOG_EXIT_CODE,
    CheckResult,
    RunError,
    Shutdown,
    Watchdog,
    db_only_policy,
    describe_error,
    make_sftp_connection,
    plan_directories,
    redact,
    run_backup,
    run_checks,
)
from .scheduler import Slot, build_slots, run_forever
from .sftp import SFTPError
from .state import StateStore, health_status

logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
#: --check must finish in less than two minutes even if every connection hangs; the service's
#: start-up checks stop waiting after the same time and start the schedule anyway.
CHECK_DEADLINE_SECONDS = 110.0
#: The watchdog waits at most this long for the state file to record the hung run.
WATCHDOG_STATE_WRITE_SECONDS = 5.0

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_CONFIG = 2

# Library loggers kept at WARNING or above even with LOG_LEVEL=DEBUG: paramiko logs every
# authentication step at INFO, urllib3 logs request lines (paths) at DEBUG.
_QUIET_LIBRARIES = ("paramiko", "urllib3")


# ------------------------------------------------------------------------------------------
# Arguments and logging
# ------------------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backup.py",
        description=(
            "Scheduled, verified Odoo backups to an SFTP server. Without an option the service runs "
            "backups at the configured times. The configuration comes from environment variables "
            "(see README.md)."
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check",
        action="store_true",
        help="verify Odoo, the master password, the database, the SFTP host key, login and target "
        "directories without taking a backup; exit 0 when everything is OK, else 1",
    )
    mode.add_argument(
        "--once",
        action="store_true",
        help="run one full backup now; exit 0 on success, else 1",
    )
    mode.add_argument(
        "--retention-plan",
        action="store_true",
        help="print what the retention would keep and delete in both backup directories; deletes nothing",
    )
    mode.add_argument(
        "--health",
        action="store_true",
        help="exit 0 when the last successful backup is recent enough (HEALTHCHECK_MAX_AGE_HOURS), else 1",
    )
    parser.add_argument(
        "--database-only",
        action="store_true",
        help="with --once: back up the database without the filestore into DB_ONLY_BACKUP_PATH",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def setup_logging(env: Mapping[str, str], *, quiet: bool = False, default_level: int = logging.INFO) -> None:
    """Configure the root logger: LOG_LEVEL when set, else ``default_level``; ``quiet`` shows errors only.

    The report commands (--check, --retention-plan) print their result on stdout and pass
    ``default_level=WARNING``, so the report is not interleaved with INFO logs on stderr; an
    explicit LOG_LEVEL still wins. Does nothing to the root handlers when they are already
    configured (e.g. by a test runner).
    """
    raw = (env.get("LOG_LEVEL") or "").strip()
    level = logging.getLevelNamesMapping().get(raw.upper(), None) if raw else default_level
    if level is None:
        level = default_level
    if quiet:
        level = max(level, logging.ERROR)
    logging.basicConfig(level=level, format=LOG_FORMAT, stream=sys.stderr)
    logging.captureWarnings(True)
    for name in _QUIET_LIBRARIES:
        logging.getLogger(name).setLevel(max(level, logging.WARNING))
    if raw and raw.upper() not in logging.getLevelNamesMapping():
        logger.warning("LOG_LEVEL=%r is not a logging level; using %s", raw, logging.getLevelName(default_level))


def main(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    configure_logging: bool = True,
) -> int:
    """Run the command line; returns the exit code. ``env`` defaults to os.environ."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.database_only and not args.once:
        parser.error("--database-only requires --once")
    environ = os.environ if env is None else env
    if configure_logging:
        report = args.check or args.retention_plan
        setup_logging(environ, quiet=args.health, default_level=logging.WARNING if report else logging.INFO)
    if args.health:
        return command_health(environ)
    if args.check:
        return command_check(environ)
    if args.retention_plan:
        return command_retention_plan(environ)
    if args.once:
        return command_once(environ, full=not args.database_only)
    return command_service(environ)


def _load_config(env: Mapping[str, str]) -> Config | None:
    try:
        return load_config(env)
    except ConfigError as exc:
        for problem in exc.problems:
            logger.error("Configuration problem: %s", problem)
        logger.error("Invalid configuration (%d problem(s)); see README.md for the variables", len(exc.problems))
        return None


# ------------------------------------------------------------------------------------------
# Signals
# ------------------------------------------------------------------------------------------


class StopEvent:
    """A stop flag whose ``set()`` is safe inside a signal handler.

    ``threading.Event.set()`` takes the event's internal lock; if the signal interrupts the main
    thread while it holds that lock inside ``Event.wait()``, the handler deadlocks. This event
    only flips a flag and writes a byte to a pipe; ``wait()`` sleeps in ``select()`` on that pipe,
    so a signal wakes it up immediately. It provides what scheduler.run_forever() uses of
    threading.Event: ``is_set()``, ``set()`` and ``wait(timeout)``.
    """

    def __init__(self) -> None:
        self._flag = False
        self._read_fd, self._write_fd = os.pipe()
        os.set_blocking(self._write_fd, False)

    def set(self) -> None:
        self._flag = True
        with contextlib.suppress(OSError):  # full pipe (already woken up) or closed
            os.write(self._write_fd, b"\0")

    def is_set(self) -> bool:
        return self._flag

    def wait(self, timeout: float | None = None) -> bool:
        if not self._flag:
            with contextlib.suppress(OSError, ValueError):
                select.select([self._read_fd], [], [], timeout)
        return self._flag

    def close(self) -> None:
        for fd in (self._read_fd, self._write_fd):
            with contextlib.suppress(OSError):
                os.close(fd)


class SignalHandler:
    """SIGTERM/SIGINT: always sets the stop event; while a run is active it raises Shutdown once.

    The Shutdown exception unwinds the run, so its ``finally`` blocks remove the temporary
    files and the run is recorded as interrupted. A second signal during that clean-up does not
    raise again.
    """

    def __init__(self, stop: StopEvent) -> None:
        self.stop = stop
        self.running = False
        self.raised = False
        self.received: str | None = None

    def __call__(self, signum: int, frame: object) -> None:
        self.received = signal.Signals(signum).name
        self.stop.set()
        if self.running and not self.raised:
            self.raised = True
            raise Shutdown(f"received {self.received}")


@contextlib.contextmanager
def installed_signal_handlers(handler: SignalHandler) -> Iterator[None]:
    """Install ``handler`` for SIGTERM and SIGINT (main thread only) and restore the old ones."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = {signum: signal.signal(signum, handler) for signum in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for signum, old in previous.items():
            signal.signal(signum, old)


# ------------------------------------------------------------------------------------------
# Service
# ------------------------------------------------------------------------------------------


class ServiceJob:
    """The scheduler's job: one watched backup run per slot (never raises for a failed run)."""

    def __init__(
        self,
        config: Config,
        state: StateStore,
        signals: SignalHandler,
        run: Callable[..., object] = run_backup,
    ) -> None:
        self.config = config
        self.state = state
        self.signals = signals
        self.run = run

    def __call__(self, slot: Slot) -> None:
        self.signals.running = True
        try:
            if self.signals.stop.is_set():
                return
            with Watchdog(self.config.max_runtime, on_timeout=watchdog_recorder(self.config, self.state, slot.full)):
                try:
                    self.run(self.config, slot.full, state=self.state)
                finally:
                    self.signals.running = False
        finally:
            self.signals.running = False


def watchdog_recorder(config: Config, state: StateStore, full: bool) -> Callable[[], None]:
    """Watchdog hook: record the hung run as a failure before the process ends.

    Best effort within WATCHDOG_STATE_WRITE_SECONDS (a hung file system must not keep the
    process alive), so ``--health`` and ``last_error`` name the reason for the restart.
    """
    kind = "full" if full else "database-only"
    error = (
        f"{kind} backup failed: the run did not finish within BACKUP_MAX_RUNTIME_MINUTES "
        f"({config.max_runtime}); the watchdog ended the process (exit code {WATCHDOG_EXIT_CODE})"
    )

    def write() -> None:
        try:
            state.record_failure(datetime.datetime.now(datetime.UTC), error)
        except OSError as exc:
            logger.error("Cannot update the state file %s: %s", state.path, exc)

    def record() -> None:
        writer = threading.Thread(target=write, name="odoo-backup-watchdog-state", daemon=True)
        writer.start()
        writer.join(WATCHDOG_STATE_WRITE_SECONDS)

    return record


def prepare_tmp_dir(path: pathlib.Path) -> int:
    """Create BACKUP_TMP_DIR (mode 0700) and delete its CONTENTS (not the directory itself).

    Leftovers are partial downloads of runs that were killed (watchdog, OOM, docker kill).
    Returns the number of removed entries.
    """
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        os.chmod(path, 0o700)
    except PermissionError as exc:
        logger.warning("Cannot restrict the permissions of BACKUP_TMP_DIR %s: %s", path, exc)
    removed = 0
    for entry in path.iterdir():
        try:
            if entry.is_dir() and not entry.is_symlink():
                shutil.rmtree(entry)
            else:
                entry.unlink()
        except OSError as exc:
            logger.warning("Could not remove %s from BACKUP_TMP_DIR: %s", entry, exc)
        else:
            removed += 1
    if removed:
        logger.info("Removed %d leftover entries from BACKUP_TMP_DIR %s", removed, path)
    return removed


def _display_url(url: str) -> str:
    """Scheme, host and port only: credentials (even malformed ones) and paths are never shown."""
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or "?"
    if ":" in host:
        host = f"[{host}]"  # IPv6 literal
    try:
        port = parts.port
    except ValueError:
        port = None
    return f"{parts.scheme}://{host}" + (f":{port}" if port is not None else "")


def describe_schedule(config: Config, slots: Sequence[Slot]) -> str:
    if not config.hourly:
        return f"daily at {config.backup_time.isoformat()} ({config.tz_name}), full backups"
    times = ", ".join(f"{slot.at.strftime('%H:%M')}{'' if slot.full else '*'}" for slot in slots)
    note = "; * = database-only" if not all(slot.full for slot in slots) else ", all full backups"
    return f"every {config.backup_every_hour} h from {config.backup_time.isoformat()} ({config.tz_name}): {times}{note}"


def log_config_summary(config: Config, slots: Sequence[Slot]) -> None:
    """Log the effective configuration without any secret."""
    logger.info("odoo-backup %s (Python %s, paramiko %s)", __version__, platform.python_version(), paramiko.__version__)
    logger.info(
        "Odoo: %s, database %r, format %s",
        redact(_display_url(config.odoo_url), config),
        config.odoo_db_name,
        config.backup_format,
    )
    logger.info("Schedule: %s", describe_schedule(config, slots))
    if config.sftp_host_keys:
        host_key = f"pinned ({len(config.sftp_host_keys)} SFTP_HOST_KEY entries)"
    else:
        host_key = "NOT pinned (set SFTP_HOST_KEY; the observed fingerprint is logged at every connection)"
    credentials = (("private key", config.sftp_private_key_file), ("password", config.sftp_password))
    methods = [name for name, value in credentials if value]
    logger.info(
        "SFTP: %s@%s:%d, full backups in %s, database-only backups in %s, host key %s, authentication: %s",
        config.sftp_user,
        config.sftp_host,
        config.sftp_port,
        config.sftp_path,
        config.db_only_path,
        host_key,
        " then ".join(methods),
    )
    if config.retention is None:
        logger.error(
            "Retention DISABLED, old backups are not deleted and every run is reported as failed: %s",
            "; ".join(config.retention_errors),
        )
    else:
        db_only = db_only_policy(config)
        logger.info(
            "Retention: %s%s; database-only backups: %s",
            config.retention.describe(),
            " (RETENTION_DRY_RUN: nothing is deleted)" if config.retention_dry_run else "",
            f"newest {db_only.keep_last}" if db_only else "never deleted (daily mode)",
        )
    full_limit = config.full_backup_max_age
    logger.info(
        "Heartbeat: %s; health check max age %s%s; max run time %s; tmp dir %s; state dir %s",
        "configured" if config.heartbeat_url else "not configured",
        config.healthcheck_max_age,
        f" (full backups {full_limit})" if full_limit is not None else "",
        config.max_runtime,
        config.tmp_dir,
        config.state_dir,
    )


def _log_check(result: CheckResult) -> None:
    logger.log(logging.INFO if result.ok else logging.WARNING, "Start-up check: %s", result.line())


def run_startup_checks(
    config: Config,
    stop: StopEvent,
    *,
    checks: Callable[..., list[CheckResult]] = run_checks,
    deadline: float = CHECK_DEADLINE_SECONDS,
) -> None:
    """Run the deployment checks once at service start; never blocks the schedule for long.

    The checks run in a daemon thread: the service waits for them at most ``deadline`` seconds
    and returns at once when a stop is requested (SIGTERM/SIGINT), so neither a hung server nor
    ``docker stop`` can keep the service in its start-up phase. A check still running after the
    deadline keeps logging in the background; every backup run connects on its own anyway.
    """
    failed: list[CheckResult] = []

    def target() -> None:
        try:
            failed.extend(result for result in checks(config, report=_log_check) if not result.ok)
        except Exception:
            logger.exception("The start-up checks failed unexpectedly")

    thread = threading.Thread(target=target, name="odoo-backup-startup-check", daemon=True)
    thread.start()
    ends = time.monotonic() + deadline
    while thread.is_alive():
        remaining = ends - time.monotonic()
        if remaining <= 0:
            logger.warning("The start-up checks did not finish within %g s; starting the schedule anyway", deadline)
            return
        if stop.wait(min(remaining, 0.5)):
            return
    if failed:
        logger.warning(
            "%d start-up check(s) failed; the service keeps running and every backup run retries", len(failed)
        )


def command_service(env: Mapping[str, str]) -> int:
    config = _load_config(env)
    if config is None:
        return EXIT_CONFIG
    if config.test_mode:
        logger.info("TEST_MODE is set: running one full backup now instead of the schedule")
        return _run_once(config, full=True)
    try:
        prepare_tmp_dir(config.tmp_dir)
    except OSError as exc:
        logger.error("Cannot prepare BACKUP_TMP_DIR %s: %s", config.tmp_dir, exc)
        return EXIT_CONFIG
    state = StateStore(config.state_dir)
    try:
        state.record_start(datetime.datetime.now(datetime.UTC))
    except OSError as exc:
        logger.error("Cannot write the state file %s: %s (the health check will fail)", state.path, exc)
    slots = build_slots(config.backup_time, config.backup_every_hour, config.hourly_backup_filestore)
    log_config_summary(config, slots)

    stop = StopEvent()
    signals = SignalHandler(stop)
    try:
        with installed_signal_handlers(signals):
            logger.info("Checking Odoo and the SFTP server before the first backup")
            run_startup_checks(config, stop)
            try:
                if not stop.is_set():  # SIGTERM/SIGINT during the start-up checks
                    run_forever(ServiceJob(config, state, signals), slots, config.tz, stop)
            except Shutdown:
                logger.info("Stopped by %s during a backup run (recorded as interrupted)", signals.received)
                return EXIT_OK
    finally:
        stop.close()
    logger.info("Stopped by %s", signals.received or "a stop request")
    return EXIT_OK


# ------------------------------------------------------------------------------------------
# One-shot modes
# ------------------------------------------------------------------------------------------


def _run_once(config: Config, full: bool) -> int:
    state = StateStore(config.state_dir)
    stop = StopEvent()
    signals = SignalHandler(stop)
    try:
        with installed_signal_handlers(signals):
            signals.running = True
            with Watchdog(config.max_runtime, on_timeout=watchdog_recorder(config, state, full)):
                try:
                    result = run_backup(config, full, state=state)
                finally:
                    signals.running = False
    except Shutdown:
        logger.info("Interrupted by %s; the run was recorded as failed", signals.received)
        return EXIT_FAILURE
    finally:
        stop.close()
    return EXIT_OK if result.ok else EXIT_FAILURE


def command_once(env: Mapping[str, str], full: bool) -> int:
    config = _load_config(env)
    if config is None:
        return EXIT_CONFIG
    return _run_once(config, full)


def _emit_check(result: CheckResult) -> None:
    """Print one ``--check`` result; stdout is the report, so the line is not logged again."""
    print(result.line(), flush=True)


def _config_line(config: Config) -> str:
    mode = f"every {config.backup_every_hour} h" if config.hourly else "daily"
    if config.hourly and not config.hourly_backup_filestore:
        mode += " (database-only between full backups)"
    return (
        f"database {config.odoo_db_name!r}, format {config.backup_format}, {mode} from "
        f"{config.backup_time.isoformat()} ({config.tz_name})"
    )


def command_check(env: Mapping[str, str]) -> int:
    """Post-deployment verification: one ``OK|FAIL <component>: <detail>`` line per check."""
    try:
        config = load_config(env)
    except ConfigError as exc:
        for problem in exc.problems:
            _emit_check(CheckResult("config", False, problem))
        return EXIT_FAILURE
    _emit_check(CheckResult("config", True, _config_line(config)))

    def timed_out() -> None:
        print(f"FAIL check: did not finish within {CHECK_DEADLINE_SECONDS:.0f} s", flush=True)

    with Watchdog(CHECK_DEADLINE_SECONDS, what="The deployment check", exit_code=EXIT_FAILURE, on_timeout=timed_out):
        results = run_checks(config, report=_emit_check)
    return EXIT_OK if all(result.ok for result in results) else EXIT_FAILURE


def command_retention_plan(env: Mapping[str, str]) -> int:
    """Print the retention plan of both backup directories; nothing is deleted."""
    config = _load_config(env)
    if config is None:
        return EXIT_CONFIG
    if config.retention is None:
        print("Retention is disabled: " + "; ".join(config.retention_errors), flush=True)
        return EXIT_FAILURE
    now = datetime.datetime.now(config.tz).replace(tzinfo=None)
    try:
        with make_sftp_connection(config) as sftp:
            entries = plan_directories(config, sftp, now)
    except SFTPError as exc:
        print(f"FAIL sftp: {describe_error(exc, config)}", flush=True)
        return EXIT_FAILURE
    except RunError as exc:
        print(f"FAIL directories: {describe_error(exc, config)}", flush=True)
        return EXIT_FAILURE
    for entry in entries:
        if entry.plan is None or entry.policy is None:
            print(f"{entry.directory} ({entry.kind} backups): skipped, {entry.skipped}")
            continue
        print(f"{entry.directory} ({entry.kind} backups; {entry.policy.describe()}): {entry.plan.summary()}")
        for backup in entry.plan.delete:
            print(f"  would delete {backup.name}")
        for backup in entry.plan.future:
            print(f"  kept, dated in the future: {backup.name}")
    print("Nothing was deleted (retention plan only).", flush=True)
    return EXIT_OK


def command_health(env: Mapping[str, str]) -> int:
    """Docker HEALTHCHECK: 0 when the last success is recent enough (or still starting), else 1."""
    try:
        config = load_config(env)
    except ConfigError as exc:
        print("unhealthy: invalid configuration: " + "; ".join(exc.problems), flush=True)
        return EXIT_FAILURE
    state = StateStore(config.state_dir).load()
    ok, message = health_status(
        state, datetime.datetime.now(datetime.UTC), config.healthcheck_max_age, config.full_backup_max_age
    )
    print(message, flush=True)
    return EXIT_OK if ok else EXIT_FAILURE
