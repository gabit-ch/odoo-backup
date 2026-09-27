"""Synchronous wall-clock scheduler (stdlib zoneinfo; replaces the ``schedule`` package).

Backups run at fixed local wall-clock times ("slots") in the configured time zone. The
loop runs the job in the calling thread, so two backups can never overlap: a run that
takes longer than the gap to the next slot makes the scheduler skip the slots that passed
meanwhile (and log how many) instead of starting them late or in parallel.

Daylight saving time follows PEP 495 ``fold=0`` semantics:

* spring forward (gap): a slot whose wall time does not exist that day runs at the same
  offset from the transition, e.g. 02:30 runs at 03:30 CEST;
* fall back (fold): a slot whose wall time occurs twice runs only at its first occurrence.
"""

import logging
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# A wake-up later than max_sleep plus this tolerance means the process was suspended or the
# clock jumped; the run still happens, but it is logged.
_LATE_TOLERANCE = timedelta(seconds=5)
# Upper bound for counting skipped slots after a clock jump of years.
_MAX_SKIPPED_COUNT = 10_000


@dataclass(frozen=True)
class Slot:
    """A daily wall-clock start time; ``full`` means database + filestore, else database only."""

    at: time
    full: bool

    @property
    def kind(self) -> str:
        return "full" if self.full else "database-only"


def build_slots(backup_time: time, every_hours: int | None, hourly_filestore: bool) -> list[Slot]:
    """Return the sorted daily slots.

    Daily mode (``every_hours`` None): one full slot at ``backup_time``. Hourly mode:
    ``24 // every_hours`` slots every ``every_hours`` hours starting at ``backup_time``
    (modulo 24, same minute and second). When 24 is not a multiple of ``every_hours`` the
    last gap of the day is longer; the next day starts again at ``backup_time``. The slot at
    ``backup_time`` is always full; the others are full only if ``hourly_filestore``.
    """
    if not isinstance(backup_time, time) or backup_time.tzinfo is not None:
        raise ValueError(f"backup_time must be a naive datetime.time, got {backup_time!r}")
    # fold=1 on the time would select the SECOND occurrence in a DST fold (PEP 495).
    backup_time = backup_time.replace(fold=0)
    if every_hours is None:
        return [Slot(backup_time, True)]
    if not isinstance(every_hours, int) or isinstance(every_hours, bool) or not 1 <= every_hours <= 24:
        raise ValueError(f"every_hours must be an integer between 1 and 24, got {every_hours!r}")
    hours = sorted({(backup_time.hour + i * every_hours) % 24 for i in range(24 // every_hours)})
    return [
        Slot(backup_time.replace(hour=hour), hour == backup_time.hour or hourly_filestore)
        for hour in hours
    ]


def next_run_after(now: datetime, slots: Sequence[Slot], tz: ZoneInfo) -> tuple[datetime, Slot]:
    """Return the next instant (aware, UTC) strictly after ``now`` that is a slot in ``tz``.

    ``now`` must be aware (any zone). Wall times are resolved with ``fold=0``: a time in a DST
    gap maps forward by the gap, a time in a DST fold maps to its first occurrence (the second
    occurrence is never returned). When two slots resolve to the same instant (the gap slot
    and the one after it in hourly mode), the full slot is returned so the filestore is not
    lost for that day.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be an aware datetime")
    if not slots:
        raise ValueError("no backup slots configured")
    local_day = now.astimezone(tz).date()
    best: tuple[datetime, Slot] | None = None
    # One day back covers zones that skipped a whole calendar day (e.g. Pacific/Apia 2011);
    # two days ahead always contain a slot.
    for offset in range(-1, 3):
        day = local_day + timedelta(days=offset)
        for slot in slots:
            instant = datetime.combine(day, slot.at, tzinfo=tz).astimezone(timezone.utc)
            if instant <= now:
                continue
            if best is None or instant < best[0] or (instant == best[0] and slot.full and not best[1].full):
                best = (instant, slot)
    if best is None:  # unreachable: every local day has at least one slot
        raise AssertionError("no slot found within three days")
    return best


def _count_slots_between(start: datetime, end: datetime, slots: Sequence[Slot], tz: ZoneInfo) -> int:
    """Number of distinct slot instants in the open interval (start, end)."""
    count = 0
    probe, _ = next_run_after(start, slots, tz)
    while probe < end and count < _MAX_SKIPPED_COUNT:
        count += 1
        probe, _ = next_run_after(probe, slots, tz)
    return count


def _log_next(at: datetime, slot: Slot, tz: ZoneInfo) -> None:
    logger.info("Next backup at %s (%s)", at.astimezone(tz).isoformat(), slot.kind)


def run_forever(
    job: Callable[[Slot], object],
    slots: Sequence[Slot],
    tz: ZoneInfo,
    stop: threading.Event,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    max_sleep: float = 60.0,
) -> None:
    """Run ``job(slot)`` at every slot until ``stop`` is set.

    The job runs synchronously in the calling thread. Exceptions (``Exception`` subclasses)
    of the job are logged with the traceback and never end the loop; ``BaseException``s such
    as KeyboardInterrupt or SystemExit propagate. ``stop`` is checked before sleeping and
    right after every job, so a signal handler that sets it ends the loop without starting
    another run. Sleeping happens in steps of at most ``max_sleep`` seconds, so wall-clock
    changes (NTP corrections, suspended hosts) are noticed within that time.

    After a job the next slot is computed from the CURRENT time: slots that passed while the
    job ran are skipped and counted in a WARNING, never run late or in parallel. A job never
    starts before its slot by the ``clock``, so a file name stamped at run start is never
    earlier than the slot time. If the loop wakes up long after a slot (suspended process,
    clock jump), that one slot runs immediately with a WARNING; slots missed beyond it are
    counted as skipped after the run.
    """
    if max_sleep <= 0:
        raise ValueError("max_sleep must be positive")
    next_at, slot = next_run_after(clock(), slots, tz)
    _log_next(next_at, slot, tz)
    while not stop.is_set():
        now = clock()
        remaining = (next_at - now).total_seconds()
        if remaining > 0:
            stop.wait(min(remaining, max_sleep))
            continue
        late = now - next_at
        if late > timedelta(seconds=max_sleep) + _LATE_TOLERANCE:
            logger.warning(
                "Starting the %s backup planned for %s %s late (process suspended or clock changed?)",
                slot.kind, next_at.astimezone(tz).isoformat(), late,
            )
        try:
            job(slot)
        except Exception:
            logger.exception("Backup job for the slot %s failed", next_at.astimezone(tz).isoformat())
        if stop.is_set():
            break
        finished = clock()
        following_at, following_slot = next_run_after(finished, slots, tz)
        skipped = _count_slots_between(next_at, following_at, slots, tz)
        if skipped:
            logger.warning(
                "Skipped %d backup slot(s): the run planned for %s ended at %s",
                skipped, next_at.astimezone(tz).isoformat(), finished.astimezone(tz).isoformat(),
            )
        next_at, slot = following_at, following_slot
        _log_next(next_at, slot, tz)
