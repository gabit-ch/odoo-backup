"""Count-based retention planning for the backup files on the SFTP server.

The module is pure: no I/O, no logging and no time zone arithmetic. Every timestamp is the
naive local wall-clock value the service writes into the file name
(``odoo{serie}-{db}-{YYYYmmdd-HHMMSS}.{fmt}``), and all comparisons use those values only.

A plan is computed from a directory listing and is deterministic, idempotent and
independent of the listing order. The rules are ORed; a backup is kept if any rule keeps
it. Representatives are computed over ALL eligible backups, hierarchically:

* ``hourly``: the ``keep_last`` newest backups.
* ``daily``: per calendar day, the first backup at or after the anchor time (the configured
  ``BACKUP_TIME``); if there is none that day, the earliest one after midnight. The
  representatives of the ``keep_daily`` newest days that have backups are kept. There is no
  grace period before the anchor: the scheduler never starts a run before its slot.
* ``monthly``: the daily representative of the earliest day with backups in the month.
* ``yearly``: the monthly representative of the earliest month with backups in the year.

The newest backup and the protected names (the file just uploaded) are always kept, even
when every rule keeps nothing. Because the rules count buckets that contain backups, and not
calendar time, a period without new backups (outage, stopped service) never deletes the
last good ones.
"""

import re
from collections import defaultdict
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from types import MappingProxyType
from typing import Any

TS_FORMAT = "%Y%m%d-%H%M%S"
PARTIAL_SUFFIX = ".upload"

REASON_PROTECTED = "protected"
REASON_NEWEST = "newest"
REASON_HOURLY = "hourly"
REASON_DAILY = "daily"
REASON_MONTHLY = "monthly"
REASON_YEARLY = "yearly"
REASON_FUTURE = "future"

_SECONDS_PER_DAY = 86400

# re.ASCII: "\d" must not match non-ASCII digits, which strptime would accept as well.
_NAME_RE = re.compile(
    r"odoo(?P<serie>[^-/]+)-(?P<db>[^/]+)-(?P<ts>\d{8}-\d{6})"
    r"\.(?P<fmt>zip|dump|tar|tar\.gz|tar\.bz2|tar\.xz|tar\.zst)"
    r"(?P<partial>\.upload)?",
    re.ASCII,
)


@dataclass(frozen=True, order=True)
class BackupFile:
    """A parsed backup file name. Ordering is chronological, ties broken by the name."""

    ts: datetime
    name: str
    serie: str
    db: str
    fmt: str
    partial: bool


def build_file_name(serie: str, db: str, ts: datetime, fmt: str) -> str:
    """Return ``odoo{serie}-{db}-{YYYYmmdd-HHMMSS}.{fmt}`` for the wall-clock time ``ts``.

    Raises ValueError when the result would not be recognised by parse_backup_name(),
    because such a file would never be managed (nor protected) by the retention.
    """
    name = f"odoo{serie}-{db}-{ts.strftime(TS_FORMAT)}.{fmt}"
    parsed = parse_backup_name(name)
    if parsed is None or parsed.partial or (parsed.serie, parsed.db, parsed.fmt) != (serie, db, fmt):
        raise ValueError(f"cannot build a backup file name from serie={serie!r}, db={db!r}, fmt={fmt!r}")
    return name


def parse_backup_name(name: str) -> BackupFile | None:
    """Parse a backup (or ``.upload`` partial) file name; anything else returns None."""
    match = _NAME_RE.fullmatch(name)
    if match is None:
        return None
    try:
        ts = datetime.strptime(match["ts"], TS_FORMAT)
    except ValueError:  # e.g. month 13 or hour 25
        return None
    return BackupFile(
        ts=ts,
        name=name,
        serie=match["serie"],
        db=match["db"],
        fmt=match["fmt"],
        partial=match["partial"] is not None,
    )


def _is_count(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class RetentionPolicy:
    """How many buckets of each kind to keep.

    ``keep_yearly`` = -1 keeps every year, 0 none. ``anchor`` is the preferred time of day of
    the daily representative (the configured BACKUP_TIME).
    """

    keep_last: int
    keep_daily: int
    keep_monthly: int
    keep_yearly: int
    anchor: time

    def __post_init__(self) -> None:
        for attr in ("keep_last", "keep_daily", "keep_monthly"):
            value = getattr(self, attr)
            if not _is_count(value) or value < 0:
                raise ValueError(f"{attr} must be an integer >= 0, got {value!r}")
        if not _is_count(self.keep_yearly) or self.keep_yearly < -1:
            raise ValueError(f"keep_yearly must be an integer >= -1, got {self.keep_yearly!r}")
        if not (self.keep_last or self.keep_daily or self.keep_monthly or self.keep_yearly):
            raise ValueError("all keep values are 0: refusing a policy that keeps nothing")
        if not isinstance(self.anchor, time) or self.anchor.tzinfo is not None:
            raise ValueError(f"anchor must be a naive datetime.time, got {self.anchor!r}")

    def describe(self) -> str:
        """Human readable one-liner for log lines, e.g. "last 12, daily 30, monthly 12, yearly all (anchor 01:00:00)"."""
        yearly = "all" if self.keep_yearly == -1 else str(self.keep_yearly)
        return (
            f"last {self.keep_last}, daily {self.keep_daily}, monthly {self.keep_monthly}, "
            f"yearly {yearly} (anchor {self.anchor.isoformat()})"
        )


@dataclass(frozen=True)
class RetentionPlan:
    """Result of plan_retention(); nothing is deleted by computing it.

    ``keep`` maps every kept name (oldest first) to the reasons that keep it. ``delete`` is
    ordered oldest first. ``stale_partials`` are leftover ``.upload`` files of earlier runs.
    ``ignored`` counts names that are not backups of the planned database (never deleted).
    ``future`` lists backups dated too far in the future; they are kept and excluded from
    the rules so they cannot displace real backups.
    """

    keep: Mapping[str, frozenset[str]]
    delete: tuple[BackupFile, ...]
    stale_partials: tuple[BackupFile, ...]
    ignored: int
    future: tuple[BackupFile, ...]

    def count(self, reason: str) -> int:
        """Number of kept files carrying ``reason`` (a file can carry several reasons)."""
        return sum(reason in reasons for reasons in self.keep.values())

    def summary(self) -> str:
        """E.g. "keep 54 [hourly 12, daily 30, monthly 12, yearly 3], delete 936, ignored 3, future 0"."""
        rules = ", ".join(
            f"{reason} {self.count(reason)}"
            for reason in (REASON_HOURLY, REASON_DAILY, REASON_MONTHLY, REASON_YEARLY)
        )
        return (
            f"keep {len(self.keep)} [{rules}], delete {len(self.delete)}, "
            f"ignored {self.ignored}, future {len(self.future)}"
        )


def _wall_clock(value: datetime) -> datetime:
    """Return the naive wall-clock value of ``value``.

    File names carry naive local wall-clock times. An aware datetime is reduced to its own
    wall-clock time (no conversion), so callers must pass it in the zone the names use.
    """
    return value.replace(tzinfo=None)


def _seconds_of_day(value: time | datetime) -> int:
    return value.hour * 3600 + value.minute * 60 + value.second


def _daily_representatives(backups: Iterable[BackupFile], anchor: time) -> dict[date, BackupFile]:
    """Map each day to its representative, ordered by day.

    The representative has the minimal rank ``(seconds_of_day(ts) - seconds_of_day(anchor))
    mod 86400``: the first backup at or after the anchor, wrapping to the earliest one after
    midnight. Several files with the same timestamp (other formats or series) share a rank;
    the one whose name sorts last wins, independent of the listing order.
    """
    anchor_seconds = _seconds_of_day(anchor)
    by_day: dict[date, list[BackupFile]] = defaultdict(list)
    for backup in backups:
        by_day[backup.ts.date()].append(backup)
    return {
        day: max(
            candidates,
            key=lambda b: (-((_seconds_of_day(b.ts) - anchor_seconds) % _SECONDS_PER_DAY), b.name),
        )
        for day, candidates in sorted(by_day.items())
    }


def _first_per_bucket(reps: Mapping[Any, BackupFile], bucket: Callable[[Any], Any]) -> dict[Any, BackupFile]:
    """Group the representatives by ``bucket(key)`` and keep the one of the earliest key per bucket."""
    result: dict[Any, BackupFile] = {}
    for key in sorted(reps):
        result.setdefault(bucket(key), reps[key])
    return result


def _newest_buckets(keys: Iterable[Any], count: int) -> list[Any]:
    """The ``count`` newest keys; -1 means all, 0 none (a plain ``[-count:]`` slice would return all for 0)."""
    ordered = sorted(keys)
    if count == -1:
        return ordered
    if count <= 0:
        return []
    return ordered[-count:]


def plan_retention(
    names: Iterable[str],
    db_name: str,
    policy: RetentionPolicy,
    now: datetime,
    protect: Collection[str] = (),
    future_tolerance: timedelta = timedelta(days=1),
    partial_cutoff: datetime | None = None,
) -> RetentionPlan:
    """Plan which backups of ``db_name`` to keep and which to delete.

    ``names`` is a plain directory listing. Only names that parse and whose database equals
    ``db_name`` exactly are managed; all series and formats of that database form one
    timeline. ``now`` and ``partial_cutoff`` are local wall-clock times (see _wall_clock()).
    Backups dated after ``now + future_tolerance`` are kept as "future". Partials are never
    kept or deleted by the rules; those older than ``partial_cutoff`` (if given) and not in
    ``protect`` are reported as ``stale_partials``.
    """
    if future_tolerance < timedelta(0):
        raise ValueError("future_tolerance must not be negative")
    horizon = _wall_clock(now) + future_tolerance
    cutoff = None if partial_cutoff is None else _wall_clock(partial_cutoff)
    protected = frozenset(protect)

    backups: list[BackupFile] = []
    partials: list[BackupFile] = []
    ignored = 0
    for name in dict.fromkeys(names):  # a name listed twice is still one file
        parsed = parse_backup_name(name)
        if parsed is None or parsed.db != db_name:
            ignored += 1
        elif parsed.partial:
            partials.append(parsed)
        else:
            backups.append(parsed)
    backups.sort()
    partials.sort()

    future = tuple(b for b in backups if b.ts > horizon)
    eligible = [b for b in backups if b.ts <= horizon]

    reasons: dict[str, set[str]] = defaultdict(set)
    for backup in future:
        reasons[backup.name].add(REASON_FUTURE)
    for backup in backups:
        if backup.name in protected:
            reasons[backup.name].add(REASON_PROTECTED)
    if eligible:
        reasons[eligible[-1].name].add(REASON_NEWEST)
    if policy.keep_last:
        for backup in eligible[-policy.keep_last:]:
            reasons[backup.name].add(REASON_HOURLY)

    daily = _daily_representatives(eligible, policy.anchor)
    monthly = _first_per_bucket(daily, lambda day: (day.year, day.month))
    yearly = _first_per_bucket(monthly, lambda month: month[0])
    for reps, count, reason in (
        (daily, policy.keep_daily, REASON_DAILY),
        (monthly, policy.keep_monthly, REASON_MONTHLY),
        (yearly, policy.keep_yearly, REASON_YEARLY),
    ):
        for key in _newest_buckets(reps, count):
            reasons[reps[key].name].add(reason)

    keep = {b.name: frozenset(reasons[b.name]) for b in backups if b.name in reasons}
    return RetentionPlan(
        keep=MappingProxyType(keep),
        delete=tuple(b for b in eligible if b.name not in keep),
        stale_partials=tuple(
            p for p in partials if cutoff is not None and p.ts < cutoff and p.name not in protected
        ),
        ignored=ignored,
        future=future,
    )
