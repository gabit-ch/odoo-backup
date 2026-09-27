import random
import unittest
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from odoo_backup.retention import (
    PARTIAL_SUFFIX,
    REASON_DAILY,
    REASON_FUTURE,
    REASON_HOURLY,
    REASON_MONTHLY,
    REASON_NEWEST,
    REASON_PROTECTED,
    REASON_YEARLY,
    BackupFile,
    RetentionPolicy,
    build_file_name,
    parse_backup_name,
    plan_retention,
)
from odoo_backup.scheduler import build_slots, next_run_after

FORMATS = ("zip", "dump", "tar", "tar.gz", "tar.bz2", "tar.xz", "tar.zst")
ZURICH = ZoneInfo("Europe/Zurich")
# Production: BACKUP_EVERY_HOUR=2 from 01:00, HOURLY/DAILY/MONTHLY/YEARLY = 12/30/12/-1.
PROD = RetentionPolicy(keep_last=12, keep_daily=30, keep_monthly=12, keep_yearly=-1, anchor=time(1, 0))


def n(ts, serie="19.0", db="master", fmt="tar.gz"):
    return build_file_name(serie, db, ts, fmt)


def ts_of(name):
    return parse_backup_name(name).ts


def schedule(start, end, every=2, first_hour=1, minute=0, second=0, skip=()):
    """Naive wall-clock run times from the date of ``start`` until ``end`` (inclusive)."""
    hours = sorted({(first_hour + i * every) % 24 for i in range(24 // every)})
    day, result = date(start.year, start.month, start.day), []
    while day <= end.date():
        for hour in hours:
            ts = datetime(day.year, day.month, day.day, hour, minute, second)
            if start <= ts <= end and ts not in skip:
                result.append(ts)
        day += timedelta(days=1)
    return result


def kept_with(plan, reason):
    return sorted((name for name, reasons in plan.keep.items() if reason in reasons), key=ts_of)


def simulate(policy, names, db="master", initial=()):
    """Upload ``names`` in order and apply each plan like the service does after an upload.

    Returns (remaining names, deleted names in deletion order, largest deletion of one run).
    """
    files, deleted, largest = set(initial), [], 0
    for name in names:
        files.add(name)
        plan = plan_retention(files, db, policy, ts_of(name), protect={name})
        doomed = [b.name for b in plan.delete]
        files.difference_update(doomed)
        deleted.extend(doomed)
        largest = max(largest, len(doomed))
    return files, deleted, largest


def scheduler_names(start, end, tz, slots, fmt="tar.gz"):
    """Names written by the real scheduler between two aware instants (wall clock of ``tz``)."""
    names, now = [], start
    while True:
        now, _slot = next_run_after(now, slots, tz)
        if now > end:
            return names
        names.append(n(now.astimezone(tz).replace(tzinfo=None), fmt=fmt))


class ParseAndBuildTest(unittest.TestCase):
    TS = datetime(2026, 9, 27, 1, 0, 3)

    def test_round_trip_all_formats(self):
        for fmt in FORMATS:
            with self.subTest(fmt=fmt):
                name = build_file_name("19.0", "master", self.TS, fmt)
                self.assertEqual(name, f"odoo19.0-master-20260927-010003.{fmt}")
                self.assertEqual(
                    parse_backup_name(name),
                    BackupFile(ts=self.TS, name=name, serie="19.0", db="master", fmt=fmt, partial=False),
                )
                partial = parse_backup_name(name + PARTIAL_SUFFIX)
                self.assertEqual((partial.ts, partial.db, partial.fmt, partial.partial), (self.TS, "master", fmt, True))

    def test_db_names_with_separators_and_prefix_collisions(self):
        dbs = ("master", "master-old", "master_copy", "master-test", "tz-prod.v2_1", "a-20260101-010000")
        for db in dbs:
            with self.subTest(db=db):
                parsed = parse_backup_name(n(self.TS, db=db))
                self.assertEqual((parsed.serie, parsed.db, parsed.ts), ("19.0", db, self.TS))
        old = datetime(2020, 1, 1, 1)
        listing = [n(old, db=db) for db in dbs] + [n(self.TS)]
        policy = RetentionPolicy(keep_last=1, keep_daily=1, keep_monthly=0, keep_yearly=0, anchor=time(1))
        plan = plan_retention(listing, "master", policy, self.TS)
        self.assertEqual(plan.delete, (BackupFile(old, n(old), "19.0", "master", "tar.gz", False),))
        self.assertEqual(plan.ignored, len(dbs) - 1)

    def test_saas_serie(self):
        parsed = parse_backup_name("odoosaas~19.1-master-20260927-010003.zip")
        self.assertEqual((parsed.serie, parsed.db, parsed.fmt), ("saas~19.1", "master", "zip"))
        self.assertEqual(build_file_name("saas~19.1", "master", self.TS, "zip"), parsed.name)

    def test_partial_suffix(self):
        parsed = parse_backup_name("odoo19.0-master-20260927-010003.tar.gz.upload")
        self.assertTrue(parsed.partial)
        self.assertEqual(parsed.fmt, "tar.gz")
        self.assertIsNone(parse_backup_name("odoo19.0-master-20260927-010003.tar.gz.upload.upload"))

    def test_rejects_everything_else(self):
        rejected = (
            # foreign names
            "restore-notes-20260901-090000.txt",
            "master-before-cutover-20260101-120000.tar.gz",
            "Copy of odoo19.0-master-20260101-010000.tar.gz",
            "odoo19.0-master-20260101-010000 (1).tar.gz",
            "odoo19.0-master-20260101-010000-copy.tar.gz",
            "odoo19.0-master-20260927-010003",
            "odoo-master-20260927-010003.zip",
            "odoo19.0--20260927-010003.zip",
            ".odoo-backup-check-123-abc",
            "",
            # invalid dates and times
            "odoo19.0-master-20261327-010003.tar.gz",
            "odoo19.0-master-20260230-010003.tar.gz",
            "odoo19.0-master-20260927-250000.zip",
            "odoo19.0-master-20260927-016000.zip",
            "odoo19.0-master-2026927-010003.zip",
            "odoo19.0-master-\u0662\u0660\u0662\u06660927-010003.zip",  # non-ASCII (Arabic-Indic) digits
            # unknown extensions
            "odoo19.0-master-20260927-010003.rar",
            "odoo19.0-master-20260927-010003.TAR.GZ",
            "odoo19.0-master-20260927-010003.tar.gz.bak",
            "odoo19.0-master-20260927-010003.tgz",
            "odoo19.0-master-20260927-010003.zip.part",
            # '/' in the name
            "sub/odoo19.0-master-20260927-010003.zip",
            "odoo19.0-mas/ter-20260927-010003.zip",
            "odoo19/0-master-20260927-010003.zip",
        )
        for name in rejected:
            with self.subTest(name=name):
                self.assertIsNone(parse_backup_name(name))

    def test_build_refuses_names_retention_could_not_manage(self):
        for serie, db, fmt in (
            ("saas-19.1", "master", "zip"),
            ("19.0", "a/b", "zip"),
            ("19.0", "master", "rar"),
            ("19.0", "", "zip"),
        ):
            with self.subTest(serie=serie, db=db, fmt=fmt), self.assertRaises(ValueError):
                build_file_name(serie, db, self.TS, fmt)

    def test_backup_files_order_by_time_then_name(self):
        a, b, c = (
            parse_backup_name(x)
            for x in (n(self.TS, fmt="zip"), n(self.TS, fmt="tar.gz"), n(self.TS - timedelta(seconds=1), fmt="zip"))
        )
        self.assertEqual(sorted([a, b, c]), [c, b, a])


class PolicyValidationTest(unittest.TestCase):
    def policy(self, **overrides):
        values = {"keep_last": 0, "keep_daily": 30, "keep_monthly": 12, "keep_yearly": -1, "anchor": time(1, 0)}
        values.update(overrides)
        return RetentionPolicy(**values)

    def test_invalid_values(self):
        for overrides in (
            {"keep_last": -1},
            {"keep_daily": -1},
            {"keep_monthly": -5},
            {"keep_yearly": -2},
            {"keep_daily": True},
            {"keep_last": 1.0},
            {"keep_yearly": "1"},
            {"anchor": "01:00"},
            {"anchor": time(1, tzinfo=UTC)},
            {"keep_last": 0, "keep_daily": 0, "keep_monthly": 0, "keep_yearly": 0},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self.policy(**overrides)

    def test_valid_edge_values(self):
        self.assertEqual(self.policy(keep_yearly=-1).keep_yearly, -1)
        self.assertEqual(self.policy(keep_daily=0, keep_monthly=0, keep_yearly=1).keep_yearly, 1)
        self.assertEqual(self.policy(keep_last=1, keep_daily=0, keep_monthly=0, keep_yearly=0).keep_last, 1)

    def test_describe(self):
        self.assertEqual(PROD.describe(), "last 12, daily 30, monthly 12, yearly all (anchor 01:00:00)")


class PlanBasicsTest(unittest.TestCase):
    NOW = datetime(2026, 9, 27, 3, 0, 5)

    def test_empty_listing(self):
        plan = plan_retention([], "master", PROD, self.NOW)
        self.assertEqual(
            (dict(plan.keep), plan.delete, plan.stale_partials, plan.ignored, plan.future), ({}, (), (), 0, ())
        )
        self.assertEqual(
            plan.summary(), "keep 0 [hourly 0, daily 0, monthly 0, yearly 0], delete 0, ignored 0, future 0"
        )

    def test_summary_counts_reasons_per_rule(self):
        names = [n(ts) for ts in schedule(datetime(2024, 1, 1), self.NOW)]
        plan = plan_retention([*names, "notes.txt", "odoo19.0-other-20260101-010000.zip"], "master", PROD, self.NOW)
        self.assertEqual(
            plan.summary(),
            f"keep {len(plan.keep)} [hourly 12, daily 30, monthly 12, yearly 3], "
            f"delete {len(plan.delete)}, ignored 2, future 0",
        )
        self.assertEqual(len(plan.keep) + len(plan.delete), len(names))

    def test_delete_is_oldest_first_and_disjoint_from_keep(self):
        names = [n(ts) for ts in schedule(datetime(2025, 1, 1), self.NOW)]
        random.Random(3).shuffle(names)
        plan = plan_retention(names, "master", PROD, self.NOW)
        self.assertEqual(list(plan.delete), sorted(plan.delete))
        self.assertFalse({b.name for b in plan.delete} & set(plan.keep))
        self.assertEqual(list(plan.keep), sorted(plan.keep, key=ts_of))

    def test_aware_now_is_used_as_wall_clock(self):
        names = [n(datetime(2026, 9, 27, 1)), n(datetime(2026, 9, 28, 5))]
        naive = plan_retention(names, "master", PROD, datetime(2026, 9, 27, 3))
        aware = plan_retention(names, "master", PROD, datetime(2026, 9, 27, 3, tzinfo=ZURICH))
        self.assertEqual(naive, aware)
        self.assertEqual([b.name for b in aware.future], [names[1]])

    def test_negative_future_tolerance_is_rejected(self):
        with self.assertRaises(ValueError):
            plan_retention([], "master", PROD, self.NOW, future_tolerance=timedelta(seconds=-1))


class ProductionSimulationTest(unittest.TestCase):
    """BACKUP_EVERY_HOUR=2 from 01:00 with 12/30/12/-1, 2025-06-01 .. 2026-09-27."""

    @classmethod
    def setUpClass(cls):
        cls.runs = [n(ts) for ts in schedule(datetime(2025, 6, 1), datetime(2026, 9, 27, 23, 0))]
        cls.files, cls.deleted, cls.largest = simulate(PROD, cls.runs)
        cls.now = ts_of(cls.runs[-1])
        cls.plan = plan_retention(cls.files, "master", PROD, cls.now)

    def test_plan_is_a_fixed_point(self):
        self.assertEqual(self.plan.delete, ())
        self.assertEqual(len(self.files) + len(self.deleted), len(self.runs))

    def test_steady_state_counts(self):
        self.assertEqual(len(self.files), 53)
        counts = Counter(reason for reasons in self.plan.keep.values() for reason in reasons)
        self.assertEqual(
            (counts[REASON_HOURLY], counts[REASON_DAILY], counts[REASON_MONTHLY], counts[REASON_YEARLY]),
            (12, 30, 12, 2),
        )
        # A steady-state run deletes at most one or two files, never a burst.
        self.assertLessEqual(self.largest, 2)

    def test_hourly_are_the_last_twelve(self):
        self.assertEqual(kept_with(self.plan, REASON_HOURLY), self.runs[-12:])

    def test_daily_are_the_last_thirty_days_at_the_anchor(self):
        daily = [ts_of(name) for name in kept_with(self.plan, REASON_DAILY)]
        self.assertEqual(daily, [datetime(2026, 8, 29, 1) + timedelta(days=i) for i in range(30)])

    def test_monthly_are_the_first_of_the_last_twelve_months(self):
        monthly = [ts_of(name) for name in kept_with(self.plan, REASON_MONTHLY)]
        expected = [datetime(2025, 10, 1, 1), datetime(2025, 11, 1, 1), datetime(2025, 12, 1, 1)]
        expected += [datetime(2026, month, 1, 1) for month in range(1, 10)]
        self.assertEqual(monthly, expected)

    def test_yearly_are_the_first_backup_of_each_year(self):
        yearly = [ts_of(name) for name in kept_with(self.plan, REASON_YEARLY)]
        self.assertEqual(yearly, [datetime(2025, 6, 1, 1), datetime(2026, 1, 1, 1)])

    def test_no_new_backup_means_no_deletion(self):
        for later in (self.now + timedelta(days=1), self.now + timedelta(days=400)):
            with self.subTest(now=later):
                self.assertEqual(plan_retention(self.files, "master", PROD, later).delete, ())


class PlanRulesTest(unittest.TestCase):
    NOW = datetime(2026, 9, 27, 3, 0, 5)

    def test_current_bucket_counts(self):
        # The current (incomplete) day and month count as buckets.
        names = [n(datetime(2026, 9, day, 1)) for day in range(1, 28)] + [n(datetime(2026, 8, 31, 1))]
        policy = RetentionPolicy(keep_last=0, keep_daily=3, keep_monthly=1, keep_yearly=0, anchor=time(1))
        plan = plan_retention(names, "master", policy, self.NOW)
        self.assertEqual([ts_of(x).day for x in kept_with(plan, REASON_DAILY)], [25, 26, 27])
        self.assertEqual([ts_of(x) for x in kept_with(plan, REASON_MONTHLY)], [datetime(2026, 9, 1, 1)])
        self.assertEqual(len(plan.keep), 4)

    def test_outage_keeps_daily_backups_from_before_the_outage(self):
        runs = [
            n(ts)
            for ts in schedule(datetime(2026, 5, 1), self.NOW)
            if not date(2026, 7, 10) <= ts.date() <= date(2026, 9, 20)
        ]
        files, _deleted, largest = simulate(PROD, runs)
        plan = plan_retention(files, "master", PROD, self.NOW)
        daily = [ts_of(x).date() for x in kept_with(plan, REASON_DAILY)]
        self.assertEqual(len(daily), 30)
        # 7 days after the outage (09-21..09-27) + 23 days before it (06-17..07-09)
        self.assertEqual(daily[0], date(2026, 6, 17))
        self.assertEqual(daily[22], date(2026, 7, 9))
        self.assertEqual(daily[23], date(2026, 9, 21))
        # The first backups after the outage do not delete anything the outage made precious.
        self.assertLessEqual(largest, 2)
        monthly = [(ts_of(x).year, ts_of(x).month) for x in kept_with(plan, REASON_MONTHLY)]
        self.assertEqual(monthly, [(2026, 5), (2026, 6), (2026, 7), (2026, 9)])

    def test_missing_first_slot_of_the_month_falls_back_to_the_next_slot(self):
        runs = [n(ts) for ts in schedule(datetime(2026, 2, 1), self.NOW, skip={datetime(2026, 3, 1, 1)})]
        files, _, _ = simulate(PROD, runs)
        monthly = [ts_of(x) for x in kept_with(plan_retention(files, "master", PROD, self.NOW), REASON_MONTHLY)]
        self.assertIn(datetime(2026, 3, 1, 3), monthly)

    def test_missing_first_day_of_the_month_falls_back_to_the_next_day(self):
        runs = [n(ts) for ts in schedule(datetime(2026, 2, 1), self.NOW) if ts.date() != date(2026, 3, 1)]
        files, _, _ = simulate(PROD, runs)
        monthly = [ts_of(x) for x in kept_with(plan_retention(files, "master", PROD, self.NOW), REASON_MONTHLY)]
        self.assertIn(datetime(2026, 3, 2, 1), monthly)

    def test_anchor_wraps_to_the_earliest_backup_after_midnight(self):
        policy = RetentionPolicy(keep_last=0, keep_daily=1, keep_monthly=0, keep_yearly=0, anchor=time(1))
        day = [n(datetime(2026, 9, 26, 0, 0)), n(datetime(2026, 9, 26, 0, 30))]
        self.assertEqual(kept_with(plan_retention(day, "master", policy, self.NOW), REASON_DAILY), [day[0]])
        # A backup at/after the anchor wins over earlier ones, even late in the evening.
        day.append(n(datetime(2026, 9, 26, 23, 0)))
        self.assertEqual(kept_with(plan_retention(day, "master", policy, self.NOW), REASON_DAILY), [day[2]])

    def test_manual_run_before_midnight_does_not_displace_the_midnight_anchor(self):
        policy = RetentionPolicy(keep_last=0, keep_daily=3, keep_monthly=12, keep_yearly=-1, anchor=time(0))
        names = [n(datetime(2026, 9, day, 0, 0, 1)) for day in range(1, 11)]
        names.append(n(datetime(2026, 9, 1, 23, 50)))
        plan = plan_retention(names, "master", policy, datetime(2026, 9, 10, 1))
        self.assertEqual(kept_with(plan, REASON_MONTHLY), [names[0]])

    def test_no_grace_period_before_the_anchor(self):
        # The scheduler never starts before the slot, so an earlier backup is not an anchor backup.
        policy = RetentionPolicy(keep_last=0, keep_daily=1, keep_monthly=0, keep_yearly=0, anchor=time(1))
        names = [n(datetime(2026, 9, 26, 0, 59, 59)), n(datetime(2026, 9, 26, 3, 0))]
        self.assertEqual(kept_with(plan_retention(names, "master", policy, self.NOW), REASON_DAILY), [names[1]])

    def test_protected_and_newest_are_kept_without_hourly_and_daily_rules(self):
        policy = RetentionPolicy(keep_last=0, keep_daily=0, keep_monthly=1, keep_yearly=0, anchor=time(1))
        names = [n(ts) for ts in schedule(datetime(2026, 8, 1), self.NOW)]
        protected = names[-5]
        plan = plan_retention(names, "master", policy, self.NOW, protect={protected, "not-listed.zip"})
        self.assertEqual(plan.keep[names[-1]], frozenset({REASON_NEWEST}))
        self.assertEqual(plan.keep[protected], frozenset({REASON_PROTECTED}))
        self.assertEqual(set(plan.keep), {names[-1], protected, n(datetime(2026, 9, 1, 1))})

    def test_yearly_zero_keeps_no_yearly_but_never_the_newest_or_protected(self):
        """Regression: YEARLY_BACKUP_KEEP=0 used to delete everything outside the rules, incl. the upload."""
        policy = RetentionPolicy(keep_last=12, keep_daily=30, keep_monthly=12, keep_yearly=0, anchor=time(1))
        names = [n(ts) for ts in schedule(datetime(2024, 1, 1), self.NOW)]
        plan = plan_retention(names, "master", policy, self.NOW, protect={names[-1]})
        self.assertEqual(plan.count(REASON_YEARLY), 0)
        self.assertIn(REASON_NEWEST, plan.keep[names[-1]])
        self.assertIn(REASON_PROTECTED, plan.keep[names[-1]])
        self.assertIn(n(datetime(2024, 1, 1, 1)), {b.name for b in plan.delete})
        # Only one yearly representative -> the older years disappear, the rest is kept by the other rules.
        only_yearly = RetentionPolicy(keep_last=0, keep_daily=0, keep_monthly=0, keep_yearly=1, anchor=time(1))
        plan = plan_retention(names, "master", only_yearly, self.NOW, protect={names[-3]})
        self.assertEqual(set(plan.keep), {n(datetime(2026, 1, 1, 1)), names[-3], names[-1]})

    def test_other_databases_and_foreign_files_are_never_deleted(self):
        ours = [n(ts) for ts in schedule(datetime(2025, 1, 1), self.NOW, every=12)]
        foreign = [
            n(ts, db=db)
            for ts in schedule(datetime(2020, 1, 1), datetime(2020, 3, 1), every=12)
            for db in ("staging", "master-old", "Master")
        ]
        foreign += [
            "notes.txt",
            "odoo19.0-master-20200101-010000.zip.bak",
            "odoo19.0-staging-20200101-010000.zip.upload",
        ]
        plan = plan_retention(ours + foreign, "master", PROD, self.NOW, partial_cutoff=self.NOW)
        self.assertEqual(plan.ignored, len(foreign))
        touched = {b.name for b in plan.delete + plan.stale_partials} | set(plan.keep)
        self.assertFalse(touched & set(foreign))
        self.assertEqual(len(plan.keep) + len(plan.delete), len(ours))

    def test_mixed_series_and_formats_form_one_timeline(self):
        """17.0 zip backups until the cutover, 19.0 tar.gz afterwards: one database, one timeline."""
        cutover = datetime(2026, 9, 26, 12, 0)
        runs = [
            n(ts, serie="17.0", fmt="zip") if ts < cutover else n(ts)
            for ts in schedule(datetime(2025, 9, 15), self.NOW)
        ]
        files, _, largest = simulate(PROD, runs)
        plan = plan_retention(files, "master", PROD, self.NOW)
        self.assertEqual(plan.delete, ())
        self.assertLessEqual(largest, 2)
        self.assertEqual(plan.count(REASON_DAILY), 30)
        self.assertEqual(plan.count(REASON_MONTHLY), 12)
        self.assertEqual(plan.count(REASON_HOURLY), 12)
        hourly = kept_with(plan, REASON_HOURLY)
        self.assertEqual(Counter(parse_backup_name(x).serie for x in hourly), Counter({"19.0": 8, "17.0": 4}))
        self.assertEqual({parse_backup_name(x).fmt for x in kept_with(plan, REASON_MONTHLY)}, {"zip"})

    def test_stale_partials(self):
        fresh = n(self.NOW - timedelta(minutes=30)) + PARTIAL_SUFFIX
        stale = n(self.NOW - timedelta(days=3)) + PARTIAL_SUFFIX
        stale_protected = n(self.NOW - timedelta(days=2)) + PARTIAL_SUFFIX
        other_db = "odoo19.0-staging-20200101-010000.tar.gz.upload"
        complete = n(self.NOW - timedelta(hours=1))
        listing = [fresh, stale, stale_protected, other_db, complete]
        cutoff = self.NOW - timedelta(hours=1)
        plan = plan_retention(listing, "master", PROD, self.NOW, protect={stale_protected}, partial_cutoff=cutoff)
        self.assertEqual([p.name for p in plan.stale_partials], [stale])
        self.assertEqual(set(plan.keep), {complete})
        self.assertEqual((plan.delete, plan.ignored), ((), 1))
        # Without a cutoff nothing is stale.
        self.assertEqual(plan_retention(listing, "master", PROD, self.NOW).stale_partials, ())
        # Partials never occupy hourly slots or become the newest backup.
        partials = [n(self.NOW - timedelta(hours=2 * i)) + PARTIAL_SUFFIX for i in range(20)]
        old = n(datetime(2020, 1, 1, 1))
        plan = plan_retention([*partials, old], "master", PROD, self.NOW)
        self.assertEqual((plan.delete, set(plan.keep)), ((), {old}))

    def test_future_dated_backups_are_kept_and_excluded_from_the_rules(self):
        future = [n(datetime(2027, 1, 1, hour)) for hour in range(0, 24, 2)]
        real = [n(ts) for ts in schedule(datetime(2026, 9, 20), self.NOW)]
        plan = plan_retention(future + real, "master", PROD, self.NOW)
        self.assertEqual([b.name for b in plan.future], future)
        self.assertTrue(all(plan.keep[name] == frozenset({REASON_FUTURE}) for name in future))
        self.assertEqual(kept_with(plan, REASON_HOURLY), real[-12:])
        self.assertIn(REASON_NEWEST, plan.keep[real[-1]])
        self.assertTrue(plan.summary().endswith("future 12"))
        # The tolerance boundary: exactly now + 1 day is still a regular backup.
        edge = n(self.NOW.replace(microsecond=0) + timedelta(days=1))
        beyond = n(self.NOW.replace(microsecond=0) + timedelta(days=1, seconds=1))
        plan = plan_retention([edge, beyond], "master", PROD, self.NOW.replace(microsecond=0))
        self.assertEqual(
            (plan.keep[edge], plan.keep[beyond]),
            (
                frozenset({REASON_NEWEST, REASON_HOURLY, REASON_DAILY, REASON_MONTHLY, REASON_YEARLY}),
                frozenset({REASON_FUTURE}),
            ),
        )

    def test_same_timestamp_in_two_formats(self):
        ts = datetime(2026, 9, 27, 1, 0, 3)
        names = [n(ts, fmt="zip"), n(ts, fmt="tar.gz")]
        policy = RetentionPolicy(keep_last=1, keep_daily=1, keep_monthly=0, keep_yearly=0, anchor=time(1))
        for listing in (names, names[::-1]):
            plan = plan_retention(listing, "master", policy, self.NOW)
            # "…tar.gz" < "…zip": the zip sorts last and is chosen by every rule.
            self.assertEqual([b.name for b in plan.delete], [names[1]])
            self.assertEqual(plan.keep[names[0]], frozenset({REASON_NEWEST, REASON_HOURLY, REASON_DAILY}))

    def test_all_rules_are_evaluated_in_hourly_mode(self):
        """Regression: the old code consumed its file generator in the hourly rule, so the daily,
        monthly and yearly rules saw no files and old backups were never deleted in hourly mode."""
        names = [n(ts) for ts in schedule(datetime(2024, 6, 1), self.NOW)]
        policy = RetentionPolicy(keep_last=12, keep_daily=30, keep_monthly=12, keep_yearly=1, anchor=time(1))
        deleted = {b.name for b in plan_retention(names, "master", policy, self.NOW).delete}
        # Anchor-hour backups older than 30 days that are no month representative: daily rule.
        self.assertIn(n(datetime(2026, 7, 15, 1)), deleted)
        # Month representatives older than 12 months: monthly rule.
        self.assertIn(n(datetime(2025, 3, 1, 1)), deleted)
        # Year representative of an older year beyond keep_yearly: yearly rule.
        self.assertIn(n(datetime(2024, 6, 1, 1)), deleted)
        # Non-anchor slots older than the last 12: hourly rule.
        self.assertIn(n(datetime(2026, 9, 25, 3)), deleted)


class TimeZoneListingTest(unittest.TestCase):
    SLOTS_HOURLY_FROM_2 = build_slots(time(2, 0), 1, True)
    POLICY = RetentionPolicy(keep_last=24, keep_daily=30, keep_monthly=12, keep_yearly=-1, anchor=time(2))
    DAILY_ONLY = RetentionPolicy(keep_last=0, keep_daily=400, keep_monthly=0, keep_yearly=0, anchor=time(2))

    def test_dst_gap_in_europe_zurich(self):
        runs = scheduler_names(
            datetime(2026, 3, 27, tzinfo=ZURICH),
            datetime(2026, 3, 30, 23, 30, tzinfo=ZURICH),
            ZURICH,
            self.SLOTS_HOURLY_FROM_2,
        )
        spring = [x for x in runs if ts_of(x).date() == date(2026, 3, 29)]
        # 02:00 does not exist on 2026-03-29: the run happened at 03:00 (once) and represents the day.
        self.assertEqual(len(spring), 23)
        self.assertNotIn(n(datetime(2026, 3, 29, 2)), spring)
        plan = plan_retention(runs, "master", self.DAILY_ONLY, datetime(2026, 3, 31))
        self.assertEqual(
            [ts_of(x) for x in kept_with(plan, REASON_DAILY)],
            [datetime(2026, 3, 27, 2), datetime(2026, 3, 28, 2), datetime(2026, 3, 29, 3), datetime(2026, 3, 30, 2)],
        )
        files, _, largest = simulate(self.POLICY, runs)
        self.assertEqual(plan_retention(files, "master", self.POLICY, ts_of(runs[-1])).delete, ())
        self.assertLessEqual(largest, 2)

    def test_dst_fold_in_europe_zurich(self):
        runs = scheduler_names(
            datetime(2026, 10, 1, tzinfo=ZURICH),
            datetime(2026, 11, 5, 12, tzinfo=ZURICH),
            ZURICH,
            self.SLOTS_HOURLY_FROM_2,
        )
        # The repeated hour runs once, so the fold never produces the same name twice.
        self.assertEqual(len(runs), len(set(runs)))
        self.assertEqual(Counter(ts_of(x).date() for x in runs)[date(2026, 10, 25)], 24)
        files, _, largest = simulate(self.POLICY, runs)
        plan = plan_retention(files, "master", self.POLICY, ts_of(runs[-1]))
        self.assertEqual((plan.delete, plan.count(REASON_DAILY), plan.count(REASON_MONTHLY)), ((), 30, 2))
        self.assertLessEqual(largest, 2)
        self.assertTrue(all(ts_of(x).time() == time(2) for x in kept_with(plan, REASON_DAILY)))
        # A manual run in the repeated hour (02:30 CET, after 02:00 CEST) sorts by wall clock only.
        autumn = [x for x in runs if ts_of(x).date() == date(2026, 10, 25)] + [n(datetime(2026, 10, 25, 2, 30))]
        plan = plan_retention(autumn, "master", self.DAILY_ONLY, datetime(2026, 10, 26))
        self.assertEqual(kept_with(plan, REASON_DAILY), [n(datetime(2026, 10, 25, 2))])
        self.assertEqual(len(plan.delete), len(autumn) - 2)  # everything but the anchor and the newest

    def test_time_zone_change_mid_series(self):
        """TZ switched from Europe/Zurich to UTC on 2026-09-20 22:00 UTC with BACKUP_TIME unchanged."""
        slots = build_slots(time(1, 0), 2, True)
        switch = datetime(2026, 9, 20, 22, tzinfo=UTC)
        before = scheduler_names(datetime(2026, 5, 1, tzinfo=ZURICH), switch, ZURICH, slots)
        after = scheduler_names(switch, datetime(2026, 10, 15, tzinfo=UTC), ZoneInfo("UTC"), slots)
        # The first UTC slot repeats a wall-clock name; the service never overwrites, so that run fails.
        collisions = set(before) & set(after)
        self.assertEqual(collisions, {n(datetime(2026, 9, 20, 23))})
        runs = before + [x for x in after if x not in collisions]
        files, _, largest = simulate(PROD, runs)
        plan = plan_retention(files, "master", PROD, ts_of(runs[-1]))
        self.assertLessEqual(largest, 2)
        self.assertEqual(plan.delete, ())
        self.assertEqual((plan.count(REASON_HOURLY), plan.count(REASON_DAILY), plan.count(REASON_MONTHLY)), (12, 30, 6))


class DeterminismTest(unittest.TestCase):
    NOW = datetime(2026, 9, 27, 3, 0, 5)

    def setUp(self):
        self.names = [n(ts) for ts in schedule(datetime(2025, 1, 1), self.NOW)]
        self.names += [
            n(ts, serie="17.0", fmt="zip") for ts in schedule(datetime(2024, 11, 1), datetime(2025, 3, 1), every=6)
        ]
        self.names += ["notes.txt", n(self.NOW - timedelta(days=2)) + PARTIAL_SUFFIX, n(datetime(2027, 1, 1))]

    def test_idempotent(self):
        first = plan_retention(self.names, "master", PROD, self.NOW)
        rest = set(self.names) - {b.name for b in first.delete}
        second = plan_retention(rest, "master", PROD, self.NOW)
        self.assertEqual(second.delete, ())
        self.assertEqual(second.keep, first.keep)

    def test_listing_order_and_duplicates_do_not_matter(self):
        reference = plan_retention(self.names, "master", PROD, self.NOW, partial_cutoff=self.NOW)
        rng = random.Random(20260927)
        for _ in range(5):
            listing = self.names + rng.sample(self.names, 50)
            rng.shuffle(listing)
            self.assertEqual(plan_retention(listing, "master", PROD, self.NOW, partial_cutoff=self.NOW), reference)

    def test_monotonic_over_a_multi_year_history(self):
        """A deleted backup never shows up in a later keep set computed over the full history."""
        rng = random.Random(7)
        policy = RetentionPolicy(keep_last=3, keep_daily=5, keep_monthly=3, keep_yearly=2, anchor=time(1))
        scheduled = [ts for ts in schedule(datetime(2023, 1, 1), self.NOW, every=12) if rng.random() > 0.07]
        manual = [datetime(2023, 1, 2) + timedelta(minutes=rng.randrange(0, 60 * 24 * 1360)) for _ in range(60)]
        timeline = sorted(set(scheduled) | {t.replace(second=11) for t in manual})
        history, files, deleted = [], set(), set()
        for index, ts in enumerate(timeline):
            name = n(ts)
            history.append(name)
            files.add(name)
            plan = plan_retention(files, "master", policy, ts, protect={name})
            doomed = {b.name for b in plan.delete}
            self.assertFalse(doomed & set(plan.keep))
            files -= doomed
            deleted |= doomed
            if index % 25 == 0 or index == len(timeline) - 1:
                full = plan_retention(history, "master", policy, ts)
                self.assertFalse(set(full.keep) & deleted, f"resurrected at {ts}")
        self.assertGreater(len(deleted), 1500)
        self.assertEqual(set(plan_retention(history, "master", policy, self.NOW).keep), files)


if __name__ == "__main__":
    unittest.main()
