import itertools
import threading
import unittest
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

from odoo_backup.scheduler import Slot, build_slots, next_run_after, run_forever

ZURICH = ZoneInfo("Europe/Zurich")
LOGGER = "odoo_backup.scheduler"


def zurich(*args):
    return datetime(*args, tzinfo=ZURICH)


class FakeWorld:
    """Fake clock that doubles as the stop event: wait() advances the time instead of sleeping."""

    def __init__(self, start, stop_at, job_duration=timedelta(0), error=None):
        self.now = start.astimezone(UTC)
        self.stop_at = stop_at.astimezone(UTC)
        self.job_duration = job_duration
        self.error = error
        self.runs = []  # (local start time, slot)
        self.waits = []
        self.jumps = {}  # wait number -> extra time (suspended process)

    def clock(self):
        return self.now

    def is_set(self):
        return self.now >= self.stop_at

    def wait(self, timeout):
        self.waits.append(timeout)
        self.now += timedelta(seconds=timeout) + self.jumps.get(len(self.waits), timedelta(0))
        return self.is_set()

    def job(self, slot):
        self.runs.append((self.now.astimezone(ZURICH), slot))
        self.now += self.job_duration
        if self.error is not None:
            raise self.error

    def run(self, slots, tz=ZURICH):
        run_forever(self.job, slots, tz, stop=self, clock=self.clock)
        return self.runs


def local_times(runs):
    return [start.strftime("%m-%d %H:%M%z") for start, _slot in runs]


class BuildSlotsTest(unittest.TestCase):
    def test_daily(self):
        for hourly_filestore in (True, False):
            self.assertEqual(build_slots(time(2, 30), None, hourly_filestore), [Slot(time(2, 30), True)])

    def test_every_two_hours_from_one(self):
        slots = build_slots(time(1, 0), 2, True)
        self.assertEqual([s.at for s in slots], [time(h) for h in range(1, 24, 2)])
        self.assertTrue(all(s.full for s in slots))

    def test_database_only_slots_keep_only_the_anchor_full(self):
        slots = build_slots(time(1, 0), 2, False)
        self.assertEqual(len(slots), 12)
        self.assertEqual([s.at for s in slots if s.full], [time(1)])
        self.assertEqual({s.kind for s in slots}, {"full", "database-only"})

    def test_every_five_hours(self):
        slots = build_slots(time(1, 0), 5, False)
        self.assertEqual(
            slots, [Slot(time(1), True), Slot(time(6), False), Slot(time(11), False), Slot(time(16), False)]
        )

    def test_every_twenty_four_hours(self):
        self.assertEqual(build_slots(time(1, 0), 24, False), [Slot(time(1), True)])

    def test_wraps_midnight_sorted_and_keeps_minutes_and_seconds(self):
        self.assertEqual(
            build_slots(time(22, 15, 30), 7, False),
            [Slot(time(5, 15, 30), False), Slot(time(12, 15, 30), False), Slot(time(22, 15, 30), True)],
        )

    def test_fold_is_normalised(self):
        (slot,) = build_slots(time(2, 30, fold=1), None, True)
        self.assertEqual(slot.at.fold, 0)

    def test_invalid_arguments(self):
        for every in (0, 25, -1, True, 2.0):
            with self.subTest(every=every), self.assertRaises(ValueError):
                build_slots(time(1), every, True)
        with self.assertRaises(ValueError):
            build_slots(time(1, tzinfo=UTC), None, True)


class NextRunAfterTest(unittest.TestCase):
    DAILY_0230 = build_slots(time(2, 30), None, True)

    def test_strictly_after_and_utc(self):
        slots = build_slots(time(1, 0), 2, False)
        at, slot = next_run_after(zurich(2026, 9, 27, 1, 0), slots, ZURICH)
        self.assertEqual(at, zurich(2026, 9, 27, 3, 0))
        self.assertIs(at.tzinfo, UTC)
        self.assertEqual(slot, Slot(time(3), False))
        at, slot = next_run_after(zurich(2026, 9, 27, 23, 30), slots, ZURICH)
        self.assertEqual((at, slot), (zurich(2026, 9, 28, 1, 0), Slot(time(1), True)))

    def test_now_in_another_zone(self):
        new_york = datetime(2026, 9, 26, 19, 30, tzinfo=ZoneInfo("America/New_York"))  # 01:30 in Zurich
        at, _slot = next_run_after(new_york, build_slots(time(1, 0), 2, True), ZURICH)
        self.assertEqual(at, zurich(2026, 9, 27, 3, 0))

    def test_invalid_arguments(self):
        with self.assertRaises(ValueError):
            next_run_after(datetime(2026, 9, 27, 1), self.DAILY_0230, ZURICH)
        with self.assertRaises(ValueError):
            next_run_after(zurich(2026, 9, 27, 1), [], ZURICH)

    def test_spring_gap_is_shifted_forward(self):
        at, _slot = next_run_after(zurich(2026, 3, 29, 0, 0), self.DAILY_0230, ZURICH)
        self.assertEqual(at.astimezone(ZURICH).isoformat(), "2026-03-29T03:30:00+02:00")

    def test_autumn_fold_runs_only_the_first_occurrence(self):
        first, _ = next_run_after(zurich(2026, 10, 25, 0, 0), self.DAILY_0230, ZURICH)
        self.assertEqual(first.astimezone(ZURICH).isoformat(), "2026-10-25T02:30:00+02:00")
        second, _ = next_run_after(first, self.DAILY_0230, ZURICH)
        self.assertEqual(second.astimezone(ZURICH).isoformat(), "2026-10-26T02:30:00+01:00")

    def test_full_slot_wins_when_the_gap_merges_two_slots(self):
        # Every hour, anchor 03:00: the 02:00 database-only slot maps onto the 03:00 full slot.
        slots = build_slots(time(3, 0), 1, False)
        at, slot = next_run_after(zurich(2026, 3, 29, 1, 30), slots, ZURICH)
        self.assertEqual((at.astimezone(ZURICH).isoformat(), slot), ("2026-03-29T03:00:00+02:00", Slot(time(3), True)))
        # Anchor 02:00: the shifted full 02:00 slot wins over the database-only 03:00 slot.
        slots = build_slots(time(2, 0), 1, False)
        at, slot = next_run_after(zurich(2026, 3, 29, 1, 30), slots, ZURICH)
        self.assertEqual((at.astimezone(ZURICH).isoformat(), slot), ("2026-03-29T03:00:00+02:00", Slot(time(2), True)))
        at, slot = next_run_after(at, slots, ZURICH)
        self.assertEqual((at.astimezone(ZURICH).isoformat(), slot), ("2026-03-29T04:00:00+02:00", Slot(time(4), False)))


class RunForeverDstTest(unittest.TestCase):
    EVERY_2_FROM_1 = build_slots(time(1, 0), 2, False)

    def test_spring_forward_every_two_hours(self):
        runs = FakeWorld(zurich(2026, 3, 29, 0, 30), zurich(2026, 3, 30, 0, 30)).run(self.EVERY_2_FROM_1)
        self.assertEqual(local_times(runs), ["03-29 01:00+0100"] + [f"03-29 {h:02d}:00+0200" for h in range(3, 24, 2)])
        self.assertEqual([slot.full for _start, slot in runs], [True] + [False] * 11)

    def test_fall_back_every_two_hours(self):
        runs = FakeWorld(zurich(2026, 10, 25, 0, 30), zurich(2026, 10, 26, 0, 30)).run(self.EVERY_2_FROM_1)
        self.assertEqual(local_times(runs), ["10-25 01:00+0200"] + [f"10-25 {h:02d}:00+0100" for h in range(3, 24, 2)])
        # The 25-hour day. Same-tzinfo subtraction is wall-clock arithmetic, so compare in UTC.
        self.assertEqual(runs[1][0].astimezone(UTC) - runs[0][0].astimezone(UTC), timedelta(hours=3))

    def test_daily_0230_across_both_transitions(self):
        slots = build_slots(time(2, 30), None, True)
        spring = FakeWorld(zurich(2026, 3, 27, 12), zurich(2026, 3, 31, 12)).run(slots)
        self.assertEqual(
            local_times(spring), ["03-28 02:30+0100", "03-29 03:30+0200", "03-30 02:30+0200", "03-31 02:30+0200"]
        )
        autumn = FakeWorld(zurich(2026, 10, 23, 12), zurich(2026, 10, 27, 12)).run(slots)
        self.assertEqual(
            local_times(autumn), ["10-24 02:30+0200", "10-25 02:30+0200", "10-26 02:30+0100", "10-27 02:30+0100"]
        )

    def test_every_hour_has_no_duplicate_or_missing_run(self):
        slots = build_slots(time(0, 0), 1, False)
        # 23-hour day: 02:00 does not exist, the 02:00 slot merges into 03:00 CEST.
        spring = FakeWorld(zurich(2026, 3, 28, 23, 30), zurich(2026, 3, 29, 23, 30)).run(slots)
        self.assertEqual([start.hour for start, _slot in spring], [0, 1, *range(3, 24)])
        starts = [start.astimezone(UTC) for start, _slot in spring]
        self.assertTrue(all(b - a == timedelta(hours=1) for a, b in itertools.pairwise(starts)))
        # 25-hour day: every wall time once; 02:00 CET (the repeated hour) is not a second run.
        autumn = FakeWorld(zurich(2026, 10, 24, 23, 30), zurich(2026, 10, 25, 23, 30)).run(slots)
        self.assertEqual([start.hour for start, _slot in autumn], list(range(24)))
        self.assertEqual(local_times(autumn)[2:4], ["10-25 02:00+0200", "10-25 03:00+0100"])


class RunForeverLoopTest(unittest.TestCase):
    EVERY_2_FROM_1 = build_slots(time(1, 0), 2, False)

    def test_logs_the_next_slot_with_its_kind(self):
        world = FakeWorld(zurich(2026, 9, 27, 0, 30), zurich(2026, 9, 27, 2, 0))
        with self.assertLogs(LOGGER, "INFO") as logs:
            world.run(self.EVERY_2_FROM_1)
        self.assertEqual(
            logs.output,
            [
                f"INFO:{LOGGER}:Next backup at 2026-09-27T01:00:00+02:00 (full)",
                f"INFO:{LOGGER}:Next backup at 2026-09-27T03:00:00+02:00 (database-only)",
            ],
        )
        self.assertTrue(world.waits and all(0 < w <= 60 for w in world.waits))

    def test_overrun_skips_slots_and_counts_them(self):
        world = FakeWorld(
            zurich(2026, 9, 27, 0, 30), zurich(2026, 9, 27, 10, 30), job_duration=timedelta(hours=2, minutes=30)
        )
        with self.assertLogs(LOGGER, "WARNING") as logs:
            runs = world.run(self.EVERY_2_FROM_1)
        self.assertEqual(local_times(runs), ["09-27 01:00+0200", "09-27 05:00+0200", "09-27 09:00+0200"])
        self.assertEqual(
            [r.getMessage() for r in logs.records],
            [
                "Skipped 1 backup slot(s): the run planned for 2026-09-27T01:00:00+02:00 "
                "ended at 2026-09-27T03:30:00+02:00",
                "Skipped 1 backup slot(s): the run planned for 2026-09-27T05:00:00+02:00 "
                "ended at 2026-09-27T07:30:00+02:00",
            ],
        )

    def test_long_overrun_counts_every_skipped_slot(self):
        world = FakeWorld(zurich(2026, 9, 27, 0, 30), zurich(2026, 9, 27, 7, 30), job_duration=timedelta(hours=5))
        with self.assertLogs(LOGGER, "WARNING") as logs:
            runs = world.run(self.EVERY_2_FROM_1)
        self.assertEqual(local_times(runs), ["09-27 01:00+0200", "09-27 07:00+0200"])
        self.assertIn("Skipped 2 backup slot(s)", logs.output[0])

    def test_late_start_after_a_suspension_runs_the_planned_slot_once(self):
        world = FakeWorld(zurich(2026, 9, 27, 0, 59), zurich(2026, 9, 27, 4, 0))
        world.jumps[1] = timedelta(hours=2, minutes=30)  # the first sleep lasts 2.5 h longer
        with self.assertLogs(LOGGER, "WARNING") as logs:
            runs = world.run(self.EVERY_2_FROM_1)
        self.assertEqual(local_times(runs), ["09-27 03:30+0200"])
        self.assertEqual(runs[0][1], Slot(time(1), True))
        self.assertIn("Starting the full backup planned for 2026-09-27T01:00:00+02:00 2:30:00 late", logs.output[0])
        self.assertIn("Skipped 1 backup slot(s)", logs.output[1])

    def test_job_exception_is_logged_and_does_not_stop_the_loop(self):
        world = FakeWorld(zurich(2026, 9, 27, 0, 30), zurich(2026, 9, 27, 6, 0), error=RuntimeError("boom"))
        with self.assertLogs(LOGGER, "ERROR") as logs:
            runs = world.run(self.EVERY_2_FROM_1)
        self.assertEqual(len(runs), 3)
        self.assertEqual(len(logs.records), 3)
        self.assertIsNotNone(logs.records[0].exc_info)
        self.assertEqual(logs.records[0].getMessage(), "Backup job for the slot 2026-09-27T01:00:00+02:00 failed")

    def test_base_exceptions_propagate(self):
        world = FakeWorld(zurich(2026, 9, 27, 0, 30), zurich(2026, 9, 28), error=KeyboardInterrupt())
        with self.assertRaises(KeyboardInterrupt):
            world.run(self.EVERY_2_FROM_1)
        self.assertEqual(len(world.runs), 1)

    def test_stop_set_by_the_job_ends_the_loop_immediately(self):
        world = FakeWorld(zurich(2026, 9, 27, 0, 30), zurich(2026, 9, 28))

        def job(slot):
            world.runs.append(slot)
            world.stop_at = world.now

        with self.assertLogs(LOGGER, "INFO") as logs:
            run_forever(job, self.EVERY_2_FROM_1, ZURICH, stop=world, clock=world.clock)
        self.assertEqual(world.runs, [Slot(time(1), True)])
        self.assertEqual(len(logs.output), 1)  # only the initial "Next backup" line

    def test_invalid_max_sleep(self):
        with self.assertRaises(ValueError):
            run_forever(lambda slot: None, self.EVERY_2_FROM_1, ZURICH, threading.Event(), max_sleep=0)


class RunForeverStopEventTest(unittest.TestCase):
    """With the real clock and a real threading.Event."""

    def test_stop_event_ends_a_waiting_loop(self):
        stop, calls = threading.Event(), []
        slots = build_slots(time(12, 0), None, True)
        # A clock pinned one hour before the slot: the loop would wait forever without the event.
        clock = lambda: datetime(2026, 9, 27, 11, tzinfo=UTC)  # noqa: E731
        thread = threading.Thread(
            target=run_forever, args=(calls.append, slots, UTC, stop), kwargs={"clock": clock, "max_sleep": 30.0}
        )
        with self.assertLogs(LOGGER, "INFO"):
            thread.start()
            stop.wait(0.2)
            stop.set()
            thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(calls, [])

    def test_stop_already_set_returns_without_running(self):
        stop, calls = threading.Event(), []
        stop.set()
        with self.assertLogs(LOGGER, "INFO"):
            run_forever(calls.append, build_slots(time(1), 1, True), UTC, stop)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
