"""
End-to-end download scenarios against the local mock googlevideo server (no YouTube access).
Each scenario must finish within a time limit, otherwise it is reported as a hang.
"""
import logging
import os
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest

from tests.support import (AUDIO_ITAG, VIDEO_ITAG, FastTimings, MockServer, MockStream, expected_ts, fake_segment,
                           make_info_dict, stream_url_for)

import download_Live
import getUrls

LOGGER = logging.getLogger("tests.downloads")
LOGGER.setLevel(logging.WARNING)
if os.environ.get("TEST_DEBUG"):
    LOGGER.setLevel(logging.DEBUG)
    LOGGER.addHandler(logging.StreamHandler())
else:
    LOGGER.addHandler(logging.NullHandler())
LOGGER.propagate = False

SCENARIO_TIMEOUT = 60.0


def run_with_timeout(test: unittest.TestCase, func, timeout: float = SCENARIO_TIMEOUT):
    """Run func in a thread; fail the test if it doesn't finish in time (a hang)."""
    result: dict = {}

    def target():
        try:
            result["value"] = func()
        except BaseException as e:  # noqa: BLE001 - re-raised in the test thread
            result["error"] = e

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        test.fail(f"Scenario did not finish within {timeout}s (possible hang)")
    if "error" in result:
        raise result["error"]
    return result.get("value")


class ScenarioBase(unittest.TestCase):
    total_segments = 20
    initial_head = 5
    # Segment length reported by the mock (drives the optimistic request pacing at the live edge)
    nominal_duration = 0.25

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.timings = FastTimings().__enter__()
        self.stream = MockStream(total_segments=self.total_segments, initial_head=self.initial_head, segment_duration=0.15,
                                 nominal_duration=self.nominal_duration)
        self.server = MockServer(self.stream).__enter__()
        self.info = make_info_dict(self.server)
        self.extraction_calls = 0
        self.extraction_behaviour = None  # optional callable raising / returning info
        self._orig_get_info = getUrls.get_Video_Info
        getUrls.get_Video_Info = self._fake_get_video_info

    def tearDown(self):
        getUrls.get_Video_Info = self._orig_get_info
        self.server.__exit__()
        self.timings.__exit__()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _fake_get_video_info(self, *args, **kwargs):
        self.extraction_calls += 1
        if self.extraction_behaviour:
            return self.extraction_behaviour()
        status = "post_live" if self.stream.ended() else "is_live"
        info = make_info_dict(self.server, live_status=status)
        return info, status

    def coordinator(self) -> download_Live.LiveStreamDownloader:
        coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
        coordinator.MIN_REFRESH_INTERVAL = 0.0
        coordinator.minimum_refresh_time = 0.0
        return coordinator

    def options(self, **extra) -> dict:
        options = {
            "filename": "out",
            "download_folder": self.tmp,
            "threads": 3,
            "segment_retries": 2,
            "batch_size": 3,
            "wait_limit": 0,
        }
        options.update(extra)
        return options

    def segments(self, itag: int, numbers) -> list[bytes]:
        return [fake_segment(itag, n) for n in numbers]

    def read(self, path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()


class LiveDownloadTests(ScenarioBase):
    def test_database_mode_full_stream_with_faults(self):
        self.stream.fail(VIDEO_ITAG, 3, 403, times=2)
        self.stream.fail(VIDEO_ITAG, 7, 500, times=4)
        self.stream.fail(VIDEO_ITAG, 12, 404, times=1)
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        file, filetype = run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, self.options(), manifest=url.manifest))
        self.assertEqual(filetype, "video")
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(self.total_segments))))
        # Temp database removed after merging to .ts
        self.assertFalse(os.path.exists(os.path.join(self.tmp, f"out.{VIDEO_ITAG}.temp")))
        self.assertIn(url.manifest, coordinator.file_names["streams"])
        # Header probes must not download segment bodies repeatedly; extraction only used to detect the end
        self.assertLessEqual(self.extraction_calls, 3)

    def test_audio_and_keep_database(self):
        coordinator = self.coordinator()
        url = stream_url_for(self.info, AUDIO_ITAG)
        options = self.options(keep_database_file=True, temp_db_journal="truncate")
        file, filetype = run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, options, manifest=url.manifest))
        self.assertEqual(filetype, "audio")
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(AUDIO_ITAG, range(self.total_segments))))
        db = os.path.join(self.tmp, f"out.{AUDIO_ITAG}.temp")
        self.assertTrue(os.path.exists(db))
        conn = sqlite3.connect(db)
        try:
            self.assertEqual(conn.execute("PRAGMA page_size").fetchone()[0], download_Live.SQLITE_AUDIO_PAGE_SIZE)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0], self.total_segments)
        finally:
            conn.close()
        self.assertEqual(len(coordinator.file_names["databases"]), 1)

    def test_direct_mode(self):
        self.stream.fail(VIDEO_ITAG, 4, 503, times=3)
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream_direct(self.info, url, self.options(), manifest=url.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(self.total_segments))))
        self.assertFalse(os.path.exists(os.path.join(self.tmp, f"out.{VIDEO_ITAG}.state")))

    def test_direct_mode_skips_permanently_missing_segment(self):
        self.stream.fail(VIDEO_ITAG, 6, 404, times=1000)
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream_direct(self.info, url, self.options(), manifest=url.manifest))
        expected = [n for n in range(self.total_segments) if n != 6]
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, expected)))

    def test_resume_from_existing_database(self):
        # A previous run already stored some segments
        db = os.path.join(self.tmp, f"out.{VIDEO_ITAG}.temp")
        store = download_Live.SQLiteSegmentStore(db, logger=LOGGER, batch_size=1)
        for n in (0, 1, 2):
            store.add(n, fake_segment(VIDEO_ITAG, n))
        store.flush(force=True)
        store.close()
        requests_before = len(self.stream.requests)
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, self.options(), manifest=url.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(self.total_segments))))
        requested = {sq for itag, sq in self.stream.requests[requests_before:] if sq is not None}
        self.assertFalse({0, 1, 2} & requested, "already stored segments must not be downloaded again")

    def test_graceful_stop(self):
        self.stream.total_segments = 10_000  # never ends on its own
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        threading.Timer(1.5, coordinator.graceful_stop.set).start()
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, self.options(), manifest=url.manifest), timeout=20)
        data = self.read(str(file))
        self.assertTrue(data.startswith(download_Live.clean_segment(fake_segment(VIDEO_ITAG, 0), first=True)))

    def test_kill(self):
        self.stream.total_segments = 10_000
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        threading.Timer(1.0, coordinator.kill_this.set).start()
        with self.assertRaises(KeyboardInterrupt):
            run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, self.options(), manifest=url.manifest), timeout=20)
        # Downloaded segments were flushed to the database on the way out (resumable)
        conn = sqlite3.connect(os.path.join(self.tmp, f"out.{VIDEO_ITAG}.temp"))
        try:
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0], 0)
        finally:
            conn.close()

    def test_url_refresh_on_403(self):
        # Expired URL in the info dict: every request is 403 until the URL is refreshed
        self.info = make_info_dict(self.server, expire=int(time.time()) + 2)
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        time.sleep(2.5)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, self.options(), manifest=url.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(self.total_segments))))
        self.assertGreaterEqual(self.extraction_calls, 1)


class TimeWindowTests(ScenarioBase):
    total_segments = 40
    initial_head = 30
    nominal_duration = 2.0

    def test_positive_window(self):
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        # Nominal segment duration is 2s: 10s..20s -> segments 5..10
        options = self.options(start_time=10, end_time=20)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, options, manifest=url.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(5, 11))))

    def test_negative_window_shared_between_formats(self):
        self.stream.segment_duration = 30.0  # head stays at 30 during the test
        coordinator = self.coordinator()
        video = stream_url_for(self.info, VIDEO_ITAG)
        audio = stream_url_for(self.info, AUDIO_ITAG)
        options = self.options(start_time=-10.0, end_time=-0.0)
        results = {}

        def run(url, key):
            results[key] = coordinator.download_stream(self.info, url, dict(options), manifest=url.manifest)

        def both():
            threads = [threading.Thread(target=run, args=(video, "v")), threading.Thread(target=run, args=(audio, "a"))]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        run_with_timeout(self, both)
        # Head 30 at 2s/segment = 60s; -10s..-0s -> 50s..60s -> segments 25..30
        self.assertEqual(self.read(str(results["v"][0])), expected_ts(self.segments(VIDEO_ITAG, range(25, 31))))
        self.assertEqual(self.read(str(results["a"][0])), expected_ts(self.segments(AUDIO_ITAG, range(25, 31))))
        self.assertEqual(coordinator._time_reference, 60.0)

    def test_direct_mode_window(self):
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        options = self.options(start_time=20, end_time=30)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream_direct(self.info, url, options, manifest=url.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(10, 16))))


class RecoveryTests(ScenarioBase):
    total_segments = 15
    initial_head = 14  # the whole stream is available (ended)

    def test_recovery_mode_fills_missing_segments(self):
        db = os.path.join(self.tmp, f"out.{VIDEO_ITAG}.temp")
        store = download_Live.SQLiteSegmentStore(db, logger=LOGGER, batch_size=1)
        for n in range(0, 15, 2):
            store.add(n, fake_segment(VIDEO_ITAG, n))
        store.flush(force=True)
        store.close()
        self.stream.fail(VIDEO_ITAG, 5, 403, times=3)
        self.info["live_status"] = "post_live"
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        options = self.options(recovery=True, segment_retry_time=0.1)
        file, _ = run_with_timeout(self, lambda: coordinator.recover_stream(self.info, url, options, manifest=url.manifest, stream_urls=[url]))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(15))))

    def test_recovery_failure_raises_and_keeps_database(self):
        self.stream.fail(VIDEO_ITAG, 3, 404, times=1000)
        self.info["live_status"] = "post_live"
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        options = self.options(recovery=True, segment_retry_time=0.05, segment_retries=1)
        with self.assertRaises(getUrls.VideoDownloadError):
            run_with_timeout(self, lambda: coordinator.recover_stream(self.info, url, options, manifest=url.manifest, stream_urls=[url]))
        self.assertTrue(os.path.exists(os.path.join(self.tmp, f"out.{VIDEO_ITAG}.temp")))
        # With a tolerance the partial result is merged instead
        options = self.options(recovery=True, segment_retry_time=0.05, segment_retries=1, recovery_failure_tolerance=1)
        file, _ = run_with_timeout(self, lambda: coordinator.recover_stream(self.info, url, options, manifest=url.manifest, stream_urls=[url]))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, [n for n in range(15) if n != 3])))

    def test_private_handoff_writes_into_original_database(self):
        # Segment 10 fails with 403s; the refresh then reports the video as private, so the live
        # downloader hands over to recovery, which must complete the SAME database.
        self.stream.fail(VIDEO_ITAG, 10, 403, times=6)

        def private_extraction():
            raise getUrls.VideoInaccessibleError("Video abcdefghijk is private")
        self.extraction_behaviour = private_extraction
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        options = self.options(segment_retry_time=0.1)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, options, manifest=url.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(15))))
        # Nothing was written to a stray database named after the video ID in the working directory
        self.assertFalse(os.path.exists(f"abcdefghijk.{VIDEO_ITAG}.temp"))


class LiveChatTimeoutTests(unittest.TestCase):
    def test_hanging_chat_is_abandoned_and_kept(self):
        tmp = tempfile.mkdtemp()
        try:
            with FastTimings():
                coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
                options = {"filename": "out", "temp_folder": tmp, "stop_chat_when_done": 1}
                info = {"id": "abcdefghijk"}
                chat_file = os.path.join(tmp, "out.live_chat.json")
                with open(chat_file, "w") as f:
                    f.write('{"chat": 1}\n')
                never = threading.Event()
                hanging = threading.Thread(target=never.wait, daemon=True)
                hanging.start()
                start = time.monotonic()
                coordinator._wait_for_live_chat(hanging, info, options)
                self.assertLess(time.monotonic() - start, 8)
                self.assertTrue(coordinator.chat_stop.is_set())
                self.assertIn("live_chat", coordinator.file_names)
                self.assertTrue(os.path.exists(os.path.join(tmp, "out.live_chat.zip")))
                never.set()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
