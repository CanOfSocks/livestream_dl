"""
End-to-end download scenarios against the local mock googlevideo server (no YouTube access).
Each scenario must finish within a time limit, otherwise it is reported as a hang.
"""
import logging
import math
import os
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest

from tests.support import (AUDIO_ITAG, VIDEO_ITAG, FastTimings, MockServer, MockStream, expected_ts, fake_segment,
                           make_info_dict, make_post_live_info, stream_url_for)

import yt_dlp

import download_Live
import getUrls
import YoutubeURL

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
        # The end-of-download report counts the gap
        self.assertEqual(coordinator.stats["video"]["missing_segments"], 1)
        self.assertEqual(coordinator.stats["video"]["segments_written"], self.total_segments - 1)

    def test_database_mode_reports_missing_segments(self):
        self.stream.fail(VIDEO_ITAG, 6, 404, times=1000)
        self.stream.fail(VIDEO_ITAG, 7, 404, times=1000)
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        with self.assertLogs(LOGGER, level="WARNING") as logs:
            run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, self.options(), manifest=url.manifest))
        self.assertEqual(coordinator.stats["video"]["missing_segments"], 2)
        self.assertTrue(any("2 of 20 segments (0-19) could not be downloaded: 6-7" in line for line in logs.output))

    def test_direct_mode_many_threads_gap_does_not_stall(self):
        # Regression: with 6+ threads the reorder window (10 x threads) exceeded the store's backlog
        # limit (50), so a failed gap segment was never resubmitted and the download hung
        self.stream.total_segments = 150
        self.stream.initial_head = 149
        self.stream.fail(VIDEO_ITAG, 1, 500, times=6)
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        options = self.options(threads=8, segment_retries=3)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream_direct(self.info, url, options, manifest=url.manifest),
                                   timeout=45)
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(150))))

    def test_direct_mode_survives_transient_write_errors(self):
        real_write = download_Live.write_segment
        failures = {"left": 3}

        def flaky_write(f, data, clean, first):
            if failures["left"] > 0 and f.tell() > 0:
                failures["left"] -= 1
                raise PermissionError(13, "File locked by another process")
            return real_write(f, data, clean, first)

        download_Live.write_segment = flaky_write
        try:
            coordinator = self.coordinator()
            url = stream_url_for(self.info, VIDEO_ITAG)
            file, _ = run_with_timeout(self, lambda: coordinator.download_stream_direct(self.info, url, self.options(), manifest=url.manifest))
        finally:
            download_Live.write_segment = real_write
        self.assertEqual(failures["left"], 0)
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(self.total_segments))))

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


class StreamEndRefreshTests(ScenarioBase):
    def test_post_live_dash_formats_without_base_url_mark_stream_ended(self):
        """
        Regression: at stream end the refreshed info only had DASH formats without a
        fragment_base_url. Building a URL from one raised a TypeError that skipped the live
        status update, so the downloader never learned the stream had ended.
        """
        post_live = make_info_dict(self.server, live_status="post_live")
        post_live["formats"] = [
            {"format_id": "399", "protocol": "http_dash_segments", "url": "https://h/api/manifest/dash/id/x",
             "ext": "mp4", "vcodec": "av01.0.08M.08", "acodec": "none", "height": 1080, "tbr": 3000},
            {"format_id": "140", "protocol": "http_dash_segments", "url": "https://h/api/manifest/dash/id/x",
             "ext": "m4a", "vcodec": "none", "acodec": "mp4a.40.2", "abr": 128, "tbr": 128},
        ]
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options(dash=True)) as stream:
            with self.assertNoLogs(stream.logger, level="ERROR"):
                new_manifest = stream._process_refresh_result((post_live, "post_live"), None, follow_manifest=False)
            self.assertFalse(new_manifest)
            self.assertEqual(stream.live_status, "post_live")
            self.assertEqual(stream.stream_url, url)

    def test_ended_stream_skips_manifest_search_without_warnings(self):
        """
        Once a refresh reports the stream has ended, its live formats are gone. The live status
        must be applied before the new-manifest search, which is skipped, so no warnings are logged.
        """
        ended = make_info_dict(self.server, live_status="was_live")
        ended["formats"] = []
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            with self.assertNoLogs(stream.logger, level="WARNING"):
                new_manifest = stream._process_refresh_result((ended, "was_live"), None, follow_manifest=False)
            self.assertFalse(new_manifest)
            self.assertEqual(stream.live_status, "was_live")
            self.assertEqual(stream.stream_url, url)


class HeaderProbeTests(ScenarioBase):
    def _download(self):
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, self.options(), manifest=url.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(self.total_segments))))

    def test_head_requests_probe_the_stream_head(self):
        self._download()
        self.assertGreater(self.stream.head_requests, 0)
        # No segment body is requested just for its headers
        self.assertEqual(self.stream.header_only_requests, 0)

    def test_get_fallback_when_head_unsupported(self):
        self.stream.head_supported = False
        self._download()
        self.assertGreater(self.stream.header_only_requests, 0)
        # HEAD is given up after the first probe shows GET is needed
        self.assertLessEqual(self.stream.head_requests, 1)


class RefreshValidationTests(ScenarioBase):
    def _refresh_with(self, stream, info):
        return stream._process_refresh_result((info, "is_live"), None, follow_manifest=False)

    def test_broken_refresh_keeps_working_url(self):
        url = stream_url_for(self.info, VIDEO_ITAG)
        # A broken extraction: its URLs are refused (the mock answers 403 for an expired URL)
        broken = make_info_dict(self.server, expire=int(time.time()) - 10)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            with self.assertLogs(stream.logger, level="WARNING") as logs:
                self._refresh_with(stream, broken)
            self.assertEqual(str(stream.stream_url), str(url))
            self.assertTrue(any("keeping the current URL" in line for line in logs.output))
            self.assertNotIn(str(stream_url_for(broken, VIDEO_ITAG)), [str(u) for u in stream.stream_urls])

    def test_working_refresh_is_used(self):
        url = stream_url_for(self.info, VIDEO_ITAG)
        fresh = make_info_dict(self.server, expire=int(time.time()) + 9000)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            self._refresh_with(stream, fresh)
            self.assertEqual(stream.stream_url.expire, stream_url_for(fresh, VIDEO_ITAG).expire)

    def test_unchecked_refresh_used_when_current_url_expiring(self):
        self.info = make_info_dict(self.server, expire=int(time.time()) + 60)
        url = stream_url_for(self.info, VIDEO_ITAG)
        broken = make_info_dict(self.server, expire=int(time.time()) - 10)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            self._refresh_with(stream, broken)
            self.assertEqual(str(stream.stream_url), str(stream_url_for(broken, VIDEO_ITAG)))


class RefreshRequestTests(ScenarioBase):
    def test_requested_refresh_is_made_on_the_next_check(self):
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options(), livestream_coordinator=coordinator) as stream:
            stream.url_checked = time.time()
            stream.next_refresh_at = time.time() + 3600
            self.assertIsNone(stream.refresh_Check(wait=True))
            calls = self.extraction_calls
            time.sleep(0.01)
            coordinator.request_refresh()
            self.assertIsNotNone(stream.refresh_Check(wait=True))
            self.assertEqual(self.extraction_calls, calls + 1)
            # Satisfied by that refresh
            self.assertIsNone(stream.refresh_Check(wait=True))


class PrivateReportTests(ScenarioBase):
    PRIVATE = getUrls.VideoInaccessibleError("Video abcdefghijk is private")

    def test_single_private_report_with_working_url_is_checked_again(self):
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            stream._process_refresh_result(None, self.PRIVATE, follow_manifest=False)
            self.assertFalse(stream.is_private)
            # A successful refresh in between resets the count
            stream._process_refresh_result((self.info, "is_live"), None, follow_manifest=False)
            stream._process_refresh_result(None, self.PRIVATE, follow_manifest=False)
            self.assertFalse(stream.is_private)
            stream._process_refresh_result(None, self.PRIVATE, follow_manifest=False)
            self.assertTrue(stream.is_private)

    def test_private_report_with_403s_is_believed(self):
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            stream.is_403 = True
            stream._process_refresh_result(None, self.PRIVATE, follow_manifest=False)
            self.assertTrue(stream.is_private)

    def test_unconfirmed_report_schedules_an_early_refresh(self):
        self.extraction_behaviour = lambda: (_ for _ in ()).throw(self.PRIVATE)
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            stream.refresh_url(wait=True)
            self.assertFalse(stream.is_private)
            self.assertLessEqual(stream.next_refresh_at, time.time() + download_Live.PRIVATE_CONFIRM_DELAY + 1)


class PostLiveTests(ScenarioBase):
    """Streams that have ended: yt-dlp relabels the live adaptive formats (see make_post_live_info)."""
    total_segments = 20
    initial_head = 19

    def _post_live_extraction(self):
        return make_post_live_info(self.server), "post_live"

    def test_refresh_after_end_uses_relabelled_format(self):
        url = stream_url_for(self.info, VIDEO_ITAG)
        expire = int(time.time()) + 7200
        post_live = make_post_live_info(self.server, expire=expire)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            new_manifest = stream._process_refresh_result((post_live, "post_live"), None, follow_manifest=False)
            self.assertFalse(new_manifest)
            self.assertEqual(stream.live_status, "post_live")
            self.assertEqual(stream.stream_url.protocol, "http_dash_segments")
            self.assertEqual(stream.stream_url.url_kind, "adaptive")
            self.assertEqual(stream.stream_url.expire, expire)
        self.assertEqual(post_live["formats"][0]["protocol"], "http_dash_segments")

    def test_target_duration_is_the_first_estimate(self):
        # Before any response headers arrive, yt-dlp's target_duration gives the segment length
        info = make_post_live_info(self.server)
        info["formats"][0]["target_duration"] = 5.0
        url = stream_url_for(info, VIDEO_ITAG)
        with download_Live.DownloadStream(info, stream_url=url, options=self.options()) as stream:
            # The header estimate (head time / head sequence) replaced it
            self.assertAlmostEqual(stream.estimated_segment_duration, self.nominal_duration, delta=0.05)
            stream._init_state()
            self.assertEqual(stream.estimated_segment_duration, 5.0)

    def test_download_starts_after_stream_ended(self):
        self.extraction_behaviour = self._post_live_extraction
        info = make_post_live_info(self.server)
        coordinator = self.coordinator()
        options = self.options(download_folder=self.tmp)
        formats = coordinator._select_formats(info, "bv+ba/best", options)
        urls = coordinator._stream_urls_from_formats(formats)
        self.assertEqual(sorted(u.itag for u in urls), sorted([VIDEO_ITAG, AUDIO_ITAG]))
        video = next(u for u in urls if u.itag == VIDEO_ITAG)
        self.assertEqual(video.protocol, "http_dash_segments")
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(info, video, options, manifest=video.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(self.total_segments))))
        # The extraction that found the stream ended is recent: no further refresh is needed to finish
        self.assertEqual(self.extraction_calls, 0)

    def test_extracted_post_live_urls_are_downloadable(self):
        """
        Getting post-live URLs through the real extraction path (with yt-dlp's extractor replaced by
        output shaped like yt-dlp 2026.10.26's for an ended stream) and downloading with them.
        """
        from yt_dlp.utils import LazyList
        expanded = []

        def fragment_generator():
            # yt-dlp's post-live generator requests the stream head and yields every segment
            expanded.append(True)
            yield {"url": "never used"}

        raw = make_post_live_info(self.server)
        for fmt in raw["formats"]:
            fmt["fragments"] = LazyList(fragment_generator())
        seen_options = {}

        class FakeYoutubeDL(yt_dlp.YoutubeDL):
            def __init__(self, params=None, *args, **kwargs):
                seen_options.update(params or {})
                super().__init__({"quiet": True}, *args, **kwargs)

            def extract_info(self, url, download=True, **kwargs):
                return raw

        original = getUrls.yt_dlp.YoutubeDL
        getUrls.yt_dlp.YoutubeDL = FakeYoutubeDL
        try:
            info, status = self._orig_get_info(self.info["id"], wait=False)
        finally:
            getUrls.yt_dlp.YoutubeDL = original

        self.assertEqual(status, "post_live")
        self.assertEqual(seen_options["extractor_args"]["youtube"]["formats"], ["incomplete", "duplicate"])
        self.assertFalse(expanded, "fragment lists must be dropped before sanitize_info expands them")
        self.assertTrue(all("fragments" not in fmt for fmt in info["formats"]))
        # The protocol returned by yt-dlp is kept
        self.assertEqual({fmt["protocol"] for fmt in info["formats"]}, {"http_dash_segments"})

        coordinator = self.coordinator()
        self.extraction_behaviour = self._post_live_extraction
        options = self.options()
        urls = coordinator._stream_urls_from_formats(coordinator._select_formats(info, "bv+ba/best", options))
        self.assertEqual(sorted(u.itag for u in urls), sorted([VIDEO_ITAG, AUDIO_ITAG]))
        self.assertTrue(all(u.url_kind == "adaptive" for u in urls))
        for url in urls:
            itag = url.itag
            file, _ = run_with_timeout(self, lambda: coordinator.download_stream(info, url, options, manifest=url.manifest))
            self.assertEqual(self.read(str(file)), expected_ts(self.segments(itag, range(self.total_segments))))
        # All URLs of the format, whatever yt-dlp's label, are available for recovery
        all_urls = YoutubeURL.Formats().getAllFormatURL(info, urls[0])
        self.assertEqual([u.itag for u in all_urls], [urls[0].itag])

    def _tail_requests(self):
        return sum(1 for itag, sq in self.stream.requests if itag == VIDEO_ITAG and sq in (18, 19))

    def test_missing_tail_of_ended_stream_is_given_up_quickly(self):
        # The last two sequence numbers of an ended stream often never exist
        self.stream.fail(VIDEO_ITAG, 18, 404, times=10000)
        self.stream.fail(VIDEO_ITAG, 19, 503, times=10000)
        self.extraction_behaviour = self._post_live_extraction
        info = make_post_live_info(self.server)
        url = stream_url_for(info, VIDEO_ITAG)
        coordinator = self.coordinator()
        options = self.options(segment_retries=10)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(info, url, options, manifest=url.manifest), timeout=30)
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(18))))
        self.assertEqual(coordinator.stats["video"]["missing_segments"], 2)
        # Full retries would be 11 submissions x 11 attempts per segment
        per_submission = download_Live.ENDED_TAIL_REQUEST_RETRIES + 1
        self.assertLessEqual(self._tail_requests(), 2 * download_Live.ENDED_TAIL_MAX_SUBMISSIONS * per_submission)

    def test_tail_answering_204_like_youtube(self):
        # YouTube answers the last segments of an ended stream with 204 (no content)
        self.stream.fail(VIDEO_ITAG, 18, 204, times=10000)
        self.stream.fail(VIDEO_ITAG, 19, 204, times=10000)
        self.extraction_behaviour = self._post_live_extraction
        info = make_post_live_info(self.server)
        url = stream_url_for(info, VIDEO_ITAG)
        coordinator = self.coordinator()
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(info, url, self.options(segment_retries=10), manifest=url.manifest),
                                   timeout=30)
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(18))))
        # After the end a 204 is final: one request per segment, no retries
        self.assertEqual(self._tail_requests(), 2)
        self.assertEqual(coordinator.stats["video"]["empty_segments"], 2)
        self.assertEqual(coordinator.stats["video"]["missing_segments"], 0)

    def test_retrying_204_stops_once_the_stream_ended(self):
        """Requests retrying a 204 when the end is detected stop at their next response."""
        self.stream.fail(VIDEO_ITAG, 19, 204, times=10000)
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            stream.latest_sequence = 19
            segment_url = stream._download_args(19)[0]
            # While live, a 204 is retried
            result = stream.download_segment(segment_url, 19, max_retries=2)
            self.assertEqual((result.status, result.data), (204, b""))
            self.assertEqual(self._tail_requests(), 3)
            # Once the stream has ended, the first 204 is the answer
            stream.live_status = "post_live"
            stream.download_segment(segment_url, 19, max_retries=2)
            self.assertEqual(self._tail_requests(), 4)
            # Older segments of an ended stream are still retried
            self.stream.fail(VIDEO_ITAG, 10, 204, times=10000)
            stream.download_segment(stream._download_args(10)[0], 10, max_retries=2)
            self.assertEqual(sum(1 for itag, sq in self.stream.requests if itag == VIDEO_ITAG and sq == 10), 3)

    def test_empty_tail_while_live_requests_early_refresh(self):
        # A live stream whose newest reported segment answers 204 after all retries has probably ended
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            stream._on_start()
            stream.url_checked = time.time() - download_Live.TAIL_FAILURE_REFRESH_MIN_INTERVAL - 1
            stream.next_refresh_at = time.time() + 3600
            stream._on_segment_result(19, download_Live.SegmentResult(19, b"", 19, 204, None), None)
            self.assertLessEqual(stream.next_refresh_at, time.time())
            # A 204 for a segment beyond the head (not created yet at the live edge) is normal
            stream.next_refresh_at = time.time() + 3600
            stream._on_segment_result(20, download_Live.SegmentResult(19, b"", 20, 204, None), None)
            self.assertGreater(stream.next_refresh_at, time.time() + 3000)

    def test_missing_tail_direct_mode(self):
        self.stream.fail(VIDEO_ITAG, 18, 404, times=10000)
        self.stream.fail(VIDEO_ITAG, 19, 404, times=10000)
        self.extraction_behaviour = self._post_live_extraction
        info = make_post_live_info(self.server)
        url = stream_url_for(info, VIDEO_ITAG)
        coordinator = self.coordinator()
        options = self.options(segment_retries=10)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream_direct(info, url, options, manifest=url.manifest), timeout=30)
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(18))))

    def test_existing_tail_segments_are_still_downloaded(self):
        # A transient 503 on the last segment must not lose it: it is tried more than once
        self.stream.fail(VIDEO_ITAG, 19, 503, times=3)
        self.extraction_behaviour = self._post_live_extraction
        info = make_post_live_info(self.server)
        url = stream_url_for(info, VIDEO_ITAG)
        coordinator = self.coordinator()
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(info, url, self.options(segment_retries=10), manifest=url.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(20))))


class DvrWindowTests(ScenarioBase):
    """
    A stream longer than YouTube's 7-day DVR window. Each segment is reported as 10000 s long, so
    7 days are about 60 segments: the head (100) is 11.6 days in, segments before 40 are gone.
    """
    total_segments = 101
    initial_head = 100
    nominal_duration = 10000.0

    def setUp(self):
        super().setUp()
        self.stream.oldest_available = 40
        self.extraction_behaviour = lambda: (make_post_live_info(self.server), "post_live")
        self.info = make_post_live_info(self.server)

    def _segment_requests(self, below: int) -> int:
        return sum(1 for itag, sq in self.stream.requests if sq is not None and sq < below)

    def test_oldest_available_segment_found_with_few_requests(self):
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            self.assertEqual(stream._find_dvr_floor(), 40)
        self.assertLessEqual(len(self.stream.requests), 12)

    def test_database_mode_skips_expired_segments(self):
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, self.options(), manifest=url.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(40, 101))))
        self.assertLessEqual(self._segment_requests(40), 12)

    def test_direct_mode_skips_expired_segments(self):
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream_direct(self.info, url, self.options(), manifest=url.manifest))
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(40, 101))))
        self.assertLessEqual(self._segment_requests(40), 12)

    def test_formats_share_one_check(self):
        coordinator = self.coordinator()
        video, audio = stream_url_for(self.info, VIDEO_ITAG), stream_url_for(self.info, AUDIO_ITAG)
        for url in (video, audio):
            run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, self.options(), manifest=url.manifest))
        self.assertLessEqual(self._segment_requests(40), 12)

    def test_constant_used_when_check_inconclusive(self):
        # Every old segment answers 403: nothing can be learned, so the 7-day constant is used
        for sq in range(0, 60):
            self.stream.fail(VIDEO_ITAG, sq, 403, times=5)
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            expected = 100 - math.ceil(download_Live.DVR_WINDOW_SECONDS / stream.estimated_segment_duration) + 1
            self.assertEqual(stream._find_dvr_floor(), expected)

    def test_window_option(self):
        url = stream_url_for(self.info, VIDEO_ITAG)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options(dvr_window=0)) as stream:
            self.assertEqual(stream._find_dvr_floor(), 0)
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options(dvr_window=24)) as stream:
            # 24 hours of 10000 s segments: the last 9 segments, no requests needed
            requests = len(self.stream.requests)
            self.assertEqual(stream._find_dvr_floor(), 100 - 9 + 1)
            self.assertEqual(len(self.stream.requests), requests)

    def test_short_stream_needs_no_check(self):
        self.stream.oldest_available = 0
        url = stream_url_for(self.info, VIDEO_ITAG)
        self.stream.nominal_duration = 1000.0  # 100 segments = 1.2 days
        with download_Live.DownloadStream(self.info, stream_url=url, options=self.options()) as stream:
            requests = len(self.stream.requests)
            self.assertEqual(stream._find_dvr_floor(), 0)
            self.assertEqual(len(self.stream.requests), requests)


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

    def test_capture_duration(self):
        # Stop 10 s of stream time after the live edge at the start (the stream keeps going)
        self.stream.total_segments = 60
        coordinator = self.coordinator()
        url = stream_url_for(self.info, VIDEO_ITAG)
        options = self.options(start_time=-0.0, capture_duration=10)
        file, _ = run_with_timeout(self, lambda: coordinator.download_stream(self.info, url, options, manifest=url.manifest))
        reference = coordinator._time_reference
        first, last = int(reference // 2.0), math.ceil((reference + 10) / 2.0)
        self.assertEqual(self.read(str(file)), expected_ts(self.segments(VIDEO_ITAG, range(first, last + 1))))

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

    def test_abandoned_chat_sources_removed_only_once_in_final_zip(self):
        import zipfile
        tmp = tempfile.mkdtemp()
        try:
            coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
            base = os.path.join(tmp, "out")
            json_file, part_file, frag_file = base + ".live_chat.json", base + ".live_chat.json.part", base + ".live_chat.json.part-Frag5"
            with open(part_file, "wb") as f:
                f.write(b'{"line": 1}\n')
            with open(frag_file, "wb") as f:
                f.write(b'{"responseContext": {}}')
            coordinator._package_live_chat(base, snapshot=True)
            self.assertTrue(os.path.exists(part_file), "snapshot must not delete sources")

            # Chat written after the snapshot is picked up by the refresh before the final move
            with open(part_file, "ab") as f:
                f.write(b'{"line": 2}\n')
            coordinator._refresh_live_chat_snapshot()

            final_zip = os.path.join(tmp, "final.live_chat.zip")
            # Before the zip is in its final location nothing is deleted
            coordinator._cleanup_live_chat_sources(final_zip)
            self.assertTrue(os.path.exists(part_file))
            shutil.move(base + ".live_chat.zip", final_zip)
            with zipfile.ZipFile(final_zip) as z:
                self.assertEqual(z.read("out.live_chat.json"), b'{"line": 1}\n{"line": 2}\n')

            # A file that grew after it was zipped is kept (its new data is not in the zip)
            with open(part_file, "ab") as f:
                f.write(b'{"line": 3}\n')
            coordinator._cleanup_live_chat_sources(final_zip)
            self.assertTrue(os.path.exists(part_file))

            # Unchanged since the snapshot: removed; raw fragment files are never zipped so they stay
            coordinator._live_chat_snapshot = {part_file: os.path.getsize(part_file)}
            coordinator._cleanup_live_chat_sources(final_zip)
            self.assertFalse(os.path.exists(part_file))
            self.assertFalse(os.path.exists(json_file))
            self.assertTrue(os.path.exists(frag_file))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
