"""Unit tests for pure helpers, stores and parsing. Run: python -m unittest discover -s tests -t ."""
import argparse
import json
import logging
import math
import os
import shutil
import struct
import tempfile
import unittest
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from tests.support import atom, fake_segment

import download_Live
import getUrls
import monitor_channel
import runner
import YoutubeURL

LOGGER = logging.getLogger("tests")
LOGGER.addHandler(logging.NullHandler())
LOGGER.propagate = False


# --- Reference copy of the previous atom cleaning implementation (for byte-for-byte comparison) ---
def _old_get_atoms(data):
    atoms = {}
    ofs = 0
    view = memoryview(data)
    while ofs + 8 <= len(data):
        alen = struct.unpack_from('>I', view, ofs)[0]
        if alen < 8 or ofs + alen > len(data):
            break
        aname = view[ofs + 4:ofs + 8].tobytes().decode(errors='ignore')
        atoms[aname] = {"ofs": ofs, "len": alen}
        ofs += alen
    return atoms


def _old_clean_segments(data, first=True):
    bad_atoms = ["sidx"]
    if not first:
        bad_atoms.extend(["ftyp", "moov"])
    atoms = _old_get_atoms(data)
    buf = bytearray(data)
    to_remove = sorted([atoms[n] for n in bad_atoms if n in atoms], key=lambda x: x["ofs"], reverse=True)
    for a in to_remove:
        del buf[a["ofs"]:a["ofs"] + a["len"]]
    return bytes(buf)


class TimeParsingTests(unittest.TestCase):
    def test_positive_formats(self):
        self.assertEqual(runner.parse_time_to_seconds("01:02:03"), 3723)
        self.assertEqual(runner.parse_time_to_seconds("02:03"), 123)
        self.assertEqual(runner.parse_time_to_seconds("45"), 45)
        self.assertIsNone(runner.parse_time_to_seconds(""))

    def test_negative_formats(self):
        self.assertEqual(runner.parse_time_to_seconds("-10:00"), -600)
        self.assertEqual(runner.parse_time_to_seconds("-01:00:00"), -3600)
        zero = runner.parse_time_to_seconds("-00:00")
        self.assertEqual(zero, 0)
        self.assertTrue(download_Live.is_relative_time(zero), "-00:00 must stay relative (sign bit)")
        self.assertFalse(download_Live.is_relative_time(runner.parse_time_to_seconds("00:00")))

    def test_invalid(self):
        for bad in ("aa", "1:2:3:4", "--1", "1::2", "-"):
            with self.assertRaises(argparse.ArgumentTypeError, msg=bad):
                runner.parse_time_to_seconds(bad)

    def test_join_negative_values(self):
        argv = ["--start-time", "-10:00", "--end-time", "-09:00", "--threads", "4", "ID"]
        self.assertEqual(runner.join_negative_option_values(argv),
                         ["--start-time=-10:00", "--end-time=-09:00", "--threads", "4", "ID"])
        # Normal values and following options are untouched
        self.assertEqual(runner.join_negative_option_values(["--start-time", "10:00", "--live-chat"]),
                         ["--start-time", "10:00", "--live-chat"])

    def test_validate_window(self):
        runner.validate_time_window(10, 20)
        runner.validate_time_window(-600.0, -540.0)
        runner.validate_time_window(-600.0, 30)  # mixed: only checkable at runtime
        with self.assertRaises(argparse.ArgumentTypeError):
            runner.validate_time_window(20, 10)
        with self.assertRaises(argparse.ArgumentTypeError):
            runner.validate_time_window(-540.0, -600.0)

    def test_resolve_window_seconds(self):
        self.assertEqual(download_Live.resolve_window_seconds(None, None, None), (0.0, None))
        self.assertEqual(download_Live.resolve_window_seconds(60, 120, None), (60.0, 120.0))
        self.assertEqual(download_Live.resolve_window_seconds(-600.0, -540.0, 3600.0), (3000.0, 3060.0))
        self.assertEqual(download_Live.resolve_window_seconds(-600.0, -0.0, 3600.0), (3000.0, 3600.0))
        # Negative start before the stream started clamps to 0
        self.assertEqual(download_Live.resolve_window_seconds(-600.0, None, 30.0), (0.0, None))
        with self.assertRaises(ValueError):
            download_Live.resolve_window_seconds(-60.0, 10, 3600.0)
        with self.assertRaises(ValueError):
            download_Live.resolve_window_seconds(-60.0, None, None)

    def test_segment_window(self):
        self.assertEqual(download_Live.seconds_to_segment_window(10.0, 20.0, 2.0), (5, 10))
        self.assertEqual(download_Live.seconds_to_segment_window(11.0, 21.0, 2.0), (5, 11))
        start, end = download_Live.seconds_to_segment_window(0.0, None, 2.0)
        self.assertEqual(start, 0)
        self.assertTrue(math.isinf(end))


class UrlTests(unittest.TestCase):
    def test_extract_video_id(self):
        vid = "dQw4w9WgXcQ"
        for url in (vid, f"https://www.youtube.com/watch?v={vid}", f"https://youtube.com/watch?v={vid}&t=120s",
                    f"https://www.youtube.com/live/{vid}?si=abc", f"https://youtu.be/{vid}?t=5",
                    f"https://m.youtube.com/watch?feature=share&v={vid}", f"https://music.youtube.com/watch?v={vid}",
                    f"https://www.youtube.com/shorts/{vid}", f"https://www.youtube.com/embed/{vid}",
                    f"youtube.com/live/{vid}", f" {vid} "):
            self.assertEqual(getUrls.extract_video_id(url), vid, url)
            self.assertEqual(getUrls.normalize_video_url(url), f"https://www.youtube.com/watch?v={vid}")
        for other in ("https://www.youtube.com/@channel/streams", "https://www.youtube.com/playlist?list=UU123",
                      "https://example.com/watch?v=dQw4w9WgXcQ", "UCxsZ6NCzjU_t4YSxQLBcM5A"):
            self.assertIsNone(getUrls.extract_video_id(other), other)
            self.assertEqual(getUrls.normalize_video_url(other), other)

    def test_proxy_helpers(self):
        self.assertIsNone(getUrls.proxy_for_ytdlp(None))
        self.assertEqual(getUrls.proxy_for_ytdlp("socks5://h:1"), "socks5://h:1")
        self.assertEqual(getUrls.proxy_for_ytdlp({"all://": "http://p:1", "https://": "http://q:2"}), "http://p:1")
        proxy, mounts = download_Live.proxy_for_httpx("http://p:1")
        self.assertEqual((proxy, mounts), ("http://p:1", None))
        proxy, mounts = download_Live.proxy_for_httpx({"https": "http://p:1", "all://": None})
        self.assertIsNone(proxy)
        self.assertEqual(set(mounts), {"https://", "all://"})
        self.assertIsNone(mounts["all://"])

    def test_wait_tuple(self):
        # A single value tuple must still be passed to yt-dlp as a (min, max) tuple
        captured = {}

        class FakeYDL:
            def __init__(self, opts):
                captured.update(opts)

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def extract_info(self, url, download=False):
                captured["url"] = url
                return {"live_status": "is_live", "formats": []}

            def sanitize_info(self, info_dict, remove_private_keys=False):
                return info_dict

        original = getUrls.yt_dlp.YoutubeDL
        getUrls.yt_dlp.YoutubeDL = FakeYDL
        try:
            user_opts = {"extractor_args": {"youtube": {"player_client": ["web"]}}}
            getUrls.get_Video_Info("https://youtu.be/dQw4w9WgXcQ", wait=(30,), additional_options=user_opts)
            self.assertEqual(captured["wait_for_video"], (30, None))
            self.assertEqual(captured["url"], "https://www.youtube.com/watch?v=dQw4w9WgXcQ")
            getUrls.get_Video_Info("dQw4w9WgXcQ", wait=False, additional_options=user_opts)
            # The caller's options must not be mutated by repeated calls
            self.assertEqual(user_opts, {"extractor_args": {"youtube": {"player_client": ["web"]}}})
            self.assertEqual(captured["extractor_args"]["youtube"]["skip"], ["dash", "hls"])
        finally:
            getUrls.yt_dlp.YoutubeDL = original


class YoutubeURLTests(unittest.TestCase):
    URL = ("https://rr1---sn-x.googlevideo.com/videoplayback?expire=1700000000&ei=abc&id=Xyz123.2~4567"
           "&itag=299&source=yt_live_broadcast&sq=50&mime=video%2Fmp4")

    def test_parse(self):
        u = YoutubeURL.YoutubeURL(self.URL, protocol="https", vcodec="avc1", acodec="none", ext="mp4", language="none")
        self.assertEqual((u.id, u.manifest, u.itag, u.expire), ("Xyz123", "2", 299, 1700000000))
        self.assertEqual(u.ext, "mp4")
        self.assertIsNone(u.language)
        self.assertIsNone(u.acodec)
        self.assertEqual(u.format_id, "299")
        self.assertEqual(u.fomat_note, u.format_note)

    def test_segment_matches_previous_implementation(self):
        u = YoutubeURL.YoutubeURL(self.URL, protocol="https")
        for n in (0, 1, 12345):
            # Previous implementation: add_url_param("sq", n, base)
            parsed = urlparse(u.base)
            query = parse_qs(parsed.query)
            query["sq"] = [n]
            old = urlunparse(parsed._replace(query=urlencode(query, doseq=True)))
            self.assertEqual(u.segment(n), old)
            self.assertNotIn("sq=50", u.segment(n))

    def test_restful_url_and_missing_expire(self):
        url = "https://host/videoplayback/id/Abc.1/itag/140/source/yt/sq/9"
        u = YoutubeURL.YoutubeURL(url, protocol="https")
        self.assertEqual((u.id, u.manifest, u.itag, u.expire), ("Abc", "1", 140, None))
        self.assertTrue(u.segment(3).endswith("sq=3"))

    def test_id_without_manifest(self):
        u = YoutubeURL.YoutubeURL("https://h/videoplayback?id=Abc&itag=140", protocol="https")
        self.assertEqual((u.id, u.manifest), ("Abc", 0))

    def test_get_all_format_urls(self):
        info = {"formats": [
            {"format_id": "140", "protocol": "https", "url": "https://a/videoplayback?id=A.1&itag=140&expire=5", "acodec": "mp4a", "vcodec": "none"},
            {"format_id": "140-1", "protocol": "https", "url": "https://b/videoplayback?id=A.1&itag=140&expire=6", "acodec": "mp4a", "vcodec": "none"},
            {"format_id": "299", "protocol": "https", "url": "https://a/videoplayback?id=A.1&itag=299&expire=5", "acodec": "none", "vcodec": "avc1"},
            {"format_id": "140d", "protocol": "http_dash_segments", "fragment_base_url": "https://a/videoplayback/id/A.1/itag/140/", "acodec": "mp4a", "vcodec": "none"},
        ]}
        target = YoutubeURL.YoutubeURL(info["formats"][0]["url"], protocol="https")
        urls = YoutubeURL.Formats().getAllFormatURL(info, target)
        self.assertEqual([u.expire for u in urls], [5, 6])
        dash_target = YoutubeURL.YoutubeURL(info["formats"][3]["fragment_base_url"], protocol="http_dash_segments")
        dash_urls = YoutubeURL.Formats().getAllFormatURL(info, dash_target)
        self.assertEqual(len(dash_urls), 1)
        self.assertIsInstance(dash_urls[0], YoutubeURL.YoutubeURL)

    def test_missing_or_unusable_url_raises_value_error(self):
        # urlparse(None) returns bytes, which used to surface as a confusing TypeError
        for url in (None, "", "https://h/api/manifest/dash/expire/1/sparams/x"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                YoutubeURL.YoutubeURL(url, protocol="https")
        with self.assertRaises(ValueError):
            YoutubeURL.YoutubeURL("https://h/videoplayback?id=Abc&itag=abc", protocol="https")

    def test_post_live_dash_format_without_fragment_base_url(self):
        # After a stream ends yt-dlp may list DASH formats with a fragment list but no fragment_base_url
        fmt = {"format_id": "399", "protocol": "http_dash_segments", "url": "https://h/api/manifest/dash/id/x",
               "vcodec": "av01", "acodec": "none"}
        with self.assertRaisesRegex(ValueError, "fragment_base_url"):
            YoutubeURL.YoutubeURL.from_format(fmt)


class AtomCleaningTests(unittest.TestCase):
    def test_matches_previous_implementation(self):
        for sq in range(3):
            seg = fake_segment(299, sq)
            for first in (True, False):
                self.assertEqual(download_Live.clean_segment(seg, first=first), _old_clean_segments(seg, first=first))

    def test_non_mp4_and_truncated_data(self):
        for data in (b"", b"\x00\x00", b"not an mp4 at all" * 10, atom(b"mdat", b"x" * 10)[:-3],
                     struct.pack(">I", 1) + b"mdat" + b"\x00" * 20):
            for first in (True, False):
                self.assertEqual(download_Live.clean_segment(data, first=first), _old_clean_segments(data, first=first))

    def test_write_segment_zero_copy(self):
        import io
        seg = fake_segment(140, 7)
        buf = io.BytesIO()
        written = download_Live.write_segment(buf, seg, clean=True, first=False)
        self.assertEqual(buf.getvalue(), _old_clean_segments(seg, first=False))
        self.assertEqual(written, len(buf.getvalue()))
        buf = io.BytesIO()
        download_Live.write_segment(buf, seg, clean=False, first=False)
        self.assertEqual(buf.getvalue(), seg)

    def test_should_clean(self):
        self.assertTrue(download_Live.should_clean("mp4"))
        self.assertTrue(download_Live.should_clean(".m4a"))
        self.assertTrue(download_Live.should_clean(None))
        self.assertFalse(download_Live.should_clean("webm"))


class RefreshScheduleTests(unittest.TestCase):
    def test_expiry_based(self):
        now = 1000.0
        for _ in range(50):
            t = download_Live.next_refresh_time(now, expire=now + 21600, interval_override=None, jitter=300)
            self.assertGreaterEqual(t, now + 21600 - 1800 - 300)
            self.assertLessEqual(t, now + 21600 - 1800)

    def test_expiry_soon_is_rate_limited(self):
        now = 1000.0
        self.assertEqual(download_Live.next_refresh_time(now, expire=now + 10, interval_override=None, jitter=0),
                         now + download_Live.URL_REFRESH_MIN_INTERVAL)

    def test_override_and_fallback(self):
        now = 0.0
        self.assertEqual(download_Live.next_refresh_time(now, expire=99999, interval_override=600, jitter=0), 600)
        t = download_Live.next_refresh_time(now, expire=99999, interval_override=600, jitter=100)
        self.assertTrue(500 <= t <= 600)
        self.assertEqual(download_Live.next_refresh_time(now, expire=None, interval_override=None, jitter=0),
                         download_Live.URL_REFRESH_FALLBACK_INTERVAL)

    def test_estimate_segment_duration(self):
        self.assertIsNone(download_Live.estimate_segment_duration(100, 0))
        self.assertIsNone(download_Live.estimate_segment_duration(0, 10))
        self.assertIsNone(download_Live.estimate_segment_duration(None, 10))
        self.assertEqual(download_Live.estimate_segment_duration(100, 50), 2.0)

    def test_content_type(self):
        self.assertEqual(download_Live.parse_content_type("video/mp4; codecs=avc1"), ("video", "mp4"))
        self.assertEqual(download_Live.parse_content_type("garbage"), (None, None))
        self.assertEqual(download_Live.parse_content_type(None), (None, None))


class SQLiteStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.path = os.path.join(self.tmp, "test.temp")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _store(self, **kwargs):
        return download_Live.SQLiteSegmentStore(self.path, logger=LOGGER, batch_size=kwargs.pop("batch_size", 2), **kwargs)

    def test_page_size_applied_before_wal(self):
        store = self._store(page_size=65536)
        try:
            self.assertEqual(store.conn.execute("PRAGMA page_size").fetchone()[0], 65536)
            self.assertEqual(store.conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(store.conn.execute("PRAGMA mmap_size").fetchone()[0], 0)
        finally:
            store.delete()
        store = self._store(page_size=16384, journal_mode="truncate")
        try:
            self.assertEqual(store.conn.execute("PRAGMA page_size").fetchone()[0], 16384)
            self.assertEqual(store.conn.execute("PRAGMA journal_mode").fetchone()[0], "truncate")
        finally:
            store.delete()
        self.assertFalse(os.path.exists(self.path))

    def test_batched_verified_commit(self):
        store = self._store(batch_size=3)
        store.add(1, b"a" * 10)
        store.add(0, b"b" * 10)
        store.flush()
        self.assertEqual(store.downloaded, set(), "below batch size nothing is committed")
        self.assertTrue(store.has(1))
        store.add(2, b"c")
        store.flush()
        self.assertEqual(store.downloaded, {0, 1, 2})
        self.assertEqual(store.pending, {})
        store.close()
        # Reopening loads the committed IDs (resume)
        store = self._store()
        self.assertEqual(store.downloaded, {0, 1, 2})
        store.close()

    def test_failed_insert_keeps_segments(self):
        store = self._store(batch_size=1)
        store.conn.execute("DROP TABLE segments")
        store.conn.execute("CREATE TABLE segments (id INTEGER PRIMARY KEY, segment_data BLOB CHECK (length(segment_data) < 5))")
        store.add(4, b"too long")
        store.flush()
        self.assertIn(4, store.pending)
        self.assertNotIn(4, store.downloaded)
        # Once the problem is resolved the segment is written on the next flush
        store.conn.execute("DROP TABLE segments")
        store.conn.execute("CREATE TABLE segments (id INTEGER PRIMARY KEY, segment_data BLOB)")
        store.flush()
        self.assertEqual(store.downloaded, {4})
        store.delete()

    def test_unverified_segment_is_rewritten(self):
        store = self._store(batch_size=1)
        # A trigger removes the row right after insertion, simulating a lost write
        store.conn.execute("CREATE TRIGGER lose AFTER INSERT ON segments WHEN NEW.id = 7 BEGIN DELETE FROM segments WHERE id = 7; END")
        store.add(7, b"data")
        store.add(8, b"data")
        store.flush(force=True)
        self.assertEqual(store.downloaded, {8})
        self.assertIn(7, store.pending)
        store.conn.execute("DROP TRIGGER lose")
        store.flush(force=True)
        self.assertEqual(store.downloaded, {7, 8})
        store.delete()

    def test_upsert_keeps_longer_copy_and_verifies(self):
        store = self._store(batch_size=1)
        store.add(1, b"long data")
        store.flush()
        store.add(1, b"short")
        store.flush()
        self.assertEqual(store.downloaded, {1})
        self.assertEqual(store.conn.execute("SELECT segment_data FROM segments WHERE id=1").fetchone()[0], b"long data")
        store.delete()

    def test_merge_includes_uncommitted(self):
        store = self._store(batch_size=1)
        segs = {i: fake_segment(299, i) for i in range(4)}
        for i in (0, 2):
            store.add(i, segs[i])
            store.flush()
        # Make further commits fail; segments 1 and 3 stay in memory
        store.conn.execute("CREATE TRIGGER block BEFORE INSERT ON segments BEGIN SELECT RAISE(ABORT, 'disk full'); END")
        store.add(1, segs[1])
        store.add(3, segs[3])
        out = os.path.join(self.tmp, "out.ts")
        store.merge_to_file(out, clean=True)
        with open(out, "rb") as f:
            merged = f.read()
        expected = b"".join(download_Live.clean_segment(segs[i], first=(i == 0)) for i in range(4))
        self.assertEqual(merged, expected)
        store.delete()

    def test_backlog(self):
        store = self._store(batch_size=1)
        for i in range(60):
            store.pending[i] = b"x"
        self.assertTrue(store.is_backlogged())
        store.delete()


class DirectStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ts = os.path.join(self.tmp, "x.ts")
        self.state = os.path.join(self.tmp, "x.state")
        self.backup = os.path.join(self.tmp, "x.state.bkup")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _store(self):
        return download_Live.DirectTSStore(self.ts, self.state, self.backup, logger=LOGGER, clean=True)

    def test_in_order_writes_and_resume(self):
        segs = [fake_segment(140, i) for i in range(5)]
        store = self._store()
        store.add(1, segs[1])
        store.flush()
        self.assertFalse(os.path.exists(self.ts), "segment 1 must wait for segment 0")
        store.add(0, segs[0])
        store.flush()
        self.assertEqual(store.last_written, 1)
        with open(self.state) as f:
            self.assertEqual(json.load(f)["last_written"], 1)
        # Simulate data written after the last state save (crash): resume truncates it
        with open(self.ts, "ab") as f:
            f.write(b"garbage")
        store = self._store()
        self.assertEqual(store.last_written, 1)
        for i in (2, 3, 4):
            store.add(i, segs[i])
        store.flush()
        with open(self.ts, "rb") as f:
            self.assertEqual(f.read(), b"".join(download_Live.clean_segment(s, first=(i == 0)) for i, s in enumerate(segs)))

    def test_skip_and_write_remaining(self):
        segs = [fake_segment(140, i) for i in range(6)]
        store = self._store()
        store.add(0, segs[0])
        store.flush()
        store.add(2, segs[2])
        store.add(5, segs[5])
        store.skip_to(1)  # segment 1 exceeded its retries
        self.assertEqual(store.last_written, 2)
        store.write_remaining()  # segments 3 and 4 are missing at the end of the download
        self.assertEqual(store.last_written, 5)
        with open(self.ts, "rb") as f:
            self.assertEqual(f.read(), b"".join(download_Live.clean_segment(segs[i], first=(i == 0)) for i in (0, 2, 5)))

    def test_set_start_only_when_fresh(self):
        store = self._store()
        store.set_start(100)
        self.assertEqual(store.first_needed(), 100)
        store.add(100, b"x")
        store.flush()
        store.set_start(5)
        self.assertEqual(store.first_needed(), 101)

    def test_corrupt_state_falls_back_to_backup(self):
        store = self._store()
        store.add(0, b"a" * 10)
        store.flush()
        store.add(1, b"b" * 10)
        store.flush()
        # Newest state claims more data than the file holds (e.g. OS crash): backup is used
        with open(self.state, "w") as f:
            json.dump({"last_written": 9, "file_size": 10 ** 6}, f)
        self.assertEqual(self._store().last_written, 0)


class MonitorCacheTests(unittest.TestCase):
    def test_cache(self):
        cache = monitor_channel.VideoStatusCache(max_entries=2)
        now = 1_000_000.0
        cache.record("a", "was_live", None, 24, now=now)
        self.assertTrue(cache.should_skip("a", now=now + 10 ** 6))
        cache.record("b", "is_upcoming", now + 48 * 3600, 24, now=now)
        self.assertTrue(cache.should_skip("b", now=now + 10))
        self.assertFalse(cache.should_skip("b", now=now + 3601), "upcoming streams are re-checked at least hourly")
        cache.record("c", "is_live", None, 24, now=now)
        self.assertFalse(cache.should_skip("c", now=now))
        self.assertFalse(cache.should_skip("a", now=now), "oldest entry evicted when full")

    def test_lookahead_option(self):
        self.assertEqual(monitor_channel._lookahead_hours({"upcoming_lookahead": 5}), 5)
        self.assertEqual(monitor_channel._lookahead_hours({}), 24)


if __name__ == "__main__":
    unittest.main()
