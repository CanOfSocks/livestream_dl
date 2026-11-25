"""
Online tests against YouTube (skipped unless enabled): getting and using the stream URLs of a stream
that has ended but isn't processed yet (post_live).

    LIVESTREAM_DL_ONLINE_POST_LIVE=<video id> python -m unittest tests.test_online

The video must still be post_live: a stream that ended recently (usually hours to a day or two).
These tests use the network and the installed yt-dlp, so they check what yt-dlp currently returns.
"""
import logging
import os
import shutil
import tempfile
import unittest

import download_Live
import getUrls
import YoutubeURL

POST_LIVE_ID = os.environ.get("LIVESTREAM_DL_ONLINE_POST_LIVE")

LOGGER = logging.getLogger("tests.online")
LOGGER.addHandler(logging.NullHandler())
LOGGER.propagate = False


@unittest.skipUnless(POST_LIVE_ID, "set LIVESTREAM_DL_ONLINE_POST_LIVE=<video id of a post-live stream> to run")
class OnlinePostLiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.info, cls.status = getUrls.get_Video_Info(POST_LIVE_ID, wait=False, logger=LOGGER)

    def setUp(self):
        if self.status != "post_live":
            self.skipTest(f"{POST_LIVE_ID} is {self.status}, not post_live")
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_getting_post_live_urls(self):
        formats = self.info["formats"]
        self.assertTrue(all("fragments" not in fmt for fmt in formats), "fragment lists are dropped")
        adaptive = [fmt for fmt in formats if YoutubeURL.stream_url_kind(fmt) == YoutubeURL.URL_KIND_ADAPTIVE]
        self.assertTrue(adaptive, "no live adaptive formats found in a post-live extraction")
        audio = YoutubeURL.Formats(logger=LOGGER).getFormatURL(self.info, "ba", stream_type="audio")
        self.assertEqual(audio.url_kind, YoutubeURL.URL_KIND_ADAPTIVE)
        # yt-dlp's protocol label is kept as returned (http_dash_segments in yt-dlp 2026.10.26)
        source = next(fmt for fmt in formats if fmt.get("format_id") == audio.format_id)
        self.assertEqual(audio.protocol, source.get("protocol"))
        self.assertTrue(YoutubeURL.Formats(logger=LOGGER).getAllFormatURL(self.info, audio))

    def test_using_post_live_urls(self):
        info = dict(self.info)
        coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
        options = {"filename": "online", "download_folder": self.tmp, "threads": 2, "segment_retries": 3,
                   "wait_limit": 0, "start_time": -20.0}
        formats = coordinator._select_formats(info, "ba", options)
        urls = coordinator._stream_urls_from_formats(formats)
        self.assertEqual(len(urls), 1)
        url = urls[0]
        with download_Live.DownloadStream(info, stream_url=url, options=options, livestream_coordinator=coordinator) as stream:
            # The stream head is known from a HEAD probe of the post-live URL
            self.assertGreater(stream.latest_sequence, 0)
            self.assertGreater(stream.estimated_segment_duration, 0)
        file, _ = coordinator.download_stream(info, url, options, manifest=url.manifest)
        self.assertGreater(os.path.getsize(str(file)), 10_000, "the last 20 seconds of audio were downloaded")


if __name__ == "__main__":
    unittest.main()
