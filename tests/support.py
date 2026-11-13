"""
Test helpers: a mock googlevideo server and fake yt-dlp info dicts.

The mock server imitates the parts of googlevideo's /videoplayback endpoint the downloader relies on:
- segments are requested with ?sq=<n>; requests without sq return the head segment
- X-Head-Seqnum / X-Head-Time-Sec report the newest available segment
- segments beyond the head return 404, an expired URL returns 403
- faults (403/500 responses, a private video) can be injected per test
"""
from __future__ import annotations

import os
import struct
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Optional
from urllib.parse import parse_qs, urlparse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

VIDEO_ID = "abcdefghijk"
VIDEO_ITAG = 299
AUDIO_ITAG = 140


def atom(name: bytes, payload: bytes) -> bytes:
    return struct.pack(">I", 8 + len(payload)) + name + payload


def fake_segment(itag: int, sq: int, size: int = 2000) -> bytes:
    """A fake fragmented-MP4 segment: ftyp + moov + sidx + mdat (headers repeated per segment like YouTube)."""
    body = (f"{itag}:{sq}:".encode() * (size // 8 + 1))[:size]
    return (atom(b"ftyp", b"isomiso6dash")
            + atom(b"moov", b"M" * 40)
            + atom(b"sidx", b"S" * 24)
            + atom(b"moof", f"moof{sq}".encode())
            + atom(b"mdat", body))


def expected_ts(segments: list[bytes]) -> bytes:
    """Expected merged output: sidx removed everywhere, ftyp/moov kept only for the first segment."""
    import download_Live
    return b"".join(download_Live.clean_segment(seg, first=(i == 0)) for i, seg in enumerate(segments))


class MockStream:
    """State of one simulated livestream (shared by all formats)."""

    def __init__(self, total_segments: int = 20, initial_head: int = 5, segment_duration: float = 0.2,
                 segment_provider: Optional[Callable[[int, int], bytes]] = None, nominal_duration: float = 2.0):
        self.total_segments = total_segments
        self.initial_head = initial_head
        # Real seconds between new segments, and the segment length reported in X-Head-Time-Sec
        self.segment_duration = segment_duration
        self.nominal_duration = nominal_duration
        self.start = time.monotonic()
        self.lock = threading.Lock()
        self.segment_provider = segment_provider or (lambda itag, sq: fake_segment(itag, sq))
        # sq -> remaining number of failures (status code) to return
        self.failures: dict[tuple[int, int], list[int]] = {}
        self.private = False
        self.manifest = 1
        self.requests: list[tuple[int, Optional[int]]] = []
        self.header_only_requests = 0

    def head(self) -> int:
        elapsed = time.monotonic() - self.start
        return min(self.total_segments - 1, self.initial_head + int(elapsed / self.segment_duration))

    def ended(self) -> bool:
        return self.head() >= self.total_segments - 1

    def fail(self, itag: int, sq: int, status: int, times: int = 1) -> None:
        self.failures.setdefault((itag, sq), []).extend([status] * times)


class _Handler(BaseHTTPRequestHandler):
    stream: MockStream = None

    def log_message(self, *args):  # silence
        pass

    def do_GET(self):
        stream = self.server.stream
        parsed = urlparse(self.path)
        qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        itag = int(qs.get("itag", 0))
        sq = int(qs["sq"]) if "sq" in qs else None
        with stream.lock:
            stream.requests.append((itag, sq))
        head = stream.head()
        headers = {
            "X-Head-Seqnum": str(head),
            "X-Head-Time-Sec": str(int(round(head * stream.nominal_duration))) if head > 0 else "0",
            "Content-Type": "audio/mp4" if itag == AUDIO_ITAG else "video/mp4",
        }
        expire = qs.get("expire")
        if stream.private or (expire and int(expire) < time.time()):
            return self._send(403, b"", headers)
        if sq is None:
            stream.header_only_requests += 1
            sq = head
        with stream.lock:
            pending_failures = stream.failures.get((itag, sq))
            status = pending_failures.pop(0) if pending_failures else None
        if status is not None:
            return self._send(status, b"", headers)
        if sq > head:
            return self._send(404, b"", headers)
        return self._send(200, stream.segment_provider(itag, sq), headers)

    def _send(self, status: int, body: bytes, headers: dict):
        try:
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # The client may close the connection early (header-only probes)
            pass


class MockServer:
    def __init__(self, stream: MockStream):
        self.stream = stream
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.httpd.daemon_threads = True
        self.httpd.stream = stream
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def base(self) -> str:
        host, port = self.httpd.server_address
        return f"http://{host}:{port}"

    def url(self, itag: int, expire: Optional[int] = None, manifest: Optional[int] = None) -> str:
        expire = expire if expire is not None else int(time.time()) + 6 * 3600
        manifest = manifest if manifest is not None else self.stream.manifest
        return f"{self.base}/videoplayback?expire={expire}&id={VIDEO_ID}x.{manifest}&itag={itag}&source=yt_live_broadcast"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()


def make_info_dict(server: MockServer, live_status: str = "is_live", expire: Optional[int] = None) -> dict:
    """Minimal yt-dlp style info dict with one video and one audio adaptive format."""
    return {
        "id": VIDEO_ID,
        "title": "Test stream",
        "fulltitle": "Test stream",
        "extractor": "youtube",
        "extractor_key": "Youtube",
        "webpage_url": f"https://www.youtube.com/watch?v={VIDEO_ID}",
        "original_url": f"https://www.youtube.com/watch?v={VIDEO_ID}",
        "live_status": live_status,
        "is_live": live_status == "is_live",
        "ext": "mp4",
        "formats": [
            {"format_id": str(VIDEO_ITAG), "url": server.url(VIDEO_ITAG, expire), "protocol": "https", "ext": "mp4",
             "vcodec": "avc1.64002a", "acodec": "none", "width": 1920, "height": 1080, "fps": 60, "tbr": 6000,
             "format_note": "1080p60"},
            {"format_id": str(AUDIO_ITAG), "url": server.url(AUDIO_ITAG, expire), "protocol": "https", "ext": "m4a",
             "vcodec": "none", "acodec": "mp4a.40.2", "abr": 128, "tbr": 128, "format_note": "medium"},
        ],
    }


def stream_url_for(info_dict: dict, itag: int):
    import YoutubeURL
    fmt = next(f for f in info_dict["formats"] if f["format_id"] == str(itag))
    return YoutubeURL.YoutubeURL.from_format(fmt)


class FastTimings:
    """Context manager shrinking the downloader's waits so scenarios complete in seconds."""
    OVERRIDES = {
        "OPTIMISTIC_MIN_WAIT": 0.05,
        "WAIT_INTERVAL_SECONDS": 0.1,
        "REFRESH_EVERY_N_WAITS": 3,
        "REFRESH_403_AFTER_SECONDS": 0.2,
        "RECOVERY_HANDOFF_DELAY": 0.1,
        "RECOVERY_401_SLEEP": 0.1,
        "EXHAUSTED_REFRESH_MIN_INTERVAL": 0.5,
        "PRIVATE_DRAIN_WAIT": 0.1,
        "LIVE_CHAT_STOP_GRACE": 0.5,
    }

    def __enter__(self):
        import download_Live
        self.module = download_Live
        self.saved = {name: getattr(download_Live, name) for name in self.OVERRIDES}
        for name, value in self.OVERRIDES.items():
            setattr(download_Live, name, value)
        self.saved_cls = {
            "BACKOFF_FACTOR": download_Live.StreamDownloaderBase.BACKOFF_FACTOR,
            "BACKOFF_MAX": download_Live.StreamDownloaderBase.BACKOFF_MAX,
            "POLL_INTERVAL": download_Live.StreamDownloaderBase.POLL_INTERVAL,
        }
        download_Live.StreamDownloaderBase.BACKOFF_FACTOR = 0.01
        download_Live.StreamDownloaderBase.BACKOFF_MAX = 0.02
        download_Live.StreamDownloaderBase.POLL_INTERVAL = 0.1
        return self

    def __exit__(self, *exc):
        for name, value in self.saved.items():
            setattr(self.module, name, value)
        for name, value in self.saved_cls.items():
            setattr(self.module.StreamDownloaderBase, name, value)
