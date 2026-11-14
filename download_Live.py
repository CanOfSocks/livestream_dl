"""
Core livestream downloader.

Overview
--------
``LiveStreamDownloader`` (the coordinator) selects formats, starts one stream downloader per
selected format (video / audio) plus auxiliary/live-chat tasks, then muxes and moves the results.

Stream downloaders (one per format) share a common lifecycle defined in ``StreamDownloaderBase``:

    StreamDownloaderBase            shared lifecycle + template main loop (live_dl)
    ├── DownloadStream              live edge download into an SQLite temp database
    │   └── DownloadStreamDirect    live edge download written directly (in order) to a .ts file
    └── StreamRecovery              re-download of missing segments from (possibly expired) URLs

Segments are numbered by YouTube's sequence number ("sq"). The server reports the newest
available segment via the ``X-Head-Seqnum`` header and the stream duration at that segment via
``X-Head-Time-Sec``, which together are used to estimate the segment duration.

Downloaded segment data is buffered and persisted by a ``SegmentStore``:

    SegmentStore
    ├── SQLiteSegmentStore          batched + verified commits, merged to .ts at the end
    └── DirectTSStore               appends segments in order to the .ts file with a resumable state file

Where to start reading
----------------------
1. LiveStreamDownloader.download_segments: the whole download of one video, start to finish.
2. LiveStreamDownloader._run_stream: runs one stream downloader (live_dl, then finalize).
3. StreamDownloaderBase.live_dl: the main loop (its class docstring lists the steps).
4. DownloadStream: how segments are scheduled at the live edge (_evaluate, _on_idle, ...).

Sections of this file, in order: tunables, generic helpers, MP4 atom cleaning, FileInfo,
segment stores, stream downloaders, FFmpeg merger, coordinator (LiveStreamDownloader).

Threads: the coordinator runs each format's downloader in its own thread, and each downloader
downloads segments with its own thread pool (--threads). A URL refresh (yt-dlp) runs in a
background thread so downloads continue meanwhile; the live chat also has its own thread.
"""
from __future__ import annotations

import concurrent.futures
import contextlib
import copy
import enum
import functools
import gc
import heapq
import itertools
import json
import logging
import math
import mimetypes
import os
import random
import re
import shutil
import signal
import sqlite3
import struct
import subprocess
import threading
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Callable, Iterable, NamedTuple, Optional, Union
from urllib.parse import urlparse

import httpx
import yt_dlp.utils
import yt_dlp.YoutubeDL

try:
    import getUrls
    import YoutubeURL
    import setup_logger
    import headers as customheaders
except ModuleNotFoundError:
    from . import getUrls
    from . import YoutubeURL
    from . import setup_logger
    from . import headers as customheaders

# Backward compatibility
setup_logging = setup_logger.setup_logging

LoggerLike = Union[logging.Logger, logging.LoggerAdapter]


class SegmentResult(NamedTuple):
    """The outcome of one segment request (returned by StreamDownloaderBase.download_segment)."""
    head_seq: int                       # newest segment the server reported (X-Head-Seqnum), -1 if unknown
    data: Optional[bytes]               # segment bytes; b"" for a 204 response, None if the download failed
    seg: int                            # the segment (sequence) number that was requested
    status: Optional[int]               # HTTP status code, None if no response was received
    headers: Optional[httpx.Headers]    # response headers, None if no response was received

    @classmethod
    def failed(cls, seg: int, status: Optional[int] = None) -> "SegmentResult":
        return cls(-1, None, int(seg), status, None)


class RefreshOutcome(enum.Enum):
    """What happened when a stream downloader refreshed its URL (see StreamDownloaderBase.refresh_url)."""
    IN_PROGRESS = "in progress"     # a background refresh is still running (only when wait=False)
    NEW_MANIFEST = "new manifest"   # the stream restarted with a new manifest, now downloaded by another thread
    ENDED = "ended"                 # the stream is no longer live
    STILL_LIVE = "still live"       # refreshed (or the refresh failed) and the stream is still live

# --- Tunables ---------------------------------------------------------------------------------
# Seconds before a URL's "expire" time at which it is refreshed (default refresh mode)
URL_REFRESH_EXPIRY_MARGIN: float = 1800.0
# Refresh interval used when a URL has no expire parameter
URL_REFRESH_FALLBACK_INTERVAL: float = 3600.0
# Never schedule periodic refreshes closer together than this
URL_REFRESH_MIN_INTERVAL: float = 60.0
DEFAULT_URL_REFRESH_JITTER: float = 300.0
# Upper bound for the back-off between yt-dlp refresh attempts after rate limiting
MAX_MINIMUM_REFRESH_TIME: float = 1800.0

# Number of wait intervals (WAIT_INTERVAL_SECONDS each) with no new segments between URL refreshes
REFRESH_EVERY_N_WAITS: int = 10
WAIT_INTERVAL_SECONDS: float = 10.0
# Optimistic (live edge) segment requests use this many retries at most. Segment retries used to
# be fixed at 5 regardless of --segment-retries; keeping the optimistic requests at that value
# keeps the end-of-stream detection time unchanged now that --segment-retries is honoured.
OPTIMISTIC_SEGMENT_RETRIES: int = 5
OPTIMISTIC_FAILS_MAX: int = 10
# Minimum wait before requesting the next (optimistic) segment at the live edge
OPTIMISTIC_MIN_WAIT: float = 2.0
# Refresh the URL once 403 errors have persisted this long since the last refresh
REFRESH_403_AFTER_SECONDS: float = 30.0
# Wait before handing a stream that turned private over to stream recovery (improves success)
RECOVERY_HANDOFF_DELAY: float = 300.0
# Wait for in-flight requests before handing a private stream over to recovery
PRIVATE_DRAIN_WAIT: float = 5.0
# How long the live chat thread gets to stop after its timeout before it is abandoned
LIVE_CHAT_STOP_GRACE: float = 10.0
# Pause in stream recovery when 401 errors are received
RECOVERY_401_SLEEP: float = 60.0
# Minimum seconds since the last URL refresh before segments that exhausted their retries trigger another one
EXHAUSTED_REFRESH_MIN_INTERVAL: float = 300.0

# SQLite page sizes. Large pages mean fewer overflow pages for large segment BLOBs and align better
# with ZFS' default 128K recordsize; audio segments are small so a smaller page limits the
# partially filled final overflow page of each BLOB.
SQLITE_VIDEO_PAGE_SIZE: int = 65536
SQLITE_AUDIO_PAGE_SIZE: int = 16384
# Checkpoint / truncate the WAL after roughly this many bytes
SQLITE_WAL_TARGET_BYTES: int = 64 * 1024 * 1024
# Max host parameters per statement (SQLite's lowest historical default limit is 999)
SQLITE_MAX_VARIABLES: int = 900
# Number of pending segments after which no new downloads are submitted (e.g. disk full)
STORE_BACKLOG_FACTOR: int = 10

WRITE_BUFFER_SIZE: int = 1024 * 1024
# Segment bodies are read in chunks of this size so the per-segment time limit can be checked
SEGMENT_READ_CHUNK_SIZE: int = 64 * 1024
ATOM_BASED_EXTENSIONS = frozenset({".mp4", ".m4a", ".mov", ".m4v", ".f4v", ".3gp"})
# Threads of the coordinator's task pool: the auxiliary file download plus one per selected format
COORDINATOR_WORKERS: int = 8

# ANSI terminal colour codes used to highlight some log lines (see colour())
RED, GREEN, YELLOW, DARK_YELLOW = "31", "92", "93", "33"


# ==============================================================================================
# Generic helpers
# ==============================================================================================

def colour(text: Any, code: str) -> str:
    """Wrap text in an ANSI colour code (RED, GREEN, YELLOW or DARK_YELLOW) for terminal output."""
    return f"\033[{code}m{text}\033[0m"


def _opt(options: dict, *keys: str, default: Any = None) -> Any:
    """Return the first non-None value of `keys` in options (supports internal + CLI key names)."""
    for key in keys:
        value = options.get(key)
        if value is not None:
            return value
    return default


@functools.lru_cache(maxsize=1)
def http2_available() -> bool:
    try:
        import h2  # noqa: F401
        return True
    except ImportError:
        return False


def proxy_for_httpx(proxies: Optional[Union[str, dict]]) -> tuple[Optional[str], Optional[dict[str, httpx.HTTPTransport]]]:
    """
    Convert the --proxy option into httpx's (proxy, mounts) arguments.
    A string is a single proxy; a dict maps URL patterns to proxies (https://www.python-httpx.org/advanced/proxies).
    requests-style keys ("http"/"https") are converted to httpx patterns ("http://"/"https://").
    """
    if not proxies:
        return None, None
    if isinstance(proxies, str):
        return proxies.strip() or None, None
    if isinstance(proxies, dict):
        mounts = {}
        for pattern, url in proxies.items():
            pattern = str(pattern)
            if "://" not in pattern:
                pattern = f"{pattern}://"
            mounts[pattern] = httpx.HTTPTransport(proxy=url) if url else None
        return None, mounts
    return None, None


def create_http_client(proxies=None, headers: Optional[dict] = None, max_connections: int = 10,
                       timeout: Optional[httpx.Timeout] = None) -> httpx.Client:
    proxy, mounts = proxy_for_httpx(proxies)
    limits = httpx.Limits(max_keepalive_connections=max_connections, max_connections=max_connections, keepalive_expiry=30)
    return httpx.Client(timeout=timeout or StreamDownloaderBase.timeout_config, limits=limits, proxy=proxy, mounts=mounts,
                        http1=True, http2=http2_available(), follow_redirects=True,
                        headers=headers or customheaders.get_httpx_header())


_httpx_logging_lock = threading.Lock()


def _quiet_httpx_logging(logger: LoggerLike) -> None:
    """Set httpx/httpcore loggers one level above our logger so request lines don't flood output."""
    current_level = logger.getEffectiveLevel()
    target_level = logging.WARNING if current_level == logging.NOTSET else min(current_level + 10, logging.ERROR)
    with _httpx_logging_lock:
        logging.getLogger("httpx").setLevel(target_level)
        logging.getLogger("httpcore").setLevel(target_level)


def parse_content_type(value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Split e.g. 'video/mp4; codecs=...' into ('video', 'mp4'). Returns (None, None) if malformed."""
    if not value or "/" not in str(value):
        return None, None
    major, _, minor = str(value).partition("/")
    minor = minor.split(";", 1)[0].strip()
    return major.strip().lower() or None, minor or None


def _int_header(headers: Optional[httpx.Headers], name: str) -> Optional[int]:
    if headers is None:
        return None
    value = headers.get(name)
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def estimate_segment_duration(head_time_sec: Optional[float], head_seq: Optional[int]) -> Optional[float]:
    """Average segment duration from X-Head-Time-Sec / X-Head-Seqnum. None if it can't be known yet."""
    if head_time_sec is None or head_seq is None or head_seq <= 0 or head_time_sec <= 0:
        return None
    return head_time_sec / head_seq


def is_relative_time(value: Optional[float]) -> bool:
    """
    True if a --start-time/--end-time value is relative to the live edge (negative).
    Uses the sign bit so that -0.0 ("-00:00", i.e. exactly the live edge) also counts as relative.
    """
    return value is not None and math.copysign(1.0, value) < 0


def resolve_window_seconds(start_time: Optional[float], end_time: Optional[float],
                           reference: Optional[float]) -> tuple[float, Optional[float]]:
    """
    Resolve --start-time/--end-time into absolute stream seconds.
    Negative values are relative to `reference` (the live edge's stream time when recording began).
    Returns (start, end) where end may be None (no end limit).
    """
    def absolute(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        if is_relative_time(value):
            if reference is None:
                raise ValueError("A live edge reference time is required for negative start/end times")
            return max(0.0, reference + value)
        return float(value)

    start = absolute(start_time) or 0.0
    end = absolute(end_time)
    if end is not None and end <= start:
        raise ValueError(f"End time ({end:.0f}s) must be after start time ({start:.0f}s) of the stream")
    return start, end


def seconds_to_segment_window(start_seconds: float, end_seconds: Optional[float], segment_duration: float) -> tuple[int, float]:
    """Convert absolute seconds into an inclusive (start_seg, end_seg) range. end_seg may be inf."""
    start_seg = int(start_seconds // segment_duration)
    end_seg: float = float('inf') if end_seconds is None else -int(-end_seconds // segment_duration)
    return start_seg, end_seg


def next_refresh_time(now: float, expire: Optional[float], interval_override: Optional[float],
                      jitter: float, margin: float = URL_REFRESH_EXPIRY_MARGIN) -> float:
    """
    When the next periodic URL refresh is due.
    - With an interval override: now + interval - random jitter.
    - Otherwise refresh `margin` seconds before the URL expires, minus random jitter, so that many
      processes started together don't all run yt-dlp at the same moment.
    - Without an expire time: fall back to an hourly refresh.
    """
    jitter_value = random.uniform(0.0, max(0.0, jitter or 0.0))
    if interval_override and interval_override > 0:
        return now + max(URL_REFRESH_MIN_INTERVAL, interval_override - jitter_value)
    if expire:
        return max(now + URL_REFRESH_MIN_INTERVAL, float(expire) - margin - jitter_value)
    return now + max(URL_REFRESH_MIN_INTERVAL, URL_REFRESH_FALLBACK_INTERVAL - jitter_value)


def base_output_path(options: dict, info_dict: dict) -> str:
    """Temp path prefix (without extension) for files of this video: <temp_folder>/<filename>."""
    filename = options.get('filename') or info_dict.get('id')
    temp_folder = options.get("temp_folder")
    return os.path.join(temp_folder, filename) if temp_folder else filename


def _guess_mime_type(path) -> tuple[Optional[str], Optional[str]]:
    """(mime type, encoding) from a file's extension."""
    # mimetypes.guess_file_type was added in Python 3.13; guess_type is deprecated for paths there
    guess = getattr(mimetypes, 'guess_file_type', mimetypes.guess_type)
    return guess(path)


def _remove_file(path: Optional[str], logger: Optional[LoggerLike] = None) -> None:
    if not path:
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except OSError as e:
        if logger:
            logger.warning(f"Unable to remove {path}: {e}")


# ==============================================================================================
# MP4 atom cleaning (via ytarchive)
# ==============================================================================================
# Each fragmented MP4 segment carries its own ftyp/moov header atoms and a sidx index. When
# segments are concatenated, only the first segment's headers are kept and every sidx is dropped.

def get_atoms(data) -> dict[str, dict[str, int]]:
    """Parse top level atoms -> {name: {"ofs", "len"}} (last occurrence wins, as before)."""
    return {name: {"ofs": ofs, "len": length} for name, ofs, length in iter_atoms(data)}


def iter_atoms(data) -> Iterable[tuple[str, int, int]]:
    """Yield (name, offset, length) for each top level atom until the data stops looking like MP4."""
    ofs = 0
    data_len = len(data)
    view = memoryview(data)
    while ofs + 8 <= data_len:
        # '>I' reads 4 bytes as a Big-Endian Unsigned Int (the standard MP4 length format)
        alen = struct.unpack_from('>I', view, ofs)[0]
        # Length can't be smaller than the 8 byte header (this also stops at size 0 "to end of file"
        # and size 1 "64-bit size" atoms, which are left untouched).
        if alen < 8 or ofs + alen > data_len:
            break
        aname = view[ofs + 4:ofs + 8].tobytes().decode(errors='ignore')
        yield aname, ofs, alen
        ofs += alen


def clean_segment_parts(data, first: bool = True) -> list[memoryview]:
    """
    Return zero-copy slices of `data` with the unwanted atoms removed.
    sidx is always removed; ftyp and moov are only kept for the first segment.
    """
    bad_atoms = {"sidx"} if first else {"sidx", "ftyp", "moov"}
    view = memoryview(data)
    parts: list[memoryview] = []
    cursor = 0
    for name, ofs, length in iter_atoms(data):
        if name in bad_atoms:
            if ofs > cursor:
                parts.append(view[cursor:ofs])
            cursor = ofs + length
    if cursor == 0:
        return [view]
    if cursor < len(data):
        parts.append(view[cursor:])
    return parts


def clean_segment(data, first: bool = True) -> bytes:
    """Cleaned copy of a segment (prefer write_segment for writing to files, which avoids the copy)."""
    return b"".join(clean_segment_parts(data, first=first))


def write_segment(f: BinaryIO, data, clean: bool, first: bool) -> int:
    """Write a segment to f, optionally cleaning atoms. Returns the number of bytes written."""
    if not clean:
        return f.write(data)
    return sum(f.write(part) for part in clean_segment_parts(data, first=first))


def should_clean(ext: Optional[str]) -> bool:
    """Atom cleaning only applies to MP4 style containers (unknown extensions are assumed MP4)."""
    if not ext:
        return True
    ext = str(ext).lower()
    if not ext.startswith("."):
        ext = f".{ext}"
    return ext in ATOM_BASED_EXTENSIONS


# ==============================================================================================
# FileInfo
# ==============================================================================================

class FileInfo(Path):
    """
    A Path to a produced file that also records what it contains (file_type, e.g. 'video',
    'thumbnail' or 'merged'; and for streams the format ID, codecs, language and protocol),
    which the muxing and moving steps use.
    """
    _file_type: str = None
    _format: str = None
    _acodec: str = None
    _vcodec: str = None
    _language: str = None
    _protocol: str = None

    def __new__(cls, *args, **kwargs):
        # 1. Pop custom arguments so they don't go to Path.__new__
        file_type = kwargs.pop("file_type", None)
        format = kwargs.pop("format", None)
        acodec = kwargs.pop("acodec", None)
        vcodec = kwargs.pop("vcodec", None)
        language = kwargs.pop("language", None)
        protocol = kwargs.pop("protocol", None)
        # 2. Create the instance using Path's machinery
        instance = super().__new__(cls, *args, **kwargs)

        # 3. Store the values on the instance
        instance._file_type = file_type
        instance._format = format
        instance._acodec = acodec
        instance._vcodec = vcodec
        instance._language = language
        instance._protocol = protocol
        return instance

    def __init__(self, *args, **kwargs):
        # 4. Overriding __init__ is CRITICAL.
        # We must NOT pass file_type or format to super().__init__
        for key in ("file_type", "format", "acodec", "vcodec", "language", "protocol"):
            kwargs.pop(key, None)
        super().__init__(*args, **kwargs)

    @property
    def file_type(self):
        return self._file_type

    @file_type.setter
    def file_type(self, value):
        self._file_type = value

    def __repr__(self):
        return f"{super().__repr__()} (file_type={self._file_type})"

    def to_dict(self):
        return {
            "filename": str(self),
            "filetype": str(self._file_type),
            "format": str(self._format)
        }


# ==============================================================================================
# Segment stores
# ==============================================================================================

class SegmentStore:
    """
    Buffers downloaded segments and persists them. `downloaded` contains only segments confirmed
    to be on disk; `pending` holds downloaded segments not yet confirmed.
    """
    def __init__(self, logger: LoggerLike, batch_size: int = 5):
        self.logger = logger
        self.batch_size: int = max(1, int(batch_size or 1))
        self.downloaded: set[int] = set()
        self.pending: dict[int, bytes] = {}

    def add(self, seg: int, data: bytes) -> None:
        """Buffer a segment, keeping the larger copy if it was downloaded more than once."""
        existing = self.pending.get(seg)
        if existing is None or len(data) > len(existing):
            self.pending[seg] = data

    def has(self, seg: int) -> bool:
        return seg in self.downloaded or seg in self.pending

    def count(self) -> int:
        return len(self.downloaded) + sum(1 for seg in self.pending if seg not in self.downloaded)

    def is_backlogged(self) -> bool:
        """True if persisting is failing (e.g. disk full) and new downloads should pause."""
        return len(self.pending) >= max(self.batch_size * STORE_BACKLOG_FACTOR, 50)

    def first_needed(self) -> int:
        """Lowest segment number that may still need downloading."""
        return 0

    def flush(self, force: bool = False) -> None:
        raise NotImplementedError

    def size_on_disk(self) -> int:
        raise NotImplementedError

    def close(self) -> None:
        pass


class SQLiteSegmentStore(SegmentStore):
    """
    Segments stored as BLOBs in an SQLite database (one row per segment, keyed by sequence number).

    Commits are batched (--batch-size) and every commit is verified by reading back the IDs and
    BLOB lengths of the batch; only verified segments leave the in-memory buffer.
    """
    UPSERT_SQL = '''
        INSERT INTO segments (id, segment_data)
        VALUES (?, ?)
        ON CONFLICT(id) DO UPDATE SET
            segment_data = excluded.segment_data
        WHERE LENGTH(excluded.segment_data) > LENGTH(segments.segment_data);
    '''

    def __init__(self, path: str, logger: LoggerLike, batch_size: int = 5, page_size: int = SQLITE_VIDEO_PAGE_SIZE,
                 journal_mode: str = "wal"):
        super().__init__(logger=logger, batch_size=batch_size)
        self.path: str = path
        self.in_memory: bool = path == ':memory:'
        self.page_size: int = page_size
        self.journal_mode: str = str(journal_mode or "wal").lower()
        if self.journal_mode not in ("wal", "truncate"):
            raise ValueError(f"Unsupported journal mode: {journal_mode}")
        self._failed_flushes: int = 0
        self.conn: Optional[sqlite3.Connection] = None
        self.open()

    def open(self) -> None:
        # isolation_level=None: autocommit mode, transactions are managed explicitly in flush()
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        if not self.in_memory:
            # page_size only takes effect before the database is initialised, so it MUST be set before
            # journal_mode (switching to WAL initialises the file). Ignored for existing databases.
            conn.execute(f'PRAGMA page_size = {int(self.page_size)};')
            conn.execute(f'PRAGMA journal_mode = {self.journal_mode.upper()};')
            conn.execute('PRAGMA synchronous = NORMAL;')
            conn.execute('PRAGMA temp_store = MEMORY;')
            # No memory mapping: on ZFS mmap'd files are cached twice (ARC + page cache), and the only
            # large read (the final merge) is sequential so it gains nothing from mmap elsewhere.
            conn.execute('PRAGMA mmap_size = 0;')
            conn.execute('PRAGMA cache_size = -4000;')
            if self.journal_mode == "wal":
                actual_page_size = conn.execute('PRAGMA page_size;').fetchone()[0] or self.page_size
                conn.execute(f'PRAGMA wal_autocheckpoint = {max(1000, SQLITE_WAL_TARGET_BYTES // actual_page_size)};')
            # Keep the WAL / journal file from staying at its peak size after a checkpoint
            conn.execute(f'PRAGMA journal_size_limit = {SQLITE_WAL_TARGET_BYTES};')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS segments (
                id INTEGER PRIMARY KEY,
                segment_data BLOB
            )
        ''')
        self.conn = conn
        self.downloaded = {row[0] for row in conn.execute("SELECT id FROM segments")}

    def flush(self, force: bool = False) -> None:
        if not self.pending or self.conn is None:
            return
        if len(self.pending) < self.batch_size and not force:
            return

        self.logger.debug(f"Writing {len(self.pending)} segments to file...")
        # Sort by ID for linear B-Tree insertion (avoids page splitting)
        batch = sorted(self.pending.items())
        try:
            self.conn.execute('BEGIN')
            self.conn.executemany(self.UPSERT_SQL, batch)
            self.conn.execute('COMMIT')
        except Exception:
            self._failed_flushes += 1
            # Log the full trace once, then only briefly so a persistent failure (disk full) doesn't flood logs
            if self._failed_flushes <= 1 or self._failed_flushes % 50 == 0:
                self.logger.exception(f"Batch insert failed ({self._failed_flushes} consecutive failures), segments kept in memory for retry")
            with contextlib.suppress(Exception):
                self.conn.execute('ROLLBACK')
            return

        self._failed_flushes = 0
        verified = self._verify(batch)
        for seg, _ in batch:
            if seg in verified:
                self.pending.pop(seg, None)
                self.downloaded.add(seg)
        unverified = len(batch) - len(verified)
        if unverified:
            self.logger.warning(f"{unverified} segment(s) could not be verified in {self.path} after commit, they will be written again")

    def _verify(self, batch: list[tuple[int, bytes]]) -> set[int]:
        """Return the segment IDs from batch that are present with at least the inserted length."""
        expected = {seg: len(data) for seg, data in batch}
        verified: set[int] = set()
        ids = list(expected.keys())
        try:
            for i in range(0, len(ids), SQLITE_MAX_VARIABLES):
                chunk = ids[i:i + SQLITE_MAX_VARIABLES]
                placeholders = ",".join("?" * len(chunk))
                rows = self.conn.execute(
                    f"SELECT id, length(segment_data) FROM segments WHERE id IN ({placeholders})", chunk)
                for seg, stored_len in rows:
                    # The upsert keeps the longer copy, so a longer stored BLOB is also valid
                    if (stored_len or 0) >= expected[seg]:
                        verified.add(seg)
        except Exception:
            self.logger.exception("Unable to verify committed segments")
        return verified

    def size_on_disk(self) -> int:
        if self.in_memory:
            return 0
        total = 0
        for path in (self.path, f"{self.path}-wal"):
            with contextlib.suppress(OSError):
                total += os.path.getsize(path)
        return total

    def merge_to_file(self, output_file: str, clean: bool) -> str:
        """
        Write all segments in order into output_file. Segments that could not be committed are
        merged from memory so that nothing that was downloaded is lost.
        """
        self.flush(force=True)
        if self.pending:
            self.logger.error(f"{len(self.pending)} segment(s) could not be written to {self.path}, merging them from memory")
        if not self.in_memory and self.journal_mode == "wal":
            # Fold the WAL into the main file first so the merge read is a plain sequential read
            with contextlib.suppress(Exception):
                self.conn.execute('PRAGMA wal_checkpoint(TRUNCATE);')

        leftovers = sorted((seg, data) for seg, data in self.pending.items() if seg not in self.downloaded)
        leftover_idx = 0
        is_first = True
        with open(output_file, 'wb', buffering=WRITE_BUFFER_SIZE) as f:
            # Iterating the cursor directly streams rows instead of loading everything into memory
            for seg, segment_data in self.conn.execute('SELECT id, segment_data FROM segments ORDER BY id'):
                # Interleave in-memory leftovers that belong before this row
                while leftover_idx < len(leftovers) and leftovers[leftover_idx][0] < seg:
                    write_segment(f, leftovers[leftover_idx][1], clean, is_first)
                    is_first = False
                    leftover_idx += 1
                pending_copy = self.pending.get(seg)
                if pending_copy is not None and len(pending_copy) > len(segment_data or b""):
                    segment_data = pending_copy
                write_segment(f, segment_data or b"", clean, is_first)
                is_first = False
            for _, data in leftovers[leftover_idx:]:
                write_segment(f, data, clean, is_first)
                is_first = False
        return output_file

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.flush(force=True)
            except Exception:
                self.logger.exception("Unable to flush segments while closing database")
            with contextlib.suppress(Exception):
                self.conn.close()
            self.conn = None

    def delete(self) -> None:
        self.close()
        if not self.in_memory:
            for path in (self.path, f"{self.path}-wal", f"{self.path}-shm", f"{self.path}-journal"):
                _remove_file(path, self.logger)


class DirectTSStore(SegmentStore):
    """
    Writes segments straight to the .ts file in sequence order. Out-of-order segments wait in
    memory until the gap before them is filled (or skipped after exceeding retries).
    Progress is recorded in a JSON state file (with a backup copy) so a restart can resume.
    """
    def __init__(self, ts_path: str, state_path: str, backup_path: str, logger: LoggerLike, clean: bool,
                 batch_size: int = 1):
        super().__init__(logger=logger, batch_size=batch_size)
        self.ts_path: str = ts_path
        self.state_path: str = state_path
        self.backup_path: str = backup_path
        self.clean: bool = clean
        self.state: dict[str, int] = {'last_written': -1, 'file_size': 0}
        # Truncate once after resuming, as the file may contain data written after the last state save
        self._needs_truncate: bool = True
        self._load_existing_state()

    @property
    def last_written(self) -> int:
        return int(self.state.get('last_written', -1))

    def _load_existing_state(self) -> None:
        """Restore download progress if a state file exists and agrees with the .ts file."""
        if not os.path.exists(self.ts_path):
            return
        ts_size = os.path.getsize(self.ts_path)
        # The newest state file is tried first. It is only valid if the .ts file is at least as
        # large as the recorded size (it may not be after an OS crash), otherwise the backup is used.
        for path in (self.state_path, self.backup_path):
            if not os.path.exists(path):
                continue
            try:
                with open(path, "r") as file:
                    loaded = json.load(file)
                if ts_size >= loaded.get('file_size', 0) and loaded.get('last_written', None) is not None:
                    self.state = {'last_written': int(loaded['last_written']), 'file_size': int(loaded.get('file_size', 0))}
                    self.logger.debug(f"Resumed state: {self.state}")
                    return
            except Exception as e:
                self.logger.warning(f"Failed to load state file {path}: {e}")

    def _save_state(self) -> None:
        """Atomically write the state file, keeping the previous one as a backup."""
        tmp_path = f"{self.state_path}.tmp"
        try:
            with open(tmp_path, "w") as f:
                json.dump(self.state, f, indent=4)
            if os.path.exists(self.state_path):
                os.replace(self.state_path, self.backup_path)
            os.replace(tmp_path, self.state_path)
        except Exception as e:
            self.logger.warning(f"Failed to save state file: {e}")

    def is_fresh(self) -> bool:
        return self.state.get('file_size', 0) == 0 and self.last_written < 0

    def set_start(self, start_seg: int) -> None:
        """Begin writing at start_seg (used for --start-time) if nothing has been written yet."""
        if self.is_fresh() and start_seg > 0:
            self.state['last_written'] = start_seg - 1

    def has(self, seg: int) -> bool:
        return seg <= self.last_written or seg in self.pending

    def count(self) -> int:
        return max(0, self.last_written + 1) + len(self.pending)

    def first_needed(self) -> int:
        return self.last_written + 1

    def add(self, seg: int, data: bytes) -> None:
        if seg > self.last_written:
            super().add(seg, data)

    def skip_to(self, seg: int) -> None:
        """Give up on segment `seg` (exceeded retries) so later segments can be written."""
        if seg == self.last_written + 1:
            self.state['last_written'] = seg
            self.flush()

    def flush(self, force: bool = False) -> None:
        if self.pending.get(self.last_written + 1) is None:
            return
        mode = 'wb' if self.state.get('file_size', 0) == 0 else 'r+b'
        is_first = self.state.get('file_size', 0) == 0
        written_any = False
        with open(self.ts_path, mode, buffering=WRITE_BUFFER_SIZE) as f:
            if mode != 'wb':
                f.seek(self.state['file_size'])
                if self._needs_truncate:
                    f.truncate()
            self._needs_truncate = False
            # Write all available consecutive segments
            while self.pending.get(self.last_written + 1) is not None:
                seg = self.last_written + 1
                write_segment(f, self.pending.pop(seg), self.clean, is_first)
                is_first = False
                written_any = True
                self.state['last_written'] = seg
                self.state['file_size'] = f.tell()
                self.logger.debug(f"Written segment {seg}, file size: {self.state['file_size']} bytes")
        if written_any:
            self._save_state()
        # Drop anything that can no longer be written (at or before last_written)
        for seg in [s for s in self.pending if s <= self.last_written]:
            self.pending.pop(seg, None)

    def write_remaining(self) -> None:
        """At the end of a download, write buffered segments after gaps in order (gaps are skipped)."""
        self.flush(force=True)
        if not self.pending:
            return
        self.logger.warning(f"Writing {len(self.pending)} buffered segment(s) after missing segment(s) {self.last_written + 1}")
        for seg in sorted(self.pending):
            if seg > self.last_written + 1:
                self.state['last_written'] = seg - 1
            self.flush(force=True)

    def size_on_disk(self) -> int:
        return int(self.state.get('file_size', 0))

    def delete_state_files(self) -> None:
        for path in (self.state_path, self.backup_path, f"{self.state_path}.tmp"):
            _remove_file(path, self.logger)


# ==============================================================================================
# Stream downloaders
# ==============================================================================================

class LoopAction(enum.Enum):
    PROCEED = 0    # carry on to submitting downloads this iteration
    CONTINUE = 1   # skip submitting this iteration
    BREAK = 2      # leave the download loop


class BackgroundRefresh:
    """
    A yt-dlp URL refresh running in a background thread, so segment downloads continue while
    yt-dlp works (an extraction can take many seconds). At most one runs per stream downloader.
    """
    def __init__(self) -> None:
        self._thread: Optional[threading.Thread] = None
        self._result: Optional[tuple[dict, str]] = None
        self._error: Optional[BaseException] = None

    @property
    def started(self) -> bool:
        """True from start() until the result is collected (even if the thread already finished)."""
        return self._thread is not None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, fetch: Callable[[], tuple[dict, str]]) -> None:
        def run() -> None:
            try:
                self._result = fetch()
            except BaseException as e:  # KeyboardInterrupt from an interrupted wait is also stored
                self._error = e

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def wait(self, timeout: Optional[float] = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout)

    def collect(self) -> tuple[Optional[tuple[dict, str]], Optional[BaseException]]:
        """Wait for the refresh to finish and return (result, error), ready for the next refresh."""
        self.wait()
        result, error = self._result, self._error
        self._thread = self._result = self._error = None
        return result, error


class StreamPaths:
    """All file paths used for one format of one stream."""
    def __init__(self, options: dict, video_id: str, format_id: str, in_memory: bool):
        # 'file_name' and 'folder' are legacy keys that were used inconsistently by older code paths
        self.filename: str = str(_opt(options, 'filename', 'file_name', default=video_id))
        self.folder: Optional[str] = _opt(options, 'download_folder', 'folder', default=None) or None
        if self.folder:
            os.makedirs(self.folder, exist_ok=True)

        def in_folder(name: str) -> str:
            return os.path.join(self.folder, name) if self.folder else name

        self.file_base_name: str = in_folder(self.filename)
        self.merged_file_name: str = in_folder(f"{self.filename}.{format_id}.ts")
        self.temp_db_file: str = ':memory:' if in_memory else in_folder(f'{self.filename}.{format_id}.temp')
        self.state_file: str = in_folder(f"{self.filename}.{format_id}.state")
        self.state_backup: str = in_folder(f"{self.filename}.{format_id}.state.bkup")


class StreamDownloaderBase:
    """
    Shared lifecycle for downloading the segments of a single format (one video or audio stream).

    Use as a context manager, then call live_dl() followed by finalize():

        with DownloadStream(info_dict, stream_url, options, livestream_coordinator) as stream:
            stream.live_dl()
            ts_file = stream.finalize()

    live_dl() runs the main loop, shared by all subclasses. Each iteration:

        check for kill / graceful stop
        _pre_iteration()            e.g. start or collect a background URL refresh
        _collect_results()          wait for finished downloads -> _on_segment_result()
        store.flush()               persist buffered segments (in batches)
        _evaluate()                 BREAK / CONTINUE / PROCEED (may wait for new segments)
        _submit_segments()          start downloads for _next_segments()

    Subclasses provide the storage (_make_store), the scheduling (_next_segments), the handling
    of results (_on_segment_result), the per-iteration decisions (_evaluate) and finalize().
    """
    timeout_config = httpx.Timeout(10.0, connect=5.0)
    ATOM_BASED_EXTENSIONS = ATOM_BASED_EXTENSIONS
    # 30-second hard limit for the entire download of one segment per attempt
    MAX_SEGMENT_DURATION: float = 30.0
    STATUS_FORCELIST = frozenset({204, 400, 401, 403, 404, 408, 413, 429, 500, 502, 503, 504})
    # Per segment request retry back-off: min(BACKOFF_MAX, BACKOFF_FACTOR * 2**attempt) seconds
    BACKOFF_FACTOR: float = 1.0
    BACKOFF_MAX: float = 4.0
    # Poll interval while waiting for futures
    POLL_INTERVAL: float = 1.0
    mode_name: str = "stream"

    def __init__(self, info_dict: dict, stream_url: Optional[YoutubeURL.YoutubeURL], options: Optional[dict] = None,
                 livestream_coordinator: Optional["LiveStreamDownloader"] = None, **kwargs):
        self.livestream_coordinator = livestream_coordinator
        if self.livestream_coordinator:
            self.logger: LoggerLike = self.livestream_coordinator.logger
            self.kill_all: threading.Event = self.livestream_coordinator.kill_all
            self.kill_this: threading.Event = self.livestream_coordinator.kill_this
        else:
            self.logger = logging.getLogger()
            self.kill_all = threading.Event()
            self.kill_this = threading.Event()
        self._local_stats: dict = {}

        self.options: dict = dict(options or {})
        self.info_dict: dict = info_dict
        self.live_status: Optional[str] = info_dict.get('live_status') or kwargs.get('live_status')
        self._read_options()

        # --- Stream URL(s) ---
        self.stream_urls: list[YoutubeURL.YoutubeURL] = self._initial_stream_urls(stream_url, kwargs)
        if not self.stream_urls:
            raise ValueError(f"Stream URL not found for {self.resolution}, unable to continue")
        self.stream_url: YoutubeURL.YoutubeURL = self._choose_initial_url()
        self.id: str = info_dict.get('id') or self.stream_url.id
        # A stream with a video codec is video, everything else is audio
        self.type: str = "video" if (stream_url or self.stream_url).vcodec else "audio"
        self.format = self.stream_url.format_id
        self.ext: Optional[str] = self.stream_url.ext
        self.logger.debug(f"{self.id} stream URL parameters: {json.dumps(self.stream_url.url_parameters)}")
        self._validate_urls()

        self.paths = StreamPaths(self.options, self.id, self.format, self.database_in_memory)
        self._init_state()

        _quiet_httpx_logging(self.logger)
        self.client: httpx.Client = self._create_client()
        self.store: SegmentStore = self._make_store()
        self.already_downloaded: set[int] = self.store.downloaded

        try:
            self.update_latest_segment()
        except BaseException:
            # Don't leak the open database / HTTP client if construction fails
            self.close()
            raise
        self.url_checked: float = time.time()
        self._schedule_next_refresh()
        self._stats()["latest_sequence"] = self.latest_sequence

    def _read_options(self) -> None:
        """Copy the options this class uses into attributes (internal key names first, then CLI names)."""
        self.batch_size: int = int(_opt(self.options, 'batch_size', default=10))
        self.max_workers: int = max(1, int(_opt(self.options, 'max_workers', 'threads', default=2)))
        self.fragment_retries: int = int(_opt(self.options, 'fragment_retries', 'segment_retries', default=10))
        self.yt_dlp_options: Optional[dict] = _opt(self.options, 'ytdlp_options', 'yt_dlp_options')
        self.include_dash: bool = bool(_opt(self.options, 'include_dash', 'dash', default=False))
        self.include_m3u8: bool = bool(_opt(self.options, 'include_m3u8', 'm3u8', default=False))
        self.force_m3u8: bool = bool(self.options.get('force_m3u8', False))
        self.resolution: str = self.options.get("resolution") or "bv+ba/best"
        self.yt_dlp_sort: Optional[str] = _opt(self.options, 'custom_sort', 'yt_dlp_sort')
        self.wait_limit: int = int(self.options.get("wait_limit") or 0)
        self.cookies: Optional[str] = self.options.get('cookies')
        self.proxies = _opt(self.options, 'proxies', 'proxy')
        self.database_in_memory: bool = bool(self.options.get('database_in_memory', False))
        self.url_refresh_interval: Optional[float] = self.options.get('url_refresh_interval')
        self.url_refresh_jitter: float = float(_opt(self.options, 'url_refresh_jitter', default=DEFAULT_URL_REFRESH_JITTER))
        self.recovery_thread_multiplier: int = int(self.options.get('recovery_thread_multiplier', 2))
        # --start-time / --end-time in seconds (negative = relative to the live edge), None if not set
        self.start_time: Optional[float] = self.options.get('start_time')
        self.end_time: Optional[float] = self.options.get('end_time')

    def _init_state(self) -> None:
        """Initial values of the state that changes while downloading."""
        # Newest segment number the server has reported, and the estimated duration of one segment
        self.latest_sequence: int = -1
        self.estimated_segment_duration: float = 0
        self.is_403: bool = False
        self.is_401: bool = False
        self.is_private: bool = False
        self.is_members_error: bool = False
        self.refresh_retries: int = 0
        self.following_manifest_thread: Optional[threading.Thread] = None
        self._background_refresh = BackgroundRefresh()
        self._last_head_time: Optional[float] = None
        # SQLite connections may only be used by the thread that created them
        self.sqlite_thread = threading.get_ident()

        # The requested time window as segment numbers (inclusive). It can only be calculated once
        # the segment duration is known, see _try_resolve_window.
        self.start_seg: int = 0
        self.end_seg: float = float('inf')
        self._window_seconds: Optional[tuple[float, Optional[float]]] = None
        self._window_resolved: bool = self.start_time is None and self.end_time is None

    # --- Construction hooks (called by __init__, overridden by subclasses) ---

    def _initial_stream_urls(self, stream_url: Optional[YoutubeURL.YoutubeURL], kwargs: dict) -> list[YoutubeURL.YoutubeURL]:
        """All URLs this downloader may use. StreamRecovery uses several; the others only one."""
        return [stream_url] if stream_url is not None else []

    def _choose_initial_url(self) -> YoutubeURL.YoutubeURL:
        """The URL used for header probes and (by default) segment downloads."""
        return self.stream_urls[0]

    def _validate_urls(self) -> None:
        """Raise if the URLs can't be used (StreamRecovery: all of them have expired)."""
        pass

    def _make_store(self) -> SegmentStore:
        """Where downloaded segments are stored. Default: the SQLite temp database (or in memory)."""
        if self.database_in_memory:
            return SQLiteSegmentStore(':memory:', logger=self.logger, batch_size=self.batch_size)
        page_size = SQLITE_VIDEO_PAGE_SIZE if self.type == "video" else SQLITE_AUDIO_PAGE_SIZE
        journal_mode = self.options.get("temp_db_journal") or "wal"
        return SQLiteSegmentStore(self.paths.temp_db_file, logger=self.logger, batch_size=self.batch_size,
                                  page_size=page_size, journal_mode=journal_mode)

    # --- Context management ---

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self) -> None:
        self.close_connection()
        with contextlib.suppress(Exception):
            self.client.close()

    def close_connection(self) -> None:
        """Close the segment store (flushing buffered segments). Kept under its historical name."""
        store = getattr(self, "store", None)
        if store is not None and threading.get_ident() == self.sqlite_thread:
            store.close()

    # --- Small helpers ---

    def _create_client(self) -> httpx.Client:
        return create_http_client(proxies=self.proxies, headers=self.info_dict.get("http_headers"),
                                  max_connections=self.max_workers + 2, timeout=self.timeout_config)

    def _ensure_client(self) -> httpx.Client:
        if self.client is None or self.client.is_closed:
            self.client = self._create_client()
        return self.client

    def _stats(self) -> dict:
        """Stats dictionary of this format (shared with the coordinator for printing)."""
        if self.livestream_coordinator:
            return self.livestream_coordinator.stats.setdefault(self.type, {})
        return self._local_stats.setdefault(self.type, {})

    def _graceful_stop_requested(self) -> bool:
        return bool(self.livestream_coordinator and self.livestream_coordinator.graceful_stop.is_set())

    def get_expire_time(self, url: YoutubeURL.YoutubeURL) -> Optional[int]:
        return url.expire

    def _url_is_valid(self, url: YoutubeURL.YoutubeURL, now: Optional[float] = None) -> bool:
        """URLs without an expire parameter are assumed to stay valid."""
        expire = self.get_expire_time(url)
        return expire is None or expire > (now or time.time())

    def unique_stream_urls(self, stream_urls: list) -> list:
        return list({str(stream_url): stream_url for stream_url in stream_urls}.values())

    def smart_sleep(self, timeout: float) -> None:
        """Sleep, but wake up early if a graceful stop or kill is requested."""
        deadline = time.monotonic() + max(0.0, timeout)
        graceful = self.livestream_coordinator.graceful_stop if self.livestream_coordinator else None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self.kill_all.is_set() or self.kill_this.is_set():
                return
            if graceful is not None:
                if graceful.wait(min(remaining, 0.5)):
                    return
            else:
                time.sleep(min(remaining, 0.5))

    def check_kill(self, executor: Optional[concurrent.futures.ThreadPoolExecutor] = None, tasks=None) -> None:
        """Checks kill flags. If set, cancels outstanding work and raises KeyboardInterrupt."""
        if self.kill_all.is_set() or self.kill_this.is_set():
            try:
                self.logger.debug("Kill command detected, ending...")
                if executor is not None:
                    executor.shutdown(wait=True, cancel_futures=True)
                if tasks is not None:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                self.close_connection()
            except Exception:
                self.logger.exception("Unable to perform cleanup")
            raise KeyboardInterrupt("Kill command executed")

    # --- HTTP ---

    def get_Headers(self, url, client: Optional[httpx.Client] = None) -> Optional[httpx.Headers]:
        """
        Request `url` and return the response headers without downloading the body.
        The body (a full media segment) is not needed; closing the streamed response early saves
        downloading a whole segment every time the live edge is probed.
        """
        client = client if (client is not None and not client.is_closed) else self._ensure_client()
        try:
            with client.stream("GET", str(url)) as response:
                # 200 and 204 responses appear to have valid headers so far
                if response.status_code in (200, 204):
                    self.is_403 = self.is_401 = False
                elif response.status_code == 403:
                    self.logger.warning("Received 403 error, marking for URL refresh...")
                    self.is_403 = True
                elif response.status_code == 401:
                    self.is_401 = True
                else:
                    self.logger.debug(f"Error retrieving headers: {response.status_code}")
                    self.logger.debug(json.dumps(dict(response.headers), indent=4))
                return response.headers
        except httpx.TimeoutException as e:
            self.logger.info(f"Timed out updating fragments: {e}")
        except Exception as e:
            self.logger.exception(colour(e, RED))
        return None

    def download_segment(self, segment_url: str, segment_order: int, client: Optional[httpx.Client] = None,
                         immediate_403s: bool = False, max_retries: Optional[int] = None) -> SegmentResult:
        """
        Download one segment, retrying failed requests up to `max_retries` times (default
        --segment-retries). Runs in the worker threads of live_dl's thread pool.
        """
        total_retries = self.fragment_retries if max_retries is None else max_retries

        for attempt in range(total_retries + 1):
            if self._graceful_stop_requested():
                return SegmentResult.failed(segment_order)
            if client is None or client.is_closed:
                client = self._ensure_client()
            is_last_attempt = attempt >= total_retries
            try:
                self.check_kill()
                return self._request_segment(client, segment_url, segment_order, immediate_403s,
                                             retry_on_error_status=not is_last_attempt)
            except httpx.ConnectTimeout:
                if is_last_attempt:
                    raise
            except (httpx.RequestError, httpx.HTTPStatusError, httpx.TimeoutException) as e:
                if is_last_attempt:
                    self.logger.warning(f"({self.format}) Unexpected error downloading segment {segment_order} file after {attempt} attempts {e}")
                    response = getattr(e, 'response', None) if isinstance(e, httpx.HTTPStatusError) else None
                    return SegmentResult.failed(segment_order, getattr(response, 'status_code', None))
            except KeyboardInterrupt:
                raise
            except Exception as e:
                self.logger.exception(f"Unknown error: {e}")
                if is_last_attempt:
                    return SegmentResult.failed(segment_order)

            # Exponential backoff with jitter so parallel workers don't retry in lock-step
            sleep_time = min(self.BACKOFF_MAX, self.BACKOFF_FACTOR * (2 ** attempt)) * random.uniform(0.75, 1.0)
            self.smart_sleep(sleep_time)
        return SegmentResult.failed(segment_order)

    def _request_segment(self, client: httpx.Client, segment_url: str, segment_order: int, immediate_403s: bool,
                         retry_on_error_status: bool) -> SegmentResult:
        """
        One GET request for a segment. Raises httpx.HTTPStatusError for a status in STATUS_FORCELIST
        while retries remain (retry_on_error_status), so download_segment retries it.
        """
        start_time = time.monotonic()
        with client.stream("GET", segment_url) as response:
            status = response.status_code
            headers = response.headers
            head_seq = _int_header(headers, "X-Head-Seqnum")
            head_seq = -1 if head_seq is None else head_seq

            if status == 403 and immediate_403s:
                # Recovery mode reacts to the first 403, not only once retries are used up
                self.is_403 = True
            if status in self.STATUS_FORCELIST and retry_on_error_status:
                raise httpx.HTTPStatusError("Retryable Status", request=response.request, response=response)
            self._note_auth_status(status, segment_order)

            if status == 200:
                chunks: list[bytes] = []
                # Read in chunks to regularly check progress against the hard time limit
                for chunk in response.iter_bytes(chunk_size=SEGMENT_READ_CHUNK_SIZE):
                    if (time.monotonic() - start_time) > self.MAX_SEGMENT_DURATION:
                        raise httpx.TimeoutException(f"Download exceeded hard limit of {self.MAX_SEGMENT_DURATION}")
                    chunks.append(chunk)
                # A single join is the only copy of the data
                return SegmentResult(head_seq, b"".join(chunks), int(segment_order), status, headers)
            if status == 204:
                return SegmentResult(head_seq, b"", int(segment_order), status, headers)
            self.logger.warning(f"({self.format}) Unexpected status returned {status}")
            return SegmentResult(head_seq, None, int(segment_order), status, headers)

    def _note_auth_status(self, status: int, segment_order: int) -> None:
        """Track 401/403 responses (they trigger URL refreshes / recovery back-off); success clears them."""
        if status == 403:
            self.is_403 = True
            self.logger.warning(f"({self.format}) Encountered 403 status when downloading segment {segment_order}")
        elif status == 401:
            self.is_401 = True
            self.logger.warning(f"({self.format}) Encountered 401 status when downloading segment {segment_order}. This can be cause by using too many threads.")
        elif status in (200, 204):
            self.is_403 = self.is_401 = False

    def _apply_head_headers(self, headers: Optional[httpx.Headers], set_type_from_content: bool = False) -> None:
        """Update latest sequence, segment duration and extension from response headers."""
        if headers is None:
            return
        head_seq = _int_header(headers, "X-Head-Seqnum")
        if head_seq is not None and head_seq > self.latest_sequence:
            self._on_head_advanced(self.latest_sequence, head_seq)
            self.latest_sequence = head_seq
            self.logger.debug(f"Latest sequence: {self.latest_sequence}")

        head_time = _int_header(headers, "X-Head-Time-Sec")
        if head_time is not None:
            self._last_head_time = float(head_time)
            # Prefer the response's own head sequence; guards against dividing by a zero sequence
            duration = estimate_segment_duration(head_time, head_seq if head_seq else self.latest_sequence)
            if duration:
                self.estimated_segment_duration = duration
                self._recalculate_window()

        file_type, ext = parse_content_type(headers.get('Content-Type'))
        if file_type in ("video", "audio"):
            if set_type_from_content:
                self.type, self.ext = file_type, ext
            elif not self.ext:
                self.ext = ext

    def _on_head_advanced(self, old_head: int, new_head: int) -> None:
        """Called before latest_sequence moves from old_head to new_head."""
        pass

    def update_latest_segment(self, client: Optional[httpx.Client] = None, url=None) -> None:
        # Kill if keyboard interrupt is detected
        self.check_kill()
        headers = self.get_Headers(url=url or self.stream_url, client=client)
        self._apply_head_headers(headers)
        self._stats()["latest_sequence"] = self._window_latest()

    # --- Time window (--start-time / --end-time) ---

    def _time_reference(self, head_time: float) -> float:
        """Shared live-edge reference so video and audio resolve negative times identically."""
        if self.livestream_coordinator:
            return self.livestream_coordinator.resolve_time_reference(head_time)
        if not hasattr(self, "_own_time_reference"):
            self._own_time_reference = head_time
        return self._own_time_reference

    def _try_resolve_window(self) -> bool:
        """Resolve the requested time window into segment numbers. Returns True once resolved."""
        if self._window_resolved:
            return True
        if self.estimated_segment_duration <= 0:
            headers = self.get_Headers(url=self.stream_url)
            self._apply_head_headers(headers)
            if self.estimated_segment_duration <= 0:
                return False
        reference = None
        if is_relative_time(self.start_time) or is_relative_time(self.end_time):
            if self._last_head_time is None:
                return False
            reference = self._time_reference(self._last_head_time)
        self._window_seconds = resolve_window_seconds(self.start_time, self.end_time, reference)
        self._window_resolved = True
        self._recalculate_window()
        self.logger.debug(f"Segments calculated: Start={self.start_seg}, End={self.end_seg}")
        self._on_window_resolved()
        return True

    def _recalculate_window(self) -> None:
        # The estimate is refined as more segments arrive, so the segment window is recalculated
        if self._window_seconds is None or self.estimated_segment_duration <= 0:
            return
        self.start_seg, self.end_seg = seconds_to_segment_window(*self._window_seconds, self.estimated_segment_duration)

    def _on_window_resolved(self) -> None:
        pass

    def _window_limited(self) -> bool:
        return self.start_seg != 0 or self.end_seg != float('inf')

    def _in_window(self, seg: int) -> bool:
        return self.start_seg <= seg <= self.end_seg

    def _window_latest(self) -> int:
        """Latest sequence number expressed relative to the window (for stats)."""
        if self._window_limited():
            return int(max(0, min(self.latest_sequence, self.end_seg) - self.start_seg))
        return self.latest_sequence

    # --- URL refresh ---

    def _schedule_next_refresh(self) -> None:
        self.next_refresh_at: float = next_refresh_time(time.time(), self.get_expire_time(self.stream_url),
                                                        self.url_refresh_interval, self.url_refresh_jitter)

    def refresh_Check(self, wait: bool = True) -> Optional[RefreshOutcome]:
        """
        Refresh URLs if a periodic refresh is due, 403s persist, or no unexpired URL remains.
        Returns None if no refresh was needed, otherwise the outcome of refresh_url.
        """
        now = time.time()
        self.stream_urls = [url for url in self.unique_stream_urls(self.stream_urls) if self._url_is_valid(url, now)]

        # By this stage, a stream would have a URL. Keep using it if the video becomes private or a membership
        due = now >= self.next_refresh_at
        recent_403 = (now - self.url_checked >= REFRESH_403_AFTER_SECONDS and self.is_403)
        if (due or recent_403 or len(self.stream_urls) <= 0) and not self.is_private:
            return self.refresh_url(wait=wait)
        return None

    def _fetch_refreshed_info(self, ignore_no_formats: bool) -> tuple[dict, str]:
        common = dict(cookies=self.cookies, additional_options=self.yt_dlp_options, include_dash=self.include_dash,
                      include_m3u8=(self.include_m3u8 or self.force_m3u8), ignore_no_formats=ignore_no_formats,
                      logger=self.logger)
        if self.livestream_coordinator:
            # The coordinator shares one extraction between all formats (and rate limits them)
            return self.livestream_coordinator.refresh_info_json(
                update_threshold=min(900.0, time.time() - self.url_checked), id=self.id, proxy=self.proxies, **common)
        return getUrls.get_Video_Info(self.id, wait=False, proxy=self.proxies, **common)

    def refresh_url(self, follow_manifest: bool = True, wait: bool = True, ignore_no_formats: bool = False) -> RefreshOutcome:
        """
        Refresh the stream URL with a yt-dlp extraction, which runs in a background thread.

        With wait=False this returns IN_PROGRESS while the extraction runs; call it again on a later
        loop iteration to collect the result. With wait=True it blocks until the result is in.
        """
        refresh = self._background_refresh
        # No new extraction while a manifest-follow thread is active or the download is stopping
        if not refresh.started and self.following_manifest_thread is None and not self._graceful_stop_requested():
            self.logger.info(f"Starting background URL refresh for {self.format}")
            refresh.start(lambda: self._fetch_refreshed_info(ignore_no_formats))

        result = error = None
        if refresh.started:
            if refresh.is_running() and not wait:
                return RefreshOutcome.IN_PROGRESS
            result, error = refresh.collect()

        if self.following_manifest_thread is None and (error is not None or result is not None):
            if self._process_refresh_result(result, error, follow_manifest):
                return RefreshOutcome.NEW_MANIFEST

        self.url_checked = time.time()
        self._schedule_next_refresh()

        if self.live_status not in ['is_live', 'is_upcoming']:
            self.logger.debug("Livestream has ended.")
            return RefreshOutcome.ENDED

        expire = self.get_expire_time(self.stream_url)
        if expire is not None and expire < time.time():
            raise TimeoutError(f"Video is unavailable and stream url for {self.format} has expired, unable to continue...")

        return RefreshOutcome.STILL_LIVE

    def _process_refresh_result(self, result: Optional[tuple[dict, str]], error: Optional[BaseException],
                                follow_manifest: bool) -> bool:
        """Apply a refresh result. Returns True if a new manifest was detected."""
        try:
            if error:
                # Re-raised so the extraction errors below are handled in one place
                raise error
            info_dict, live_status = result

            if self.detect_manifest_change(info_json=info_dict, follow_manifest=follow_manifest) is True:
                return True

            if live_status:
                self.live_status = live_status
            self._use_refreshed_url(info_dict)
            if info_dict:
                self.info_dict = info_dict

        except getUrls.VideoInaccessibleError as e:
            self._on_video_inaccessible(e)
        except getUrls.VideoUnavailableError as e:
            self.logger.warning(f"Video unavailable; treating the active recording as post-live: {e}")
            self.live_status = "post_live"
        except getUrls.VideoProcessedError as e:
            # Livestream has been processed
            self.logger.exception(f"Error refreshing URL: {e}")
            self.logger.info("Livestream has ended and processed.")
            self.live_status = "was_live"
        except getUrls.LivestreamError:
            self.logger.debug("Livestream has ended.")
            self.live_status = "post_live"
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.logger.exception(f"Error: {e}")
        return False

    def _use_refreshed_url(self, info_dict: dict) -> None:
        """Switch to the fresh URL of this stream's format from a new extraction."""
        resolution = self._same_format_selector()
        stream_url = YoutubeURL.Formats(logger=self.logger).getFormatURL(
            info_json=info_dict, resolution=resolution, sort=self.yt_dlp_sort, include_dash=self.include_dash,
            include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8, stream_type=self.type, logger=self.logger)
        if stream_url is None:
            self.logger.warning(f"Unable to refresh URLs for {self.id} on format {resolution} ({self.format})")
            return
        self.stream_url = stream_url
        self.stream_urls.append(stream_url)
        now = time.time()
        self.stream_urls = [url for url in self.unique_stream_urls(self.stream_urls) if self._url_is_valid(url, now)]
        self.refresh_retries = 0
        self.logger.log(setup_logger.VERBOSE_LEVEL_NUM, f"Refreshed stream URL with {stream_url.format_id} - {stream_url.format_note}")
        self.is_members_error = False

    def _on_video_inaccessible(self, error: Exception) -> None:
        """
        The video can't be extracted any more. Work out why from the error message:
        private (or 403s already seen) -> stop / hand over to recovery once segments run out;
        members-only / premium -> keep downloading with the current URL until it returns 403s;
        anything else -> treat the stream as ended (post-live).
        """
        error_msg = str(error).lower()
        self.logger.warning(f"Video Inaccessible error: {error}")
        if "private" in error_msg or self.is_403:
            self.logger.warning(f"{self.id} is Inaccessible and is either private or experiencing 403 errors. Marking as private.")
            self.is_private = True
        elif "membership" in error_msg or "premium" in error_msg:
            self.logger.warning(f"{self.id} is now members only or premium. Continuing until 403 errors")
            self.is_members_error = True
        else:
            if "recording is not available" in error_msg:
                self.logger.warning("Recording is not available, marking as post-live")
            else:
                self.logger.warning("Stream is inaccessible but not encountering 403 errors. Marking as post-live.")
            self.live_status = "post_live"

    # --- New manifest handling ---

    def _follow_new_manifest(self, info_json: dict, new_url: YoutubeURL.YoutubeURL, manifest_key, extra_options: Optional[dict] = None) -> None:
        """Start a thread downloading `new_url` into its own files, registered under manifest_key."""
        self.store.flush(force=True)
        new_options = copy.copy(self.options)
        new_options.update(extra_options or {})
        # Distinct file name (without folder; the folder is applied separately) so the new manifest
        # never shares a temp database / state file with this one
        new_options.update({
            "filename": f"{self.paths.filename}.{new_url.manifest}",
            "download_folder": self.paths.folder,
        })
        new_options.pop("file_name", None)
        new_params = {
            "info_dict": info_json,
            "stream_url": new_url,
            "manifest": manifest_key,
            "options": new_options,
        }
        download_function: Optional[Callable] = new_options.get("download_function")
        if download_function is not None:
            target, kwargs = download_function, new_params
        else:
            stream_cls = self.__class__
            coordinator = self.livestream_coordinator

            def run_standalone():
                # The instance must be created inside the thread: its SQLite connection can only be
                # used by the thread that created it
                with stream_cls(info_dict=info_json, stream_url=new_url, options=new_options,
                                livestream_coordinator=coordinator) as instance:
                    instance.live_dl()
                    instance.finalize()

            target, kwargs = run_standalone, {}
        self.following_manifest_thread = threading.Thread(target=target, kwargs=kwargs, daemon=True)
        self.following_manifest_thread.start()

    def _same_format_selector(self) -> str:
        """yt-dlp format selector matching this stream's format (itag) and protocol."""
        return f"(bv/ba/best)[format_id~='^{self.stream_url.itag}(?:-.*)?$'][protocol={self.stream_url.protocol}]"

    def _find_format_url(self, info_json: dict, resolution: str, sort: Optional[str] = None) -> Optional[YoutubeURL.YoutubeURL]:
        return YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=resolution, sort=sort,
                                                 include_dash=self.include_dash, include_m3u8=self.include_m3u8,
                                                 force_m3u8=self.force_m3u8, stream_type=self.type)

    def _start_new_manifest(self, info_json: dict, new_url: YoutubeURL.YoutubeURL, manifest_key, follow_manifest: bool,
                            extra_options: Optional[dict] = None) -> None:
        if follow_manifest:
            self._follow_new_manifest(info_json, new_url, manifest_key=manifest_key, extra_options=extra_options)
        else:
            self.store.flush(force=True)

    def detect_manifest_change(self, info_json, follow_manifest: bool = True) -> bool:
        """
        A restarted stream gets a new manifest number in its URLs and restarts sequence numbers.
        Detect that and download the new manifest in a separate thread (and separate files).
        Returns True if a new manifest was found.
        """
        # 1. Look for the same format (itag) under a new manifest number
        resolution = self._same_format_selector()
        try:
            self.logger.debug(f"Searching for new manifest of same format {resolution}")
            new_url = self._find_format_url(info_json, resolution)
            if new_url is not None:
                is_new_manifest = (new_url.itag is not None and new_url.itag == self.stream_url.itag
                                   and new_url.protocol == self.stream_url.protocol
                                   and new_url.manifest != self.stream_url.manifest)
                if is_new_manifest:
                    self.logger.warning(f"({self.id}) New manifest for format {self.format} detected, starting a new instance for the new manifest")
                    self._start_new_manifest(info_json, new_url, new_url.manifest, follow_manifest)
                return is_new_manifest
        except (yt_dlp.utils.ExtractorError, ValueError):
            self.logger.warning(f"Unable to find stream of same format ({resolution}) for {self.id}")

        # 2. The same format is no longer available. Try the same resolution, then the best stream.
        searches = [(self.resolution, "resolution", {})]
        if self.resolution != "audio_only":
            searches.append(("bv+ba/best", "best", {"resolution": "bv+ba/best"}))
        for search_resolution, description, extra_options in searches:
            try:
                new_url = self._find_format_url(info_json, search_resolution, sort=self.yt_dlp_sort)
                if new_url is None:
                    continue
                self.logger.debug(f"Searching for new manifest of same {description} {search_resolution}")
                if new_url.itag is None or new_url.itag == self.stream_url.itag:
                    return False
                self.logger.warning(f"({self.id}) New manifest for {description} {search_resolution} detected, but not the same format as {self.format}, starting a new instance for the new manifest")
                # Register under the new manifest number. If the manifest number is unchanged
                # (only the format changed) use the old itag so this format's files aren't replaced.
                manifest_key = self.stream_url.itag if self.stream_url.manifest == new_url.manifest else new_url.manifest
                self._start_new_manifest(info_json, new_url, manifest_key, follow_manifest, extra_options)
                return True
            except (yt_dlp.utils.ExtractorError, ValueError):
                self.logger.warning(f"Unable to find any stream for {self.id} when searching {description} {search_resolution}")
        return False

    # --- Main loop hooks (called by live_dl, overridden by subclasses) ---

    def _on_start(self) -> None:
        """Called once before the first iteration. Subclasses set up their scheduling state here."""
        self._stats()['status'] = "recording"

    def _pre_iteration(self) -> None:
        """Called at the start of every iteration, before finished downloads are collected."""
        pass

    def _on_segment_result(self, seg: Optional[int], result: Optional[SegmentResult], error: Optional[BaseException]) -> None:
        """
        Called for every finished download_segment task. `result` is None if the task raised
        `error`. Store the data (self.store.add) or schedule a retry.
        """
        raise NotImplementedError

    def _evaluate(self) -> LoopAction:
        """
        Decide what the loop does next, after results are collected and stored:
        BREAK to finish, CONTINUE to skip submitting new downloads, PROCEED to submit them.
        May sleep (e.g. while waiting for new segments at the live edge).
        """
        raise NotImplementedError

    def _next_segments(self, capacity: int) -> list[int]:
        """Return up to `capacity` segment numbers to download next."""
        raise NotImplementedError

    def _url_for_segment(self, seg: int) -> str:
        return self.stream_url.segment(seg)

    def _download_args(self, seg: int) -> tuple:
        """Positional arguments for download_segment."""
        return (self._url_for_segment(seg), seg, self.client)

    def _on_finish(self) -> None:
        """Called once after the loop ends (also after a graceful stop)."""
        if self.following_manifest_thread is not None:
            self.following_manifest_thread.join()

    def _loop_result(self):
        """The value live_dl returns."""
        return True

    def _collect_results(self) -> tuple[int, int]:
        """Wait for finished downloads (or sleep if none are running) and process them."""
        if not self.future_to_seg:
            # concurrent.futures.wait returns immediately for an empty set, which would busy-loop
            self.smart_sleep(self.POLL_INTERVAL)
            return 0, 0
        done, not_done = concurrent.futures.wait(self.future_to_seg, timeout=self.POLL_INTERVAL,
                                                 return_when=concurrent.futures.FIRST_COMPLETED)
        for future in done:
            seg = self.future_to_seg.pop(future, None)
            result = error = None
            try:
                result = future.result()
            except BaseException as e:
                error = e
            try:
                if result is not None:
                    self._learn_from_response(result)
                else:
                    self._log_download_error(seg, error)
                self._on_segment_result(result.seg if result is not None else seg, result, error)
            except Exception:
                self.logger.exception(f"An unknown error occurred processing segment {seg}")
            finally:
                self.submitted_segments.discard(seg)
                if result is not None:
                    self.submitted_segments.discard(result.seg)
        return len(done), len(not_done)

    def _learn_from_response(self, result: SegmentResult) -> None:
        """Every segment response reports the stream head, which may reveal new segments."""
        size = "None" if result.data is None else f"{len(result.data)} bytes"
        self.logger.debug(colour(f"Format: {self.format}, Segnum: {result.seg}, Status: {result.status}, Data: {size}", GREEN))
        self._apply_head_headers(result.headers)
        if result.head_seq > self.latest_sequence:
            self.logger.debug(f"More segments available: {result.head_seq}, previously {self.latest_sequence}")
            self._on_head_advanced(self.latest_sequence, result.head_seq)
            self.latest_sequence = result.head_seq

    def _log_download_error(self, seg: Optional[int], error: Optional[BaseException]) -> None:
        """Log an exception raised by a download_segment task (KeyboardInterrupt means it was killed)."""
        if isinstance(error, httpx.ConnectTimeout):
            self.logger.warning(f'({self.format}) Experienced connection timeout while fetching segment {seg}')
        elif error is not None and not isinstance(error, KeyboardInterrupt):
            self.logger.error(f"An unknown error occurred downloading segment {seg}: {error}", exc_info=error)

    def _submit_segments(self, executor: concurrent.futures.ThreadPoolExecutor, done_count: int, not_done_count: int) -> None:
        # Grow the submission window while all submitted work completes quickly, shrink otherwise
        if not_done_count <= 0 and done_count > 0:
            self.thread_window_size = min(3 * self.thread_window_size, max(10, 20 * self.max_workers))
        else:
            self.thread_window_size = max(self.thread_window_size // 2, 2 * self.max_workers)
        limit = max(10, 2 * self.max_workers, self.thread_window_size)
        if self.store.is_backlogged():
            # Segments can't be persisted (e.g. disk full); don't download more into memory
            return
        capacity = limit - len(self.future_to_seg) + 1
        if capacity <= 0:
            return
        for seg in self._next_segments(capacity):
            self._submit(executor, seg)

    def _submit(self, executor: concurrent.futures.ThreadPoolExecutor, seg: int, max_retries: Optional[int] = None) -> None:
        args = self._download_args(seg)
        future = executor.submit(self.download_segment, *args, max_retries=max_retries) if max_retries is not None \
            else executor.submit(self.download_segment, *args)
        self.future_to_seg[future] = seg
        self.submitted_segments.add(seg)

    def live_dl(self):
        self.logger.info(colour(f"Starting download of live fragments ({self.format} - {self.stream_url.format_note}) [{self.mode_name}]", DARK_YELLOW))
        self.future_to_seg: dict[concurrent.futures.Future, int] = {}
        self.submitted_segments: set[int] = set()
        self.thread_window_size: int = 2 * self.max_workers
        self._on_start()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers,
                                                   thread_name_prefix=f"{self.id}-{self.format}") as executor:
            self._executor = executor
            try:
                while True:
                    self.check_kill(executor)
                    if self._graceful_stop_requested():
                        self.logger.info(f"Graceful stop triggered for {self.format}. Finalizing downloaded segments...")
                        break

                    self._pre_iteration()
                    done_count, not_done_count = self._collect_results()
                    self.store.flush()
                    self._update_progress_stats()

                    action = self._evaluate()
                    if action is LoopAction.BREAK:
                        break
                    if action is LoopAction.CONTINUE:
                        continue
                    self._submit_segments(executor, done_count, not_done_count)
            finally:
                self._executor = None
            self.store.flush(force=True)
        self._update_progress_stats()
        self._on_finish()
        return self._loop_result()

    def _update_progress_stats(self) -> None:
        stats = self._stats()
        stats["latest_sequence"] = self._window_latest()
        stats["downloaded_segments"] = self.store.count()
        stats["current_filesize"] = self.store.size_on_disk()

    # --- Finalisation ---

    def finalize(self) -> FileInfo:
        """
        Called once after live_dl() returns: produce the format's .ts file and return its FileInfo
        (the coordinator then deletes or keeps the temp database, see LiveStreamDownloader._run_stream).
        """
        raise NotImplementedError

    def _file_info(self, file_name: str) -> FileInfo:
        return FileInfo(file_name, file_type=self.type, format=self.format, vcodec=self.stream_url.vcodec,
                        acodec=self.stream_url.acodec, language=self.stream_url.language, protocol=self.stream_url.protocol)

    def combine_segments_to_file(self, output_file: str) -> str:
        """Write all segments of the SQLite store, in order, to output_file (the format's .ts file)."""
        self._stats()['status'] = "merging"
        self.logger.debug(f"Merging segments to {output_file}")
        self.store.merge_to_file(output_file, clean=should_clean(self.ext))
        self._stats()['status'] = "merged"
        return output_file

    def delete_temp_database(self) -> None:
        if isinstance(self.store, SQLiteSegmentStore):
            self.store.delete()

    # --- Backward-compatible aliases (not used by livestream_dl itself) ---

    @property
    def folder(self) -> Optional[str]:
        return self.paths.folder

    @property
    def file_base_name(self) -> str:
        return self.paths.file_base_name

    @property
    def merged_file_name(self) -> str:
        return self.paths.merged_file_name

    @property
    def temp_db_file(self) -> str:
        return self.paths.temp_db_file

    def should_clean(self, ext=None) -> bool:
        return should_clean(ext)

    def get_atoms(self, data):
        return get_atoms(data)

    def clean_segments(self, data, first=True):
        return clean_segment(data, first=first)


class DownloadStream(StreamDownloaderBase):
    """Downloads a live stream from the live edge (and any available DVR history) into SQLite."""
    mode_name = "Database"
    # Whether this mode can hand over to StreamRecovery when the stream turns private
    supports_recovery_handoff: bool = True
    new_manifest_message: str = "Video finished downloading via new manifest"

    # --- Live edge scheduling ---

    def _on_start(self) -> None:
        super()._on_start()
        self.latest_downloaded_segment: int = -1
        self.optimistic_fails: int = 0
        self.optimistic_seg: int = 0
        self.wait: int = 0
        self.segment_retries: dict[int, int] = {}
        # Min-heap of segment numbers waiting to be submitted, plus a membership set. New segment
        # numbers are added incrementally as the head advances, rather than rebuilding the full
        # range every iteration (which would cost O(stream length) per loop).
        self._queue: list[int] = []
        self._queued: set[int] = set()
        # Segments that exceeded their retries; re-queued if the stream URL changes
        self._exhausted: set[int] = set()
        self._frontier: int = self.store.first_needed()
        self._refresh_result = None
        self._enqueue_up_to(max(self.latest_sequence, self.latest_downloaded_segment))

    def _enqueue(self, seg: int) -> None:
        if seg not in self._queued and not self.store.has(seg) and seg not in self._exhausted:
            heapq.heappush(self._queue, seg)
            self._queued.add(seg)

    def _enqueue_up_to(self, top: int) -> None:
        """Queue all segments from the frontier up to `top` (inclusive)."""
        if not hasattr(self, "_frontier"):
            return
        start = max(self._frontier, self.start_seg)
        upper = top if self.end_seg == float('inf') else min(top, int(self.end_seg))
        for seg in range(start, upper + 1):
            self._enqueue(seg)
        self._frontier = max(self._frontier, top + 1)

    def _on_head_advanced(self, old_head: int, new_head: int) -> None:
        self._enqueue_up_to(new_head)

    def _on_window_resolved(self) -> None:
        # Drop queued segments now outside the window and queue any that are now inside it
        if hasattr(self, "_queue"):
            self._queue = [seg for seg in self._queue if self._in_window(seg)]
            heapq.heapify(self._queue)
            self._queued = set(self._queue)
            self._frontier = min(self._frontier, max(self.start_seg, self.store.first_needed()))
            self._enqueue_up_to(max(self.latest_sequence, self.latest_downloaded_segment))

    def _reset_retries(self, reason: str) -> None:
        """A new stream URL may succeed where the old one failed, so retry exhausted segments."""
        self.logger.info(f"({self.format}) {reason}, resetting segment retry log")
        self.segment_retries.clear()
        exhausted, self._exhausted = self._exhausted, set()
        for seg in exhausted:
            self._enqueue(seg)

    def _pre_iteration(self) -> None:
        previous_url = self.stream_url
        self._refresh_result = self.refresh_Check(wait=False)
        if previous_url != self.stream_url:
            self.logger.debug("Stream URL has been updated, reseting segment retry counts")
            self._reset_retries("New stream URL detected")

    def _on_segment_result(self, seg: Optional[int], result: Optional[SegmentResult], error: Optional[BaseException]) -> None:
        if seg is None:
            return
        status = result.status if result is not None else None
        data = result.data if result is not None else None

        # Track success of optimistic (beyond the known head) requests
        if seg >= self.optimistic_seg and status != 200:
            self.optimistic_fails += 1
            self.logger.debug(f"Unable to optimistically grab segment {seg} for {self.format}. Up to {self.optimistic_fails} attempts")
        elif seg >= self.optimistic_seg and status == 200:
            self.optimistic_fails = 0
            self.latest_downloaded_segment = max(self.latest_downloaded_segment, seg)

        if data is not None:
            self.store.add(seg, data)
            self.segment_retries.pop(seg, None)
            if status == 200 and seg > self.latest_downloaded_segment:
                self.latest_downloaded_segment = seg
                self._enqueue_up_to(seg)
        elif seg > self.latest_sequence:
            # A segment beyond the known head may simply not exist yet (optimistic request). Those
            # failures are tracked by optimistic_fails only; counting them as retries would make
            # the end of every stream look like "all segments exhausted" and trigger URL refreshes.
            pass
        elif status != 200:
            self.segment_retries[seg] = self.segment_retries.get(seg, 0) + 1
            self.logger.debug(f"Unable to download {seg} ({self.format}). Currently at {self.segment_retries.get(seg)} retries")
            if self.segment_retries[seg] > self.fragment_retries:
                self._exhausted.add(seg)
            elif self._in_window(seg) and seg <= max(self.latest_sequence, self.latest_downloaded_segment):
                self._enqueue(seg)

    def _next_segments(self, capacity: int) -> list[int]:
        """Pop the lowest queued segments that still need downloading."""
        limit = self._submission_limit()
        segments: list[int] = []
        while self._queue and len(segments) < capacity:
            seg = self._queue[0]
            if seg > limit:
                break
            heapq.heappop(self._queue)
            self._queued.discard(seg)
            if seg in self.submitted_segments or self.store.has(seg) or not self._in_window(seg):
                continue
            if self.segment_retries.get(seg, 0) > self.fragment_retries:
                self._exhausted.add(seg)
                continue
            segments.append(seg)
        return segments

    def _submission_limit(self) -> float:
        return self.end_seg

    def _has_queued_work(self) -> bool:
        limit = self._submission_limit()
        return bool(self._queue) and self._queue[0] <= limit

    def _submit_optimistic(self) -> None:
        """
        Request the segment after the newest known one before the server reports it. Its response
        headers report the new head, which is how new segments are discovered at the live edge.
        Only attempted a limited number of times in a row so it can't loop at the end of a stream.
        """
        self.optimistic_seg = max(self.latest_sequence, self.latest_downloaded_segment) + 1
        opt = self.optimistic_seg
        if (self.max_workers > 1 or not self._has_queued_work()) and self.optimistic_fails < OPTIMISTIC_FAILS_MAX \
                and not self.store.has(opt) and opt not in self.submitted_segments \
                and self._in_window(opt) and opt <= self._submission_limit():
            # Wait estimated fragment time +0.1s to make sure it would exist. Wait a minimum of 2s if no segments are to be submitted
            if not self._has_queued_work():
                self.smart_sleep(max(self.estimated_segment_duration, OPTIMISTIC_MIN_WAIT) + 0.1)
            self.logger.debug(colour(f"Adding segment {opt} optimistically ({self.format}). Currently at {self.optimistic_fails} fails", YELLOW))
            self._submit(self._executor, opt, max_retries=min(self.fragment_retries, OPTIMISTIC_SEGMENT_RETRIES))
            # Ensure wait isn't triggered while optimistic segments is enabled
            self.wait = 0

    def _evaluate(self) -> LoopAction:
        # Resolve --start-time/--end-time before downloading anything (needs the segment duration)
        if not self._window_resolved:
            if not self._try_resolve_window():
                self.logger.debug("Unable to determine segment duration, retrying...")
                self.smart_sleep(2)
                return LoopAction.CONTINUE

        self._before_evaluate()
        self._submit_optimistic()

        if self._refresh_result is RefreshOutcome.IN_PROGRESS:
            if not self._has_queued_work() and not self.submitted_segments:
                # Wait up to 0.5 seconds if there's no work, to prevent excessive loop processing
                self._background_refresh.wait(0.5)
            return LoopAction.PROCEED

        if self.is_private and (self._has_queued_work() or self._exhausted):
            # Segments remain (including ones that exhausted their retries on 403s) but the
            # video is private: hand over to stream recovery where supported
            return self._on_private_with_remaining()

        if not self._has_queued_work() and not self.future_to_seg:
            return self._on_idle()

        if self._exhausted and not self._has_queued_work():
            # Everything still missing has exceeded its retries (only in-flight requests remain)
            return self._on_all_exhausted()

        self.wait = 0
        return LoopAction.PROCEED

    def _before_evaluate(self) -> None:
        pass

    def _on_idle(self) -> LoopAction:
        """No segments queued or in flight: wait for new segments, refreshing the URL periodically."""
        if max(self.latest_sequence, self.latest_downloaded_segment) >= self.end_seg:
            self.logger.info("All segments in range downloaded, ending...")
            return LoopAction.BREAK

        self.logger.debug(f"No new fragments available for {self.format}, attempted {self.wait} times...")

        # If waited for more than (non-zero) wait limit break. Zero wait limit is intended to keep checking until stream ends
        if self.wait_limit > 0 and self.wait > self.wait_limit:
            self.logger.debug("Wait time for new fragment exceeded, ending download...")
            return LoopAction.BREAK

        # Every REFRESH_EVERY_N_WAITS wait loops, get page for new URL and update status if necessary
        if self.wait % REFRESH_EVERY_N_WAITS == 0 and self.wait > 0:
            previous_url = self.stream_url
            refresh = self.refresh_url(ignore_no_formats=True)
            if self.is_private:
                self.logger.debug("Video is private and no more segments are available. Ending...")
                return LoopAction.BREAK
            elif self.is_members_error:
                self.logger.debug("Video turned members only, not accessible and no more segments are available. Ending...")
                return LoopAction.BREAK
            elif refresh is RefreshOutcome.ENDED:
                self.logger.debug("Stream has ended and no outstanding segments remain. Ending download process.")
                return LoopAction.BREAK
            elif refresh is RefreshOutcome.NEW_MANIFEST:
                self.logger.info(self.new_manifest_message)
                return LoopAction.BREAK
            elif previous_url != self.stream_url:
                self._reset_retries("New stream URL detected")
        self.smart_sleep(WAIT_INTERVAL_SECONDS)
        self.update_latest_segment(client=self.client)
        self.wait += 1
        return LoopAction.CONTINUE

    def _on_all_exhausted(self) -> LoopAction:
        # Don't re-run yt-dlp over and over for segments that are simply gone: only refresh for
        # exhausted segments if the URL hasn't been refreshed recently.
        if time.time() - self.url_checked < EXHAUSTED_REFRESH_MIN_INTERVAL:
            return LoopAction.PROCEED
        self.logger.warning("All remaining segments have exceeded the retry threshold, attempting URL refresh...")
        previous_url = self.stream_url
        refresh = self.refresh_url(ignore_no_formats=True)
        if refresh is RefreshOutcome.NEW_MANIFEST:
            self.logger.info(self.new_manifest_message)
            return LoopAction.BREAK
        if previous_url != self.stream_url:
            # If stream URL has changed, refresh retry count and continue
            self._reset_retries("New stream URL detected")
            return LoopAction.CONTINUE
        if self.is_private or refresh is RefreshOutcome.ENDED or self.is_members_error:
            if self.future_to_seg:
                # Let in-flight requests finish first
                return LoopAction.PROCEED
            self.logger.warning("Failed to refresh URL or stream is private/members only, ending...")
            return LoopAction.BREAK
        # Same URL and still live: segments may simply not be available yet, try them again
        self._reset_retries("URL refreshed")
        return LoopAction.PROCEED

    def _on_private_with_remaining(self) -> LoopAction:
        if self.future_to_seg:
            self.logger.debug("Video is private, waiting for remaining threads to finish before going to stream recovery")
            self.smart_sleep(PRIVATE_DRAIN_WAIT)
            return LoopAction.CONTINUE
        if self.supports_recovery_handoff and self.stream_url.protocol == "https":
            self.logger.debug("Video is private and still has segments remaining, moving to stream recovery")
            self._handoff_to_recovery()
        else:
            self.logger.warning(f"{self.format} - Stream is now private and segments remain. Current stream protocol does not support stream recovery, ending...")
        return LoopAction.BREAK

    def _handoff_to_recovery(self) -> None:
        """
        Download the remaining segments with StreamRecovery into the SAME database. The store is
        closed while recovery has the database open, then reopened so finalize() merges everything.
        """
        self.store.close()
        try:
            remaining = RECOVERY_HANDOFF_DELAY
            while remaining > 0:
                self.logger.debug(f"Waiting {math.ceil(remaining / 60):.0f} minutes before starting stream recovery to improve chances of success")
                step = min(60.0, remaining)
                self.smart_sleep(step)
                remaining -= step
                if self._graceful_stop_requested():
                    return
            self.logger.warning(f"Sending stream URLs of {self.format} to stream recovery: {self.stream_urls}")
            recovery_options = dict(self.options)
            recovery_options.update({
                "max_workers": max((self.recovery_thread_multiplier * self.max_workers * len(self.stream_urls)), self.recovery_thread_multiplier),
                "filename": self.paths.filename,
                "download_folder": self.paths.folder,
                "no_merge": True,
                "recovery": True,
                # The URLs are already known; keep the window already resolved for this stream
                "start_time": None, "end_time": None,
            })
            recovery_options.pop("file_name", None)
            try:
                with StreamRecovery(info_dict=self.info_dict, stream_url=self.stream_url, options=recovery_options,
                                    livestream_coordinator=self.livestream_coordinator, stream_urls=self.stream_urls,
                                    live_status=self.live_status,
                                    segment_range=(self.start_seg, self.end_seg)) as recovery:
                    recovery.type = self.type
                    recovery.live_dl()
            except KeyboardInterrupt:
                raise
            except Exception:
                self.logger.exception("An error occurred while trying to recover the stream")
        finally:
            self.smart_sleep(1)
            self.store = self._make_store()
            self.already_downloaded = self.store.downloaded

    def finalize(self) -> FileInfo:
        file_name = self.combine_segments_to_file(self.paths.merged_file_name)
        return self._file_info(file_name)

    def delete_ts_file(self) -> None:
        _remove_file(self.paths.merged_file_name, self.logger)

    # --- Backward-compatible aliases (not used by livestream_dl itself) ---

    @property
    def conn(self):
        return getattr(self.store, "conn", None)

    def commit_segments(self, force: bool = False) -> None:
        self.store.flush(force=force)

    def commit_batch(self, conn=None) -> None:
        self.store.flush(force=True)

    def segment_exists_batch(self) -> set:
        return set(self.store.downloaded)


class DownloadStreamDirect(DownloadStream):
    """Live edge download written directly to the .ts file in sequence order (no database)."""
    mode_name = "Direct Mode"
    supports_recovery_handoff = False
    new_manifest_message = "Video has new manifest. This cannot be handled by current implementation of Direct to .ts implementation"

    def __init__(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, livestream_coordinator: LiveStreamDownloader = None, **kwargs):
        super().__init__(info_dict=info_dict, stream_url=stream_url, options=options, livestream_coordinator=livestream_coordinator, **kwargs)
        self.state_file_name = self.paths.state_file
        self.state_file_backup = self.paths.state_backup
        self.logger.debug(f"DownloadStreamDirect initialized for {self.id} ({self.format})")

    def _make_store(self) -> SegmentStore:
        return DirectTSStore(self.paths.merged_file_name, self.paths.state_file, self.paths.state_backup,
                             logger=self.logger, clean=should_clean(self.ext), batch_size=1)

    @property
    def state(self) -> dict:
        return self.store.state

    def _reorder_window(self) -> int:
        """How far beyond the last written segment downloads may run ahead (bounds buffered RAM)."""
        return max(50, 10 * self.max_workers)

    def _submission_limit(self) -> float:
        return min(self.end_seg, self.store.last_written + self._reorder_window())

    def _on_window_resolved(self) -> None:
        self.store.set_start(self.start_seg)
        super()._on_window_resolved()

    def _before_evaluate(self) -> None:
        # Original direct-mode behaviour: once the next segment to write has exceeded its retries,
        # give up on it and advance so later segments can be written.
        next_seg = self.store.last_written + 1
        if self.segment_retries.get(next_seg, 0) > self.fragment_retries:
            self.logger.warning(f"({self.format}) Segment {next_seg} has exceeded maximum segment retries, advancing count to save data...")
            self.store.skip_to(next_seg)
            self._exhausted.discard(next_seg)
        # Keep the queue frontier ahead of the written position
        self._frontier = max(self._frontier, self.store.first_needed())

    def _update_progress_stats(self) -> None:
        super()._update_progress_stats()
        self._stats()["downloaded_segments"] = self.store.last_written

    def _on_finish(self) -> None:
        self.store.write_remaining()
        super()._on_finish()
        self._stats()['status'] = "merged"
        self.logger.info(f"Completed direct download for {self.format}")

    def _loop_result(self):
        return self.paths.merged_file_name

    def delete_state_file(self) -> None:
        self.store.delete_state_files()

    def remove_folder(self):
        """Remove folder and associated files"""
        if self.paths.folder:
            self.delete_state_file()
            self.delete_ts_file()
            with contextlib.suppress(Exception):
                os.rmdir(self.paths.folder)

    def finalize(self) -> FileInfo:
        if not self.options.get("keep_state", False):
            self.delete_state_file()
        return self._file_info(self.paths.merged_file_name)


class StreamRecovery(StreamDownloaderBase):
    """
    Downloads segments missing from the database after a stream ended or turned private, using
    every known URL of the format until they expire. Missing segments are retried with back-off.
    """
    mode_name = "Recovery"
    POLL_INTERVAL = 0.5
    # Recompute the retry priority order at most this often unless something changed
    REORDER_INTERVAL = 1.0
    # Minimum seconds between header probes of all URLs while 401/403 errors persist
    PROBE_INTERVAL = 10.0

    def __init__(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, stream_urls: Optional[list[YoutubeURL.YoutubeURL]] = None,
                 live_status="is_live", livestream_coordinator: LiveStreamDownloader = None, segment_range: Optional[tuple[int, float]] = None, **kwargs):
        self.expires: Optional[float] = None
        self.segments_retries: dict[int, dict] = {}
        self._segment_range = segment_range
        super().__init__(info_dict=info_dict, stream_url=stream_url, options=options, livestream_coordinator=livestream_coordinator,
                         stream_urls=stream_urls, live_status=live_status, **kwargs)
        self.live_status = info_dict.get('live_status', live_status)
        self.segment_retry_time: float = float(self.options.get('segment_retry_time', 30))
        self.recover = self.options.get('recovery', False)
        self.sequential: bool = False
        self.count_400s = 0
        self.sleep_time = 1
        self.count_403s: dict = {}
        self.user_agent_403s: dict = {}
        self.user_agent_full_403s: dict = {}
        self._url_index: int = 0
        self.logger.debug(f"Recovery - Resolution: {self.resolution}, Format: {self.format}")
        self.logger.debug(f"Number of stream URLs available: {len(self.stream_urls)}")
        # Ensure stats are set for the correct type
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type] = {}

    def _initial_stream_urls(self, stream_url, kwargs) -> list[YoutubeURL.YoutubeURL]:
        stream_urls = kwargs.get('stream_urls') or []
        if stream_urls:
            self.logger.debug(f"{len(stream_urls)} stream urls available")
            return list(stream_urls)
        stream_type = "video" if stream_url is not None and stream_url.vcodec else "audio"
        return YoutubeURL.Formats(logger=self.logger).getFormatURL(
            info_json=self.info_dict,
            resolution=self.resolution,
            sort=self.yt_dlp_sort,
            get_all=True,
            include_dash=False,
            include_m3u8=False,
            stream_type=stream_type,
        ) or []

    def _choose_initial_url(self) -> YoutubeURL.YoutubeURL:
        return random.choice(self.stream_urls)

    def _validate_urls(self) -> None:
        self.check_Expiry()
        if self.expires and time.time() > self.expires:
            self._raise_expired()

    def _raise_expired(self) -> None:
        self.logger.error(colour(f"({self.format}) Current time is beyond highest expire time, unable to recover", RED))
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        format_exp = datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S') if self.expires else "unknown"
        raise TimeoutError(f"Current time {now} exceeds latest URL expiry time of {format_exp}")

    def check_Expiry(self) -> None:
        """Remove expired URLs and update the latest expiry time of the remaining ones."""
        now = time.time()
        self.stream_urls = [url for url in self.stream_urls if self._url_is_valid(url, now)]
        expires = [int(self.get_expire_time(url)) for url in self.stream_urls if self.get_expire_time(url) is not None]
        if expires:
            self.expires = max(expires)

    def _new_retry_entry(self) -> dict:
        return {'retries': 0, 'last_retry': 0, 'ideal_retry_time': random.uniform(max(self.segment_retry_time, 900), max(self.segment_retry_time + 300, 1200))}

    def _on_head_advanced(self, old_head: int, new_head: int) -> None:
        if old_head > -1 and hasattr(self, "segment_retry_time"):
            for key in range(old_head, new_head):
                if key not in self.store.downloaded and self._in_window(key):
                    self.segments_retries.setdefault(key, self._new_retry_entry())
            self._order_dirty = True

    def update_latest_segment(self, url=None, client=None) -> None:
        # Overrides base method to handle multiple, expiring URLs
        self.check_kill()
        self._stop_if_all_urls_expired()
        if url is None:
            url = random.choice(self.stream_urls)
        headers = self.get_Headers(url, client)
        self._apply_head_headers(headers, set_type_from_content=True)
        self._stats()["latest_sequence"] = self.latest_sequence

    def _url_for_segment(self, seg: int) -> str:
        # Round-robin through available stream URLs
        url = self.stream_urls[self._url_index % len(self.stream_urls)]
        self._url_index += 1
        return url.segment(seg)

    def _download_args(self, seg: int) -> tuple:
        return (self._url_for_segment(seg), seg, self.client, True)

    def _on_start(self) -> None:
        super()._on_start()
        if self._segment_range is not None:
            self.start_seg, self.end_seg = self._segment_range
            self._window_resolved = True
        elif not self._window_resolved and not self._try_resolve_window():
            self.logger.warning("Unable to determine segment duration, ignoring requested start/end time for recovery")
            self._window_resolved = True
        self.sleep_time = max(self.estimated_segment_duration, 0.1)
        # Track retries of all missing segments in database
        top = self.latest_sequence if self.end_seg == float('inf') else min(self.latest_sequence, int(self.end_seg))
        self.segments_retries = {key: self._new_retry_entry() for key in range(self.start_seg, top + 1) if key not in self.store.downloaded}
        self._order_dirty = True
        self._ordered: list[int] = []
        self._last_order_time = 0.0
        self._last_print = time.time()
        self._last_probe = 0.0
        if self.expires is not None:
            self.logger.debug(f"Recovery mode active, URL expected to expire at {datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            self.logger.debug("Recovery mode active")

    def _pre_iteration(self) -> None:
        self._stop_if_all_urls_expired()

    def _stop_if_all_urls_expired(self) -> None:
        """Raise TimeoutError (after saving buffered segments) once no unexpired URL is left."""
        self.check_Expiry()
        if (not self.stream_urls) or (self.expires and time.time() > self.expires):
            self.logger.critical(colour("Current time is beyond highest expire time and no valid URLs remain, unable to recover", RED))
            # getattr: also safe if called before the store has been created
            if getattr(self, "store", None) is not None:
                self.store.flush(force=True)
            self._raise_expired()

    def _record_failure(self, seg: int) -> None:
        entry = self.segments_retries.setdefault(seg, self._new_retry_entry())
        entry['retries'] = entry.get('retries', 0) + 1
        entry['last_retry'] = time.time()
        if entry['retries'] >= self.fragment_retries:
            self.logger.debug(f"Segment {seg} of {self.latest_sequence} has exceeded maximum number of retries")

    def _on_segment_result(self, seg: Optional[int], result: Optional[SegmentResult], error: Optional[BaseException]) -> None:
        if seg is None:
            return
        self._order_dirty = True
        data = result.data if result is not None else None
        if data is not None:
            self.store.add(seg, data)
            self.segments_retries.pop(seg, None)
        else:
            self._record_failure(seg)

    def _update_progress_stats(self) -> None:
        stats = self._stats()
        stats["latest_sequence"] = self.latest_sequence
        stats["downloaded_segments"] = self.latest_sequence - len(self.segments_retries)
        stats["current_filesize"] = self.store.size_on_disk()

    def _evaluate(self) -> LoopAction:
        if len(self.segments_retries) <= 0:
            self.logger.info("All segment downloads complete, ending...")
            return LoopAction.BREAK
        if all(value['retries'] > self.fragment_retries for value in self.segments_retries.values()):
            if not self.future_to_seg:
                self.logger.error("All remaining segments have exceeded their retry count, ending...")
                return LoopAction.BREAK
        elif self.is_403 and self.expires is not None and time.time() > self.expires:
            self.logger.critical("URL(s) have expired and failures being detected, ending...")
            return LoopAction.BREAK
        elif self.is_401:
            self.logger.debug(f"401s detected for {self.format}, sleeping for a minute")
            self.smart_sleep(RECOVERY_401_SLEEP)
            self._probe_urls()
        elif self.is_403:
            self._probe_urls()

        self._log_remaining()
        return LoopAction.PROCEED

    def _probe_urls(self) -> None:
        # Probing every URL costs one request per URL, so it happens at most every PROBE_INTERVAL
        if time.monotonic() - self._last_probe < self.PROBE_INTERVAL:
            return
        self._last_probe = time.monotonic()
        for url in list(self.stream_urls):
            if self.live_status == 'post_live':
                self.update_latest_segment(url=self.stream_url.segment(self.latest_sequence + 1), client=self.client)
            else:
                self.update_latest_segment(url=url, client=self.client)

    def _log_remaining(self) -> None:
        if self.submitted_segments:
            return
        if len(self.segments_retries) < 11 and time.time() - self._last_print > self.segment_retry_time:
            self.logger.debug(f"{len(self.segments_retries)} remaining segments for {self.format}: {self.segments_retries}")
            self._last_print = time.time()
        elif time.time() - self._last_print > self.segment_retry_time + 5:
            self.logger.debug(f"{len(self.segments_retries)} segments remain for {self.format}")
            self._last_print = time.time()

    def _priority_order(self) -> list[int]:
        """
        Order of segments to try: segments that failed before and haven't been tried for their
        "ideal retry time" first, then the rest; each group by fewest retries.
        Recomputed only when something changed (or periodically, as priority depends on time).
        """
        now = time.time()
        if not self._order_dirty and now - self._last_order_time < self.REORDER_INTERVAL:
            return self._ordered
        if self.sequential:
            ordered = sorted(self.segments_retries.items(), key=lambda item: (item[1]['retries'], item[0]))
        else:
            priority, non_priority = [], []
            for key, value in self.segments_retries.items():
                if (now - value.get('last_retry', 0)) > value.get('ideal_retry_time', 0) and value.get('retries', 0) > 0:
                    priority.append((key, value))
                else:
                    non_priority.append((key, value))
            priority.sort(key=lambda item: item[1].get('retries', 0))
            non_priority.sort(key=lambda item: item[1].get('retries', 0))
            ordered = priority + non_priority
        self._ordered = [key for key, _ in ordered]
        self._order_dirty = False
        self._last_order_time = now
        return self._ordered

    def _next_segments(self, capacity: int) -> list[int]:
        now = time.time()
        segments: list[int] = []
        for seg in self._priority_order():
            if len(segments) >= capacity:
                break
            entry = self.segments_retries.get(seg)
            if entry is None or seg in self.submitted_segments or seg in self.store.pending:
                continue
            if self.store.has(seg):
                self.segments_retries.pop(seg, None)
                self._order_dirty = True
                continue
            # Retry a failed segment only after segment_retry_time has passed since its last attempt
            if entry.get('retries', 0) <= self.fragment_retries and now - entry.get('last_retry', 0) > self.segment_retry_time:
                segments.append(seg)
        return segments

    def _loop_result(self) -> int:
        return len(self.segments_retries)

    def save_stats(self) -> None:
        # Stats files
        for suffix, data in (("_seg_403s", self.count_403s), ("_usr_ag_403s", self.user_agent_403s), ("_usr_ag_full_403s", self.user_agent_full_403s)):
            with open(f"{self.paths.file_base_name}.{self.format}{suffix}.json", 'w', encoding='utf-8') as outfile:
                json.dump(data, outfile, indent=4)

    def finalize(self) -> FileInfo:
        """Merge to .ts if all segments (or enough, per options) were recovered, else raise."""
        remaining = len(self.segments_retries)
        force_merge = bool(_opt(self.options, 'force_recover_merge', 'force_recovery_merge', 'force_merge', default=False))
        tolerance = int(self.options.get('recovery_failure_tolerance') or 0)
        if (force_merge or remaining <= 0 or remaining <= tolerance) and not self.options.get('no_merge', False):
            if remaining > 0:
                self.logger.warning(f"({self.id}) Stream recovery of format {self.format} has {remaining} outstanding segments which were not able to complete. Exitting")
            file_name = self.combine_segments_to_file(self.paths.merged_file_name)
            return self._file_info(file_name)
        raise getUrls.VideoDownloadError(f"({self.id}) Stream recovery of format {self.format} has {remaining} outstanding segments which were not able to complete. Exitting")


# ==============================================================================================
# FFmpeg merger
# ==============================================================================================

@dataclass
class FFmpegMuxArgs:
    """The ffmpeg inputs and output arguments being built up to mux one manifest (see create_mp4)."""
    inputs: list[str]
    args: list[str] = field(default_factory=lambda: ['-c', 'copy'])
    # Output stream counters, used to address per-stream options such as -metadata:s:a:<n>
    audio_streams: int = 0
    video_streams: int = 0
    attachments: int = 0


def _make_merger(ydl, ffmpeg_command_file: FileInfo):
    """FFmpegMetadataPP subclass that writes the ffmpeg command and optionally runs it."""
    from yt_dlp.postprocessor.ffmpeg import (FFmpegMetadataPP, FFmpegPostProcessorError, PostProcessor,
                                             encodeArgument, shell_quote, variadic, Popen)

    class LiveStreamDLMerger(FFmpegMetadataPP):
        def __init__(self, downloader, ffmpeg_command_file: FileInfo = None):
            self.ffmpeg_command_file = ffmpeg_command_file
            super().__init__(downloader=downloader)

        @PostProcessor._restrict_to()
        def run(self, info):
            super().run(info)

        def real_run_ffmpeg(self, input_path_opts, output_path_opts, *, merge=True, expected_retcodes=(0,)):
            self.check_version()

            oldest_mtime = min(os.stat(path).st_mtime for path, _ in input_path_opts if path)

            cmd = [self.executable, encodeArgument('-y')]
            # avconv does not have repeat option
            if self.basename == 'ffmpeg':
                cmd += [encodeArgument('-loglevel'), encodeArgument('repeat+info')]

            def make_args(file, args, name, number):
                keys = [f'_{name}{number}', f'_{name}']
                if name == 'o':
                    args += ['-movflags', '+faststart']
                    if number == 1:
                        keys.append('')
                args += self._configuration_args(self.basename, keys)
                if name == 'i':
                    args.append('-i')
                return [encodeArgument(arg) for arg in args] + [self._ffmpeg_filename_argument(file)]

            for arg_type, path_opts in (('i', input_path_opts), ('o', output_path_opts)):
                cmd += itertools.chain.from_iterable(
                    make_args(path, list(opts), arg_type, i + 1)
                    for i, (path, opts) in enumerate(path_opts) if path)

            command_string = shell_quote(cmd)
            self.write_debug(f'ffmpeg command line: {command_string}')
            with open(self.ffmpeg_command_file, 'w', encoding="utf-8") as f:
                f.write(command_string + "\n")

            if not merge:
                return None
            _, stderr, returncode = Popen.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
            if returncode not in variadic(expected_retcodes):
                raise FFmpegPostProcessorError(stderr)
            for out_path, _ in output_path_opts:
                if out_path:
                    self.try_utime(out_path, oldest_mtime, oldest_mtime)
            return stderr

    return LiveStreamDLMerger(downloader=ydl, ffmpeg_command_file=ffmpeg_command_file)


# ==============================================================================================
# Coordinator
# ==============================================================================================

class LiveStreamDownloader:
    """
    Downloads one video: start with download_segments(info_dict, resolution, options).

    Also holds the state its stream downloaders share: the kill / graceful-stop events, the
    progress stats (self.stats, printed by print_stats), one shared yt-dlp URL refresh for all
    formats (refresh_info_json) and the live edge reference for negative start/end times.
    Produced files are collected in self.file_names:

        {"streams": {<manifest>: {"video": FileInfo, "audio": FileInfo, "merged": ..., "ffmpeg_cmd": ...}},
         "databases": [FileInfo, ...], "info_json": FileInfo, "thumbnail": FileInfo, "live_chat": FileInfo, ...}
    """
    # Minimum seconds between yt-dlp URL refresh extractions (doubles after rate limiting)
    MIN_REFRESH_INTERVAL: float = 30.0

    def __init__(self, kill_all: Optional[threading.Event] = None, cleanup: Optional[threading.Event] = None,
                 logger: Optional[LoggerLike] = None, kill_this: Optional[threading.Event] = None):
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.propagate = False
        # Events are created per instance (a default argument would be shared by every instance)
        self.kill_this: threading.Event = kill_this or threading.Event()
        self.kill_all: threading.Event = kill_all or threading.Event()
        self.cleanup: threading.Event = cleanup or threading.Event()
        self.live_chat_result = None
        self.chat_timeout: Optional[float] = None
        # File name dictionary
        self.file_names: dict = {
            'databases': [],
            "streams": {}
        }
        self.stats: dict = {}

        self.refresh_json: dict = {}
        self.live_status: str = ""
        self.lock: threading.Lock = threading.Lock()
        self.last_refresh_attempt: float = time.monotonic()
        self.minimum_refresh_time: float = self.MIN_REFRESH_INTERVAL

        self.graceful_stop = threading.Event()
        # Signals the live chat thread to stop (set on timeout)
        self.chat_stop = threading.Event()
        self._live_chat_lock = threading.Lock()
        self._live_chat_packaged = False
        # Bytes taken from each live chat source file when an abandoned chat was zipped
        self._live_chat_snapshot: dict[str, int] = {}
        self._live_chat_source: Optional[tuple[str, str]] = None

        self._time_reference: Optional[float] = None
        self._time_reference_lock = threading.Lock()
        self._filename_ydl = None

    # --- Signals ---

    @contextlib.contextmanager
    def graceful_signal_catcher(self, disable_graceful=False):
        # Signal handlers can only be installed from the main thread
        is_main = (threading.current_thread() is threading.main_thread())

        original_sigint = None
        original_sigterm = None

        if is_main and not disable_graceful:
            original_sigint = signal.getsignal(signal.SIGINT)
            original_sigterm = signal.getsignal(signal.SIGTERM)

            def handler(signum, frame):
                self.logger.warning(f"\n[Caught Signal {signum}] Stopping downloads gracefully...")
                self.graceful_stop.set()
                # Restore original handlers so a second Ctrl+C forces a hard exit
                signal.signal(signal.SIGINT, original_sigint)
                signal.signal(signal.SIGTERM, original_sigterm)

            try:
                signal.signal(signal.SIGINT, handler)
                signal.signal(signal.SIGTERM, handler)
            except ValueError:
                # Fallback in case of weird environment issues
                is_main = False

        try:
            yield
        finally:
            # Only attempt to restore if we actually changed them
            if is_main and not disable_graceful:
                signal.signal(signal.SIGINT, original_sigint)
                signal.signal(signal.SIGTERM, original_sigterm)

    # --- Shared state for stream downloaders ---

    def resolve_time_reference(self, head_time: float) -> float:
        """The live edge time (stream seconds) used for negative --start-time/--end-time values.
        The first format to ask sets it so all formats cut at the same point."""
        with self._time_reference_lock:
            if self._time_reference is None:
                self._time_reference = float(head_time)
                self.logger.debug(f"Live edge reference for relative start/end times: {self._time_reference:.0f}s")
            return self._time_reference

    def _interruptible_wait(self, seconds: float) -> bool:
        """Wait up to `seconds`. Returns False if a stop/kill was requested during the wait."""
        deadline = time.monotonic() + seconds
        while True:
            if self.kill_all.is_set() or self.kill_this.is_set():
                raise KeyboardInterrupt("Kill command executed")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            if self.graceful_stop.wait(min(remaining, 0.5)):
                return False

    def refresh_info_json(self, update_threshold: float, id, cookies=None, additional_options=None, include_dash=False,
                          include_m3u8=False, ignore_no_formats=False, logger: Optional[LoggerLike] = None, proxy=None):
        """
        Shared URL refresh for all formats of this video. A cached extraction newer than
        `update_threshold` seconds is reused, so video and audio refreshing together cost one
        yt-dlp call. yt-dlp calls are spaced by minimum_refresh_time, which doubles after rate limiting.
        """
        if logger is None:
            logger = self.logger
        with self.lock:
            if time.time() - self.refresh_json.get("refresh_time", 0.0) <= update_threshold:
                return self.refresh_json, self.live_status

            # Wait for the rest of the minimum interval since the last extraction
            remaining = self.minimum_refresh_time - (time.monotonic() - self.last_refresh_attempt)
            if remaining > 0:
                self.logger.log(setup_logger.VERBOSE_LEVEL_NUM, f"Waiting for {remaining:.1f}s before attempting to refresh URL")
                if not self._interruptible_wait(remaining) and self.refresh_json:
                    # Stopping gracefully: don't start a new extraction
                    return self.refresh_json, self.live_status

            try:
                # Set last attempt time before and after to ensure that a time is captured on an error
                self.last_refresh_attempt = time.monotonic()
                refresh_json, live_status = getUrls.get_Video_Info(id=id, wait=False, cookies=cookies, additional_options=additional_options,
                                                                   include_dash=include_dash, include_m3u8=include_m3u8, clean_info_dict=True,
                                                                   ignore_no_formats=ignore_no_formats, logger=logger, proxy=proxy)
                self.last_refresh_attempt = time.monotonic()
            except getUrls.RateLimitException:
                self.minimum_refresh_time = min(self.minimum_refresh_time * 2, MAX_MINIMUM_REFRESH_TIME)
                self.logger.error(f"Received rate limit error, increasing refresh attempt timeout to {self.minimum_refresh_time:.0f}s")
                raise

            # Remove unnecessary items for info.json used purely for url refresh
            self.remove_format_segment_playlist_from_info_dict(refresh_json)
            for key in ("thumbnails", "tags", "description"):
                refresh_json.pop(key, None)

            # Add refresh time for reference
            refresh_json["refresh_time"] = time.time()
            self.refresh_json, self.live_status = refresh_json, live_status
            self.minimum_refresh_time = max(self.MIN_REFRESH_INTERVAL, self.minimum_refresh_time / 2)
            return self.refresh_json, self.live_status

    # --- Stream runners ---

    def _register_stream_file(self, manifest, file: FileInfo, filetype: str) -> None:
        self.file_names.setdefault("streams", {}).setdefault(manifest, {}).update({str(filetype).lower(): file})

    def _run_stream(self, stream_cls: type, download_function: Callable, info_dict, stream_url, options=None, manifest=0, **kwargs):
        """Run one stream downloader through download + finalize and register its output file."""
        current_options = dict(options or {})
        # Used by manifest-follow threads to run the same kind of download
        current_options["download_function"] = download_function
        try:
            with stream_cls(info_dict, stream_url=stream_url, options=current_options, livestream_coordinator=self, **kwargs) as downloader:
                self.stats["status"] = "Recording"
                downloader.live_dl()
                file = downloader.finalize()
                filetype = downloader.type
                if isinstance(downloader.store, SQLiteSegmentStore):
                    self._dispose_database(downloader, current_options)
            self._register_stream_file(manifest, file, filetype)
            return file, filetype
        except Exception:
            self.logger.exception("Unexpected error occurred while downloading stream")
            raise

    def _dispose_database(self, downloader: StreamDownloaderBase, options: dict) -> None:
        keep_database = options.get('keep_database', False) or options.get('keep_temp_files', False) or options.get('keep_database_file', False)
        if not keep_database:
            self.logger.info(f"Merging to ts complete, removing {downloader.paths.temp_db_file}")
            downloader.store.delete()
        elif downloader.paths.temp_db_file != ':memory:':
            downloader.store.close()
            self.file_names['databases'].append(FileInfo(downloader.paths.temp_db_file, file_type='database', format=downloader.format))

    def download_stream(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, manifest=0, **kwargs):
        return self._run_stream(DownloadStream, self.download_stream, info_dict, stream_url, options, manifest, **kwargs)

    def download_stream_direct(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, manifest=0, **kwargs):
        return self._run_stream(DownloadStreamDirect, self.download_stream_direct, info_dict, stream_url, options, manifest, **kwargs)

    def recover_stream(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, manifest=0, stream_urls: Optional[list] = None, **kwargs):
        return self._run_stream(StreamRecovery, self.recover_stream, info_dict, stream_url, options, manifest,
                                stream_urls=stream_urls or [], **kwargs)

    # --- Main entry point ---

    def _stream_urls_from_formats(self, formats_info: dict) -> list[YoutubeURL.YoutubeURL]:
        stream_urls = []
        with httpx.Client(timeout=30, follow_redirects=True) as m3u8_client:
            for format_info in formats_info.get('requested_formats') or [formats_info]:
                if not (format_info.get('url') or format_info.get('fragment_base_url')):
                    continue
                stream_urls.append(YoutubeURL.YoutubeURL.from_format(format_info, logger=self.logger, m3u8_client=m3u8_client))
        return stream_urls

    def download_segments(self, info_dict: dict, resolution='bv+ba/best', options: Optional[dict] = None, thread_event: Optional[threading.Event] = None):
        """
        Download a video: every selected format (video / audio), the auxiliary files (info.json,
        thumbnail, description) and optionally the live chat, then mux them with ffmpeg and move the
        results from the temp folder to the output location.
        """
        options = dict(options or {})
        if thread_event is not None:
            self.kill_all = thread_event

        self.stats['id'] = info_dict.get('id', None)
        self.stats["status"] = "Starting"
        self.logger.debug(json.dumps(options, indent=4, default=str))
        output_file = self._prepare_download_paths(info_dict, options)

        futures: set[concurrent.futures.Future] = set()
        with concurrent.futures.ThreadPoolExecutor(max_workers=COORDINATOR_WORKERS) as executor:
            try:
                formats_info = self._select_formats(info_dict, resolution, options)
                stream_urls = self._stream_urls_from_formats(formats_info)
                futures.add(executor.submit(self.download_auxiliary_files, info_dict=formats_info, options=options))
                if not stream_urls:
                    raise ValueError("Resolution is not valid or does not exist in stream")
                self._start_stream_downloads(executor, futures, info_dict, stream_urls, options)

                info_dict = formats_info
                live_chat_thread = self._start_live_chat(info_dict, options)
                with self.graceful_signal_catcher(disable_graceful=options.get("disable_graceful_shutdown", False)):
                    self._wait_for_tasks(futures, info_dict, options)
                    self._wait_for_live_chat(live_chat_thread, info_dict, options)
            except KeyboardInterrupt:
                self._cancel_tasks(executor, futures)
                raise

        self.stats["status"] = "Muxing"
        self.create_mp4(file_names=self.file_names, info_dict=info_dict, options=options)

        self.stats["status"] = "Moving"
        self._refresh_live_chat_snapshot()
        self.move_to_final(options=options, output_file=output_file, file_names=self.file_names)
        self._cleanup_live_chat_sources(f"{output_file}.live_chat.zip")
        self.stats["status"] = "Finished"

    def _prepare_download_paths(self, info_dict: dict, options: dict) -> str:
        """
        Work out the final output path and the temp folder that files are downloaded to. The temp
        location is stored in options ('filename', 'download_folder' and 'temp_folder') for the
        stream downloaders. Returns the output path without extension.
        """
        output_file = self.output_filename(info_dict=info_dict, outtmpl=options.get('output'))
        output_folder, file_name = os.path.split(output_file)
        temp_folder = options.get('temp_folder')
        if temp_folder is not None and temp_folder != output_folder:
            # --temp-folder may contain output template fields too
            download_folder = self.output_filename(info_dict=info_dict, outtmpl=temp_folder)
            options['temp_folder'] = download_folder
        else:
            download_folder = output_folder
        options['filename'] = file_name
        options['download_folder'] = download_folder
        if download_folder:
            os.makedirs(download_folder, exist_ok=True)
        return output_file

    def _select_formats(self, info_dict: dict, resolution: str, options: dict) -> dict:
        """Run yt-dlp's format selection. Returns info_dict with 'requested_formats' filled in."""
        base_output = os.path.join(options['download_folder'], options['filename']) if options['download_folder'] else options['filename']
        ydl_options = {}
        if options.get("ext", None):
            ydl_options["merge_output_format"] = options.get("ext", "").strip(" .")
        return YoutubeURL.Formats(logger=self.logger).getFormats(
            info_json=info_dict, resolution=resolution, sort=options.get('custom_sort', None),
            # Stream recovery only uses https (non-DASH) URLs
            include_dash=(options.get("dash", False) and not options.get('recovery', False)),
            include_m3u8=options.get("m3u8", False), force_m3u8=options.get("force_m3u8", False),
            base_path=base_output, ydl_options=ydl_options)

    def _stream_runner(self, options: dict) -> Callable:
        """The coordinator method that downloads one format in the mode the options ask for."""
        if options.get('recovery', False):
            return self.recover_stream
        if options.get('direct_to_ts', False):
            return self.download_stream_direct
        return self.download_stream

    def _start_stream_downloads(self, executor: concurrent.futures.ThreadPoolExecutor, futures: set[concurrent.futures.Future],
                                info_dict: dict, stream_urls: list[YoutubeURL.YoutubeURL], options: dict) -> None:
        """Start one download task per selected format."""
        keep_temp = options.get("keep_temp_files", False) or options.get("keep_database_file", False)
        options["keep_database"] = keep_temp
        options["keep_state"] = keep_temp
        runner = self._stream_runner(options)
        for stream_url in stream_urls:
            futures.add(executor.submit(runner, info_dict=info_dict, stream_url=stream_url, options=options, manifest=stream_url.manifest))

    def _start_live_chat(self, info_dict: dict, options: dict) -> Optional[threading.Thread]:
        if options.get('live_chat', False) is not True:
            return None
        # A daemon thread rather than a pool task: the chat may have to be abandoned (see _wait_for_live_chat)
        live_chat_thread = threading.Thread(target=self.download_live_chat, args=(info_dict, options), daemon=True)
        live_chat_thread.start()
        return live_chat_thread

    def _wait_for_tasks(self, futures: set[concurrent.futures.Future], info_dict: dict, options: dict) -> None:
        """Wait until every download task has finished, collecting their files and printing progress."""
        while futures:
            if self.kill_all.is_set() or self.kill_this.is_set():
                raise KeyboardInterrupt("Thread kill event is set, ending...")
            done, _ = concurrent.futures.wait(futures, timeout=1, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                futures.discard(future)
                self._collect_task_result(future, info_dict)
            if futures:
                self.print_stats(options=options)

    def _collect_task_result(self, future: concurrent.futures.Future, info_dict: dict) -> None:
        """
        Record the files a finished task produced. Tasks return (result, result_type). Stream
        downloads register their own files (_run_stream), so only the other types are handled here.
        """
        try:
            result, result_type = future.result()
            self.logger.log(setup_logger.VERBOSE_LEVEL_NUM, colour(result, YELLOW))
            if result_type == 'auxiliary':
                self.file_names.update(result)
                # Remove parts that would not be needed by stream downloaders
                self.remove_format_segment_playlist_from_info_dict(info_dict=info_dict)
                info_dict.pop("thumbnails", None)
            elif str(result_type).lower() not in ('video', 'audio', 'live_chat'):
                self.file_names[str(result_type)] = result
        except Exception as e:
            self.logger.exception(e)

    def _cancel_tasks(self, executor: concurrent.futures.ThreadPoolExecutor, futures: set[concurrent.futures.Future]) -> None:
        """Stop all download tasks after a kill / Ctrl+C, giving them a few seconds to finish."""
        self.kill_this.set()
        self.logger.debug("Keyboard interrupt detected")
        pending = [future for future in futures if not future.done()]
        if pending:
            self.logger.debug("Cancelling remaining threads")
        for future in pending:
            future.cancel()
        concurrent.futures.wait(futures, timeout=5, return_when=concurrent.futures.ALL_COMPLETED)
        executor.shutdown(wait=False, cancel_futures=True)
        self.logger.debug("Shutdown all threads")
        self.stats["status"] = "Cancelled"

    def output_filename(self, info_dict, outtmpl) -> str:
        # Reuse one YoutubeDL instance for filename templating (constructing one is relatively expensive)
        if self._filename_ydl is None:
            self._filename_ydl = yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True})
        # Replace normal percent sign with unicode version to prevent ffmpeg errors
        return str(self._filename_ydl.prepare_filename(info_dict, outtmpl=outtmpl)).replace("%", "％")

    # --- Moving to final destination ---

    def move_to_final(self, options, output_file, file_names):
        def maybe_move(key, dest_func, file_names_dict: dict = file_names, option_flag=None):
            """
            key: key in file_names
            dest_func: FileInfo -> destination path
            option_flag: name of a boolean option (write_thumbnail etc). If set and the option is
                         false, the file is deleted instead of moved.
            """
            f: FileInfo = file_names_dict.get(key)
            if not f:
                return
            try:
                if option_flag is not None and not options.get(option_flag, False):
                    self.logger.info(f"Removing {f.absolute()}")
                    f.unlink(missing_ok=True)
                    file_names_dict.pop(key, None)
                    return

                dest = dest_func(f)
                if os.path.abspath(str(f).strip()) != os.path.abspath(str(dest).strip()):
                    self.logger.info(f"Moving {f.absolute()} -> {dest}")
                    shutil.move(f.absolute(), dest)
                else:
                    self.logger.debug(f"{f.absolute()} is already in final destination")
            except Exception as e:
                self.logger.exception(f"unable to move {key}: {e}")

        # ensure output dir exists
        out_dir = os.path.dirname(output_file)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        maybe_move('thumbnail', lambda f: f"{output_file}{f.suffix}", option_flag='write_thumbnail')
        maybe_move('info_json', lambda f: f"{output_file}.info.json")
        maybe_move('description', lambda f: f"{output_file}{f.suffix}")

        stream_manifests = list(self.file_names["streams"].items())
        for manifest, stream in stream_manifests:
            stream_output_file = output_file
            if len(stream_manifests) > 1:
                stream_output_file = f"{output_file}.{manifest}"
            maybe_move('video', lambda f: f"{stream_output_file}.{f._format}{f.suffix}", file_names_dict=stream)
            maybe_move('audio', lambda f: f"{stream_output_file}.{f._format}{f.suffix}", file_names_dict=stream)
            # Don't append the extension twice if the output template already ends with it
            maybe_move('merged', lambda f: self._with_ext(stream_output_file, f.suffix), file_names_dict=stream)
            maybe_move('ffmpeg_cmd', lambda f: f"{stream_output_file}.ffmpeg.txt", option_flag='write_ffmpeg_command', file_names_dict=stream)

        maybe_move('live_chat', lambda f: f"{output_file}.live_chat.zip")

        # special: databases = list
        try:
            for f in file_names.get('databases', []):
                dest = f"{output_file}.{f._format}{f.suffix}"
                if str(f).strip() != str(dest).strip():
                    self.logger.info(f"Moving {f.absolute()} -> {dest}")
                    shutil.move(f.absolute(), dest)
        except Exception as e:
            self.logger.exception(f"unable to move database files: {e}")

        self.logger.info("Finished moving files from temporary directory to output destination")

    # --- Live chat ---

    def _wait_for_live_chat(self, live_chat_thread: Optional[threading.Thread], info_dict: dict, options: dict) -> None:
        """
        Wait for the live chat download after the media finished, for at most --stop-chat-when-done
        seconds. The timeout is enforced here because the downloaders only check it when a chat
        message arrives (chat-downloader) or not at all (yt-dlp), which could block forever.
        """
        if not (live_chat_thread and live_chat_thread.is_alive()):
            return
        self.logger.info("Media downloads finished, waiting for live chat to end...")
        timeout = options.get('stop_chat_when_done', None)
        if timeout:
            self.chat_timeout = time.time()
        deadline = time.monotonic() + timeout if timeout else None
        while live_chat_thread.is_alive() and not (self.kill_all.is_set() or self.kill_this.is_set() or self.graceful_stop.is_set()):
            live_chat_thread.join(3.0)
            if deadline is not None and time.monotonic() > deadline:
                self.logger.warning(f"Stopping chat download for {info_dict.get('id', 'N/A')}, timeout ({timeout}s) exceeded")
                self.chat_stop.set()
                live_chat_thread.join(LIVE_CHAT_STOP_GRACE)
                if live_chat_thread.is_alive():
                    self.logger.warning("Live chat downloader did not stop, keeping the chat downloaded so far")
                    self._package_live_chat(base_output_path(options, info_dict), snapshot=True)
                break

    def _package_live_chat(self, base_output: str, livechat_filename: Optional[str] = None, snapshot: bool = False,
                           refresh: bool = False) -> Optional[dict]:
        """
        Zip the live chat JSON (appending any .part file) and register it. Runs at most once, as it
        may be called both by an abandoned chat thread and by the coordinator after a timeout.

        snapshot: the chat thread may still be writing, so the source files are left in place and
                  the number of bytes taken from each is recorded (see _cleanup_live_chat_sources).
        refresh:  rebuild an existing snapshot so it includes chat written since the last one.
        """
        livechat_filename = livechat_filename or base_output + ".live_chat.json"
        part_file = f"{livechat_filename}.part"
        with self._live_chat_lock:
            if self._live_chat_packaged and not (refresh and self._live_chat_snapshot):
                return None
            if not (os.path.exists(livechat_filename) or os.path.exists(part_file)):
                self.logger.warning(f"No live chat file found at {livechat_filename}")
                return None
            zip_filename = base_output + ".live_chat.zip"
            # Build the zip under a temporary name so a failure never corrupts an existing snapshot
            tmp_zip = zip_filename + ".tmp"
            try:
                included: dict[str, int] = {}
                with zipfile.ZipFile(tmp_zip, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9, allowZip64=True) as zipf:
                    with zipf.open(os.path.basename(livechat_filename), 'w', force_zip64=True) as dest:
                        for source in (livechat_filename, part_file):
                            if os.path.exists(source):
                                included[source] = self._copy_counting(source, dest)
                os.replace(tmp_zip, zip_filename)
            except Exception as e:
                self.logger.exception(colour(e, RED))
                _remove_file(tmp_zip, self.logger)
                return None

            if snapshot:
                self._live_chat_snapshot = included
                self._live_chat_source = (base_output, livechat_filename)
            else:
                # Sources that can't be removed yet (still open) are retried after the final move
                leftovers: dict[str, int] = {}
                for source, size in included.items():
                    try:
                        os.remove(source)
                    except FileNotFoundError:
                        pass
                    except OSError as e:
                        self.logger.debug(f"Unable to remove {source} yet ({e}), retrying after the final move")
                        leftovers[source] = size
                self._live_chat_snapshot = leftovers
                self._live_chat_source = None
            live_chat = {'live_chat': FileInfo(zip_filename, file_type='live_chat')}
            self.file_names.update(live_chat)
            self._live_chat_packaged = True
            return live_chat

    @staticmethod
    def _copy_counting(source: str, dest: BinaryIO) -> int:
        """Copy a file into dest and return the exact number of bytes copied (it may be growing)."""
        copied = 0
        with open(source, 'rb') as src:
            while chunk := src.read(WRITE_BUFFER_SIZE):
                dest.write(chunk)
                copied += len(chunk)
        return copied

    def _refresh_live_chat_snapshot(self) -> None:
        """Re-zip an abandoned live chat right before the final move, to include anything written since."""
        # Only for a snapshot taken by the coordinator (a finished chat thread clears _live_chat_source)
        if self._live_chat_snapshot and self._live_chat_source:
            base_output, livechat_filename = self._live_chat_source
            self._package_live_chat(base_output, livechat_filename=livechat_filename, snapshot=True, refresh=True)

    def _cleanup_live_chat_sources(self, final_zip: str) -> None:
        """
        Delete the source files of an abandoned live chat download, but only once the zip that
        contains them is in its final location, and only files that haven't grown since they were
        zipped (anything newer is not in the zip). yt-dlp's raw '.part-FragN' fragment files are
        never zipped, so they are left in the temp folder.
        """
        if not self._live_chat_snapshot:
            return
        if not os.path.exists(final_zip):
            self.logger.warning(f"Live chat zip not found at {final_zip}, keeping live chat source files")
            return
        for path, included_bytes in self._live_chat_snapshot.items():
            try:
                current_size = os.path.getsize(path)
            except FileNotFoundError:
                continue
            if current_size != included_bytes:
                self.logger.warning(f"{path} changed after it was zipped, keeping it in the temp folder")
                continue
            try:
                os.remove(path)
                self.logger.debug(f"Removed live chat source file {path} (included in {final_zip})")
            except OSError as e:
                # e.g. on Windows a file still held open by the abandoned chat thread can't be deleted
                self.logger.warning(f"Unable to remove live chat source file {path}: {e}")
        self._live_chat_snapshot = {}

    def download_live_chat(self, info_dict, options):
        """
        Download the live chat with chat-downloader (falling back to yt-dlp), then zip it.
        Runs in its own thread until the chat ends or it is told to stop (see _wait_for_live_chat).
        """
        if info_dict.get("live_status", info_dict.get("status", "")) == "post_live":
            self.logger.warning("Stream is post live, unlikely to retrieve live chat")
        base_output = base_output_path(options, info_dict)
        livechat_filename = base_output + ".live_chat.json"
        self.logger.info(f"Downloading live chat to: {livechat_filename}")
        proxy = getUrls.proxy_for_ytdlp(options.get('proxy'))

        # A failed live chat download must not fail the whole download
        try:
            if not self._download_chat_with_chat_downloader(info_dict, options, livechat_filename, proxy):
                livechat_filename = self._download_chat_with_ytdlp(info_dict, options, base_output, livechat_filename, proxy)
        except Exception as e:
            self.logger.exception(colour(e, RED))

        time.sleep(1)
        live_chat = self._package_live_chat(base_output, livechat_filename=livechat_filename)
        if live_chat:
            return live_chat, 'live_chat'
        return None

    def _chat_should_stop(self) -> bool:
        return self.kill_all.is_set() or self.kill_this.is_set() or self.graceful_stop.is_set() or self.chat_stop.is_set()

    def _download_chat_with_chat_downloader(self, info_dict: dict, options: dict, livechat_filename: str, proxy) -> bool:
        """
        Download the chat with chat-downloader. Returns False if chat-downloader isn't installed or
        can't parse the chat, so the caller falls back to yt-dlp.
        """
        try:
            import chat_downloader
            from chat_downloader import ChatDownloader
        except ImportError:
            self.logger.warning("Unable to import chat-downloader, using yt-dlp")
            return False
        try:
            chat_url = f"https://www.youtube.com/watch?v={info_dict.get('id')}"
            self.logger.debug("Attempting to download with chat downloader")
            chat_download = ChatDownloader(cookies=options.get('cookies', None), proxy=proxy)
            chat = chat_download.get_chat(chat_url, output=livechat_filename, overwrite=False)
            # chat-downloader writes each message to the file as it is iterated. These stop/timeout
            # checks only run when a message arrives, so the coordinator also enforces the timeout
            # independently (see _wait_for_live_chat).
            stop_after = options.get('stop_chat_when_done', 300)
            for _message in chat:
                if self._chat_should_stop():
                    self.logger.debug("Killing live chat downloader")
                    break
                if self.chat_timeout is not None and time.time() - self.chat_timeout >= stop_after:
                    self.logger.warning(f"Stopping chat download for {info_dict.get('id', 'N/A')}, timeout ({stop_after}s) exceeded")
                    break
            chat_download.close()
            return True
        # Fallback for the known issue of chat-downloader not working after YouTube changes
        except chat_downloader.errors.ParsingError:
            self.logger.exception("Unable to parse live chat using chat-downloader, using yt-dlp")
            return False

    def _download_chat_with_ytdlp(self, info_dict: dict, options: dict, base_output: str, livechat_filename: str, proxy) -> str:
        """Download the chat as yt-dlp's 'live_chat' subtitle. Returns the path of the chat file."""
        ydl_opts = {
            'skip_download': True,               # Skip downloading video/audio
            'logger': YoutubeURL.PrefixedYTDLPLogger("live-chat", logger=self.logger),
            'cookiefile': options.get('cookies', None),
            'retries': 25,
            'concurrent_fragment_downloads': 3,
            'writesubtitles': True,              # Extract subtitles (live chat)
            'subtitlesformat': 'json',           # Set format to JSON
            'subtitleslangs': ['live_chat'],     # Only extract live chat subtitles
            'outtmpl': base_output               # Save to a JSON file
        }
        self.logger.debug(options.get('ytdlp_options', {}))
        ydl_opts.update(options.get('ytdlp_options') or {})
        if proxy:
            ydl_opts['proxy'] = proxy

        def stop_hook(_progress):
            # yt-dlp's live chat download only ends when the chat does. Raising DownloadCancelled
            # from a progress hook (called for every chat fragment) is yt-dlp's supported way to
            # stop it, which also closes its files so they can be zipped and removed normally.
            if self._chat_should_stop():
                raise yt_dlp.utils.DownloadCancelled("Live chat download stopped")
        ydl_opts['progress_hooks'] = [*(ydl_opts.get('progress_hooks') or []), stop_hook]

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                result = ydl.process_ie_result(info_dict)
                try:
                    if (result.get('requested_subtitles') or {}).get('live_chat', {}).get('filepath', None):
                        livechat_filename = result['requested_subtitles']['live_chat']['filepath']
                except Exception:
                    self.logger.warning("Unable to find live chat path, remaining with existing path")
        except yt_dlp.utils.DownloadCancelled:
            self.logger.info("Live chat download stopped, keeping the chat downloaded so far")
        except Exception as e:
            self.logger.exception(colour(e, RED))
        # After a cancellation yt-dlp's open .part file is only reachable through a reference
        # cycle (exception traceback frames); collect it so the file is closed and can be removed
        gc.collect()
        return livechat_filename

    # --- info.json helpers ---

    def replace_ip_in_json(self, file_name):
        pattern = re.compile(r'((?:[0-9]{1,3}\.){3}[0-9]{1,3})|((?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4})')
        with open(file_name, 'r', encoding="utf-8") as file:
            content = file.read()
        modified_content = re.sub(pattern, '0.0.0.0', content)
        with open(file_name, 'w', encoding="utf-8") as file:
            file.write(modified_content)

    def remove_urls_from_json(self, file_name):
        with open(file_name, 'r', encoding="utf-8") as file:
            data = json.load(file)

        watch_url = f"https://www.youtube.com/watch?v={data.get('id', '')}"
        for format in data.get('formats') or []:
            if format.get('url') is not None:
                format['url'] = watch_url
            if format.get('manifest_url') is not None:
                format['manifest_url'] = watch_url
            format.pop('fragment_base_url', None)
            format.pop('fragments', None)

        for thumbnail in data.get('thumbnails') or []:
            if thumbnail.get('url', None) is not None:
                parsed_url = urlparse(thumbnail.get('url', ""))
                thumbnail['url'] = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

        if data.get('url', None) is not None:
            data['url'] = watch_url
        if data.get('manifest_url', None) is not None:
            data['manifest_url'] = watch_url

        data['removed_urls'] = True
        with open(file_name, "w", encoding='utf-8') as file:
            json.dump(data, file)

    def download_auxiliary_files(self, info_dict, options):
        """Write the info.json, description and thumbnail with yt-dlp (no media is downloaded)."""
        base_output = base_output_path(options, info_dict)
        created_files = {}

        ydl_opts = {
            'skip_download': True,
            'quiet': True,
            'writeinfojson': options.get('write_info_json', False),
            'writedescription': options.get('write_description', False),
            # The thumbnail is also needed to embed it
            'writethumbnail': (options.get('write_thumbnail', False) or options.get("embed_thumbnail", False)),
            'outtmpl': base_output,
            'retries': 10,
            'logger': YoutubeURL.PrefixedYTDLPLogger("auxiliary", logger=self.logger),
            'format': options.get("resolution", "bv+ba/best")
        }
        proxy = getUrls.proxy_for_ytdlp(options.get('proxy'))
        if proxy:
            ydl_opts['proxy'] = proxy

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.process_ie_result(info_dict)
            self._collect_info_json(info, options, created_files, expected=ydl_opts["writeinfojson"])
            if ydl_opts["writedescription"]:
                self._write_description(ydl, info, info_dict, created_files)
            self._collect_thumbnail(info, created_files, expected=ydl_opts["writethumbnail"])

        return created_files, 'auxiliary'

    def _collect_info_json(self, info: dict, options: dict, created_files: dict, expected: bool) -> None:
        """Register the info.json yt-dlp wrote and scrub it if requested (--remove-ip-from-json, --clean-urls)."""
        info_json_path = next((item["infojson_filename"] for item in info.get("requested_downloads", [])
                               if item.get("infojson_filename", None) and os.path.exists(item["infojson_filename"])), None)
        if not info_json_path:
            if expected:
                self.logger.error("Unable to write info.json")
            return
        created_files['info_json'] = FileInfo(info_json_path, file_type='info_json')
        try:
            if options.get('remove_ip_from_json'):
                self.replace_ip_in_json(created_files['info_json'].absolute())
            if options.get('clean_urls'):
                self.remove_urls_from_json(created_files['info_json'].absolute())
        except Exception:
            self.logger.exception("Unable to find info.json file")

    def _write_description(self, ydl, info: dict, info_dict: dict, created_files: dict) -> None:
        if not info_dict.get("description"):
            self.logger.info("No description in info.json")
            return
        try:
            description_file = ydl.prepare_filename(info, 'description')
            with open(description_file, "w", encoding="utf-8") as desc_file:
                desc_file.write(info_dict["description"])
            created_files['description'] = FileInfo(description_file, file_type='description')
        except Exception:
            self.logger.exception("Unable to write description file")

    def _collect_thumbnail(self, info: dict, created_files: dict, expected: bool) -> None:
        """Register the thumbnail yt-dlp downloaded (the last thumbnail entry with a 'filepath')."""
        try:
            thumbnail_data = next((t for t in reversed(info.get('thumbnails', [])) if t.get('filepath')), None)
            thumbnail_filename = thumbnail_data.get('filepath') if thumbnail_data else None
            if thumbnail_filename and os.path.exists(thumbnail_filename):
                created_files['thumbnail'] = FileInfo(thumbnail_filename, file_type='thumbnail')
            elif expected:
                self.logger.error("Unable to download thumbnail")
        except Exception:
            self.logger.exception("Unable to download thumbnail")

    # --- Muxing ---

    def _ensure_png_or_jpeg_thumbnail(self, file_names: dict) -> None:
        """Convert the thumbnail to PNG with ffmpeg unless it already is JPEG or PNG (e.g. WebP)."""
        mime_type, _ = _guess_mime_type(file_names.get('thumbnail'))
        if str(mime_type) in ("image/jpeg", "image/png"):
            return
        self.logger.info(f"{file_names.get('thumbnail').name} is not a JPG or PNG file, converting to png")
        png_thumbnail = file_names.get('thumbnail').with_suffix(".png")
        thumbnail_conversion = ['ffmpeg', '-y', '-hide_banner', '-nostdin', '-loglevel', 'error', '-stats',
                                "-i", str(file_names.get('thumbnail').absolute()), "-c", "png", "-compression_level", "9",
                                "-pred", "mixed", str(png_thumbnail.absolute())]
        try:
            subprocess.run(thumbnail_conversion, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', check=True)
            self.logger.debug("Replacing thumbnail with .png version")
            file_names.pop('thumbnail').unlink(missing_ok=True)
            file_names['thumbnail'] = FileInfo(png_thumbnail, file_type='thumbnail')
        except subprocess.CalledProcessError as e:
            self.logger.error(e.stderr)
            self.logger.critical(e)

    @staticmethod
    def _with_ext(path: str, ext: str) -> str:
        return path if path.endswith(ext) else path + ext

    def create_mp4(self, file_names: dict, info_dict: dict, options: dict):
        """
        Mux the downloaded .ts files of each manifest with ffmpeg (with --no-merge only the ffmpeg
        command is written). The merged file is written in the temp folder; move_to_final moves it
        to the output location once all files are finished.
        """
        self.logger.log(setup_logger.VERBOSE_LEVEL_NUM, f"Files: {json.dumps(self.file_names, default=lambda o: o.to_dict())}\n")

        stream_manifests = list(self.file_names["streams"].items())
        ext = "." + str(options.get("ext") or info_dict.get('ext') or 'mp4').strip(" .")
        for manifest, stream in stream_manifests:
            output_path = base_output_path(options, info_dict)
            if len(stream_manifests) > 1:
                # A stream that restarted has one set of files per manifest
                output_path = f"{output_path}.{manifest}"
            output_path = self._with_ext(output_path, ext)
            self._mux_manifest(manifest, stream, output_path, ext, file_names, info_dict, options)
        return file_names

    def _mux_manifest(self, manifest, stream: dict, output_path: str, ext: str, file_names: dict, info_dict: dict,
                      options: dict) -> None:
        """Mux the video and audio .ts files of one manifest (plus thumbnail / metadata) into output_path."""
        from yt_dlp.postprocessor.ffmpeg import FFmpegPostProcessorError

        ydl_opts = {
            'skip_download': True,
            'quiet': True,
            'outtmpl': output_path,
            'logger': YoutubeURL.PrefixedYTDLPLogger("ffmpeg", logger=self.logger),
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ffmpeg_command_file = FileInfo(f"{output_path}.ffmpeg.txt", file_type='ffmpeg_command')
            file_names["streams"][manifest]['ffmpeg_cmd'] = ffmpeg_command_file
            merger = _make_merger(ydl, ffmpeg_command_file)

            # yt-dlp's metadata helpers read the formats from info_dict["requested_formats"]
            info_dict["requested_formats"] = self._requested_formats(stream)
            if not info_dict["requested_formats"]:
                self.logger.error(f"No downloaded streams to merge for manifest {manifest}")
                return

            mux = FFmpegMuxArgs(inputs=[fmt["filepath"] for fmt in info_dict["requested_formats"]])
            self._add_stream_maps(mux, info_dict["requested_formats"], merger)
            if file_names.get('thumbnail', None) and options.get('embed_thumbnail', True):
                self._add_thumbnail(mux, file_names, ext)
            if merger._add_metadata:
                mux.args.extend(item for pair in merger._get_metadata_opts(info_dict) for item in pair)
            if merger._add_infojson:
                self._add_info_json(mux, file_names, ext)

            do_merge = options.get("merge", True)
            merged_file = FileInfo(output_path, file_type='merged')
            if do_merge:
                self.logger.log(setup_logger.VERBOSE_LEVEL_NUM, f"Merging streams into {merged_file}")
            else:
                self.logger.log(setup_logger.VERBOSE_LEVEL_NUM, "Writing ffmpeg command instead of merging")
            input_args = ['-thread_queue_size', '1024', "-seekable", "0"]
            try:
                merger.real_run_ffmpeg([(path, input_args) for path in mux.inputs],
                                       [(str(merged_file.absolute()), mux.args)], merge=do_merge)
            except (subprocess.CalledProcessError, FFmpegPostProcessorError) as e:
                if isinstance(e, subprocess.CalledProcessError):
                    self.logger.error(e.stderr)
                    self.logger.critical(e)
                else:
                    self.logger.exception(e)
                raise

            if do_merge:
                file_names["streams"][manifest]['merged'] = merged_file
                self.logger.info(f"Successfully merged files into: {merged_file.absolute()}")
                if not (options.get('keep_ts_files') or options.get('keep_temp_files')):
                    self._remove_muxed_ts_files(file_names["streams"][manifest])

    @staticmethod
    def _requested_formats(stream: dict) -> list[dict]:
        """Describe the downloaded video and audio files of one manifest the way yt-dlp's postprocessors expect."""
        formats = []
        for key in ("video", "audio"):
            file_info: FileInfo = stream.get(key)
            if not file_info:
                continue
            formats.append({
                "filepath": str(file_info.absolute()),
                "format": file_info._format,
                "acodec": file_info._acodec or 'none',
                "vcodec": file_info._vcodec or 'none',
                "language": file_info._language,
                "protocol": file_info._protocol,
            })
        return formats

    def _add_stream_maps(self, mux: FFmpegMuxArgs, requested_formats: list[dict], merger) -> None:
        """Map the first audio / video stream of each input and label it with its format (from yt-dlp's FFmpegMergerPP)."""
        for i, fmt in enumerate(requested_formats):
            if fmt.get('acodec') != 'none':
                mux.args.extend(['-map', f'{i}:a:0'])
                try:
                    # AAC audio from HLS (m3u8) is in ADTS framing, which MP4-style containers can't hold
                    if fmt['protocol'].startswith('m3u8') and merger.get_audio_codec(fmt['filepath']) == 'aac':
                        mux.args.extend([f'-bsf:a:{mux.audio_streams}', 'aac_adtstoasc'])
                except Exception as e:
                    self.logger.warning(f"Unable to fix aac: {type(e).__name__}: {e}")
                if fmt.get("format"):
                    mux.args.extend([f'-metadata:s:a:{mux.audio_streams}', f'comment={fmt.get("format")}'])
                mux.audio_streams += 1

            if fmt.get('vcodec') != 'none':
                mux.args.extend(['-map', f'{i}:v:0'])
                if fmt.get("format"):
                    mux.args.extend([f'-metadata:s:v:{mux.video_streams}', f'comment={fmt.get("format")}'])
                mux.video_streams += 1

    def _add_thumbnail(self, mux: FFmpegMuxArgs, file_names: dict, ext: str) -> None:
        """Embed the thumbnail: as an attachment for .mkv, otherwise as an attached picture video stream."""
        if not file_names['thumbnail'].exists():
            self.logger.error(f"Thumbnail file: {file_names['thumbnail'].absolute()} is missing, continuing without embedding")
            return
        # May replace file_names['thumbnail'] with a converted .png
        self._ensure_png_or_jpeg_thumbnail(file_names)
        thumbnail: FileInfo = file_names['thumbnail']
        if ext.lower() == ".mkv":
            mime_type, _ = _guess_mime_type(thumbnail)
            mux.args.extend(['-attach', str(thumbnail.absolute()),
                             f"-metadata:s:t:{mux.attachments}", f"filename=cover{thumbnail.suffix}",
                             f"-metadata:s:t:{mux.attachments}", f"mimetype={mime_type or 'application/octet-stream'}"])
            mux.attachments += 1
        else:
            input_index = len(mux.inputs)
            mux.inputs.append(str(thumbnail.absolute()))
            mux.args.extend(['-map', f'{input_index}:v:0', f'-disposition:v:{mux.video_streams}', 'attached_pic'])
            mux.video_streams += 1

    def _add_info_json(self, mux: FFmpegMuxArgs, file_names: dict, ext: str) -> None:
        """Attach the info.json (only Matroska containers support attachments)."""
        info_json: Optional[FileInfo] = file_names.get("info_json")
        if ext.lower() not in ('.mkv', '.mka') or not info_json:
            return
        if not info_json.exists():
            self.logger.error(f"Unable to find info.json file {info_json}")
            return
        mux.args.extend([
            '-attach', str(info_json.absolute()),
            f'-metadata:s:t:{mux.attachments}', 'mimetype=application/json',
            f'-metadata:s:t:{mux.attachments}', 'filename=info.json',
        ])
        mux.attachments += 1

    def _remove_muxed_ts_files(self, stream: dict) -> None:
        """Delete the video / audio .ts files of a manifest once they are in the merged file."""
        for key in ("video", "audio"):
            stream_file = stream.get(key)
            if stream_file:
                try:
                    self.logger.info(f"Removing {stream_file.absolute()}")
                    stream_file.unlink(missing_ok=True)
                except Exception:
                    self.logger.exception(f"Unable to remove {key} .ts file")
                stream.pop(key, None)

    # --- Stats ---

    def convert_bytes(self, bytes):
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
        unit_index = 0
        while bytes >= 1024 and unit_index < len(units) - 1:
            bytes /= 1024
            unit_index += 1
        return f"{bytes:.2f} {units[unit_index]}"

    def print_stats(self, options):
        if options.get('stats_as_json', False):
            # \033[K clears the line after printing the JSON
            print(f"\r{json.dumps(self.stats)}\033[K", end="", flush=True)
            return

        if self.logger.getEffectiveLevel() > logging.INFO:
            return

        if not (self.stats.get('video') or self.stats.get('audio')):
            return

        parts = [f"{self.stats.get('id')}:"]
        for key, label in (('video', 'Video'), ('audio', 'Audio')):
            if self.stats.get(key):
                s = self.stats.get(key, {})
                text = f"{label}: {s.get('downloaded_segments', 0)}/{s.get('latest_sequence', 0)}"
                if s.get('status'):
                    text += f" ({s.get('status').capitalize()})"
                parts.append(text)

        if self.stats.get('video', {}).get('current_filesize') or self.stats.get('audio', {}).get('current_filesize'):
            size = self.stats.get('video', {}).get('current_filesize', 0) + self.stats.get('audio', {}).get('current_filesize', 0)
            parts.append(f"~{self.convert_bytes(size)} downloaded")

        full_line = " ".join(parts)
        if options.get("new_line", False):
            print(full_line)
        else:
            # \r moves to start, \033[K clears anything left over from the previous longer line
            print(f"\r{full_line}\033[K", end="", flush=True)

    def remove_format_segment_playlist_from_info_dict(self, info_dict: dict):
        for stream_format in info_dict.get('formats', []) or []:
            if isinstance(stream_format, dict):
                stream_format.pop('fragments', None)
