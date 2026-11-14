#!/usr/local/bin/python
"""
Video information extraction helpers built on top of yt-dlp.

Every call to :func:`get_Video_Info` performs a full yt-dlp extraction, which costs several
requests to YouTube. These calls are the main source of rate limiting / bot detection, so
callers should share and cache results where possible (see
``LiveStreamDownloader.refresh_info_json``).
"""
import argparse
import copy
import logging
import re
import threading
from collections import deque
from typing import Any, Optional, Union
from urllib.parse import parse_qs, urlparse

import yt_dlp

try:
    from setup_logger import VERBOSE_LEVEL_NUM
except ModuleNotFoundError:
    try:
        from .setup_logger import VERBOSE_LEVEL_NUM
    except ImportError:
        # Define fallback if module is missing
        VERBOSE_LEVEL_NUM = 15

# Set while a yt-dlp extraction is running. runner.py checks this in its signal handler so a
# Ctrl+C during yt-dlp's own "wait for video" loop interrupts yt-dlp instead of setting kill_all.
extraction_event = threading.Event()


class ExtractionLogger:
    repeat_threshold: int = 10

    # --- Configuration Lists (Static) ---
    # Log as INFO, then raise DownloadError
    INFO_RAISE_KEYWORDS = [
        "private", "sign in", "no longer live", "is not currently live",
    ]

    # Log as INFO, does not need raising
    INFO_IGNORE_KEYWORDS = [
        "this live event will begin in", "premieres in"
    ]

    # Log as WARNING, then raise DownloadError
    WARNING_RAISE_KEYWORDS = [
        "members", "premium", "live stream recording is not available",
        "removed by the uploader", "copyright claim", "age-restricted",
        "confirm your age", "video has been removed", "country",
        "not available", "uploader has not made", "blocked", "unavailable",
    ]

    # Log as ERROR, then raise DownloadError
    ERROR_RAISE_KEYWORDS = [
        "http error 429", "confirm you're not a bot", "captcha",
        "not available on this app", "terminated",
        "incomplete youtube id", "invalid video id"
    ]

    def __init__(self, logger: logging.Logger, wait: bool = True):
        self.logger = logger
        # When not waiting for a video, identical warnings repeated `repeat_threshold` times in a
        # row indicate yt-dlp is stuck in an internal retry loop, so extraction is aborted.
        self.wait = wait
        self.warning_history: deque[str] = deque(maxlen=self.repeat_threshold)

    def debug(self, msg):
        if not msg.startswith("[wait] Remaining time until next attempt:"):
            if msg.startswith('[debug] '):
                self.logger.debug(msg)
            else:
                self.info(msg)

    def info(self, msg):
        self.logger.log(VERBOSE_LEVEL_NUM, msg)

    def warning(self, msg):
        msg_str = str(msg).lower()

        # PO token provider messages are noisy and expected to repeat, so ignore them for loop detection
        if not self.wait and not ("[pot:bgutil:http]" in msg_str):
            self.warning_history.append(msg_str)

        # 1. Check Info-Level Keywords
        if any(k in msg_str for k in self.INFO_IGNORE_KEYWORDS):
            self.logger.info(msg)

        elif any(k in msg_str for k in self.INFO_RAISE_KEYWORDS):
            self.logger.info(msg)
            raise yt_dlp.utils.DownloadError(msg_str)

        # 2. Check Warning-Level Keywords
        elif any(k in msg_str for k in self.WARNING_RAISE_KEYWORDS):
            self.logger.warning(msg)
            raise yt_dlp.utils.DownloadError(msg_str)

        # 3. Check Error-Level Keywords
        elif any(k in msg_str for k in self.ERROR_RAISE_KEYWORDS):
            self.logger.error(msg)
            raise yt_dlp.utils.DownloadError(msg_str)

        else:
            self.logger.warning(msg)

        # Loop protection
        if (not self.wait) and len(self.warning_history) >= self.repeat_threshold and len(set(self.warning_history)) == 1:
            self.logger.error(f"Repeated message detected: {msg}")
            raise RepeatedWarningError(msg, self.repeat_threshold)

    def error(self, msg):
        self.logger.error(msg)


# --- Custom Exceptions ---
class VideoInaccessibleError(PermissionError): pass
class VideoProcessedError(ValueError): pass
class VideoUnavailableError(ValueError): pass
class LivestreamError(TypeError): pass
class MaxRetryExceededError(Exception): pass
class RateLimitException(ConnectionRefusedError): pass
class VideoDownloadError(RuntimeError):
    """Raised when a stream download finishes without being able to retrieve all required segments."""
    pass


class RepeatedWarningError(Exception):
    """Exception raised when a log message is repeated beyond the allowed threshold."""
    def __init__(self, message, threshold):
        self.message = message
        self.threshold = threshold
        # Pass a descriptive string to the base Exception class
        super().__init__(f"Message repeated more than {threshold} times in succession: '{message}'")


def parse_wait(string: str) -> tuple[int, Optional[int]]:
    try:
        if ":" in string:
            parts = string.split(":")
            if len(parts) != 2:
                raise ValueError
            return (int(parts[0]), int(parts[1]))
        else:
            return (int(string), None)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{string}' must be an integer or 'min:max'")


# --- URL helpers ---

# YouTube video IDs are 11 characters from the URL-safe base64 alphabet
_VIDEO_ID_RE = re.compile(r'^[0-9A-Za-z_-]{10}[048AEIMQUYcgkosw]$')
_YOUTUBE_HOSTS = {
    "youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com",
    "youtube-nocookie.com", "www.youtube-nocookie.com",
}
# Path prefixes where the video ID is the next path element (e.g. /live/<id>)
_ID_PATH_PREFIXES = ("live", "shorts", "embed", "v", "e")


def extract_video_id(value: str) -> Optional[str]:
    """
    Extract a YouTube video ID from a bare ID or any common YouTube video URL form
    (watch?v=, /live/, youtu.be/, /shorts/, /embed/, m./music. hosts).
    Returns None if the value is not recognisably a single video (e.g. a channel or playlist URL).
    """
    if not value:
        return None
    value = str(value).strip()
    if _VIDEO_ID_RE.match(value):
        return value

    # Allow URLs without a scheme (e.g. "youtu.be/abc")
    parsed = urlparse(value if "://" in value else f"https://{value}")
    host = (parsed.hostname or "").lower()
    path_parts = [p for p in parsed.path.split("/") if p]

    candidate: Optional[str] = None
    if host in ("youtu.be", "www.youtu.be"):
        candidate = path_parts[0] if path_parts else None
    elif host in _YOUTUBE_HOSTS:
        if path_parts and path_parts[0] == "watch":
            candidate = (parse_qs(parsed.query).get("v") or [None])[0]
        elif len(path_parts) >= 2 and path_parts[0] in _ID_PATH_PREFIXES:
            candidate = path_parts[1]

    if candidate and _VIDEO_ID_RE.match(candidate):
        return candidate
    return None


def normalize_video_url(value: str) -> str:
    """
    Convert any recognised YouTube video reference into the canonical
    ``https://www.youtube.com/watch?v=<id>`` form so every URL style is treated identically
    (GitHub issue #20 reported watch?v= links behaving differently from /live/ links).
    Values that aren't recognised as a single video are returned unchanged for yt-dlp to handle.
    """
    video_id = extract_video_id(value)
    if video_id:
        return f"https://www.youtube.com/watch?v={video_id}"
    return str(value).strip() if value is not None else value


def proxy_for_ytdlp(proxy: Optional[Union[str, dict]]) -> Optional[str]:
    """
    yt-dlp only accepts a single proxy string. The --proxy option may be a plain string or an
    httpx-style mapping (JSON); for a mapping the first entry is used, as documented in --help.
    """
    if not proxy:
        return None
    if isinstance(proxy, dict):
        return next((str(v) for v in proxy.values() if v), None)
    return str(proxy).strip() or None


def get_Video_Info(
    id: str,
    wait: Union[bool, int, tuple, str] = True,
    cookies: Optional[str] = None,
    additional_options: Optional[dict] = None,
    proxy: Optional[Union[str, dict]] = None,
    return_format: bool = False,
    sort: Optional[str] = None,
    include_dash: bool = False,
    include_m3u8: bool = False,
    logger: Optional[logging.Logger] = None,
    clean_info_dict: bool = False,
    ignore_no_formats: bool = False,
    **kwargs
) -> tuple[dict[str, Any], str]:
    """
    Extract video info with yt-dlp. Returns ``(info_dict, live_status)``.
    Raises one of the custom exceptions above for well-known failure states.

    wait: how long to wait for an upcoming stream to start, see _wait_for_video_option.
    additional_options / kwargs: extra yt-dlp options (they override the defaults below).
    return_format / sort: unused, kept for compatibility with older callers.
    """
    if logger is None:
        logger = logging.getLogger()
    url = normalize_video_url(str(id))
    extraction_logger = ExtractionLogger(logger=logger, wait=True)

    ydl_opts: dict[str, Any] = {
        'retries': 10,  # Socket retries
        'skip_download': True,
        'cookiefile': cookies,
        'writesubtitles': True,
        'subtitlesformat': 'json',
        'subtitleslangs': ['live_chat'],
        'logger': extraction_logger,
        'ignore_no_formats_error': ignore_no_formats,
    }
    wait_for_video = _wait_for_video_option(wait)
    if wait_for_video is not None:
        ydl_opts['wait_for_video'] = wait_for_video
    # The logger treats some warnings differently while yt-dlp waits for a stream to start
    extraction_logger.wait = bool(ydl_opts.get("wait_for_video", None))

    # Deep copy so the caller's options (which are reused for every URL refresh) are never
    # mutated - previously the extractor_args skip list grew on every call.
    merged_additional = copy.deepcopy(additional_options) if additional_options else {}
    merged_additional.update(kwargs)
    if merged_additional:
        ydl_opts.update(merged_additional)

    ytdlp_proxy = proxy_for_ytdlp(proxy)
    if ytdlp_proxy:
        ydl_opts['proxy'] = ytdlp_proxy
    _set_youtube_format_args(ydl_opts, include_dash=include_dash, include_m3u8=include_m3u8)

    # wait=False always disables waiting, even if the additional options asked for it
    if wait is False:
        ydl_opts['wait_for_video'] = None

    return _extract_live_info(ydl_opts, url, id, clean_info_dict)


def _wait_for_video_option(wait: Union[bool, int, tuple, str, None]) -> Optional[tuple]:
    """
    Convert the `wait` argument into yt-dlp's wait_for_video option, a (min, max) tuple of
    seconds where max may be None. Returns None if yt-dlp should not wait.

        True -> (5, 300)    False -> None    600 -> (600, None)
        (60,) -> (60, None) (60, 300) -> (60, 300)    "60-300" -> parse_wait("60-300")
    """
    if isinstance(wait, tuple):
        if not (0 < len(wait) <= 2):
            raise ValueError("Wait tuple must contain 1 or 2 values")
        # A single value tuple previously produced a bare int here, which yt-dlp cannot unpack
        return (wait[0], wait[1] if len(wait) >= 2 else None)
    # bool must be checked before int as bool is a subclass of int
    if isinstance(wait, bool):
        return (5, 300) if wait else None
    if isinstance(wait, int):
        return (wait, None)
    if isinstance(wait, str):
        return parse_wait(wait)
    return None


def _set_youtube_format_args(ydl_opts: dict, include_dash: bool, include_m3u8: bool) -> None:
    """Ask the YouTube extractor for incomplete (live) formats and skip DASH / HLS unless wanted."""
    youtube_args: dict = ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {})
    youtube_args.update({"formats": ["incomplete", "duplicate"]})
    skip_list: list = youtube_args.setdefault("skip", [])
    if not include_dash and "dash" not in skip_list:
        skip_list.append("dash")
    if not include_m3u8 and "hls" not in skip_list:
        skip_list.append("hls")


def _extract_live_info(ydl_opts: dict, url: str, id: str, clean_info_dict: bool) -> tuple[dict[str, Any], str]:
    """Run the yt-dlp extraction. Only upcoming, live and post-live (unprocessed) videos are accepted."""
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            # Lets the Ctrl+C handler in runner.py interrupt yt-dlp (see extraction_event)
            extraction_event.set()
            info_dict = ydl.extract_info(url, download=False)
            extraction_event.clear()

            info_dict = ydl.sanitize_info(info_dict=info_dict, remove_private_keys=clean_info_dict)

            # Cleanup fragments if present (can be very large for post_live DASH formats)
            for stream_format in info_dict.get('formats', []) or []:
                stream_format.pop('fragments', None)

            live_status = info_dict.get('live_status')
            if live_status not in ['is_upcoming', 'is_live', 'post_live']:
                raise VideoProcessedError("Video has been processed, please use yt-dlp directly")
            return info_dict, live_status

        except yt_dlp.utils.DownloadError as e:
            extraction_event.clear()
            classified = _classify_download_error(e, id)
            if classified is e:
                raise
            raise classified from e
        finally:
            extraction_event.clear()

def _classify_download_error(e: Exception, id: str) -> Exception:
    """Map a yt-dlp DownloadError message to one of the custom exceptions (order matters)."""
    err_str = str(e).lower()

    if 'video is private' in err_str or "sign in" in err_str or "private video" in err_str:
        return VideoInaccessibleError(f"Video {id} is private")
    elif "http error 429" in err_str or "confirm you're not a bot" in err_str or "captcha" in err_str:
        return RateLimitException("Rate limited or blocked by YouTube anti-bot measures")
    elif "members" in err_str or "premium" in err_str:
        return VideoInaccessibleError(f"Video {id} is a membership or premium video")
    elif "not available on this app" in err_str:
        return VideoInaccessibleError(f"Video {id} not available on this player")
    elif "live stream recording is not available" in err_str or "removed by the uploader" in err_str:
        return VideoInaccessibleError("This live stream recording is not available.")
    elif "no longer live" in err_str:
        return LivestreamError("Livestream has ended or channel is offline")
    elif "terminated" in err_str:
        return VideoInaccessibleError(f"Video {id} has been terminated")
    elif "country" in err_str and ("not available" in err_str or "uploader has not made" in err_str or "blocked" in err_str):
        return VideoInaccessibleError("Video is region-locked (Geo-restricted)")
    elif "sign in to confirm your age" in err_str or "age-restricted" in err_str:
        return VideoInaccessibleError("Video is age-restricted and requires valid cookies")
    elif "copyright claim" in err_str:
        return VideoInaccessibleError("Video removed due to a copyright claim")
    elif "video has been removed" in err_str or "incomplete youtube id" in err_str or "invalid video id" in err_str:
        return VideoInaccessibleError("Video has been removed or ID is invalid")
    elif "video unavailable" in err_str:
        return VideoUnavailableError(f"Video {id} is unavailable")
    return e


def cli_to_ytdlp_options(opts, logger: Optional[logging.Logger] = None) -> dict:
    if logger is None:
        logger = logging.getLogger()
    try:
        try:
            import cli_to_api
        except ModuleNotFoundError:
            from . import cli_to_api
    except Exception:
        logger.exception("cli_to_api script not found. Download from: https://github.com/yt-dlp/yt-dlp/blob/master/devscripts/cli_to_api.py")
        raise

    return cli_to_api.cli_to_api(opts=opts, cli_defaults=False)
