"""
Channel monitoring: find upcoming / live streams on a channel's uploads (or members) playlist.

Every yt-dlp extraction here costs requests to YouTube, and the monitor polls repeatedly, so
results of full per-video extractions are cached between polls (see VideoStatusCache) to keep
the request volume, and therefore the risk of rate limiting, low.
"""
import json
import logging
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from yt_dlp import YoutubeDL

try:
    from YoutubeURL import YTDLPLogger, VERBOSE_LEVEL_NUM
    from getUrls import proxy_for_ytdlp
except ModuleNotFoundError:
    from .YoutubeURL import YTDLPLogger, VERBOSE_LEVEL_NUM
    from .getUrls import proxy_for_ytdlp

# Live statuses that can never become downloadable again, so they never need re-checking
_TERMINAL_STATUSES = {"was_live", "not_live"}
# Even if an upcoming stream is outside the lookahead window, re-check it at least this often in
# case the scheduled time is moved earlier.
_UPCOMING_RECHECK_SECONDS = 3600.0


def withinFuture(releaseTime=None, lookahead=24) -> bool:
    # Assume true if value missing
    if not releaseTime or not lookahead:
        return True
    release = datetime.fromtimestamp(releaseTime, timezone.utc)
    limit = datetime.now(timezone.utc) + timedelta(hours=lookahead)
    return release <= limit


def _lookahead_hours(options: dict) -> Optional[float]:
    # The CLI option is --upcoming-lookahead (dest "upcoming_lookahead"). Previously only
    # "monitor_lookahead" was read, so the CLI value was silently ignored.
    if options.get("upcoming_lookahead") is not None:
        return options.get("upcoming_lookahead")
    return options.get("monitor_lookahead", 24)


class VideoStatusCache:
    """
    Bounded cache of full-extraction results keyed by video ID, used to avoid re-extracting the
    same videos on every monitor poll. Thread-safe as the monitor may be used from several threads.
    """
    def __init__(self, max_entries: int = 1000):
        self.max_entries = max_entries
        self._entries: "OrderedDict[str, dict]" = OrderedDict()
        self._lock = threading.Lock()

    def should_skip(self, video_id: str, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        with self._lock:
            entry = self._entries.get(video_id)
            if entry is None:
                return False
            return entry["recheck_after"] > now

    def record(self, video_id: str, live_status: Optional[str], release_timestamp: Optional[float],
               lookahead_hours: Optional[float], now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        if live_status in _TERMINAL_STATUSES:
            recheck_after = float("inf")
        elif live_status == "is_upcoming" and release_timestamp and lookahead_hours:
            # Re-check when the stream enters the lookahead window, but at least hourly
            enters_window = float(release_timestamp) - float(lookahead_hours) * 3600.0
            recheck_after = min(max(enters_window, now), now + _UPCOMING_RECHECK_SECONDS)
        else:
            # Live / post_live / unknown: always re-check
            recheck_after = 0.0
        with self._lock:
            self._entries[video_id] = {"recheck_after": recheck_after, "live_status": live_status}
            self._entries.move_to_end(video_id)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)


# Shared cache for the life of the process
status_cache = VideoStatusCache()


def _playlist_url(channel_id: str, tab: Optional[str], options: dict) -> str:
    if tab == "membership":
        if channel_id.startswith("UUMO"):
            return "https://www.youtube.com/playlist?list={0}".format(channel_id)
        elif (channel_id.startswith("UC") or channel_id.startswith("UU")) and not options.get("use_stream_tab", False):
            return "https://www.youtube.com/playlist?list={0}".format("UUMO" + channel_id[2:])
        return "https://www.youtube.com/channel/{0}/{1}".format(channel_id, tab)

    if tab == "streams":
        # Note: a "UUMO" (members) playlist ID also starts with "UU", so it is used as-is here.
        # This preserves existing behaviour: passing a members playlist ID monitors that playlist
        # even without --members-only.
        if channel_id.startswith("UU"):
            return "https://www.youtube.com/playlist?list={0}".format(channel_id)
        elif channel_id.startswith("UC") and not options.get("use_stream_tab", False):
            return "https://www.youtube.com/playlist?list={0}".format("UU" + channel_id[2:])
        return "https://www.youtube.com/channel/{0}/{1}".format(channel_id, tab)

    return "https://www.youtube.com/channel/{0}/{1}".format(channel_id, tab)


def get_upcoming_or_live_videos(channel_id, tab=None, options: Optional[dict] = None, logger: logging.Logger = None,
                                skip_ids: Optional[Iterable[str]] = None, cache: Optional[VideoStatusCache] = None) -> list[str]:
    """
    Return IDs of videos on the channel that are live, post_live or upcoming within the lookahead.
    `skip_ids` are IDs already being downloaded; they are not extracted again.
    """
    options = options or {}
    logger = logger or logging.getLogger()
    cache = cache if cache is not None else status_cache
    skip = set(skip_ids or ())
    lookahead = _lookahead_hours(options)

    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'sleep_interval': 1,
        'sleep_interval_requests': 1,
        'no_warnings': True,
        'cookiefile': options.get("cookies", None),
        'playlist_items': '1-{0}'.format(options.get("playlist_items", 10)),
        "logger": YTDLPLogger(logger=logger),
    }
    ytdlp_proxy = proxy_for_ytdlp(options.get("proxy"))
    if ytdlp_proxy:
        ydl_opts['proxy'] = ytdlp_proxy

    try:
        with YoutubeDL(ydl_opts) as ydl:
            url = _playlist_url(channel_id, tab, options)
            info = ydl.extract_info(url, download=False) or {}
            upcoming_or_live_videos = []
            for video in info.get('entries') or []:
                if not video:
                    # Unavailable playlist entries can be returned as None
                    continue
                video_id = video.get('id')
                if video_id in skip:
                    continue

                # Fallback if video availability doesn't exist in extracted info
                if video.get('live_status', None) is None and video.get('duration') is None:
                    if video_id and cache.should_skip(video_id):
                        logger.debug("({0}) Skipping re-extraction, status cached".format(video_id))
                        continue
                    try:
                        video = ydl.extract_info(video.get('url'), download=False)
                    except Exception as e:
                        logger.error("Unable to find availability information for video {0}: {1}".format(video.get('url'), str(e)))
                        continue
                    if video_id:
                        cache.record(video_id, video.get('live_status'), video.get('release_timestamp'), lookahead)

                if (video.get('live_status') == 'is_live' or video.get('live_status') == 'post_live'
                        or (video.get('live_status') == 'is_upcoming' and withinFuture(video.get('release_timestamp', None), lookahead=lookahead))):

                    logger.debug("({1}) live_status = {0}".format(video.get('live_status'), video.get('id')))
                    logger.debug(json.dumps(video, default=str))
                    upcoming_or_live_videos.append(video.get('id'))

            return list(dict.fromkeys(upcoming_or_live_videos))
    except Exception:
        logger.exception("An unexpected error occurred when trying to fetch videos")
        raise


def resolve_channel(url: str, logger: logging.Logger = None):
    logger = logger or logging.getLogger()
    try:
        channel_id = get_channel(channel_url=url, logger=logger)
        if channel_id and str(channel_id).startswith("UC"):
            return channel_id
        else:
            raise ValueError("Unable to find channel ID")
    except Exception as e:
        if "not currently live" in str(e):
            logger.log(VERBOSE_LEVEL_NUM, "Channel found, but not live: {0}. Waiting until a live stream is found to resolve channel ID".format(e))
            return None
        logger.warning("Unable to find channel ID with URL search using '{0}'. Attempting to use youtube search.".format(url))
        channel_id = get_by_name(channel_name=url, logger=logger)
        if channel_id and str(channel_id).startswith("UC"):
            return channel_id
        else:
            logger.error("Unable to find channel using search: {0}".format(url))
    return None


def get_channel(channel_url: str, logger: logging.Logger = None):
    logger = logger or logging.getLogger()
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'playlist_items': '1',
        "logger": YTDLPLogger(logger=logger),
    }

    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(channel_url, download=False)

        # Fallback if there is info extracted, but no channel ID (e.g. the live tab)
        if info.get('channel_id', None) is None and info.get('url'):
            try:
                info = ydl.extract_info(info.get('url'), download=False)
            except Exception as e:
                logger.error("Unable to find availability information for video {0}: {1}".format(info.get('url'), str(e)))

        return ydl.sanitize_info(info).get("channel_id", None)


def get_by_name(channel_name: str, logger: logging.Logger = None):
    logger = logger or logging.getLogger()
    # Search for the channel specifically ('sp' filter restricts results to channels)
    search_query = f"https://www.youtube.com/results?search_query={channel_name}&sp=EgIQAg%253D%253D"

    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'playlist_items': '1',
        "logger": YTDLPLogger(logger=logger),
    }

    with YoutubeDL(ydl_opts) as ydl:
        results = ydl.extract_info(search_query, download=False)
        results = ydl.sanitize_info(results)
        if results.get('entries'):
            first_result = results['entries'][0]
            logger.info(f"Found Channel: {first_result.get('uploader')}")
            return first_result.get('channel_id')

    logger.error("No channel found with query: {0}".format(channel_name))
    return None
