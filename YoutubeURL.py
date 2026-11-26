"""
Stream URL parsing and yt-dlp based format selection.

YouTube adaptive ("https"), DASH and HLS (m3u8) stream URLs all point at googlevideo
``/videoplayback`` endpoints. Individual segments are requested by adding an ``sq=<n>``
parameter to a base URL, which is what :class:`YoutubeURL` models.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Literal, Optional
from urllib.parse import parse_qs, parse_qsl, unquote, urlencode, urlparse, urlunparse

from yt_dlp import YoutubeDL

__all__ = ["YoutubeURL", "Formats", "YTDLPLogger", "PrefixedYTDLPLogger", "video_base_url", "quality_aliases",
           "stream_url_kind", "annotate_url_kinds", "is_unprocessed_live_format", "URL_KIND_FIELD", "URL_KIND_ADAPTIVE", "URL_KIND_DASH", "URL_KIND_M3U8"]

# Kinds of stream URL livestream_dl can download segment by segment (by adding sq=<n>):
#   adaptive: a live adaptive base URL ("https" while live). After a stream ends yt-dlp relabels the
#             same formats as http_dash_segments (live-from-start processing), keeping the base URL in
#             'url', so they are recognised by the URL itself rather than yt-dlp's protocol label.
#   dash:     a live DASH manifest format (fragment_base_url)
#   m3u8:     an HLS variant playlist (resolved to a stream URL)
# yt-dlp's 'protocol' is kept as returned; the kind is stored in a separate format field so the
# format selector can filter on it. URLs of different kinds are never mixed within one download.
URL_KIND_ADAPTIVE = "adaptive"
URL_KIND_DASH = "dash"
URL_KIND_M3U8 = "m3u8"
URL_KIND_FIELD = "livestream_dl_url_kind"

try:
    # Try absolute import (standard execution)
    from setup_logger import VERBOSE_LEVEL_NUM
except ModuleNotFoundError:
    # Fallback to relative import (when part of a package)
    from .setup_logger import VERBOSE_LEVEL_NUM


class YTDLPLogger:
    """Adapter so yt-dlp messages go through the project's logger (yt-dlp INFO -> VERBOSE level)."""
    def __init__(self, logger: logging.Logger | logging.LoggerAdapter | None = None):
        self.logger = logger or logging.getLogger()

    def debug(self, msg):
        if not msg.startswith("[wait] Remaining time until next attempt:"):
            if msg.startswith('[debug] ') or msg.startswith('[download] ') or msg.startswith('[live-chat] [download] '):  # Additional handlers for live-chat
                self.logger.debug(msg)
            else:
                self.info(msg)

    def info(self, msg):
        # Safe save to Verbose log level
        self.logger.log(VERBOSE_LEVEL_NUM, msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)


class PrefixedYTDLPLogger(YTDLPLogger):
    """YTDLPLogger that tags every message with a prefix such as ``[live-chat]`` or ``[ffmpeg]``."""
    def __init__(self, prefix: str, logger: logging.Logger | logging.LoggerAdapter | None = None):
        super().__init__(logger=logger)
        self.prefix_str: str = f"[{prefix.strip('[] ')}] "

    def prefix(self, msg: str) -> str:
        msg = str(msg)
        return msg if msg.startswith(self.prefix_str) else f"{self.prefix_str}{msg}"

    def debug(self, msg):
        super().debug(self.prefix(msg))

    def info(self, msg):
        super().info(self.prefix(msg))

    def warning(self, msg):
        super().warning(self.prefix(msg))

    def error(self, msg):
        super().error(self.prefix(msg))


def _get_one(qs: dict[str, list[str]], field: str) -> str:
    values = qs.get(field)
    if not values:
        raise ValueError(f"URL missing required parameter '{field}'")
    return values[0]


def video_base_url(url: str) -> str:
    """
    Convert a /key/value/... ("restful") URL into a query parameter URL
    and remove any 'sq' parameters, also removing 'sq' from existing query strings.
    """
    logging.debug(f"Attempting to parse url: {url}")
    parsed = urlparse(url)

    # Process slash-separated path into key/value pairs. The first path element is the endpoint
    # (e.g. "videoplayback"); following elements alternate key, value.
    segments = [s for s in parsed.path.split("/") if s]
    path_params: dict[str, str] = {}
    base_path = ""
    if segments:
        base_path = segments[0]
        for i in range(1, len(segments), 2):
            key = segments[i]
            value = segments[i + 1] if i + 1 < len(segments) else ""
            path_params[key] = unquote(value)

    # Process existing query string
    query_params = dict(parse_qsl(parsed.query))

    # Merge both (path overrides query), removing any 'sq'
    combined_params = {**query_params, **path_params}
    for key in list(combined_params.keys()):
        if key.lower() == "sq":
            combined_params.pop(key)

    # Reconstruct URL
    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        "/" + base_path if base_path else "",  # keep leading slash if exists
        "",  # params (unused)
        urlencode(combined_params, doseq=True),
        ""   # fragment
    ))


def _videoplayback_path_params(path: str) -> dict[str, str]:
    """
    Parameters encoded in the path of a "restful" URL: /videoplayback/key1/value1/key2/value2...
    A trailing key without a value is stored under "flag".
    """
    if "/videoplayback/" not in path:
        return {}
    segments = path.split("/videoplayback/", 1)[1].strip("/").split("/")
    params = {segments[i]: unquote(segments[i + 1]) for i in range(0, len(segments) - 1, 2)}
    if len(segments) % 2 != 0:
        params["flag"] = unquote(segments[-1])
    return params


def _segment_url_prefix(base: str) -> str:
    """
    Everything of a segment URL up to and including "sq=", so a segment URL is prefix + number.
    Produces the same URL as YoutubeURL.add_url_param("sq", n) would.
    """
    parsed_base = urlparse(base)
    base_query = {k: v for k, v in parse_qs(parsed_base.query).items() if k != "sq"}
    query_prefix = urlencode(base_query, doseq=True)
    query_prefix = f"{query_prefix}&sq=" if query_prefix else "sq="
    return urlunparse(parsed_base._replace(query=query_prefix))


def _is_live_adaptive_url(url: str) -> bool:
    """A googlevideo /videoplayback URL of a live (unprocessed) stream: noclen marks one with no fixed length."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if "/videoplayback" not in parsed.path:
        return False
    params = {**_videoplayback_path_params(parsed.path), **{k: v[0] for k, v in parse_qs(parsed.query).items()}}
    return "noclen" in params


def stream_url_kind(format_info: dict) -> Optional[str]:
    """
    The kind of segment URL a yt-dlp format provides (see URL_KIND_*), or None if livestream_dl
    can't download it by sequence number (e.g. a processed video, or a post-live DASH manifest
    format that only has a fragment list).
    """
    protocol = str(format_info.get('protocol') or "")
    url = format_info.get('url')
    if protocol == "m3u8_native":
        return URL_KIND_M3U8 if url else None
    if format_info.get('fragment_base_url'):
        return URL_KIND_DASH if protocol == "http_dash_segments" else None
    if not isinstance(url, str) or not url or format_info.get('manifest_url'):
        return None
    if protocol == "https":
        return URL_KIND_ADAPTIVE
    if protocol.startswith("m3u8"):
        return None
    # Post-live (or --live-from-start) live adaptive formats: relabelled by yt-dlp, same base URL.
    # Any protocol label is accepted, so a future relabelling by yt-dlp doesn't break downloads.
    if _is_live_adaptive_url(url) or (format_info.get('target_duration') and "/videoplayback" in url):
        return URL_KIND_ADAPTIVE
    return None


def is_unprocessed_live_format(format_info: dict) -> bool:
    """A live adaptive format of a stream that isn't processed yet (noclen URL or a target_duration)."""
    if stream_url_kind(format_info) != URL_KIND_ADAPTIVE:
        return False
    return _is_live_adaptive_url(format_info.get('url') or "") or bool(format_info.get('target_duration'))


TITLE_FIELDS = ('title', 'fulltitle')


def process_keeping_title(ydl, info_json: dict, update_original: bool = True) -> dict:
    """
    ydl.process_ie_result() for an info dict that may have been processed before (by the
    extraction, or an earlier format selection). Each run copies 'title' to 'fulltitle' and then
    adds the current date to a live stream's title, so the date was added again on every run
    ("title 2026-01-01 10:00 2026-01-01 10:00 ..."). A copy starting from the extracted title
    (fulltitle) is processed instead, and the result keeps the title and fulltitle it had before.
    update_original: put the other fields of the result (e.g. requested_formats) into info_json,
        as processing it in place did, without its title fields changing at any point (other
        threads may be reading them).
    """
    title, fulltitle = info_json.get('title'), info_json.get('fulltitle')
    if not fulltitle:
        # Never processed: yt-dlp sets both fields
        return ydl.process_ie_result(info_json if update_original else dict(info_json))
    work = dict(info_json)
    work['title'] = fulltitle
    info = ydl.process_ie_result(work)
    info['title'], info['fulltitle'] = title, fulltitle
    if update_original:
        info_json.update({key: value for key, value in info.items() if key not in TITLE_FIELDS})
    return info


def annotate_url_kinds(info_json: dict) -> dict:
    """Store stream_url_kind() of every format in URL_KIND_FIELD (used by the format selector)."""
    for format_info in info_json.get('formats') or []:
        if isinstance(format_info, dict):
            format_info[URL_KIND_FIELD] = stream_url_kind(format_info) or "none"
    return info_json


def _none_if_none_string(value: Any) -> Optional[str]:
    """yt-dlp uses the literal string 'none' for missing codecs; normalise that to None."""
    return None if value is None or str(value).lower() == 'none' else value


class YoutubeURL:
    id: str
    manifest: int | str
    itag: int
    expire: Optional[int]
    protocol: str
    url_kind: str
    base: str
    format_id: str

    vcodec: Optional[str]
    acodec: Optional[str]
    language: Optional[str]
    format_note: str
    ext: Optional[str]

    def __init__(self, url: str, protocol: str = "unknown", format_id: Optional[str] = None,
                 logger: logging.Logger | logging.LoggerAdapter | None = None, vcodec=None, acodec=None,
                 format_note=None, language=None, ext=None, url_kind: Optional[str] = None,
                 target_duration: Optional[float] = None):
        self.logger = logger or logging.getLogger()
        self.protocol = protocol
        # Segment length reported by yt-dlp for live-processed (post-live / live-from-start) formats
        try:
            self.target_duration: Optional[float] = float(target_duration) if target_duration else None
        except (TypeError, ValueError):
            self.target_duration = None
        if url_kind is None:
            # Direct construction without a format dict: infer the kind from the protocol
            if str(protocol).startswith("m3u8"):
                url_kind = URL_KIND_M3U8
            elif protocol == "http_dash_segments":
                url_kind = URL_KIND_DASH
            else:
                url_kind = URL_KIND_ADAPTIVE
        self.url_kind = url_kind
        # Raise ValueError (which format searches handle) rather than failing later with a TypeError:
        # urlparse(None) returns bytes, which broke the path checks below
        if not isinstance(url, str) or not url.strip():
            raise ValueError(f"Format {format_id} ({protocol}) has no stream URL")

        self.format_note = format_note or "Unknown"

        self.vcodec = _none_if_none_string(vcodec)
        self.acodec = _none_if_none_string(acodec)
        self.language = _none_if_none_string(language)
        self.ext = _none_if_none_string(ext)

        # If not a dash URL, convert to "parameter" style instead of "restful" style
        if self.url_kind == URL_KIND_DASH:
            self.base = url
        else:
            self.base = video_base_url(url=url)
        self._u = urlparse(url)
        self._q = parse_qs(self._u.query)
        self._path_params: dict[str, str] = _videoplayback_path_params(self._u.path)
        # Merge path params with query params (query overrides path)
        merged = {**self._path_params, **{k: v[0] for k, v in self._q.items()}}

        # Extract id and manifest. The "id" parameter looks like "<video hash>.<manifest number>"
        # optionally followed by "~<extra>". The manifest number changes when a stream is restarted.
        if not merged.get("id") or not merged.get("itag"):
            raise ValueError(f"Format {format_id} ({protocol}) URL is not a stream segment URL (no id/itag parameter)")
        id_manifest = merged["id"]
        if "~" in id_manifest:
            id_manifest = id_manifest[:id_manifest.index("~")]
        self.id, _, manifest = id_manifest.partition(".")
        self.manifest = manifest or 0

        try:
            self.itag = int(merged["itag"])
        except ValueError:
            raise ValueError(f"Format {format_id} ({protocol}) URL has an invalid itag: {merged['itag']!r}") from None

        try:
            self.expire = int(merged["expire"]) if merged.get("expire") else None
        except (TypeError, ValueError):
            self.expire = None

        self.format_id = format_id or str(self.itag)

        self.url_parameters = merged
        # Computed once, as segment() is called for every segment
        self._segment_prefix: str = _segment_url_prefix(self.base)

    @classmethod
    def from_format(cls, format_info: dict, logger: logging.Logger | logging.LoggerAdapter | None = None,
                    m3u8_client=None) -> "YoutubeURL":
        """
        Build a YoutubeURL from a yt-dlp format dictionary, handling the protocol specific URL
        location (DASH uses fragment_base_url, m3u8 needs the variant playlist resolved to a stream URL).
        """
        protocol = format_info.get('protocol', "") or ""
        kind = stream_url_kind(format_info)
        common = dict(protocol=protocol, format_id=format_info.get('format_id'), logger=logger,
                      vcodec=format_info.get('vcodec'), acodec=format_info.get('acodec'),
                      format_note=format_info.get("format_note"), language=format_info.get('language'),
                      ext=format_info.get('ext'), url_kind=kind, target_duration=format_info.get('target_duration'))
        if kind == URL_KIND_DASH:
            return cls(format_info.get('fragment_base_url'), **common)
        if kind == URL_KIND_M3U8:
            stream_url = Formats(logger=logger).getM3u8Url(format_info.get('url'), client=m3u8_client)
            obj = cls(stream_url, **common)
            if not format_info.get('format_id'):
                # yt-dlp may not provide a format ID for m3u8 formats, so fall back to the itag
                format_info['format_id'] = str(obj.itag).strip()
                obj.format_id = format_info['format_id']
            return obj
        if kind == URL_KIND_ADAPTIVE:
            return cls(format_info.get('url'), **common)
        if protocol == "http_dash_segments":
            # Only live DASH formats have a fragment_base_url. After a stream ends, yt-dlp can
            # return DASH manifest formats with a fragment list instead, which can't be downloaded
            # by sequence number.
            raise ValueError(f"DASH format {format_info.get('format_id')} has no fragment_base_url (not a live DASH stream)")
        raise ValueError(f"Format {format_info.get('format_id')} ({protocol}) is not a live stream URL that can be downloaded by segment")

    @property
    def fomat_note(self) -> str:
        """Backwards compatible alias for the previously misspelled attribute."""
        return self.format_note

    def __repr__(self) -> str:
        server = self._u.netloc
        return (f"YoutubeURL(id={self.id},itag={self.itag},manifest={self.manifest},"
                f"expire={self.expire},server={server})")

    def __str__(self):
        return str(self.base)

    def segment(self, n: int) -> str:
        """URL of segment (sequence number) n."""
        return f"{self._segment_prefix}{int(n)}"

    def video_base_url(self, url: str) -> str:
        return video_base_url(url)

    def add_url_param(self, key, value, url=None) -> str:
        if url is None:
            url = self.base
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query[key] = [value]  # add or replace parameter

        new_query = urlencode(query, doseq=True)
        new_url = parsed._replace(query=new_query)
        return str(urlunparse(new_url))

    def seconds_until_expiry(self, now: float) -> Optional[float]:
        return None if self.expire is None else self.expire - now


class Formats:
    def __init__(self, logger: logging.Logger | logging.LoggerAdapter | None = None):
        self.protocol = None
        self.logger = logger or logging.getLogger()

    @staticmethod
    def build_format_string(resolution: str, include_dash: bool = True, include_m3u8: bool = False,
                            force_m3u8: bool = False) -> str:
        """
        Restrict a format selector to live adaptive URLs, with optional DASH / m3u8 fallbacks.
        Filters on the URL kind (see annotate_url_kinds) rather than yt-dlp's protocol label, so
        post-live adaptive formats (labelled http_dash_segments by yt-dlp) are selected too.
        """
        resolution = str(resolution).strip()
        if force_m3u8:
            return f"({resolution})[{URL_KIND_FIELD}={URL_KIND_M3U8}]"
        resolutions = [f"({resolution})[{URL_KIND_FIELD}={URL_KIND_ADAPTIVE}]"]
        if include_dash:
            resolutions.append(f"({resolution})[{URL_KIND_FIELD}={URL_KIND_DASH}]")
        if include_m3u8:
            resolutions.append(f"({resolution})[{URL_KIND_FIELD}={URL_KIND_M3U8}]")
        return "/".join(resolutions)

    def getFormats(self, info_json: dict, resolution, sort=None, raw=False, include_dash=True, include_m3u8=False,
                   force_m3u8=False, logger: logging.Logger | logging.LoggerAdapter | None = None, base_path=None,
                   ydl_options: Optional[dict] = None, **kwargs) -> dict[str, Any]:
        """Run yt-dlp format selection on an already extracted info dict (no network access)."""
        if logger is not None:
            self.logger = logger
        resolution = str(resolution).strip()

        if not raw:
            # Use https (adaptive) protocol with fallback to dash / m3u8
            resolution = self.build_format_string(resolution, include_dash, include_m3u8, force_m3u8)

        ydl_opts: dict[str, Any] = {
            'quiet': True,
            'skip_download': True,
            'no_warnings': True,
            "format": resolution,
            "logger": YTDLPLogger(logger=self.logger),
        }
        if base_path:
            ydl_opts['outtmpl'] = base_path

        if sort:
            ydl_opts["format_sort"] = str(sort).split(',')

        if info_json.get('is_live') and any(isinstance(f, dict) and f.get('is_from_start') for f in info_json.get('formats') or []):
            # Formats from a live_from_start extraction: yt-dlp's selector drops them from a live
            # stream unless live_from_start is set
            ydl_opts['live_from_start'] = True

        if ydl_options:
            ydl_opts.update(ydl_options)

        self.logger.debug(f"Searching for resolution: {resolution}")

        # Remove any previous selection so yt-dlp performs a fresh one
        info_json.pop("requested_formats", None)
        annotate_url_kinds(info_json)
        with YoutubeDL(ydl_opts) as ydl:
            return process_keeping_title(ydl, info_json)

    def getFormatURL(self, info_json, resolution, sort=None, get_all=False, raw=False, include_dash=True,
                     include_m3u8=False, force_m3u8=False, logger: logging.Logger | logging.LoggerAdapter | None = None,
                     stream_type: Optional[Literal["video", "audio"]] = None) -> YoutubeURL | list[YoutubeURL]:
        """
        Select a single stream matching `resolution` and return it as a YoutubeURL.
        With get_all=True, returns every URL (all servers/manifests) of the selected itag and protocol.
        """
        if logger is not None:
            self.logger = logger

        info = self.getFormats(info_json, resolution, sort, raw, include_dash, include_m3u8, force_m3u8, self.logger)
        formats = info.get('requested_formats') or [info]

        found_format: dict = {}
        if stream_type == "video":
            found_format.update(next((d for d in formats if d.get('vcodec') != 'none'), {}))
        elif stream_type == "audio":
            found_format.update(next((d for d in formats if d.get('acodec') != 'none'), {}))
        else:
            found_format.update(next((d for d in formats if (d.get('vcodec') != 'none' or d.get('acodec') != 'none')), {}))

        if not found_format:
            raise ValueError("No stream matches resolution/format input with a video or audio stream")

        self.logger.debug(f"Formats: {json.dumps(found_format, indent=4, default=str)}")
        format_obj = YoutubeURL.from_format(found_format, logger=self.logger)
        if found_format.get('protocol', "") == "m3u8_native" and not self.protocol:
            self.protocol = format_obj.protocol

        self.logger.debug(f"Got URL: {format_obj.format_id}: {format_obj.base}")

        # Retrieves all URLs of found format
        if get_all:
            all_urls = self.getAllFormatURL(info_json=info_json, format_obj=format_obj)
            self.logger.debug(f"URLs: {all_urls}")
            return all_urls

        return format_obj

    def getM3u8Url(self, m3u8_url, first_only=True, client=None):
        """Fetch an m3u8 variant playlist and return the stream URL(s) it contains."""
        import httpx
        own_client = client is None
        if own_client:
            client = httpx.Client(timeout=30, follow_redirects=True)
        try:
            response = client.get(m3u8_url)
            response.raise_for_status()
            self.logger.debug(response)
            text = response.text
        finally:
            # Only close a client created here (a caller's client is reused for other requests)
            if own_client:
                client.close()

        urls = [
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.startswith("#")
        ]

        # De-duplicate while keeping playlist order
        stream_urls = list(dict.fromkeys(urls))

        if not stream_urls:
            raise ValueError("No m3u8 streams available")

        if first_only:
            return stream_urls[0]
        return stream_urls

    def getAllFormatURL(self, info_json, format_obj: YoutubeURL) -> list[YoutubeURL]:
        """
        Get all URLs (from every format entry) with the same itag and URL kind as format_obj.
        Entries whose yt-dlp protocol label differs but which provide the same kind of URL (a
        live adaptive format relabelled after the stream ended) are included.
        """
        target_itag = format_obj.itag
        target_kind = getattr(format_obj, "url_kind", None) or URL_KIND_ADAPTIVE

        urls: list[YoutubeURL] = []
        for ytdlp_format in info_json.get('formats', []) or []:
            # --- Primary Filter: URL kind ---
            kind = stream_url_kind(ytdlp_format)
            if kind != target_kind:
                continue
            protocol = ytdlp_format.get('protocol') or ""

            common = dict(protocol=protocol, format_id=ytdlp_format.get('format_id', None), logger=self.logger,
                          vcodec=ytdlp_format.get('vcodec', None), acodec=ytdlp_format.get('acodec', None),
                          format_note=ytdlp_format.get("format_note"), language=ytdlp_format.get('language'),
                          ext=ytdlp_format.get('ext'), url_kind=kind, target_duration=ytdlp_format.get('target_duration'))
            try:
                # --- Secondary Filter: Format ID (itag) ---
                if kind == URL_KIND_DASH:
                    candidates = [ytdlp_format.get('fragment_base_url')]
                elif kind == URL_KIND_M3U8:
                    m3u8_playlist_url = ytdlp_format.get('url')
                    # Fetch all stream URLs from the playlist
                    candidates = self.getM3u8Url(m3u8_playlist_url, first_only=False) if m3u8_playlist_url else []
                else:
                    candidates = [ytdlp_format.get('url')]
                for url in candidates:
                    if url:
                        yt_url = YoutubeURL(url=url, **common)
                        if yt_url.itag == target_itag:
                            urls.append(yt_url)
            except Exception as e:
                self.logger.warning(f"Failed to parse format {ytdlp_format.get('format_id')} URL: {e}")

        return urls


quality_aliases = {
    "audio_only": {"format": "ba", "sort": None},
    "144p": {"format": "bv+ba/best", "sort": "res:144"},
    "240p": {"format": "bv+ba/best", "sort": "res:240"},
    "360p": {"format": "bv+ba/best", "sort": "res:360"},
    "480p": {"format": "bv+ba/best", "sort": "res:480"},
    "720p": {"format": "bv+ba/best", "sort": "res:720"},
    "720p60": {"format": "bv+ba/best", "sort": "res:720,fps:60"},
    "1080p": {"format": "bv+ba/best", "sort": "res:1080"},
    "1080p60": {"format": "bv+ba/best", "sort": "res:1080,fps:60"},
    "1440p": {"format": "bv+ba/best", "sort": "res:1440"},
    "1440p60": {"format": "bv+ba/best", "sort": "res:1440,fps:60"},
    "2160p": {"format": "bv+ba/best", "sort": "res:2160"},
    "2160p60": {"format": "bv+ba/best", "sort": "res:2160,fps:60"},
    "best": {"format": "bv+ba/best", "sort": None},
}
