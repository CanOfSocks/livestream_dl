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

__all__ = ["YoutubeURL", "Formats", "YTDLPLogger", "PrefixedYTDLPLogger", "video_base_url", "quality_aliases"]

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


def _none_if_none_string(value: Any) -> Optional[str]:
    """yt-dlp uses the literal string 'none' for missing codecs; normalise that to None."""
    return None if value is None or str(value).lower() == 'none' else value


class YoutubeURL:
    id: str
    manifest: int | str
    itag: int
    expire: Optional[int]
    protocol: str
    base: str
    format_id: str

    vcodec: Optional[str]
    acodec: Optional[str]
    language: Optional[str]
    format_note: str
    ext: Optional[str]

    def __init__(self, url: str, protocol: str = "unknown", format_id: Optional[str] = None,
                 logger: logging.Logger | logging.LoggerAdapter | None = None, vcodec=None, acodec=None,
                 format_note=None, language=None, ext=None):
        self.logger = logger or logging.getLogger()
        self.protocol = protocol
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
        if self.protocol == "http_dash_segments":
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
        common = dict(protocol=protocol, format_id=format_info.get('format_id'), logger=logger,
                      vcodec=format_info.get('vcodec'), acodec=format_info.get('acodec'),
                      format_note=format_info.get("format_note"), language=format_info.get('language'),
                      ext=format_info.get('ext'))
        if protocol == "http_dash_segments":
            if not format_info.get('fragment_base_url'):
                # Only live DASH formats have a fragment_base_url. After a stream ends, yt-dlp can
                # return DASH formats with a fragment list instead, which can't be downloaded by
                # sequence number.
                raise ValueError(f"DASH format {format_info.get('format_id')} has no fragment_base_url (not a live DASH stream)")
            return cls(format_info.get('fragment_base_url'), **common)
        if protocol == "m3u8_native":
            stream_url = Formats(logger=logger).getM3u8Url(format_info.get('url'), client=m3u8_client)
            obj = cls(stream_url, **common)
            if not format_info.get('format_id'):
                # yt-dlp may not provide a format ID for m3u8 formats, so fall back to the itag
                format_info['format_id'] = str(obj.itag).strip()
                obj.format_id = format_info['format_id']
            return obj
        return cls(format_info.get('url'), **common)

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
        """Restrict a format selector to adaptive https, with optional DASH / m3u8 fallbacks."""
        resolution = str(resolution).strip()
        if force_m3u8:
            return f"({resolution})[protocol=m3u8_native]"
        resolutions = [f"({resolution})[protocol=https]"]
        if include_dash:
            resolutions.append(f"({resolution})[protocol=http_dash_segments]")
        if include_m3u8:
            resolutions.append(f"({resolution})[protocol=m3u8_native]")
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

        if ydl_options:
            ydl_opts.update(ydl_options)

        self.logger.debug(f"Searching for resolution: {resolution}")

        # Remove any previous selection so yt-dlp performs a fresh one
        info_json.pop("requested_formats", None)
        with YoutubeDL(ydl_opts) as ydl:
            return ydl.process_ie_result(info_json)

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
        """Get all URLs (from every format entry) with the same itag and protocol as format_obj."""
        target_itag = format_obj.itag
        protocol = format_obj.protocol

        urls: list[YoutubeURL] = []
        for ytdlp_format in info_json.get('formats', []) or []:
            # --- Primary Filter: Protocol ---
            if ytdlp_format.get('protocol') != protocol:
                continue

            common = dict(protocol=protocol, format_id=ytdlp_format.get('format_id', None), logger=self.logger,
                          vcodec=ytdlp_format.get('vcodec', None), acodec=ytdlp_format.get('acodec', None),
                          format_note=ytdlp_format.get("format_note"), language=ytdlp_format.get('language'),
                          ext=ytdlp_format.get('ext'))
            try:
                # --- Secondary Filter: Format ID (itag) ---
                if protocol == 'http_dash_segments':
                    url = ytdlp_format.get('fragment_base_url')
                    if url:
                        yt_url = YoutubeURL(url=url, **common)
                        if yt_url.itag == target_itag:
                            urls.append(yt_url)

                elif protocol == 'm3u8_native':
                    m3u8_playlist_url = ytdlp_format.get('url')
                    if not m3u8_playlist_url:
                        continue
                    # Fetch all stream URLs from the playlist
                    for stream_url in self.getM3u8Url(m3u8_playlist_url, first_only=False):
                        yt_url = YoutubeURL(url=stream_url, **common)
                        if yt_url.itag == target_itag:
                            urls.append(yt_url)

                else:  # Handles 'https' and any other direct protocols
                    url = ytdlp_format.get('url')
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
