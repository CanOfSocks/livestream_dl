from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, unquote, parse_qs
from typing import Optional
from random import shuffle

from yt_dlp import YoutubeDL

from typing import Literal, Optional

__all__ = ["YoutubeURL", "Formats"]

import logging


try:
    # Try absolute import (standard execution)
    from setup_logger import VERBOSE_LEVEL_NUM
except ModuleNotFoundError:
    # Fallback to relative import (when part of a package)
    from .setup_logger import VERBOSE_LEVEL_NUM

class YTDLPLogger:
    def __init__(self, logger: logging = logging.getLogger()):
        self.logger = logger
    def debug(self, msg):
        if not msg.startswith("[wait] Remaining time until next attempt:"):
            if msg.startswith('[debug] ') or msg.startswith('[download] ') or msg.startswith('[live-chat] [download] '): # Additional handlers for live-chat
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

def _get_one(qs: dict[str, list[str]], field: str) -> str:
    l = qs.get(field)
    if not l or len(l) == 0:
        raise ValueError(f"URL missing required parameter '{field}'")
    #if len(l) != 1:
    #    raise ValueError(f"URL contains multiple copies of parameter '{field}'")
    return l[0]

def video_base_url(url: str) -> str:
    """
    Convert a /key/value/... URL into a query parameter URL
    and remove any 'sq' parameters, also removing 'sq' from existing query strings.
    """
    logging.debug("Attempting to parse url: {0}".format(url))
    parsed = urlparse(url)
    
    # Process slash-separated path into key/value pairs
    segments = [s for s in parsed.path.split("/") if s]
    if segments:
        base_path = segments[0]
        path_params = {}
        i = 1
        while i < len(segments):
            key = segments[i]
            value = segments[i + 1] if i + 1 < len(segments) else ""
            #if key.lower() != "sq":
            path_params[key] = unquote(value)
            i += 2
    else:
        base_path = ""
        path_params = {}

    # Process existing query string
    query_params = dict(parse_qsl(parsed.query))
    
    # Merge both, removing any 'sq'
    combined_params = {**query_params, **path_params}
    for key in list(combined_params.keys()):
        if key.lower() == "sq":
            combined_params.pop(key)

    
    # Rebuild query string
    query_string = urlencode(combined_params, doseq=True)
    
    # Reconstruct URL
    new_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        "/" + base_path if base_path else "",  # keep leading slash if exists
        "",  # params (unused)
        query_string,
        ""   # fragment
    ))
    
    return new_url

class YoutubeURL:
    id: str
    manifest: int
    itag: int
    expire: Optional[int]
    protocol: str
    base: str
    format_id:str

    def __init__(self, url: str, protocol: str="unknown", format_id: str = None, logger: logging = logging.getLogger(), vcodec=None, acodec=None, format_note=None):
        self.logger = logger
        self.protocol = protocol

        self.fomat_note = format_note or "Unknown"

        self.vcodec = None if str(vcodec).lower() == 'none' else vcodec
        self.acodec = None if str(acodec).lower() == 'none' else acodec

        # If not a dash URL, convert to "parameter" style instead of "restful" style
        if self.protocol == "http_dash_segments":
            self.base = url
        else:
            self.base = self.video_base_url(url=url)
        self._u   = urlparse(url)
        self._q   = parse_qs(self._u.query)
        # --- Parse /-style path parameters ---
        self._path_params = {}
        path = self._u.path
        if "/videoplayback/" in path:
            param_str = path.split("/videoplayback/", 1)[1]
            segments = param_str.strip("/").split("/")
            self._path_params = {segments[i]: unquote(segments[i + 1]) 
                                 for i in range(0, len(segments) - 1, 2)}
            if len(segments) % 2 != 0:
                self._path_params["flag"] = unquote(segments[-1])

        # Merge path params with query params (query overrides path)
        merged = {**self._path_params, **{k: v[0] for k, v in self._q.items()}}

        # Extract id and manifest
        id_manifest = merged["id"]
        if "~" in id_manifest:
            id_manifest = id_manifest[:id_manifest.index("~")]
        self.id, self.manifest = id_manifest.split(".")
        
        if not self.manifest:
            self.manifest = 0

        self.itag = int(merged["itag"])

        self.expire = int(merged["expire"]) if "expire" in merged else None

        self.format_id = format_id or self.itag

        self.url_parameters = merged       

    def __repr__(self) -> str:
        server = self._u.netloc
        return (f"YoutubeURL(id={self.id},itag={self.itag},manifest={self.manifest},"
                f"expire={self.expire},server={server})")

    def __str__(self):
        return str(self.base)

    def segment(self, n) -> str:
        """
        # Merge query + path params for the URL
        params = {**self._path_params, **{k: v[0] for k, v in self._q.items()}}
        params["sq"] = n
        url = self._u._replace(query=urlencode(params))
        return urlunparse(url)
        """
        return self.add_url_param("sq", n, self.base)
    
    def video_base_url(self, url: str) -> str:
        """
        Convert a /key/value/... URL into a query parameter URL
        and remove any 'sq' parameters, also removing 'sq' from existing query strings.
        """
        self.logger.debug("Attempting to parse url: {0}".format(url))
        parsed = urlparse(url)
        
        # Process slash-separated path into key/value pairs
        segments = [s for s in parsed.path.split("/") if s]
        if segments:
            base_path = segments[0]
            path_params = {}
            i = 1
            while i < len(segments):
                key = segments[i]
                value = segments[i + 1] if i + 1 < len(segments) else ""
                #if key.lower() != "sq":
                path_params[key] = unquote(value)
                i += 2
        else:
            base_path = ""
            path_params = {}

        # Process existing query string
        query_params = dict(parse_qsl(parsed.query))
        
        # Merge both, removing any 'sq'
        combined_params = {**query_params, **path_params}
        for key in list(combined_params.keys()):
            if key.lower() == "sq":
                combined_params.pop(key)

        
        # Rebuild query string
        query_string = urlencode(combined_params, doseq=True)
        
        # Reconstruct URL
        new_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            "/" + base_path if base_path else "",  # keep leading slash if exists
            "",  # params (unused)
            query_string,
            ""   # fragment
        ))
        
        return new_url
    
    def add_url_param(self, key, value, url=None) -> str:
        if url is None:
            url = self.base
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query[key] = [value]  # add or replace parameter

        new_query = urlencode(query, doseq=True)
        new_url = parsed._replace(query=new_query)
        return str(urlunparse(new_url)) 
    
class Formats:       
    def __init__(self, logger: logging = logging.getLogger()):
        self.protocol = None
        self.logger = logger
        

    def getFormatURL(self, info_json, resolution, sort=None, get_all=False, raw=False, include_dash=True, include_m3u8=False, force_m3u8=False, logger: logging = logging.getLogger(), stream_type: Optional[Literal["video", "audio"]] = None) -> YoutubeURL: 
        self.logger = logger    
        resolution = str(resolution).strip()
        
        original_res = resolution
        
        if resolution.lower() == "best":
            resolution = "bv/best"
        elif resolution.lower() == "audio_only":
            resolution = "ba"
            
        if not raw:
            # Use https (adaptive) protocol with fallback to dash
            resolutions = ["({0})[protocol=https]".format(resolution)]
            if include_dash:
                resolutions.append("({0})[protocol=http_dash_segments]".format(resolution))

            if include_m3u8:
                resolutions.append("({0})[protocol=m3u8_native]".format(resolution))

            if force_m3u8:
                resolution = "({0})[protocol=m3u8_native]".format(resolution)
            else:
                resolution = "/".join(resolutions)
        
        #if original_res != "audio_only":
        #    resolution = "({0})[vcodec!=none]".format(resolution)
        #print(resolution)
        
        ydl_opts = {
            'quiet': True,
            'skip_download': True,
            'no_warnings': True,
            "format": resolution,
            "logger": YTDLPLogger(logger=self.logger),

        }
        
        if sort:
            ydl_opts.update({"format_sort": str(sort).split(',')})

        self.logger.debug("Searching for resolution: {0}".format(resolution))
        #print("Searching for resolution: {0}".format(resolution))
            
        self.logger.debug("Original: {0}, passed: {1}".format(original_res, ydl_opts))

        #try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.process_ie_result(info_json)
            format = info.get('requested_downloads', info.get('requested_formats', info.get('url',[{}])))
            import json
            #print("Format:", json.dumps(format))
            #print("Requested Downloads:", json.dumps(info.get('requested_downloads', {})))
            #print("Requested Formats:", json.dumps(info.get('requested_formats', {})))

            format: dict = format[0]

            if format.get('requested_formats', None):
                if stream_type == "video":
                    format.update(next((d for d in format.get('requested_formats') if d.get('vcodec') != 'none'), {}))
                elif stream_type == "audio":
                    format.update(next((d for d in format.get('requested_formats') if d.get('acodec') != 'none'), {}))
                else:
                    format.update(next((d for d in format.get('requested_formats') if (d.get('vcodec') != 'none' or d.get('acodec') != 'none')), {}))

                if not format:
                    raise ValueError("No stream matches resolution/format input with a video or audio stream")
            #Handling for known issues with m3u8
            if (not format.get('url', None)) and info.get('url', None):                
                format['url'] = info.get('url')
                format['format_id'] = info.get("format_id")
                format['format_note'] = info.get("format_note")
                format['protocol'] = info.get('protocol')
                self.logger.debug("Updated format url to: {0}".format(info.get('url', None)))

            
            self.logger.debug("Formats: {0}".format(json.dumps(format,indent=4)))
            if format.get('protocol', "") == "http_dash_segments":
                #format_url = format[0].get('fragment_base_url')
                format_obj = YoutubeURL(format.get('fragment_base_url'), format.get('protocol'), format_id=format.get('format_id'), logger=self.logger, vcodec=format.get('vcodec', None), acodec=format.get('acodec', None), format_note=format.get("format_note"),)
                #format_url = str(format_obj)
            elif format.get('protocol', "") == "m3u8_native":      
                #format_url = video_base_url(self.getM3u8Url(format[0].get('url')))  
                format_obj = YoutubeURL(url=self.getM3u8Url(format.get('url')), protocol=format.get('protocol'), format_id=format.get('format_id'), logger=self.logger, vcodec=format.get('vcodec', None), acodec=format.get('acodec', None), format_note=format.get("format_note"),)
                #format_url = str(format_obj)
                if not format.get('format_id', None):
                    format['format_id'] = str(format_obj.itag).strip() 
                if (not self.protocol) and format_obj:
                    self.protocol = format_obj.protocol
            else:
                format_obj = YoutubeURL(format.get('url'), format.get('protocol'), format_id=format.get('format_id'), logger=self.logger, vcodec=format.get('vcodec', None), acodec=format.get('acodec', None), format_note=format.get("format_note"),)
                #format_url = video_base_url(format[0].get('url'))
                #format_url = str(format_obj)
            format_id = format_obj.format_id
          
            # Fix for broken log line (original 'format_url' was not defined here)
            format_url = str(format_obj) 
            logger.debug("Got URL: {0}: {1}".format(format_id, format_url))
            
            # Retrieves all URLs of found format
            if get_all:
                # 1. Call the modified function with both parameters
                all_urls = self.getAllFormatURL(
                    info_json=info_json, 
                    format_obj=format_obj
                )
                logger.debug("URLs: {0}".format(all_urls))
                # 2. Return the list of URLs
                # Note: Your type hint `-> YoutubeURL` is now incorrect for this case.
                # It should be `-> Union[YoutubeURL, List[str]]` or similar.
                return all_urls
            
            # If get_all is False, return the single object as before
            return format_obj
            
        return None
    """
    def wildcard_search(self, resolution):
        combined_list = []
        # Remove '*' from the end of the input if it exists
        if resolution.endswith('*'):
            resolution = resolution[:-1]
        # Iterate over the keys and find matches
        for key in self.video:
            if key.startswith(resolution):
                combined_list.extend(self.video[key])
        return combined_list
    """
    def getM3u8Url(self, m3u8_url, first_only=True):
        import httpx
        client = httpx.Client(timeout=30)
        response = client.get(m3u8_url)
        response.raise_for_status()
        self.logger.debug(response)
        urls = [
            line.strip()
            for line in response.text.splitlines()
            if line.strip() and not line.startswith("#")
        ]

        stream_urls = list(set(urls))

        if not stream_urls:
            raise ValueError("No m3u8 streams available")

        if first_only:
            return stream_urls[0]
        else:
            return stream_urls
    
    
    # Get all URLs of a given format
    # Get all URLs of a given format and protocol
    def getAllFormatURL(self, info_json, format_obj: YoutubeURL): 
        format_id = format_obj.itag
        protocol = format_obj.protocol
        
        urls = []  # This will store the list of URL strings
        
        for ytdlp_format in info_json['formats']:
            
            # --- Primary Filter: Protocol ---
            # Skip if the protocol doesn't match the one we're looking for
            if ytdlp_format.get('protocol') != protocol:
                continue 
                
            current_protocol = ytdlp_format['protocol']
            
            # --- Secondary Filter: Format ID (itag) ---
            # Now that we know the protocol matches, we check the format ID
            if current_protocol == 'http_dash_segments':
                url = ytdlp_format.get('fragment_base_url')
                if not url: continue
                yt_url = YoutubeURL(url=url, protocol=current_protocol, format_id=ytdlp_format.get('format_id', None), vcodec=format_obj.get('vcodec', None), acodec=format_obj.get('acodec', None), format_note=format.get("format_note"),)
                itag = yt_url.itag
                if format_id == itag: 
                    urls.append(yt_url) # Append the matching URL
                    # self.protocol is already known, no need to set it here

            elif current_protocol == 'm3u8_native':
                m3u8_playlist_url = ytdlp_format.get('url')
                if not m3u8_playlist_url: continue

                try:
                    # Fetch all stream URLs from the playlist
                    for stream_url in self.getM3u8Url(m3u8_playlist_url, first_only=False):
                        yt_url = YoutubeURL(url=stream_url, protocol=current_protocol, format_id=ytdlp_format.get('format_id', None), vcodec=ytdlp_format.get('vcodec', None), acodec=ytdlp_format.get('acodec', None), format_note=format.get("format_note"),)
                        itag = yt_url.itag
                        if format_id == itag: 
                            # Append the matching URL (using your original video_base_url call)
                            urls.append(video_base_url(stream_url))
                except Exception as e:
                    self.logger.warning(f"Failed to parse m3u8 playlist {m3u8_playlist_url}: {e}")
                    
            else: # Handles 'https' and any other direct protocols
                url = ytdlp_format.get('url')
                if not url: 
                    continue
                yt_url = YoutubeURL(url=url, protocol=current_protocol, format_id=ytdlp_format.get('format_id', None), vcodec=ytdlp_format.get('vcodec', None), acodec=ytdlp_format.get('acodec', None), format_note=format.get("format_note"),)
                itag = yt_url.itag 
                if format_id == itag:
                    # Append the matching URL (using your original video_base_url call)
                    urls.append(yt_url)

        return urls
