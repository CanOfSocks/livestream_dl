from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, unquote, parse_qs
from typing import Optional
from random import shuffle

from yt_dlp import YoutubeDL


__all__ = ["YoutubeURL", "Formats"]

import logging

class YTDLPLogger:
    def debug(self, msg):
        logging.debug(msg)
    def info(self, msg):
        logging.info(msg)
    def warning(self, msg):
        logging.warning(msg)
    def error(self, msg):
        logging.error(msg)

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

    def __init__(self, url: str, protocol: str="unknown"):
        self.protocol = protocol
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

        self.itag = int(merged["itag"])

        self.expire = int(merged["expire"]) if "expire" in merged else None

        

    def __repr__(self) -> str:
        server = self._u.netloc
        return (f"YoutubeURL(id={self.id},itag={self.itag},manifest={self.manifest},"
                f"expire={self.expire},server={server})")

    def __str__(self):
        return str(self.base)

    def segment(self, n) -> str:
        # Merge query + path params for the URL
        params = {**self._path_params, **{k: v[0] for k, v in self._q.items()}}
        params["sq"] = n
        url = self._u._replace(query=urlencode(params))
        return urlunparse(url)
    
    def video_base_url(self, url: str) -> str:
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
    
    def add_url_param(self, url, key, value) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query[key] = [value]  # add or replace parameter

        new_query = urlencode(query, doseq=True)
        new_url = parsed._replace(query=new_query)
        return str(urlunparse(new_url)) 
    
class Formats:       
    def __init__(self):
        self.protocol = None

        

    def getFormatURL(self, info_json, resolution, return_format=False, sort=None, get_all=False, raw=False, include_dash=True, include_m3u8=False, force_m3u8=False):     
        resolution = str(resolution).strip()
        
        original_res = resolution
        
        shuffle(info_json['formats'])
        
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
        
        ydl_opts = {
            'quiet': True,
            'skip_download': True,
            'no_warnings': True,
            "format": resolution,
            "logger": YTDLPLogger()
        }
        
        if sort:
            ydl_opts.update({"format_sort": str(sort).split(',')})
            
        logging.debug("Original: {0}, passed: {1}".format(original_res, ydl_opts))

        #try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.process_ie_result(info_json)
            format = info.get('requested_downloads', info.get('requested_formats', info.get('url',[{}])))

            #Handling for known issues with m3u8
            if (not format[0].get('url', None)) and info.get('url', None):                
                format[0]['url'] = info.get('url')
                format[0]['protocol'] = info.get('protocol')
                logging.debug("Updated format[0] url to: {0}".format(info.get('url', None)))

            import json
            logging.debug("Formats: {0}".format(json.dumps(format,indent=4)))
            if format[0].get('protocol', "") == "http_dash_segments":
                format_url = format[0].get('fragment_base_url')
                format_obj = YoutubeURL(format[0].get('fragment_base_url'), format[0].get('protocol'))
                format_url = str(format_obj)
            elif format[0].get('protocol', "") == "m3u8_native":      
                format_url = video_base_url(self.getM3u8Url(format[0].get('url')))  
                format_obj = YoutubeURL(self.getM3u8Url(format[0].get('url')), format[0].get('protocol'))
                format_url = str(format_obj)
                if not format[0].get('format_id', None):
                    format[0]['format_id'] = str(format_obj.itag).strip() 
                if (not self.protocol) and format_url:
                    self.protocol = format_obj.protocol
            else:
                format_obj = YoutubeURL(format[0].get('url'), format[0].get('protocol'))
                format_url = video_base_url(format[0].get('url'))
                format_url = str(format_obj)
            format_id = format[0].get('format_id')
            #print(format)
            logging.debug("Got URL: {0}: {1}".format(format_id, format_url))
            
            # Retrieves all URLs of found format, this converts format_url to a list
            if get_all:
                format_url = self.getAllFormatURL(info_json=info_json, format=format_id)
            
            if return_format:
                return format_url, format_id
            else:
                return format_url
            
        if return_format:
            return None, None
        else:
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
        import requests
        response = requests.get(m3u8_url)
        response.raise_for_status()
        logging.debug(response)
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
    def getAllFormatURL(self, info_json, format): 
        format = str(format).strip()
        urls = []   
        
        format = str(format)
        for ytdlp_format in info_json['formats']:                
            # If format has yet to be found, match the first matching format ID, otherwise only grab URLs of the same format
            
            if ytdlp_format['protocol'] == 'http_dash_segments':
                itag = str(YoutubeURL(ytdlp_format['fragment_base_url']).itag).strip() 
                if format == itag:   
                    urls.append(ytdlp_format['fragment_base_url'])
                    self.protocol = ytdlp_format['protocol']
            #elif ytdlp_format['protocol'] == 'https':
            elif ytdlp_format['protocol'] == 'm3u8_native':
                for stream_url in self.getM3u8Url(ytdlp_format['url'], first_only=False):
                    itag = str(YoutubeURL(stream_url).itag).strip() 
                    if format == itag:   
                        urls.append(video_base_url(stream_url))
                        self.protocol = ytdlp_format['protocol']
            else:
                itag = str(YoutubeURL(ytdlp_format['url']).itag).strip() 
                if format == itag:   
                    urls.append(video_base_url(ytdlp_format['url']))
                    self.protocol = ytdlp_format['protocol']

        
        return urls
