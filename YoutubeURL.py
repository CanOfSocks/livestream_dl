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

    def __init__(self, url: str):
        self.base = url
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

    def segment(self, n) -> str:
        # Merge query + path params for the URL
        params = {**self._path_params, **{k: v[0] for k, v in self._q.items()}}
        params["sq"] = n
        url = self._u._replace(query=urlencode(params))
        return urlunparse(url)
    
class Formats:
    def __init__(self):
        self.video = {
            """ OLD:
            "2160p60" : ['337', '315', '266', '138'], # 2160p60
            "2160p" : ['313', '336'], # 2160p
            "1440p60": ['308'], # 1440p60
            "1440p": ['271', '264'], # 1440p
            "1080p60": ['335', '303', '299'], # 1080p60
            "premium": ['312', '311'], # Premium 1080p and 720p
            "1080p": ['248', '169', '137'], # 1080p
            "720p60": ['334', '302', '298'], # 720p60
            "720p": ['247', '136'], # 720p
            "480p": ['244', '135'], # 480p
            "360p": ['243', '134'], # 360p
            "240p": ['242', '133'], # 240p
            "144p": ['269', '160']  # 144p 
            """
            """
            "2160p60": {"VP9": ['337', '315'], "H264": ['138', '266']},  # 2160p60
            "2160p": {"VP9": ['313', '336'], "H264": []},  # 2160p
            "1440p60": {"VP9": ['308'], "H264": ['304']},  # 1440p60
            "1440p": {"VP9": ['271'], "H264": ['264']},  # 1440p
            "1080p60": {"VP9": ['335', '303'], "H264": ['299']},  # 1080p60
            "premium": {"VP9": ['312', '311'], "H264": []},  # Premium 1080p and 720p
            "1080p": {"VP9": ['248', '169'], "H264": ['137']},  # 1080p
            "720p60": {"VP9": ['334', '302'], "H264": ['298']},  # 720p60
            "720p": {"VP9": ['247'], "H264": ['136']},  # 720p
            "480p": {"VP9": ['244'], "H264": ['135']},  # 480p
            "360p": {"VP9": ['243'], "H264": ['134']},  # 360p
            "240p": {"VP9": ['242'], "H264": ['133']},  # 240p
            "144p": {"VP9": ['269'], "H264": ['160']}  # 144p
            """
            '337': {'resolution': '2160p60', 'codec': 'VP9'},
            '315': {'resolution': '2160p60', 'codec': 'VP9'},
            '138': {'resolution': '2160p60', 'codec': 'H264'},
            '266': {'resolution': '2160p60', 'codec': 'H264'},
            '313': {'resolution': '2160p', 'codec': 'VP9'},
            '336': {'resolution': '2160p', 'codec': 'VP9'},
            '308': {'resolution': '1440p60', 'codec': 'VP9'},
            '304': {'resolution': '1440p60', 'codec': 'H264'},
            '271': {'resolution': '1440p', 'codec': 'VP9'},
            '264': {'resolution': '1440p', 'codec': 'H264'},
            '335': {'resolution': '1080p60', 'codec': 'VP9'},
            '303': {'resolution': '1080p60', 'codec': 'VP9'},
            '299': {'resolution': '1080p60', 'codec': 'H264'},
            '312': {'resolution': 'premium', 'codec': 'VP9'},
            '311': {'resolution': 'premium', 'codec': 'VP9'},
            '248': {'resolution': '1080p', 'codec': 'VP9'},
            '169': {'resolution': '1080p', 'codec': 'VP9'},
            '137': {'resolution': '1080p', 'codec': 'H264'},
            '334': {'resolution': '720p60', 'codec': 'VP9'},
            '302': {'resolution': '720p60', 'codec': 'VP9'},
            '298': {'resolution': '720p60', 'codec': 'H264'},
            '247': {'resolution': '720p', 'codec': 'VP9'},
            '136': {'resolution': '720p', 'codec': 'H264'},
            '244': {'resolution': '480p', 'codec': 'VP9'},
            '135': {'resolution': '480p', 'codec': 'H264'},
            '243': {'resolution': '360p', 'codec': 'VP9'},
            '134': {'resolution': '360p', 'codec': 'H264'},
            '242': {'resolution': '240p', 'codec': 'VP9'},
            '133': {'resolution': '240p', 'codec': 'H264'},
            '269': {'resolution': '144p', 'codec': 'VP9'},
            '160': {'resolution': '144p', 'codec': 'H264'}            
            
        }
        self.audio = [
            '251', '141', '171', '140', '250', '249', '139', '234', '233'
        ]
        best = []
        for key in self.video:
            best.extend(self.video[key])
        self.video['best'] = best

        self.protocol = None
        
    def get_itag(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        # Get the 'expire' parameter
        itag = query_params.get("itag", [None])[0]
        return str(itag).strip()
    
    
        
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
            elif format[0].get('protocol', "") == "m3u8_native":                
                format_url = video_base_url(self.getM3u8Url(format[0].get('url')))  
                if not format[0].get('format_id', None):
                    format[0]['format_id'] = str(YoutubeURL(format_url).itag).strip() 
                if (not self.protocol) and format_url:
                    self.protocol = "m3u8_native"
            else:
                format_url = video_base_url(format[0].get('url'))
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
        """#Legacy code
        if resolution in self.video['best'] or resolution in self.audio:
            resolution = str(resolution)
            for ytdlp_format in info_json['formats']:                
                # If format has yet to be found, match the first matching format ID, otherwise only grab URLs of the same format
                if ytdlp_format['protocol'] == 'https':
                    itag = str(self.get_itag(ytdlp_format['url'])).strip() 
                    if resolution == itag and (format is None or format == itag):   
                        urls.append(ytdlp_format['url'])
                        format = itag      
        elif resolution == "audio_only":
            for audio_format in self.audio:
                audio_format = str(audio_format).strip()
                #if best['audio'] is None:
                for ytdlp_format in info_json['formats']:
                    if ytdlp_format['protocol'] == 'https':
                        itag = str(self.get_itag(ytdlp_format['url'])).strip() 
                        if audio_format == itag and (format is None or format == itag):  
                            urls.append(ytdlp_format['url'])
                            format = itag
                    
        elif self.video.get(resolution, None) is not None:  
            format_list = self.video.get(resolution)
            for video_format in format_list:
                video_format = str(video_format)

                for ytdlp_format in info_json['formats']:
                    if ytdlp_format['protocol'] == 'https':
                        itag = str(self.get_itag(ytdlp_format['url'])).strip() 
                        if video_format == itag and (format is None or format == itag):  
                            urls.append(ytdlp_format['url'])
                            format = itag
                    
        elif resolution.endswith('*'):
            format_list = self.wildcard_search(resolution)
            for video_format in format_list:
                video_format = str(video_format)
                for ytdlp_format in info_json['formats']:
                    if ytdlp_format['protocol'] == 'https':
                        itag = str(self.get_itag(ytdlp_format['url'])).strip() 
                        if video_format == itag and (format is None or format == itag):  
                            urls.append(ytdlp_format['url'])
                            format = itag
                    
        if urls:
            if return_format:
                return urls, format
            else:
                return urls
        else:
            if return_format:
                return None, None
            else:
                return None 
        """