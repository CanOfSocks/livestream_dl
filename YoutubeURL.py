from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
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

class YoutubeURL:
    id: str
    manifest: int
    itag: int
    expire: Optional[int]

    def __init__(self, url: str):
        self.base = url
        self._u   = urlparse(url)
        self._q   = parse_qs(self._u.query)

        id_manifest = _get_one(self._q, "id")
        if "~" in id_manifest:
            id_manifest = id_manifest[:id_manifest.index("~")]
        self.id, self.manifest = id_manifest.split(".")

        self.itag = int(_get_one(self._q, "itag"))

        if "expire" in self._q:
            self.expire = int(_get_one(self._q, "expire"))
        else:
            self.expire = None

    def __repr__(self) -> str:
        server = self._u.netloc
        return f"YoutubeURL(id={self.id},itag={self.itag},manifest={self.manifest},expire={self.expire},server={server})"

    def segment(self, n) -> str:
        params = dict(self._q)
        params["sq"] = [n]
        url = self._u._replace(query=urlencode(params, doseq=True))
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
        
    def get_itag(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        # Get the 'expire' parameter
        itag = query_params.get("itag", [None])[0]
        return str(itag).strip()
    
    
        
    def getFormatURL(self, info_json, resolution, return_format=False, sort=None, get_all=False, raw=False):     
        resolution = str(resolution).strip()
        
        original_res = resolution
        
        shuffle(info_json['formats'])
        
        if resolution.lower() == "best":
            resolution = "bv"
        elif resolution.lower() == "audio_only":
            resolution = "ba"
            
        if not raw:
            resolution = "({0})[protocol=https]".format(resolution)
        
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
            format = info.get('requested_downloads', info.get('requested_formats', [{}]))
            format_url = format[0].get('url')
            format_id = format[0].get('format_id')
            #print(format)
            
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
    
    
    # Get all URLs of a given format
    def getAllFormatURL(self, info_json, format): 
        format = str(format).strip()
        urls = []   
        
        format = str(format)
        for ytdlp_format in info_json['formats']:                
            # If format has yet to be found, match the first matching format ID, otherwise only grab URLs of the same format
            if ytdlp_format['protocol'] == 'https':
                itag = str(self.get_itag(ytdlp_format['url'])).strip() 
                if format == itag:   
                    urls.append(ytdlp_format['url'])
        
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