from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from typing import Optional
from random import shuffle

__all__ = ["YoutubeURL", "Formats"]


def _get_one(qs: dict[str, list[str]], field: str) -> str:
    l = qs.get(field)
    if not l or len(l) == 0:
        raise ValueError(f"URL missing required parameter '{field}'")
    if len(l) != 1:
        raise ValueError(f"URL contains multiple copies of parameter '{field}'")
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
        }
        self.audio = [
            '251', '141', '171', '140', '250', '249', '139', '234', '233'
        ]
        best = []
        for key in self.video:
            best.extend(self.video[key])
        self.video['best'] = best
        
    def getFormatURL(self, info_json, resolution, return_format=False, not_selector=None):     
        resolution = str(resolution)
        
        # If resolution is a string, check if it is a number and convert it for
        if resolution in self.video['best'] or resolution in self.audio:
            
            
            # Shuffle formats in case of multiple formats in info.json
            shuffle(info_json['formats'])
            for ytdlp_format in info_json['formats']:
                if resolution == ytdlp_format['format_id'].split('-')[0] and ytdlp_format['protocol'] == 'https':
                    # Split by dash to handle multiple instances of a format in the info.json
                    split = ytdlp_format['format_id'].split('-')
                    if len(split) > 1 and not_selector is not None:
                        if split[1] != str(not_selector):
                            if return_format:
                                return ytdlp_format['url'], split[0]
                            else:
                                return ytdlp_format['url']  
                    else:
                        if return_format:
                            return ytdlp_format['url'], split[0]
                        else:
                            return ytdlp_format['url']           
        elif resolution == "audio_only":
            for audio_format in self.audio:
                audio_format = str(audio_format)
                #if best['audio'] is None:
                # Shuffle formats in case of multiple formats in info.json
                shuffle(info_json['formats'])
                for ytdlp_format in info_json['formats']:
                    if audio_format == ytdlp_format['format_id'].split('-')[0] and ytdlp_format['protocol'] == 'https':
                        split = ytdlp_format['format_id'].split('-')
                        if len(split) > 1 and not_selector is not None:
                            if split[1] != str(not_selector):
                                if return_format:
                                    return ytdlp_format['url'], int(split[0])
                                else:
                                    return ytdlp_format['url']  
                        else:
                            if return_format:
                                return ytdlp_format['url'], int(split[0])
                            else:
                                return ytdlp_format['url'] 
                    
        elif self.video.get(resolution, None) is not None:  
            format_list = self.video.get(resolution)
            for video_format in format_list:
                video_format = str(video_format)
                # Shuffle formats in case of multiple formats in info.json
                shuffle(info_json['formats'])
                for ytdlp_format in info_json['formats']:
                    if video_format == ytdlp_format['format_id'].split('-')[0] and ytdlp_format['protocol'] == 'https':
                        split = ytdlp_format['format_id'].split('-')
                        if len(split) > 1 and not_selector is not None:
                            if split[1] != str(not_selector):
                                if return_format:
                                    return ytdlp_format['url'], split[0]
                                else:
                                    return ytdlp_format['url']  
                        else:
                            if return_format:
                                return ytdlp_format['url'], split[0]
                            else:
                                return ytdlp_format['url'] 
                    
        elif resolution.endswith('*'):
            format_list = self.wildcard_search(resolution)
            for video_format in format_list:
                video_format = str(video_format)
                # Shuffle formats in case of multiple formats in info.json
                shuffle(info_json['formats'])
                for ytdlp_format in info_json['formats']:
                    if video_format == ytdlp_format['format_id'].split('-')[0] and ytdlp_format['protocol'] == 'https':
                        split = ytdlp_format['format_id'].split('-')
                        if len(split) > 1 and not_selector is not None:
                            if split[1] != str(not_selector):
                                if return_format:
                                    return ytdlp_format['url'], split[0]
                                else:
                                    return ytdlp_format['url']  
                        else:
                            if return_format:
                                return ytdlp_format['url'], split[0]
                            else:
                                return ytdlp_format['url'] 
        
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
    
    def getAllFormatURL(self, info_json, resolution, return_format=False, not_selector=None): 
        urls = []   
        format = None    
        
        if (isinstance(resolution, int) or (isinstance(resolution, str) and resolution.isdigit())) and (int(resolution) in self.video['best'] or int(resolution) in self.audio):
            resolution = str(resolution)
            for ytdlp_format in info_json['formats']:
                
                # If format has yet to be found, match the first matching format ID, otherwise only grab URLs of the same format
                if resolution == ytdlp_format['format_id'].split('-')[0] and ytdlp_format['protocol'] == 'https' and (format is None or format == ytdlp_format['format_id'].split('-')[0]):
                    urls.append(ytdlp_format['url'])
                    format = ytdlp_format['format_id'].split('-')[0]        
        elif resolution == "audio_only":
            for audio_format in self.audio:
                audio_format = str(audio_format)
                #if best['audio'] is None:
                for ytdlp_format in info_json['formats']:
                    if audio_format == ytdlp_format['format_id'].split('-')[0] and ytdlp_format['protocol'] == 'https' and (format is None or format == ytdlp_format['format_id'].split('-')[0]):
                        urls.append(ytdlp_format['url'])
                        format = ytdlp_format['format_id'].split('-')[0]
                    
        elif self.video.get(resolution, None) is not None:  
            format_list = self.video.get(resolution)
            for video_format in format_list:
                video_format = str(video_format)

                for ytdlp_format in info_json['formats']:
                    if video_format == ytdlp_format['format_id'].split('-')[0] and ytdlp_format['protocol'] == 'https' and (format is None or format == ytdlp_format['format_id'].split('-')[0]):
                        urls.append(ytdlp_format['url'])
                        format = ytdlp_format['format_id'].split('-')[0]
                    
        elif resolution.endswith('*'):
            format_list = self.wildcard_search(resolution)
            for video_format in format_list:
                video_format = str(video_format)
                for ytdlp_format in info_json['formats']:
                    if video_format == ytdlp_format['format_id'].split('-')[0] and ytdlp_format['protocol'] == 'https' and (format is None or format == ytdlp_format['format_id'].split('-')[0]):
                        urls.append(ytdlp_format['url'])
                        format = ytdlp_format['format_id'].split('-')[0]
                    
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
            