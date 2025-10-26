#!/usr/local/bin/python
import yt_dlp
import logging
import json

class MyLogger:
    def __init__(self):
        pass

    def debug(self, msg):
        logging.debug(msg)

    def info(self, msg):
        logging.info(msg)

    def warning(self, msg):
        msg_str = str(msg)
        if ("private" in msg_str.lower() or
            "unavailable" in msg_str.lower() or
            "should already be available" in msg_str.lower()):
            logging.info(msg_str)
            raise yt_dlp.utils.DownloadError("Private video. Sign in if you've been granted access to this video")
        elif "no longer live" in msg_str.lower():
            logging.info(msg_str)
            raise yt_dlp.utils.DownloadError("Video is no longer live")
        elif "this live event will begin in" in msg_str.lower() or "premieres in" in msg_str.lower():
            logging.info(msg)
        elif "not available on this app" in msg_str:
            logging.error(msg)
            raise yt_dlp.utils.DownloadError(msg_str)
        else:
            logging.warning(msg)

    def error(self, msg):
        logging.error(msg)

class VideoInaccessibleError(PermissionError):
    pass

class VideoProcessedError(ValueError):
    pass

class VideoUnavailableError(ValueError):
    pass

class VideoDownloadError(yt_dlp.utils.DownloadError):
    pass

class LivestreamError(TypeError):
    pass
            
def get_Video_Info(id, wait=True, cookies=None, additional_options=None, proxy=None, return_format=False, sort=None, include_dash=False, include_m3u8=False):
    #url = "https://www.youtube.com/watch?v={0}".format(id)
    url = str(id)
    logger = MyLogger()
    
    ydl_opts = {
        #'live_from_start': True,
        'retries': 25,
        'skip_download': True,
        'cookiefile': cookies,
        'writesubtitles': True,              # Extract subtitles (live chat)
        'subtitlesformat': 'json',           # Set format to JSON
        'subtitleslangs': ['live_chat'],     # Only extract live chat subtitles
#        'quiet': True,
#        'no_warnings': True,
#        'extractor_args': 'skip=dash,hls;',
        'logger': logger
    }

    if isinstance(wait, tuple):
        if len(wait) <= 0:
            raise ValueError("Wait tuple must contain 1 or 2 values")
        elif len(wait) < 2:
            ydl_opts['wait_for_video'] = (wait[0], wait[0])
        else:
            ydl_opts['wait_for_video'] = (wait[0], wait[1])
    elif isinstance(wait, int):
        ydl_opts['wait_for_video'] = (wait, wait)
    elif wait is True:
        ydl_opts['wait_for_video'] = (5,300)
    
        
    if additional_options:
        ydl_opts.update(additional_options)
        
    if proxy is not None:
        print(proxy)
        ydl_opts['proxy'] = next(iter(proxy.values()), None)

    ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).update({"formats": ["incomplete","duplicate"]})
    if not include_dash:
        (ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).setdefault("skip", [])).append("dash")
    if not include_m3u8:
        (ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).setdefault("skip", [])).append("hls")

    info_dict = {}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info_dict = ydl.extract_info(url, download=False)
            info_dict = ydl.sanitize_info(info_dict)
            # Check if the video is private
            if not (info_dict.get('live_status') == 'is_live' or info_dict.get('live_status') == 'post_live'):
                #print("Video has been processed, please use yt-dlp directly")
                raise VideoProcessedError("Video has been processed, please use yt-dlp directly")
        except yt_dlp.utils.DownloadError as e:
            # If an error occurs, we can assume the video is private or unavailable
            if 'video is private' in str(e) or "Private video. Sign in if you've been granted access to this video" in str(e):
                raise VideoInaccessibleError("Video {0} is private, unable to get stream URLs".format(id))
            elif 'This live event will begin in' in str(e) or 'Premieres in' in str(e):
                raise VideoUnavailableError("Video is not yet available. Consider using waiting option")
            #elif "This video is available to this channel's members on level" in str(e) or "members-only content" in str(e):
            elif " members " in str(e) or " members-only " in str(e):
                raise VideoInaccessibleError("Video {0} is a membership video. Requires valid cookies".format(id))
            elif "not available on this app" in str(e):
                raise VideoInaccessibleError("Video {0} not available on this player".format(id))
            elif "no longer live" in str(e).lower():
                raise LivestreamError("Livestream has ended")
            else:
                raise e
        
    logging.debug("Info.json: {0}".format(json.dumps(info_dict)))
    return info_dict, info_dict.get('live_status')
