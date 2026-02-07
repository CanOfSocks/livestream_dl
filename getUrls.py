#!/usr/local/bin/python
import yt_dlp
import logging
import json
import time
import random

try:
    from setup_logger import VERBOSE_LEVEL_NUM
except ModuleNotFoundError:
    from .setup_logger import VERBOSE_LEVEL_NUM

import argparse
import threading

extraction_event = threading.Event()

class MyLogger:
    
    def __init__(self, logger: logging = logging.getLogger()):
        self.logger=logger

    def debug(self, msg):
        if not msg.startswith("[wait] Remaining time until next attempt:"):
            if msg.startswith('[debug] '):
                self.logger.debug(msg)
            else:
                self.info(msg)

    def info(self, msg):
        self.logger.log(VERBOSE_LEVEL_NUM, msg)

    def warning(self, msg):
        msg_str = str(msg)
        if ("private" in msg_str.lower() or
            "unavailable" in msg_str.lower()):
            self.logger.info(msg_str)
            raise yt_dlp.utils.DownloadError("Private video. Sign in if you've been granted access to this video")
        elif "Video is no longer live. Giving up after" in msg_str:
            self.logger.info(msg_str)
            raise yt_dlp.utils.DownloadError("Video is no longer live")
        elif "this live event will begin in" in msg_str.lower() or "premieres in" in msg_str.lower():
            self.logger.info(msg)
        elif "not available on this app" in msg_str:
            self.logger.error(msg)
            raise yt_dlp.utils.DownloadError(msg_str)
        elif "should already be available" in msg_str.lower():
            self.logger.info(msg_str)
            raise yt_dlp.utils.DownloadError("Video should already be available, waiting before retry")
        elif "release time of video is not known" in msg_str.lower():
            self.logger.info(msg_str)
            raise yt_dlp.utils.DownloadError("Release time of video is not known, waiting before retry")
        else:
            self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

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

class ShouldAlreadyAvailableError(yt_dlp.utils.DownloadError):
    pass
            
def get_Video_Info(id, wait=True, cookies=None, additional_options=None, proxy=None, return_format=False, sort=None, include_dash=False, include_m3u8=False, logger=logging.getLogger(), clean_info_dict: bool=False, max_retries=5):
    url = str(id)
    
    wait_tuple = None
    if isinstance(wait, tuple):
        if not (0 < len(wait) <= 2):
            raise ValueError("Wait tuple must contain 1 or 2 values")
        elif len(wait) < 2:
            wait_tuple = (wait[0], None)
        else:
            wait_tuple = (wait[0], wait[1])
    elif isinstance(wait, int):
        wait_tuple = (wait, None)
    elif wait is True:
        wait_tuple = (500, 600)
    elif isinstance(wait, str):
        wait_tuple = parse_wait(wait)
    
    retry_count = 0
    
    while retry_count < max_retries:
        yt_dlpLogger = MyLogger(logger=logger)
        
        ydl_opts = {
            'retries': 25,
            'skip_download': True,
            'cookiefile': cookies,
            'writesubtitles': True,
            'subtitlesformat': 'json',
            'subtitleslangs': ['live_chat'],
            'logger': yt_dlpLogger
        }
        
        if wait_tuple:
            if wait_tuple[1] is None:
                ydl_opts['wait_for_video'] = (wait_tuple[0],)
            else:
                ydl_opts['wait_for_video'] = wait_tuple
        
        if additional_options:
            ydl_opts.update(additional_options)
            
        if proxy is not None:
            if isinstance(proxy, str):
                ydl_opts['proxy'] = proxy
            elif isinstance(proxy, dict):
                ydl_opts['proxy'] = next(iter(proxy.values()), None)

        ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).update({"formats": ["adaptive","incomplete","duplicate"]})
        if not include_dash:
            (ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).setdefault("skip", [])).append("dash")
        if not include_m3u8:
            (ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).setdefault("skip", [])).append("hls")

        info_dict = {}
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                extraction_event.set()
                info_dict = ydl.extract_info(url, download=False)
                extraction_event.clear()
                info_dict = ydl.sanitize_info(info_dict=info_dict, remove_private_keys=clean_info_dict)

                for stream_format in info_dict.get('formats', []):
                    try:
                        stream_format.pop('fragments', None)
                    except:
                        pass
                
                if not (info_dict.get('live_status') == 'is_live' or info_dict.get('live_status') == 'post_live'):
                    raise VideoProcessedError("Video has been processed, please use yt-dlp directly")
                    
                logging.debug("Info.json: {0}".format(json.dumps(info_dict)))
                return info_dict, info_dict.get('live_status')
                
        except yt_dlp.utils.DownloadError as e:
            extraction_event.clear()
            
            should_retry = False
            wait_reason = ""
            
            if 'should already be available' in str(e).lower():
                should_retry = True
                wait_reason = "should already be available"
            elif 'release time of video is not known' in str(e).lower():
                should_retry = True
                wait_reason = "release time of video is not known"
            
            if should_retry:
                retry_count += 1
                
                if retry_count >= max_retries:
                    logger.warning(f"Maximum retry count reached ({max_retries}), giving up")
                    raise VideoUnavailableError(f"Stream still unavailable after {max_retries} retries (streaming room not started)")
                
                if wait_tuple and wait_tuple[1] is not None:
                    base_wait = wait_tuple[1]
                elif wait_tuple and wait_tuple[0] is not None:
                    base_wait = wait_tuple[0]
                else:
                    base_wait = 600
                
                jitter_percent = 0.1
                jitter_range = int(base_wait * jitter_percent)
                jitter = random.randint(-jitter_range, jitter_range)
                wait_time = max(300, base_wait + jitter)
                
                logger.warning(f"Encountered '{wait_reason}' warning, waiting {wait_time} seconds before retry (Attempt {retry_count}/{max_retries})")
                
                time.sleep(wait_time)
                
                continue
                
            elif 'video is private' in str(e) or "Private video. Sign in if you've been granted access to this video" in str(e):
                raise VideoInaccessibleError(f"Video {id} is private, unable to get stream URLs")
            elif 'This live event will begin in' in str(e) or 'Premieres in' in str(e):
                raise VideoUnavailableError("Video is not yet available. Consider using waiting option")
            elif " members " in str(e) or " members-only " in str(e):
                raise VideoInaccessibleError(f"Video {id} is a membership video. Requires valid cookies")
            elif "not available on this app" in str(e):
                raise VideoInaccessibleError(f"Video {id} not available on this player")
            elif "no longer live" in str(e).lower():
                raise LivestreamError("Livestream has ended")
            else:
                raise e
                
        finally:
            extraction_event.clear()
    
    raise VideoUnavailableError(f"Stream not available after {max_retries} retries with wait_for_video")

def parse_wait(string) -> tuple[int, int]:
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
