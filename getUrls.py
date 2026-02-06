import yt_dlp
import logging
import json
import time

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
            self.logger.warning(msg)
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

def get_Video_Info(id, wait=True, cookies=None, additional_options=None, proxy=None, return_format=False, sort=None, include_dash=False, include_m3u8=False, logger=logging.getLogger(), clean_info_dict: bool=False, max_retries=3, retry_delay=10):
    url = str(id)
    
    if max_retries is None:
        max_retries = 3
    if retry_delay is None:
        retry_delay = 10
    
    retry_count = 0
    
    while retry_count <= max_retries:
        try:
            return _extract_video_info(
                url=url,
                wait=wait,
                cookies=cookies,
                additional_options=additional_options,
                proxy=proxy,
                include_dash=include_dash,
                include_m3u8=include_m3u8,
                logger=logger,
                clean_info_dict=clean_info_dict
            )
            
        except VideoUnavailableError as e:
            error_str = str(e).lower()
            if "should already be available" in error_str or "live stream is not yet fully available" in error_str:
                retry_count += 1
                if retry_count <= max_retries:
                    logger.warning(f"Video not fully available yet, retrying ({retry_count}/{max_retries})...")
                    logger.info(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.5, 60)
                    continue
                else:
                    logger.error(f"Maximum retry attempts reached ({max_retries}), giving up")
                    raise VideoUnavailableError(f"Video {id} still unavailable after {max_retries} retries: {str(e)}")
            else:
                raise e
                
        except (yt_dlp.utils.DownloadError, Exception) as e:
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in ["temporary failure", "connection reset", "timeout", "http error", "429", "503"]):
                retry_count += 1
                if retry_count <= max_retries:
                    logger.warning(f"Encountered retryable error, waiting {retry_delay} seconds before retry ({retry_count}/{max_retries})...")
                    logger.warning(f"Error details: {str(e)}")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.5, 60)
                    continue
                else:
                    logger.error(f"Maximum retry attempts reached ({max_retries}), giving up")
                    raise VideoDownloadError(f"Video {id} failed after {max_retries} retries: {str(e)}")
            else:
                raise e

def _extract_video_info(url, wait, cookies, additional_options, proxy, include_dash, include_m3u8, logger, clean_info_dict):
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

    if isinstance(wait, tuple):
        if not (0 < len(wait) <= 2):
            raise ValueError("Wait tuple must contain 1 or 2 values")
        elif len(wait) < 2:
            ydl_opts['wait_for_video'] = (wait[0])
        else:
            ydl_opts['wait_for_video'] = (wait[0], wait[1])
    elif isinstance(wait, int):
        ydl_opts['wait_for_video'] = (wait, None)
    elif wait is True:
        ydl_opts['wait_for_video'] = (5,300)
    elif isinstance(wait, str):
        ydl_opts['wait_for_video'] = parse_wait(wait)
        
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
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
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
                
        except yt_dlp.utils.DownloadError as e:
            extraction_event.clear()
            error_msg = str(e)
            
            if 'video is private' in error_msg or "Private video. Sign in if you've been granted access to this video" in error_msg:
                raise VideoInaccessibleError(f"Video {url} is private, cannot retrieve stream URL")
            elif 'This live event will begin in' in error_msg or 'Premieres in' in error_msg:
                raise VideoUnavailableError("Video not available yet. Consider using wait option")
            elif " members " in error_msg or " members-only " in error_msg:
                raise VideoInaccessibleError(f"Video {url} is members-only. Valid cookies required")
            elif "not available on this app" in error_msg:
                raise VideoInaccessibleError(f"Video {url} is not available on this player")
            elif "no longer live" in error_msg.lower():
                raise LivestreamError("Live stream has ended")
            elif "should already be available" in error_msg.lower():
                raise VideoUnavailableError(f"Live stream not fully available yet: {error_msg}")
            else:
                raise e
                
    logger.debug(f"Info.json: {json.dumps(info_dict)}")
    return info_dict, info_dict.get('live_status')

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
