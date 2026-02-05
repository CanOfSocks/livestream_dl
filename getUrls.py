#!/usr/local/bin/python
import yt_dlp
import logging
import json
import time
import sys

try:
    # Try absolute import (standard execution)
    from setup_logger import VERBOSE_LEVEL_NUM
except ModuleNotFoundError:
    # Fallback to relative import (when part of a package)
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
        # Safe save to Verbose log level
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
            # This is just a live stream status warning, don't throw an error
            self.logger.warning(msg)  # Log normally as a warning
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

class RetryWithDelayError(Exception):
    """Custom exception to trigger retry with delay"""
    def __init__(self, message, delay=10):
        super().__init__(message)
        self.delay = delay

def get_Video_Info(id, wait=True, cookies=None, additional_options=None, proxy=None, return_format=False, sort=None, include_dash=False, include_m3u8=False, logger=logging.getLogger(), clean_info_dict: bool=False, max_retries=3, retry_delay=10):
    """
    Retrieve video information with automatic retry on specific errors
    
    Args:
        id: Video ID or URL
        wait: Wait setting
        cookies: Cookies file path
        additional_options: Additional options
        proxy: Proxy settings
        return_format: Whether to return format information
        sort: Sorting method
        include_dash: Whether to include DASH format
        include_m3u8: Whether to include M3U8 format
        logger: Logger instance
        clean_info_dict: Whether to clean the info dictionary
        max_retries: Maximum retry attempts
        retry_delay: Retry delay time (seconds)
    """
    url = str(id)
    
    # Retry counter
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
            if "should already be available" in str(e).lower() or "Live stream is not yet fully available" in str(e):
                retry_count += 1
                if retry_count <= max_retries:
                    logger.warning(f"Video not fully available yet, retrying ({retry_count}/{max_retries})...")
                    logger.info(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                    # Increase delay time to avoid too frequent requests
                    retry_delay = min(retry_delay * 1.5, 60)  # Maximum delay 60 seconds
                    continue
                else:
                    logger.error(f"Maximum retry attempts reached ({max_retries}), giving up")
                    raise VideoUnavailableError(f"Video {id} still unavailable after multiple retries: {str(e)}")
            else:
                raise e
                
        except RetryWithDelayError as e:
            retry_count += 1
            if retry_count <= max_retries:
                delay = e.delay
                logger.warning(f"Encountered retryable error, waiting {delay} seconds before retry ({retry_count}/{max_retries})...")
                time.sleep(delay)
                continue
            else:
                logger.error(f"Maximum retry attempts reached ({max_retries}), giving up")
                raise VideoUnavailableError(f"Video {id} still unavailable after multiple retries: {str(e)}")
                
        except Exception as e:
            # Other exceptions are directly raised
            raise e

def _extract_video_info(url, wait, cookies, additional_options, proxy, include_dash, include_m3u8, logger, clean_info_dict):
    """Internal function that actually performs video information extraction"""
    yt_dlpLogger = MyLogger(logger=logger)
    
    ydl_opts = {
        'retries': 25,
        'skip_download': True,
        'cookiefile': cookies,
        'writesubtitles': True,
        'subtitlesformat': 'json',
        'subtitleslangs': ['live_chat'],
        'logger': yt_dlpLogger,
        'ignoreerrors': False,  # Ensure all errors can be caught
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
            
            # Check if information was successfully obtained
            if not info_dict:
                logger.warning("No video information obtained, attempting retry...")
                raise RetryWithDelayError("No video information obtained", delay=5)
                
            info_dict = ydl.sanitize_info(info_dict=info_dict, remove_private_keys=clean_info_dict)

            for stream_format in info_dict.get('formats', []):
                try:
                    stream_format.pop('fragments', None)
                except:
                    pass
            
            # Check live stream status
            live_status = info_dict.get('live_status')
            if live_status not in ['is_live', 'post_live']:
                logger.debug(f"Video status: {live_status}")
                if live_status == 'is_upcoming':
                    raise VideoUnavailableError("Video has not started streaming yet")
                else:
                    raise VideoProcessedError("Video has already been processed, please use yt-dlp directly")
                    
        except yt_dlp.utils.DownloadError as e:
            error_msg = str(e)
            extraction_event.clear()
            
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
                # Trigger retry when encountering this error
                logger.warning(f"Encountered 'should already be available' error: {error_msg}")
                raise VideoUnavailableError(f"Live stream not fully available yet: {error_msg}")
            elif "HTTP Error 403" in error_msg or "HTTP Error 429" in error_msg:
                # Also retry on HTTP errors
                logger.warning(f"Encountered HTTP error, will retry: {error_msg}")
                raise RetryWithDelayError(f"HTTP error: {error_msg}", delay=30)
            else:
                logger.error(f"Unknown download error: {error_msg}")
                raise e
        except VideoUnavailableError:
            # Re-raise this exception for external handling
            raise
        except Exception as e:
            extraction_event.clear()
            logger.error(f"Unknown error occurred while extracting video information: {str(e)}")
            raise e
            
    logger.debug(f"Info.json: {json.dumps(info_dict, ensure_ascii=False)[:500]}...")
    return info_dict, info_dict.get('live_status')

def parse_wait(string) -> tuple[int, int]:
    try:
        if ":" in string:
            # Split by colon and convert both parts to integers
            parts = string.split(":")
            if len(parts) != 2:
                raise ValueError
            return (int(parts[0]), int(parts[1]))
        else:
            # Return a single-item list or just the int depending on your needs
            return (int(string), None)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{string}' must be an integer or 'min:max'")

# Keep original function name alias for backward compatibility
get_video_info = get_Video_Info
