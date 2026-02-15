#!/usr/local/bin/python
import yt_dlp
import logging
import json
import random
import time
import argparse
import threading
from typing import Optional, Union, Dict, Any, Tuple

try:
    # Try absolute import (standard execution)
    from setup_logger import VERBOSE_LEVEL_NUM
except ModuleNotFoundError:
    try:
        # Fallback to relative import (when part of a package)
        from .setup_logger import VERBOSE_LEVEL_NUM
    except ImportError:
        # Define fallback if module is missing
        VERBOSE_LEVEL_NUM = 15

extraction_event = threading.Event()

class MyLogger:
    def __init__(self, logger: logging.Logger, max_retries: int = 6, base_wait: int = 600):
        self.logger = logger
        self.retry_count = 0
        self.max_retries = max_retries
        self.base_wait = base_wait
        self.should_retry = False
        self.retry_message = None

    def debug(self, msg):
        if not msg.startswith("[wait] Remaining time until next attempt:"):
            if msg.startswith('[debug] '):
                self.logger.debug(msg)
            else:
                self.info(msg)

    def info(self, msg):
        msg_str = str(msg)
        
        # Check for specific warnings that require retry
        if ("should already be available" in msg_str.lower() or 
            "release time of video is not known" in msg_str.lower()):
            self.should_retry = True
            self.retry_message = msg_str
            self.logger.warning(f"Detected warning requiring retry: {msg_str}")
        else:
            # Safe save to Verbose log level
            self.logger.log(VERBOSE_LEVEL_NUM, msg)

    def warning(self, msg):
        msg_str = str(msg).lower()
        
        # --- HARD FAILURES ---
        if "country" in msg_str and ("not available" in msg_str or "uploader has not made" in msg_str):
            raise VideoInaccessibleError("Video is region-locked (Geo-restricted)")
            
        elif "sign in to confirm your age" in msg_str or "age-restricted" in msg_str:
            raise VideoInaccessibleError("Video is age-restricted and requires valid cookies")

        elif "video has been removed" in msg_str:
            raise VideoUnavailableError("Video has been removed/deleted")

        # --- RETRYABLE TECHNICAL ERRORS ---
        # These occur when a stream is starting but CDN isn't ready
        elif any(err in msg_str for err in ["fragment not found", "empty manifest", "playlist not found"]):
            self.should_retry = True
            self.retry_message = msg_str
            self.logger.warning(f"CDN/Manifest propagation issue, retrying: {msg_str}")

        # --- EXISTING LOGIC ---
        elif ("private" in msg_str or "unavailable" in msg_str):
            self.logger.info(msg_str)
            raise yt_dlp.utils.DownloadError("Private video. Sign in if you've been granted access to this video")
        elif "video is no longer live" in msg_str or "giving up after" in msg_str:
            self.logger.info(msg_str)
            raise yt_dlp.utils.DownloadError("Video is no longer live")
        elif "this live event will begin in" in msg_str or "premieres in" in msg_str:
            self.logger.info(msg)
        elif "not available on this app" in msg_str:
            self.logger.error(msg)
            raise yt_dlp.utils.DownloadError(msg_str)
        elif "should already be available" in msg_str:
            self.should_retry = True
            self.retry_message = msg_str
            self.logger.warning(f"Live stream not fully available yet, will retry: {msg_str}")
        else:
            self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)
        
    def reset_retry_state(self):
        """Reset retry state"""
        self.should_retry = False
        self.retry_message = None
        self.retry_count = 0
        
    def get_wait_time(self):
        """Calculate wait time with jitter"""
        jitter = random.uniform(-0.2, 0.2)  # ±20% jitter
        wait_time = self.base_wait * (1 + jitter)
        return wait_time

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

class MaxRetryExceededError(Exception):
    """Maximum retry attempts exceeded error"""
    pass

def parse_wait(string) -> Tuple[int, Optional[int]]:
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

def _handle_retry_wait(logger, yt_dlpLogger, current_try, max_retries, extraction_event):
    """Helper function to handle the sleep logic to avoid code duplication"""
    wait_time = yt_dlpLogger.get_wait_time()
    
    if current_try >= max_retries:
        error_msg = f"[Live stream offline status] Maximum retry attempts {max_retries} exceeded."
        logger.error(error_msg)
        raise MaxRetryExceededError(error_msg)
    
    logger.warning(f"Live stream not ready. Waiting {wait_time:.2f}s. Attempt {current_try}/{max_retries}")
    
    # Segmented waiting
    end_time = time.time() + wait_time
    while time.time() < end_time:
        if extraction_event.is_set():
            logger.warning("extraction_event was set, interrupting wait")
            break
        time.sleep(1)

def get_Video_Info(
    id: str,
    wait: Union[bool, int, tuple, str] = True,
    cookies: Optional[str] = None,
    additional_options: Optional[Dict] = None,
    proxy: Optional[Union[str, dict]] = None,
    return_format: bool = False,
    sort: Optional[str] = None,
    include_dash: bool = False,
    include_m3u8: bool = False,
    logger: Optional[logging.Logger] = None,
    clean_info_dict: bool = False,
    max_retries: int = 6,  # Keep original default
    **kwargs  # Allow additional options
):
    # Setup Logger
    if logger is None:
        logger = logging.getLogger()
    
    #url = "https://www.youtube.com/watch?v={0}".format(id)
    url = str(id)

    # Initialize custom logger with the passed retry limit
    yt_dlpLogger = MyLogger(logger=logger, max_retries=max_retries)
    
    # Base Options
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
        'logger': yt_dlpLogger
    }

    # Handle Wait Logic
    if isinstance(wait, tuple):
        if not (0 < len(wait) <= 2):
            raise ValueError("Wait tuple must contain 1 or 2 values")
        ydl_opts['wait_for_video'] = (wait[0], wait[1]) if len(wait) >= 2 else (wait[0])
    elif isinstance(wait, int):
        ydl_opts['wait_for_video'] = (wait, None)
    elif wait is True:
        ydl_opts['wait_for_video'] = (5, 300)
    elif isinstance(wait, str):
        ydl_opts['wait_for_video'] = parse_wait(wait)
        
    # Handle Options Merging
    if additional_options is None:
        additional_options = {}
    
    # Merge kwargs into additional_options
    additional_options.update(kwargs)
    
    if additional_options:
        ydl_opts.update(additional_options)
        
    # Handle Proxy
    if proxy is not None:
        if isinstance(proxy, str):
            ydl_opts['proxy'] = proxy
        elif isinstance(proxy, dict):
            ydl_opts['proxy'] = next(iter(proxy.values()), None)

    # Handle Formats
    ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).update({"formats": ["live_adaptive","incomplete","duplicate"]})
    
    skip_list = ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).setdefault("skip", [])
    if not include_dash:
        skip_list.append("dash")
    if not include_m3u8:
        skip_list.append("hls")

    info_dict = {}
    current_try = 0
    
    while current_try < max_retries:
        # Set ydl to None at the beginning of each iteration
        ydl = None
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                try:
                    extraction_event.set()
                    info_dict = ydl.extract_info(url, download=False)
                    extraction_event.clear()
                    
                    # 1. Logic Check: Did the custom logger flag a soft-fail?
                    if yt_dlpLogger.should_retry:
                        yt_dlpLogger.reset_retry_state()
                        current_try += 1
                        _handle_retry_wait(logger, yt_dlpLogger, current_try, max_retries, extraction_event)
                        continue
                    
                    # 2. Success Processing
                    info_dict = ydl.sanitize_info(info_dict=info_dict, remove_private_keys=clean_info_dict)

                    # Cleanup fragments if present
                    for stream_format in info_dict.get('formats', []):
                        try:
                            stream_format.pop('fragments', None)
                        except:
                            pass
                    
                    # Reset retry state
                    yt_dlpLogger.reset_retry_state()
                    
                    # Ensure required fields exist
                    if 'extractor' not in info_dict:
                        info_dict['extractor'] = 'youtube'
                    if 'extractor_key' not in info_dict:
                        info_dict['extractor_key'] = 'Youtube'
                    
                    # Check live status
                    if not (info_dict.get('live_status') == 'is_live' or info_dict.get('live_status') == 'post_live'):
                        raise VideoProcessedError("Video has been processed, please use yt-dlp directly")
                    
                    # Successful return
                    return info_dict, info_dict.get('live_status')
                    
                except yt_dlp.utils.DownloadError as e:
                    extraction_event.clear()
                    err_str = str(e).lower()
                    
                    # Specific Error Handling
                    if 'video is private' in err_str or "sign in" in err_str:
                        raise VideoInaccessibleError(f"Video {id} is private")
                    elif 'will begin in' in err_str or 'premieres in' in err_str:
                        raise VideoUnavailableError("Video is not yet available. Consider using waiting option")
                    elif "members" in err_str:
                        raise VideoInaccessibleError(f"Video {id} is a membership video. Requires valid cookies")
                    elif "not available on this app" in err_str:
                        raise VideoInaccessibleError(f"Video {id} not available on this player")
                    elif "no longer live" in err_str:
                        raise LivestreamError("Livestream has ended")
                    
                    # Retry Condition for "should already be available"
                    elif "should already be available" in err_str:
                        current_try += 1
                        _handle_retry_wait(logger, yt_dlpLogger, current_try, max_retries, extraction_event)
                        continue
                    else:
                        raise e
                except Exception as e:
                    extraction_event.clear()
                    # Other exceptions are raised directly
                    raise e
                finally:
                    # Ensure extraction_event is cleared in any case
                    extraction_event.clear()
                    
        except yt_dlp.utils.DownloadError as e:
            # If it's a DownloadError, also check if retry is needed
            if "should already be available" in str(e).lower():
                current_try += 1
                _handle_retry_wait(logger, yt_dlpLogger, current_try, max_retries, extraction_event)
                continue
            else:
                raise e
        except MaxRetryExceededError:
            raise
        except (VideoInaccessibleError, VideoProcessedError, VideoUnavailableError, LivestreamError) as e:
            # These are our custom errors, raise directly
            raise e
        except Exception as e:
            # Catch-all to check if we should retry based on the logger state
            # Sometimes yt-dlp raises errors that aren't DownloadErrors directly but were caught by logger
            if hasattr(yt_dlpLogger, 'should_retry') and yt_dlpLogger.should_retry:
                current_try += 1
                yt_dlpLogger.reset_retry_state()
                _handle_retry_wait(logger, yt_dlpLogger, current_try, max_retries, extraction_event)
                continue
            
            # Other unexpected errors, log but continue retrying
            logger.exception(f"Unexpected error: {e}")
            current_try += 1
            if current_try >= max_retries:
                raise
            time.sleep(30)  # Short wait before retry
        
    logging.debug("Info.json: {0}".format(json.dumps(info_dict)))
    return info_dict, info_dict.get('live_status')
