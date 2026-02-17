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
    from setup_logger import VERBOSE_LEVEL_NUM
except ModuleNotFoundError:
    # Define fallback if module is missing
    VERBOSE_LEVEL_NUM = 15

extraction_event = threading.Event()
# Add global lock to prevent concurrency conflicts
refresh_lock = threading.Lock()

class MyLogger:
    def __init__(self, logger: logging.Logger, max_retries: int = 10, base_wait: int = 100):
        self.logger = logger
        self.retry_count = 0
        self.max_retries = max_retries
        self.base_wait = base_wait
        self.should_retry = False
        self.retry_message = None
        self.current_backoff = 1.0  # Initial backoff factor
        self.first_warning = True  # Flag for first warning

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
            # Only output "waiting for live stream" on first warning encounter
            if self.first_warning:
                self.logger.info(f"Waiting for live stream")
                self.first_warning = False
            # Reset backoff factor to use new jitter value on next retry
            self.current_backoff = 1.0
        else:
            self.logger.log(VERBOSE_LEVEL_NUM, msg)

    def warning(self, msg):
        msg_str = str(msg).lower()       

        # --- RETRYABLE TECHNICAL ERRORS ---
        # These occur when a stream is starting but CDN isn't ready
        if ("private" in msg_str or "unavailable" in msg_str):
            self.logger.info(msg_str)
            raise yt_dlp.utils.DownloadError("Private video. Sign in if you've been granted access to this video")
        elif "video is no longer live" in msg_str:
            self.logger.info(msg_str)
            raise yt_dlp.utils.DownloadError("Video is no longer live")
        elif "this live event will begin in" in msg_str or "premieres in" in msg_str:
            self.logger.info(msg)
        elif "not available on this app" in msg_str:
            self.logger.error(msg)
            raise yt_dlp.utils.DownloadError(msg_str)
        elif "should already be available" in msg_str or "release time of video is not known" in msg_str:
            self.should_retry = True
            self.retry_message = msg_str
            # Only output "waiting for live stream" on first warning encounter
            if self.first_warning:
                self.logger.info(f"Waiting for live stream")
                self.first_warning = False
            # Reset backoff factor
            self.current_backoff = 1.0
        else:
            self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)
        
    def reset_retry_state(self):
        self.should_retry = False
        self.retry_message = None
        # Reset first warning flag
        self.first_warning = True
        
    def get_wait_time(self):
        """
        Calculate wait time using exponential backoff strategy
        Base wait: 100 seconds
        Jitter factors: 0% (1.0), 40% (1.4), 80% (1.8), 160% (2.6), 320% (4.2), 640% (6.4)
        Minimum wait time per retry: 100 seconds
        Random jitter: ±10% (starting from the second retry)
        """
        # Define incremental jitter factors (1 + jitter percentage)
        jitter_factors = [1.0, 1.4, 1.8, 2.6, 4.2, 6.4]
        
        # Select the corresponding jitter factor based on retry count
        # If retry count exceeds the list length, use the last factor
        if self.retry_count < len(jitter_factors):
            jitter_factor = jitter_factors[self.retry_count]
        else:
            jitter_factor = jitter_factors[-1]  # Use maximum jitter 6.4 (640%)
        
        # Random jitter ±10% to ensure not all threads retry simultaneously
        if self.retry_count > 0:
            random_jitter = random.uniform(-0.1, 0.1)  # Change to ±10%
            jitter_factor *= (1 + random_jitter)
        
        # Ensure minimum 100 seconds
        wait_time = max(self.base_wait, self.base_wait * jitter_factor)
        
        return wait_time

# --- Custom Exceptions ---
class VideoInaccessibleError(PermissionError): pass
class VideoProcessedError(ValueError): pass
class VideoUnavailableError(ValueError): pass
class LivestreamError(TypeError): pass
class MaxRetryExceededError(Exception): pass
class StreamTimeoutError(TimeoutError): pass

def parse_wait(string) -> Tuple[int, Optional[int]]:
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

def _handle_retry_wait(logger, yt_dlpLogger, current_try, max_retries, extraction_event, start_time=None):
    """
    Handle retry wait logic with timeout check
    Completely silent mode: only output error messages on timeout or when max retries reached
    """
    wait_time = yt_dlpLogger.get_wait_time()
    
    # Check timeout (1 hour = 3600 seconds)
    if start_time and (time.time() - start_time) > 3600:
        error_msg = "Live stream offline, please check"
        logger.error(error_msg)
        raise StreamTimeoutError(error_msg)
    
    if current_try >= max_retries:
        error_msg = "Live stream offline, please check"
        logger.error(error_msg)
        raise MaxRetryExceededError(error_msg)
    
    # Update retry count
    yt_dlpLogger.retry_count = current_try + 1
    
    # Completely silent wait, output nothing
    end_time = time.time() + wait_time
    while time.time() < end_time:
        if extraction_event.is_set():
            # Only output message when interrupted
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
    max_retries: int = 10,  # Changed to 10 attempts
    **kwargs
):
    
    # Setup Logger
    if logger is None:
        logger = logging.getLogger()
        
    url = str(id)
    
    # Initialize custom logger, set max retries to 10, base wait 100 seconds
    yt_dlpLogger = MyLogger(logger=logger, max_retries=max_retries, base_wait=100)  # Changed to 100 seconds
    
    # Record start time for timeout check
    start_time = time.time()
    
    # Base Options
    ydl_opts = {
        'retries': 25, # Socket retries
        'skip_download': True,
        'cookiefile': cookies,
        'writesubtitles': True,
        'subtitlesformat': 'json',
        'subtitleslangs': ['live_chat'],
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
    if proxy:
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

    # Use lock to ensure retries don't conflict with other threads
    with refresh_lock:
        while current_try < max_retries:
            try:
                # Reset retry state
                yt_dlpLogger.reset_retry_state()
                
                # If retried before, wait first
                if current_try > 0:
                    _handle_retry_wait(logger, yt_dlpLogger, current_try, max_retries, extraction_event, start_time)
                
                extraction_event.set()
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info_dict = ydl.extract_info(url, download=False)
                    
                extraction_event.clear()
                
                # Check if retry needed
                if yt_dlpLogger.should_retry:
                    current_try += 1
                    continue
                
                # Successfully retrieved information
                info_dict = ydl.sanitize_info(info_dict=info_dict, remove_private_keys=clean_info_dict)

                # Clean fragments
                for stream_format in info_dict.get('formats', []):
                    stream_format.pop('fragments', None)
                
                # Check live status
                live_status = info_dict.get('live_status')
                if live_status not in ['is_live', 'post_live']:
                    raise VideoProcessedError("Video has been processed, please use yt-dlp directly")
                
                return info_dict, live_status
                
            except yt_dlp.utils.DownloadError as e:
                extraction_event.clear()
                err_str = str(e).lower()
                
                # Specific error handling
                if 'video is private' in err_str or "sign in" in err_str:
                    raise VideoInaccessibleError(f"Video {id} is private")
                elif 'will begin in' in err_str or 'premieres in' in err_str:
                    raise VideoUnavailableError("Video is not yet available")
                elif "members" in err_str:
                    raise VideoInaccessibleError(f"Video {id} is a membership video")
                elif "not available on this app" in err_str:
                    raise VideoInaccessibleError(f"Video {id} not available on this player")
                elif "no longer live" in err_str:
                    raise LivestreamError("Livestream has ended")   
                elif "terminated" in err_str:
                    raise VideoInaccessibleError(f"Video {id} has been terminated")
                elif "country" in err_str and ("not available" in err_str or "uploader has not made" in err_str):
                    raise VideoInaccessibleError("Video is region-locked (Geo-restricted)")                
                elif "sign in to confirm your age" in err_str or "age-restricted" in err_str:
                    raise VideoInaccessibleError("Video is age-restricted and requires valid cookies")
                elif "video has been removed" in err_str:
                    raise VideoUnavailableError("Video has been removed/deleted")
                else:
                    # Check if retry needed
                    if yt_dlpLogger.should_retry:
                        current_try += 1
                        continue
                    
                    # If not a retryable error, raise directly
                    logger.error(f"Unhandled DownloadError: {e}")
                    raise e
                    
            except (VideoInaccessibleError, VideoUnavailableError, LivestreamError) as e:
                # These errors should not be retried
                extraction_event.clear()
                raise
                
            except Exception as e:
                extraction_event.clear()
                logger.exception(f"Unexpected error during info extraction: {e}")
                current_try += 1
                if current_try >= max_retries:
                    raise MaxRetryExceededError("Live stream offline, please check")
                
        # If loop ends without returning, max retries reached
        raise MaxRetryExceededError("Live stream offline, please check")
