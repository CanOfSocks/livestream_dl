#!/usr/local/bin/python
import yt_dlp
import logging
import json
import random
import time
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
        self.logger = logger
        # Retry related attributes
        self.retry_count = 0
        self.max_retries = 6
        self.base_wait = 600  # Base wait time 600 seconds
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
            # This is a livestream status warning, needs retry
            self.should_retry = True
            self.retry_message = msg_str
            self.logger.warning(f"Livestream not yet fully available, will retry: {msg_str}")
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

def get_Video_Info(id, wait=True, cookies=None, additional_options=None, proxy=None, return_format=False, sort=None, include_dash=False, include_m3u8=False, logger=logging.getLogger(), clean_info_dict: bool=False):
    #url = "https://www.youtube.com/watch?v={0}".format(id)
    url = str(id)

    yt_dlpLogger = MyLogger(logger=logger)
    
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

    if isinstance(wait, tuple):
        if not (0 < len(wait) <= 2) :
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
        #print(proxy)
        if isinstance(proxy, str):
            ydl_opts['proxy'] = proxy
        elif isinstance(proxy, dict):
            ydl_opts['proxy'] = next(iter(proxy.values()), None)

    ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).update({"formats": ["live_adaptive","incomplete","duplicate"]})
    #ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).update({"formats": []})
    if not include_dash:
        (ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).setdefault("skip", [])).append("dash")
    if not include_m3u8:
        (ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {}).setdefault("skip", [])).append("hls")

    info_dict = {}
    
    # Retry logic
    max_retries = 6
    retry_count = 0
    
    while retry_count < max_retries:
        # Set ydl to None at the beginning of each iteration
        ydl = None
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                try:
                    extraction_event.set()
                    info_dict = ydl.extract_info(url, download=False)
                    extraction_event.clear()
                    
                    # Check if retry is needed
                    if yt_dlpLogger.should_retry:
                        yt_dlpLogger.should_retry = False
                        wait_time = yt_dlpLogger.get_wait_time()
                        retry_count += 1
                        
                        if retry_count >= max_retries:
                            error_msg = f"[Livestream offline status please check] Maximum retry attempts {max_retries} exceeded, unable to get video information"
                            logger.error(error_msg)
                            raise MaxRetryExceededError(error_msg)
                        
                        logger.warning(f"Detected warning requiring retry, waiting {wait_time:.2f} seconds for retry attempt {retry_count}/{max_retries}")
                        
                        # Segmented waiting, can be interrupted by extraction_event
                        end_time = time.time() + wait_time
                        while time.time() < end_time:
                            if extraction_event.is_set():
                                logger.warning("extraction_event was set, interrupting wait")
                                break
                            time.sleep(1)
                        continue  # Continue to next retry
                    
                    # Successfully obtained information, proceed with processing
                    info_dict = ydl.sanitize_info(info_dict=info_dict, remove_private_keys=clean_info_dict)

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
                    
                    # Check if the video is private
                    if not (info_dict.get('live_status') == 'is_live' or info_dict.get('live_status') == 'post_live'):
                        #print("Video has been processed, please use yt-dlp directly")
                        raise VideoProcessedError("Video has been processed, please use yt-dlp directly")
                    
                    # Successful return
                    return info_dict, info_dict.get('live_status')
                    
                except yt_dlp.utils.DownloadError as e:
                    extraction_event.clear()
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
                    elif "should already be available" in str(e):
                        # Handle "should already be available" error with retry
                        wait_time = yt_dlpLogger.get_wait_time()
                        retry_count += 1
                        
                        if retry_count >= max_retries:
                            error_msg = f"[Livestream offline status please check] Maximum retry attempts {max_retries} exceeded, unable to get video information"
                            logger.error(error_msg)
                            raise MaxRetryExceededError(error_msg)
                        
                        logger.warning(f"Livestream not yet fully available, waiting {wait_time:.2f} seconds for retry attempt {retry_count}/{max_retries}")
                        
                        # Segmented waiting, can be interrupted by extraction_event
                        end_time = time.time() + wait_time
                        while time.time() < end_time:
                            if extraction_event.is_set():
                                logger.warning("extraction_event was set, interrupting wait")
                                break
                            time.sleep(1)
                        continue  # Continue to next retry
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
                wait_time = yt_dlpLogger.get_wait_time()
                retry_count += 1
                
                if retry_count >= max_retries:
                    error_msg = f"[Livestream offline status please check] Maximum retry attempts {max_retries} exceeded, unable to get video information"
                    logger.error(error_msg)
                    raise MaxRetryExceededError(error_msg)
                
                logger.warning(f"Livestream not yet fully available, waiting {wait_time:.2f} seconds for retry attempt {retry_count}/{max_retries}")
                
                # Segmented waiting, can be interrupted by extraction_event
                end_time = time.time() + wait_time
                while time.time() < end_time:
                    if extraction_event.is_set():
                        logger.warning("extraction_event was set, interrupting wait")
                        break
                    time.sleep(1)
                continue
            else:
                raise e
        except (MaxRetryExceededError, VideoInaccessibleError, VideoProcessedError, VideoUnavailableError, LivestreamError) as e:
            # These are our custom errors, raise directly
            raise e
        except Exception as e:
            # Other unexpected errors, log but continue retrying
            logger.exception(f"Unexpected error: {e}")
            retry_count += 1
            if retry_count >= max_retries:
                raise
            time.sleep(30)  # Short wait before retry
        
    logging.debug("Info.json: {0}".format(json.dumps(info_dict)))
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
