import socket
import argparse
try:
    import getUrls
    import download_Live
except ModuleNotFoundError as e:
    from . import getUrls
    from . import download_Live
import ast
import json

import threading
kill_all = threading.Event()

import signal
from time import sleep, time
import platform

# Preserve original keyboard interrupt logic as true behaviour is known
original_sigint = signal.getsignal(signal.SIGINT)

def handle_shutdown(signum, frame):
    if not getUrls.extraction_event.is_set():
        kill_all.set()
    sleep(0.5)
    if callable(original_sigint):
        original_sigint(signum, frame)

# common
signal.signal(signal.SIGINT, handle_shutdown)

if platform.system() == "Windows":
    # SIGTERM won’t fire — but SIGBREAK will on Ctrl-Break
    signal.signal(signal.SIGBREAK, handle_shutdown)
else:
    # normal POSIX termination
    signal.signal(signal.SIGTERM, handle_shutdown)

_original_getaddrinfo = socket.getaddrinfo

def force_ipv4():
    """Modify getaddrinfo to use only IPv4."""
    def ipv4_getaddrinfo(host, port, family=socket.AF_INET, *args, **kwargs):
        return _original_getaddrinfo(host, port, socket.AF_INET, *args, **kwargs)
    
    socket.getaddrinfo = ipv4_getaddrinfo

def force_ipv6():
    """Modify getaddrinfo to use only IPv6."""
    def ipv6_getaddrinfo(host, port, family=socket.AF_INET6, *args, **kwargs):
        return _original_getaddrinfo(host, port, socket.AF_INET6, *args, **kwargs)
    
    socket.getaddrinfo = ipv6_getaddrinfo

def process_proxies(proxy_string):
    
    if proxy_string is None:
        return None
    print(proxy_string)
    if proxy_string == "":
        return {
            "http": None, 
            "https": None
        }
    
    proxy_string = str(proxy_string)
    if proxy_string.startswith('{'):
        return json.loads(proxy_string)
    
    from urllib.parse import urlparse
    parsed = urlparse(proxy_string)
    
    # Extract components
    scheme = parsed.scheme  # socks5
    username = parsed.username  # user
    password = parsed.password  # pass
    hostname = parsed.hostname  # 127.0.0.1
    port = parsed.port  # 1080
    
    auth = f"{username}:{password}@" if username and password else ""
    
    # Adjust scheme for SOCKS
    if scheme.startswith("socks") and not scheme.startswith("socks5h"):
        scheme += "h"  # Ensure DNS resolution via proxy
        
    # Construct final proxy string
    proxy_address = f"{scheme}://{auth}{hostname}:{port}"
    
    return {
        "https": proxy_address,
        "http": proxy_address,        
    }
    

def parse_string_or_tuple(value):
    try:
        # Attempt to parse as a tuple using `ast.literal_eval`
        parsed_value = ast.literal_eval(value)
        # If parsed_value is not a tuple, keep it as a string
        if isinstance(parsed_value, tuple):
            return parsed_value
        else:
            return value
    except (ValueError, SyntaxError):
        # If parsing fails, treat it as a string
        return value
    


def main(id, resolution='best', options={}, info_dict=None, thread_kill: threading.Event=kill_all):
    logger = download_Live.setup_logging(log_level=options.get('log_level', "INFO"), console=options.get('no_console', True), file=options.get('log_file', None), logger_name="Live-DL Downloader", video_id=id)

    # Initialise yt-dlp logger
    #download_Live.setup_logging(log_level=options.get('ytdlp_log_level', logger.getEffectiveLevel()), console=(not options.get('no_console', False)), file=options.get('log_file', None), file_options=options.get("log_file_options",{}), logger_name="yt-dlp", video_id=options.get("ID"), metadata={"log_type", "default"})
    
    # Convert additional options to dictionary, if it exists
    if options.get('ytdlp_options', None) is not None:        
        print("JSON for ytdlp_options: {0}".format(options.get('ytdlp_options', None)))
        options['ytdlp_options'] = json.loads(options.get('ytdlp_options'))
    else:
        options['ytdlp_options'] = {}
    
    if options.get('json_file', None) is not None:
        with open(options.get('json_file'), 'r', encoding='utf-8') as file:
            info_dict = json.load(file)
    elif info_dict:
        pass
    else:        
        info_dict, live_status = getUrls.get_Video_Info(id, cookies=options.get("cookies", None), additional_options=options.get('ytdlp_options', None), proxy=options.get('proxy', None), include_dash=options.get("dash", False), wait=options.get("wait_for_video", False), include_m3u8=(options.get("m3u8", False) or options.get("force_m3u8", False)), logger=logger, clean_info_dict=options.get('clean_info_json', False))
    downloader = download_Live.LiveStreamDownloader(kill_all=kill_all, logger=logger)
    downloader.download_segments(info_dict=info_dict, resolution=resolution, options=options, thread_event=thread_kill)

def monitor_channel(options={}):
    import copy
    logger = download_Live.setup_logging(log_level=options.get('log_level', "INFO"), console=options.get('no_console', True), file=options.get('log_file', None), logger_name="Monitor")
    #download_Live.setup_logging(log_level=options.get('ytdlp_log_level', logger.getEffectiveLevel()), console=(not options.get('no_console', False)), file=options.get('log_file', None), file_options=options.get("log_file_options",{}), logger_name="yt-dlp", video_id=options.get("ID"), metadata={"log_type", "default"})
    import monitor_channel
    from typing import Dict
    threads: Dict[str, threading.Thread] = {}
    last_check = time()
    channel_id = options.get("ID")
    tab = "membership" if options.get("members_only", False) else "streams"
    if not options.get("wait_for_video", None):
        options["wait_for_video"] = (60, None)
    wait = max((num for num in options.get("wait_for_video", []) if isinstance(num, (int, float))), default=60)
    logger.debug("Starting runner for channel: '{0}' on tab: '{1}'".format(channel_id, tab))
    while True:
        for id, thread in list(threads.items()):
            if not thread.is_alive():
                threads.pop(id)
        logger.debug("Searching for streams for channel {0}".format(channel_id))
        try:
            videos_to_get = monitor_channel.get_upcoming_or_live_videos(channel_id=channel_id, tab=tab, options=options, logger=logger)
            for video_id in videos_to_get:
                if threads.get(video_id, None) is not None:
                    continue
                video_options = copy.deepcopy(options)
                t = threading.Thread(
                    target=main,
                    args=(video_id,),
                    kwargs={
                        'resolution': video_options.get("resolution"),
                        'options': video_options,
                        'thread_kill': kill_all
                    },
                    daemon=True
                )
                t.start()
                threads[video_id] = t  # store the thread in a dictionary
        except Exception as e:
            logger.exception("An error occurred fetching upcoming streams")
        time_to_next = wait-(time()-last_check)
        logger.debug("Active threads: {0}".format(list(threads.keys())))
        logger.debug("Sleeping for {0:.2f}s for next stream check".format(time_to_next))
        sleep(time_to_next)
        last_check=time()

import argparse

class VerboseHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """
    Formatter that:
    1. Skips '(default: None)'
    2. Appends '(type: name)' if a type is defined.
    """
    def _get_help_string(self, action):
        help_text = action.help or ""

        # 1. Handle the Type info
        if action.type and hasattr(action.type, '__name__'):
            type_name = action.type.__name__
            help_text += f" (type: {type_name})"
        
        # 2. Handle the Default info (Logic from ArgumentDefaultsHelpFormatter)
        # We only append the default if it's NOT None and NOT suppressed
        if action.default is not argparse.SUPPRESS and action.default is not None:
            # We manually mimic the ArgumentDefaultsHelpFormatter logic here
            # to ensure the formatting stays consistent.
            if '%(default)' not in help_text:
                help_text += ' (default: %(default)s)'
                
        return help_text
    
if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Download YouTube livestreams (https://github.com/CanOfSocks/livestream_dl)", formatter_class=VerboseHelpFormatter)

    parser.add_argument('ID', type=str, nargs='?', default=None, help='The video URL or ID')
    
    parser.add_argument('--resolution', type=str, default=None, dest='resolution', help="""Desired resolution. Can be best, audio_only or a custom filter based off yt-dlp's format filtering: https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#filtering-formats.
                        Audio will always be set as "ba" (best audio) regardless of filters set. "best" will be converted to "bv"
                        A prompt will be displayed if no value is entered""")
    
    parser.add_argument('--custom-sort', type=str, default=None, help="Custom sorting algorithm for formats based off yt-dlp's format sorting: https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#sorting-formats")
    
    parser.add_argument('--video-format', type=str, default=None, help="Specify specific video format (string). Resolution will be ignore for video if used")
    
    parser.add_argument('--audio-format', type=str, default=None, help="Specify specific audio format (string). Resolution will be ignored for audio if used")
    
    parser.add_argument('--threads', type=int, default=2, help="Number of download threads per format. This will be 2x for an video and audio download. Default: 2")
    
    parser.add_argument('--batch-size', type=int, default=5, help="Number of segments before the temporary database is committed to disk. This is useful for reducing disk access instances. Default: 5")
    
    parser.add_argument('--segment-retries', type=int, default=10, help="Number of times to retry grabbing a segment. Default: 10")
    
    parser.add_argument('--no-merge', action='store_false', dest='merge', help="Don't merge video using ffmpeg")

    parser.add_argument('--merge', action='store_true', dest='merge', help="Merge video using ffmpeg, overrides --no-merge")
    
    parser.add_argument('--cookies', type=str, default=None, help="Path to cookies file")
    
    parser.add_argument('--output', type=str, default="%(fulltitle)s (%(id)s)", help="Path/file name for output files. Supports yt-dlp output formatting")

    parser.add_argument('--ext', type=str, default=None, help="Force extension of video file. E.g. '.mp4'")

    parser.add_argument('--temp-folder', type=str, default=None, dest='temp_folder', help="Path for temporary files. Supports yt-dlp output formatting")    
    
    parser.add_argument('--write-thumbnail', action='store_true', help="Write thumbnail to file")
    
    parser.add_argument('--embed-thumbnail', action='store_true', help="Embed thumbnail into final file. Ignored if --no-merge is used")
    
    parser.add_argument('--write-info-json', action='store_true', help="Write info.json to file")
    
    parser.add_argument('--write-description', action='store_true', help="Write description to file")
    
    parser.add_argument('--keep-temp-files', action='store_true', help="Keep all temp files i.e. database and/or ts files")
    
    parser.add_argument('--keep-ts-files', action='store_true', help="Keep all ts files")
    
    parser.add_argument('--live-chat', action='store_true', help="Get Live chat")
    
    parser.add_argument('--keep-database-file', action='store_true', help="Keep database file. If using with --direct-to-ts, this keeps the state file")
    
    parser.add_argument('--recovery', action='store_true', help="Puts downloader into stream recovery mode")

    parser.add_argument('--force-recover-merge', action='store_true', help="Forces merging to final file even if all segements could not be recovered")

    parser.add_argument('--recovery-failure-tolerance', type=int, default=0, help="Maximum number of fragments that fail to download (exceed the retry limit) and not throw an error. May cause unexpected issues when merging to .ts file and remuxing. Default: 0")

    parser.add_argument('--wait-limit', type=int, default=0, help="Set maximum number of wait intervals for new segments. Each wait interval is ~10s (e.g. a value of 20 would be 200s). A mimimum of value of 20 is recommended. Stream URLs are refreshed every 10 intervals. A value of 0 wait until the video moves into 'was_live' or 'post_live' status. Default: 0")
    
    parser.add_argument('--database-in-memory', action='store_true', help="Keep stream segments database in memory. Requires a lot of RAM (Not recommended)")
    
    parser.add_argument('--direct-to-ts', action='store_true', help="Write directly to ts file instead of database. May use more RAM if a segment is slow to download. This overwrites most database options")
    
    parser.add_argument("--wait-for-video", type=getUrls.parse_wait, default=None, help="Wait time (int) or Minimum and maximum (min:max) interval to wait for a video")
    
    parser.add_argument('--json-file', type=str, default=None, help="Path to existing yt-dlp info.json file. Overrides ID and skips retrieving URLs")
    
    parser.add_argument('--remove-ip-from-json', action='store_true', help="Replaces IP entries in info.json with 0.0.0.0")
    
    parser.add_argument('--clean-urls', action='store_true', help="Removes stream URLs from info.json that contain potentially identifiable information. These URLs are usually useless once they have expired")

    parser.add_argument('--clean-info-json', action='store_true', help="Enables yt-dlp's 'clean-info-json' option")
    
    parser.add_argument("--log-level", type=str, 
                        choices=["DEBUG", "VERBOSE", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        default="INFO",
                        help="Set the logging level. Default is INFO. Verbose logging is a custom level that includes the INFO logs of yt-dlp.")
    
    parser.add_argument("--no-console", action="store_false", help="Do not log messages to the console.")
    
    parser.add_argument("--log-file", type=str, help="Path to the log file where messages will be saved.")

    parser.add_argument('--write-ffmpeg-command', action='store_true', help="Writes FFmpeg command to a txt file")
    
    parser.add_argument('--stats-as-json', action='store_true', help="Prints stats as a JSON formatted string. Bypasses logging and prints regardless of log level")
    
    parser.add_argument('--ytdlp-options', type=str, default=None, help="""Additional yt-dlp options as a JSON string. Overwrites any options that are already defined by other options. Available options: https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/YoutubeDL.py#L183. E.g. '{"extractor_args": {"youtube": {"player_client": ["web_creator"]}, "youtubepot-bgutilhttp":{ "base_url": ["http://10.1.1.40:4416"]}}}' if you have installed the potoken plugin""")

    parser.add_argument('--ytdlp-log-level', type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default=None, help="### NOT IMPLEMENTED ### Optional alternative log level for yt-dlp module tasks (such as video extraction or format selection). Uses main logger if not set")

    parser.add_argument('--dash', action='store_true', help="Gets any available DASH urls as a fallback to adaptive URLs. Dash URLs do not require yt-dlp modification to be used, but can't be used for stream recovery and can cause large info.json files when a stream is in the 'post_live' status")

    parser.add_argument('--m3u8', action='store_true', help="Gets any available m3u8 urls as a fallback to adaptive URLs. m3u8 URLs do not require yt-dlp modification to be used, but can't be used for stream recovery. m3u8 URLs provide both video and audio in each fragment and could allow for the amount of segment download requests to be halved")

    parser.add_argument('--force-m3u8', action='store_true', help="Forces use of m3u8 stream URLs")

    parser.add_argument('--proxy', type=str, default=None, nargs="?", help="(Requires testing) Specify proxy to use for web requests. Can be a string for a single proxy or a JSON formatted string to specify multiple methods. For multiple, refer to format https://requests.readthedocs.io/en/latest/user/advanced/#proxies. The first proxy specified will be used for yt-dlp and live chat functions.")

    ip_group = parser.add_mutually_exclusive_group()
    ip_group.add_argument("--ipv4", action="store_true", help="Force IPv4 only")
    ip_group.add_argument("--ipv6", action="store_true", help="Force IPv6 only")
    
    parser.add_argument("--stop-chat-when-done", type=int, default=300, help="Wait a maximum of X seconds after a stream is finished to download live chat. Default: 300. This is useful if waiting for chat to end causes hanging.")
    
    parser.add_argument('--new-line', action='store_true', help="Console messages always print to new line. (Currently only ensured for stats output)")

    monitor_group = parser.add_argument_group('Channel Monitor Options')

    monitor_group.add_argument('--monitor-channel', action='store_true', help="Use monitor channel feature (Alpha). Specify channel ID in 'ID' argument")

    monitor_group.add_argument('--members-only', action='store_true', help="Monitor 'Members Only' playlist for streams instead of 'Streams' playlist. Requires cookies.")

    monitor_group.add_argument('--upcoming-lookahead', type=int, default=24, help="Maximum time (in hours) to start a downloader instance for a video. Default: 24")

    monitor_group.add_argument('--playlist-items', type=int, default=50, help="Maximum number of playlist items to check. Default: 50")
    
    # Parse the arguments
    args = parser.parse_args()

    if args.ipv4:
        force_ipv4()
    elif args.ipv6:
        force_ipv6() 

    # Access the 'ID' value
    options = vars(args)
    
    if options.get('ID', None) is None and options.get('json_file', None) is None:
        options['ID'] = str(input("Please enter a video URL: ")).strip()

    if options.get('resolution', None) is None and (options.get('video_format', None) is None or options.get('audio_format', None) is None):
        options['resolution'] = str(input("Please enter resolution: ")).strip()
                      
    if options.get('proxy', None) is not None:
        options['proxy'] = process_proxies(options.get('proxy', None))
        
    id = options.get('ID')
    resolution = options.get('resolution')
    
    download_Live.setup_logging(log_level=options.get('log_level', "INFO"), console=options.get('no_console', True), file=options.get('log_file', None), force=True)
    # For testing
    
    #options['batch_size'] = 5
    #options['write_thumbnail'] = True
    #options['write_description'] = True
    #options['write_info_json'] = True
    
    if options.get("monitor_channel", False) is True:
        monitor_channel(options=options)
    else:
        main(id=id, resolution=resolution, options=options)