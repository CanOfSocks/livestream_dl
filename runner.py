import socket
import argparse
try:
    import getUrls
    import download_Live
    import YoutubeURL
except ModuleNotFoundError as e:
    from . import getUrls
    from . import download_Live
    from . import YoutubeURL
#import ast
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

def httpx_proxy(proxy_string: str):
    if not proxy_string:
        return None
    proxy_string = proxy_string.strip()
    if proxy_string.startswith('{'):
        try:
            return json.loads(proxy_string)
        except Exception as e:
            raise ValueError("Input is not valid JSON string")
    else:
        return proxy_string

def main(id, resolution='bv+ba/best', options: dict = None, info_dict=None, thread_kill: threading.Event = kill_all):
    options = options if options is not None else {}
    file_options = {}
    if options.get("log_rotate_when"):
        file_options = {
            "when": options.get("log_rotate_when"),
            "interval": options.get("log_rotate_interval", 1),
            "backupCount": options.get("log_backup_count", 30)
        }
    # Log with the bare video ID even if a URL was given
    log_id = getUrls.extract_video_id(id) or id
    logger = download_Live.setup_logging(log_level=options.get('log_level', "INFO"), console=options.get('no_console', True), file=options.get('log_file', None), logger_name="Live-DL Downloader", video_id=log_id, redact_ips=options.get("redact_ips", False), file_options=file_options)

    if options.get("cookies", None):
        from pathlib import Path
        if not Path(options.get("cookies")).exists():
            logger.warning("Cookie file does not exist. Check --cookies option")
    # Initialise yt-dlp logger
    #download_Live.setup_logging(log_level=options.get('ytdlp_log_level', logger.getEffectiveLevel()), console=(not options.get('no_console', False)), file=options.get('log_file', None), file_options=options.get("log_file_options",{}), logger_name="yt-dlp", video_id=options.get("ID"), metadata={"log_type", "default"})
    
    # Convert additional options to dictionary, if it exists (it may already be a dict when
    # called from the channel monitor, which deep copies already parsed options)
    if isinstance(options.get('ytdlp_options', None), str):
        print(f"JSON for ytdlp_options: {options.get('ytdlp_options', None)}")
        options['ytdlp_options'] = json.loads(options.get('ytdlp_options'))
    elif options.get('ytdlp_options', None) is None:
        options['ytdlp_options'] = {}
    
    if options.get("ytdlp_command_line_options"):
        import shlex
        options.setdefault('ytdlp_options', {}).update(getUrls.cli_to_ytdlp_options(shlex.split(options.get("ytdlp_command_line_options", ""))))

    if options.get('json_file', None) is not None:
        with open(options.get('json_file'), 'r', encoding='utf-8') as file:
            info_dict = json.load(file)
    elif info_dict:
        pass
    else:        
        info_dict, live_status = getUrls.get_Video_Info(id, cookies=options.get("cookies", None), additional_options=options.get('ytdlp_options', None), proxy=options.get('proxy', None), include_dash=options.get("dash", False), wait=options.get("wait_for_video", False), include_m3u8=(options.get("m3u8", False) or options.get("force_m3u8", False)), logger=logger, clean_info_dict=options.get('clean_info_json', False))
    downloader = download_Live.LiveStreamDownloader(kill_all=kill_all, logger=logger)
    downloader.download_segments(info_dict=info_dict, resolution=resolution, options=options, thread_event=thread_kill)

def monitor_channel(options: dict = None):
    import copy
    options = options if options is not None else {}
    file_options = {}
    if options.get("log_rotate_when"):
        file_options = {
            "when": options.get("log_rotate_when"),
            "interval": options.get("log_rotate_interval", 1),
            "backupCount": options.get("log_backup_count", 30)
        }
    logger = download_Live.setup_logging(log_level=options.get('log_level', "INFO"), console=options.get('no_console', True), file=options.get('log_file', None), logger_name="Monitor", redact_ips=options.get("redact_ips", False), file_options=file_options)
    #download_Live.setup_logging(log_level=options.get('ytdlp_log_level', logger.getEffectiveLevel()), console=(not options.get('no_console', False)), file=options.get('log_file', None), file_options=options.get("log_file_options",{}), logger_name="yt-dlp", video_id=options.get("ID"), metadata={"log_type", "default"})
    if options.get("cookies", None):
        from pathlib import Path
        if not Path(options.get("cookies")).exists():
            logger.warning("Cookie file does not exist. Check --cookies option")
    try:
        import monitor_channel
    except ModuleNotFoundError:
        from . import monitor_channel
    from typing import Dict
    threads: Dict[str, threading.Thread] = {}
    last_check = time()
    channel_id: str = options.get("ID")
    tab = "membership" if options.get("members_only", False) else "streams"
    if not options.get("wait_for_video", None):
        options["wait_for_video"] = (60, None)
    wait = max((num for num in options.get("wait_for_video", []) if isinstance(num, (int, float))), default=300)
    while not channel_id.startswith("UC"):
        new_channel_id = monitor_channel.resolve_channel(channel_id) or ""
        logger.debug(f"Found channel ID: {new_channel_id}")
        # Break if resolved and start search
        if new_channel_id.startswith("UC"):
            channel_id = new_channel_id
            logger.debug(f"Found channel ID: {channel_id}")
            break
        time_to_next = max(wait-(time()-last_check),1)
        logger.debug(f"Sleeping for {time_to_next:.2f}s for next URL resolve attempt")
        sleep(time_to_next)
        last_check=time()

    logger.debug(f"Starting runner for channel: '{channel_id}' on tab: '{tab}'")
    while True:
        for id, thread in list(threads.items()):
            if not thread.is_alive():
                threads.pop(id)
        logger.debug(f"Searching for streams for channel {channel_id}")
        try:
            # Videos already being downloaded are skipped so they aren't extracted again
            videos_to_get = monitor_channel.get_upcoming_or_live_videos(channel_id=channel_id, tab=tab, options=options, logger=logger, skip_ids=threads.keys())
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
        time_to_next = max(wait-(time()-last_check),1)
        logger.debug(f"Active threads: {list(threads.keys())}")
        logger.debug(f"Sleeping for {time_to_next:.2f}s for next stream check")
        sleep(time_to_next)
        last_check=time()

def parse_time_to_seconds(time_str):
    """
    Parse [-]hh:mm:ss, [-]mm:ss or [-]ss into seconds.
    A leading '-' means "before the live edge" (e.g. -10:00 is 10 minutes before the live edge
    at the time recording starts) and is returned as a negative number.
    """
    if not time_str:
        return None
    time_str = str(time_str).strip()
    negative = time_str.startswith('-')
    if negative:
        time_str = time_str[1:]
    try:
        parts = time_str.split(':')
        if not parts or len(parts) > 3 or any(part.strip() == "" or int(part) < 0 for part in parts):
            raise ValueError
        parts.reverse()
        seconds = sum(int(part) * (60 ** i) for i, part in enumerate(parts))
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid time format: '{time_str}'. Expected format is [-]hh:mm:ss, [-]mm:ss, or [-]ss.")
    # Relative times are returned as negative floats so that "-00:00" (exactly the live edge) keeps
    # its sign as -0.0 (see download_Live.is_relative_time)
    return -float(seconds) if negative else seconds


# Options whose values may legitimately start with '-' (negative times)
_NEGATIVE_VALUE_OPTIONS = ("--start-time", "--end-time")


def join_negative_option_values(argv: list[str]) -> list[str]:
    """
    argparse treats a value such as '-10:00' as an unknown option. Rewrite
    ['--start-time', '-10:00'] into ['--start-time=-10:00'] so it parses as a value.
    """
    import re
    result: list[str] = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in _NEGATIVE_VALUE_OPTIONS and i + 1 < len(argv) and re.match(r'^-\d', argv[i + 1]):
            result.append(f"{arg}={argv[i + 1]}")
            i += 2
            continue
        result.append(arg)
        i += 1
    return result


def validate_time_window(start_time, end_time) -> None:
    """End must be after start. Mixed signs can only be checked once the live edge is known."""
    if start_time is None or end_time is None:
        return
    if download_Live.is_relative_time(start_time) == download_Live.is_relative_time(end_time) and end_time <= start_time:
        raise argparse.ArgumentTypeError("--end-time must be after --start-time")



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
    
    parser.add_argument('--resolution', type=str, default=None, dest='resolution', help="""Resolution selection criteria. Based off yt-dlp's format selection: https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#format-selection. A value of "bv+ba/best" is recommended for most people.""")
    
    parser.add_argument('--custom-sort', type=str, default=None, help="Custom sorting algorithm for formats based off yt-dlp's format sorting: https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#sorting-formats")
    
    parser.add_argument('--threads', type=int, default=2, help="Number of download threads per format. This will be 2x for an video and audio download.")
    
    parser.add_argument('--batch-size', type=int, default=5, help="Number of segments before the temporary database is committed to disk. This is useful for reducing disk access instances.")
    
    parser.add_argument('--segment-retries', type=int, default=10, help="Number of times to retry grabbing a segment.")

    parser.add_argument('--disable-graceful-shutdown', action='store_true', help="Disable graceful shutdown for downloader. Useful for testing when you don't want merging to be triggered on a keyboard interrupt.")
    
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

    parser.add_argument('--recovery-failure-tolerance', type=int, default=0, help="Maximum number of fragments that fail to download (exceed the retry limit) and not throw an error. May cause unexpected issues when merging to .ts file and remuxing.")

    parser.add_argument('--wait-limit', type=int, default=1800, help="Set maximum number of wait intervals for new segments. Each wait interval is ~10s (e.g. a value of 20 would be 200s). A mimimum of value of 20 is recommended. Stream URLs are refreshed every 10 intervals. A value of 0 wait until the video moves into 'was_live' or 'post_live' status.")
    
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

    parser.add_argument("--log-rotate-when", type=str, default=None, help="Type of interval for log rotation (S, M, H, D, midnight, W0-W6). If not set, rotation is disabled.")
    parser.add_argument("--log-rotate-interval", type=int, default=1, help="Interval for log rotation. Default: 1")
    parser.add_argument("--log-backup-count", type=int, default=30, help="Number of rotated log files to keep. Default: 30")

    parser.add_argument('--redact-ips', action='store_true', help="Remove IP addresses from logs. Warning: May be imperfect, so check logs to make sure IP information has been redacted.")

    parser.add_argument('--write-ffmpeg-command', action='store_true', help="Writes FFmpeg command to a txt file")
    
    parser.add_argument('--stats-as-json', action='store_true', help="Prints stats as a JSON formatted string. Bypasses logging and prints regardless of log level")
    
    parser.add_argument('--ytdlp-options', type=str, default=None, help="""Additional yt-dlp options as a JSON string. Overwrites any options that are already defined by other options. Available options: https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/YoutubeDL.py#L183. E.g. '{"extractor_args": {"youtube": {"player_client": ["web_creator"]}, "youtubepot-bgutilhttp":{ "base_url": ["http://10.1.1.40:4416"]}}}' if you have installed the potoken plugin""")

    parser.add_argument('--ytdlp-log-level', type=str, choices=["DEBUG", "VERBOSE", "INFO", "WARNING", "ERROR", "CRITICAL"], default=None, help="### NOT IMPLEMENTED ### Optional alternative log level for yt-dlp module tasks (such as video extraction or format selection). Uses main logger if not set")

    parser.add_argument('--dash', action='store_true', help="Gets any available DASH urls as a fallback to adaptive URLs. Dash URLs do not require yt-dlp modification to be used, but can't be used for stream recovery and can cause large info.json files when a stream is in the 'post_live' status")

    parser.add_argument('--m3u8', action='store_true', help="Gets any available m3u8 urls as a fallback to adaptive URLs. m3u8 URLs do not require yt-dlp modification to be used, but can't be used for stream recovery. m3u8 URLs provide both video and audio in each fragment and could allow for the amount of segment download requests to be halved")

    parser.add_argument('--force-m3u8', action='store_true', help="Forces use of m3u8 stream URLs")

    parser.add_argument('--proxy', type=str, default=None, help="(ALPHA) Specify proxy to use for web requests. Can be a string for a single proxy or a JSON formatted string to specify multiple methods. For multiple, refer to format https://www.python-httpx.org/advanced/proxies. The first proxy specified will be used for yt-dlp and live chat functions. Not all functions have proxy compatibility enabled at this time.")

    ip_group = parser.add_mutually_exclusive_group()
    ip_group.add_argument("--ipv4", action="store_true", help="Force IPv4 only")
    ip_group.add_argument("--ipv6", action="store_true", help="Force IPv6 only")
    
    parser.add_argument("--stop-chat-when-done", type=int, default=300, help="Wait a maximum of X seconds after a stream is finished to download live chat. This is useful if waiting for chat to end causes hanging. The chat downloaded so far is kept.")
    
    parser.add_argument('--new-line', action='store_true', help="Console messages always print to new line. (Currently only ensured for stats output)")

    parser.add_argument('--ytdlp-command-line-options', type=str, default=None, help='Pass yt-dlp command line options. This must be passed as a single string, recommended --ytdlp-command-line-options="options..." e.g. --ytdlp-command-line-options="--cookies-from-browser firefox" . Only currently works for yt-dlp extractor. Requires https://github.com/yt-dlp/yt-dlp/blob/master/devscripts/cli_to_api.py to be available. (EXPERIMENTAL)')

    parser.add_argument('--start-time', type=parse_time_to_seconds, default=None, help="Start time of the recording in hh:mm:ss format (based on stream duration). A negative value (e.g. -10:00) is relative to the live edge when recording starts, i.e. 10 minutes ago (EXPERIMENTAL).")

    parser.add_argument('--end-time', type=parse_time_to_seconds, default=None, help="End time of the recording in hh:mm:ss format (based on stream duration). A negative value (e.g. -09:00) is relative to the live edge when recording starts (EXPERIMENTAL).")

    parser.add_argument('--url-refresh-interval', type=int, default=None, help="Fixed interval in seconds to refresh stream URLs with yt-dlp. By default URLs are refreshed shortly before they expire (~30 minutes before expiry), which minimises yt-dlp requests.")

    parser.add_argument('--url-refresh-jitter', type=int, default=300, help="Maximum random number of seconds to bring each scheduled URL refresh forward, so that many downloads started together don't refresh at the same time.")

    parser.add_argument('--temp-db-journal', type=str.lower, choices=["wal", "truncate"], default="wal", help="SQLite journal mode for the temporary segment database. 'wal' is the most robust against crashes; 'truncate' (rollback journal) writes roughly half as much data to disk but has a small risk of corruption if the OS crashes or loses power.")

    monitor_group = parser.add_argument_group('Channel Monitor Options')

    monitor_group.add_argument('--monitor-channel', action='store_true', help="Use monitor channel feature (Alpha). Specify channel ID in 'ID' argument (e.g. UCxsZ6NCzjU_t4YSxQLBcM5A). Not using the channel ID will attempt to resolve the channel ID.")

    monitor_group.add_argument('--members-only', action='store_true', help="Monitor 'Members Only' playlist for streams instead of 'Streams' playlist. Requires cookies.")

    monitor_group.add_argument('--use-stream-tab', action='store_true', help='Use "Streams" tab instead of user upload playlist.')

    monitor_group.add_argument('--upcoming-lookahead', type=int, default=24, help="Maximum time (in hours) to start a downloader instance for a video.")

    monitor_group.add_argument('--playlist-items', type=int, default=10, help="Maximum number of playlist items to check.")
    
    # Parse the arguments
    import sys
    args = parser.parse_args(join_negative_option_values(sys.argv[1:]))
    try:
        validate_time_window(args.start_time, args.end_time)
    except argparse.ArgumentTypeError as e:
        parser.error(str(e))

    # A JSON proxy string (multiple proxies) is converted to a dict once here
    try:
        args.proxy = httpx_proxy(args.proxy)
    except ValueError as e:
        parser.error(f"--proxy: {e}")

    if args.ipv4:
        force_ipv4()
    elif args.ipv6:
        force_ipv6() 

    # Access the 'ID' value
    options: dict = vars(args)
    
    if options.get('ID', None) is None and options.get('json_file', None) is None:
        options['ID'] = str(input("Please enter a video URL: ")).strip()

    if options.get('resolution', None) is None and (options.get('video_format', None) is None or options.get('audio_format', None) is None):
        resolution_input = str(input("Please enter resolution (default: bv+ba/best): ")).strip()
        options['resolution'] = resolution_input or "bv+ba/best"
        
    id = options.get('ID')
    resolution = options.get('resolution')

    alias: dict = YoutubeURL.quality_aliases.get(resolution, None)
    if alias:
        options['resolution'] = alias.get("format")
        resolution = options.get('resolution')
        if alias.get("sort"):
            options['custom_sort'] = f"{options.get('custom_sort')},{alias.get("sort")}" if options.get('custom_sort') else alias.get("sort")

    file_options = {}
    if options.get("log_rotate_when"):
        file_options = {
            "when": options.get("log_rotate_when"),
            "interval": options.get("log_rotate_interval", 1),
            "backupCount": options.get("log_backup_count", 30)
        }
    download_Live.setup_logging(log_level=options.get('log_level', "INFO"), console=options.get('no_console', True), file=options.get('log_file', None), force=True, redact_ips=options.get("redact_ips", False), file_options=file_options)

    if options.get("redact_ips", False):
        import logging
        loggers = [logging.getLogger(), logging.getLogger("httpx")]
        scrubber = download_Live.setup_logger.IPAddressScrubber()
        for logger in loggers:
            if not any(isinstance(f, download_Live.setup_logger.IPAddressScrubber) for f in logger.filters):
                logger.addFilter(scrubber)
    # For testing
    
    #options['batch_size'] = 5
    #options['write_thumbnail'] = True
    #options['write_description'] = True
    #options['write_info_json'] = True

    
    
    if options.get("monitor_channel", False) is True:
        monitor_channel(options=options)
    else:
        main(id=id, resolution=resolution, options=options)