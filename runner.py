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
    


def main(id, resolution='best', options={}, info_dict=None):
    logger = download_Live.setup_logging(log_level=options.get('log_level', "INFO"), console=options.get('no_console', False), file=options.get('log_file', None))
    
    # Convert additional options to dictionary, if it exists
    if options.get('ytdlp_options', None) is not None:        
        options['ytdlp_options'] = json.loads(options.get('ytdlp_options'))
    
    if options.get('json_file', None) is not None:
        with open(options.get('json_file'), 'r', encoding='utf-8') as file:
            info_dict = json.load(file)
    elif info_dict:
        pass
    else:        
        info_dict, live_status = getUrls.get_Video_Info(id, cookies=options.get("cookies", None), additional_options=options.get('ytdlp_options', None), proxy=options.get('proxy', None))
    download_Live.download_segments(info_dict, resolution, options, logger)
    
if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Download YouTube livestreams (https://github.com/CanOfSocks/livestream_dl)")

    parser.add_argument('ID', type=str, nargs='?', default=None, help='The video URL or ID')
    
    parser.add_argument('--resolution', type=str, default=None, dest='resolution', help="""Desired resolution. Can be best, audio_only or a custom filter based off yt-dlp's format filtering: https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#filtering-formats.
                        Audio will always be set as "ba" (best audio) regardless of filters set. "best" will be converted to "bv"
                        A prompt will be displayed if no value is entered""")
    
    parser.add_argument('--custom-sort', type=str, default=None, help="Custom sorting algorithm for formats based off yt-dlp's format sorting: https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#sorting-formats")
    
    #parser.add_argument('--video-format', type=int, help="Specify specific video format. Resolution will be ignored if used")
    
    #parser.add_argument('--audio-format', type=int, help="Specify specific audio format. Resolution will be ignored if used")
    
    parser.add_argument('--threads', type=int, default=1, help="Number of download threads per format. This will be 2x for an video and audio download. Default: 1")
    
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
    
    parser.add_argument('--database-in-memory', action='store_true', help="Keep stream segments database in memory. Requires a lot of RAM (Not recommended)")
    
    parser.add_argument('--direct-to-ts', action='store_true', help="Write directly to ts file instead of database. May use more RAM if a segment is slow to download. This overwrites most database options")
    
    parser.add_argument("--wait-for-video", type=int, nargs="*", help="(min, max) Minimum and maximum interval to wait for a video")
    
    parser.add_argument('--json-file', type=str, default=None, help="Path to existing yt-dlp info.json file. Overrides ID and skips retrieving URLs")
    
    parser.add_argument('--remove-ip-from-json', action='store_true', help="Replaces IP entries in info.json with 0.0.0.0")
    
    parser.add_argument('--clean-urls', action='store_true', help="Removes stream URLs from info.json that contain potentially identifiable information. These URLs are usually useless once they have expired")
    
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level. Default is INFO.")
    
    parser.add_argument("--no-console", action="store_false", help="Do not log messages to the console.")
    
    parser.add_argument("--log-file", type=str, help="Path to the log file where messages will be saved.")

    parser.add_argument('--write-ffmpeg-command', action='store_true', help="Writes FFmpeg command to a txt file")
    
    parser.add_argument('--stats-as-json', action='store_true', help="Prints stats as a JSON formatted string. Bypasses logging and prints regardless of log level")
    
    parser.add_argument('--ytdlp-options', type=str, default=None, help="""Additional yt-dlp options as a JSON string. Overwrites any options that are already defined by other options. Available options: https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/YoutubeDL.py#L183. E.g. '{"extractor_args": {"youtube": {"getpot_bgutil_baseurl": ["http://10.0.67.10:4416"]}}}' if you have installed the potoken plugin""")

    parser.add_argument('--proxy', type=str, default=None, nargs="?", help="(Requires testing) Specify proxy to use for web requests. Can be a string for a single proxy or a JSON formatted string to specify multiple methods. For multiple, refer to format https://requests.readthedocs.io/en/latest/user/advanced/#proxies. The first proxy specified will be used for yt-dlp and live chat functions.")

    ip_group = parser.add_mutually_exclusive_group()
    ip_group.add_argument("--ipv4", action="store_true", help="Force IPv4 only")
    ip_group.add_argument("--ipv6", action="store_true", help="Force IPv6 only")
    
    parser.add_argument("--stop-chat-when-done", type=int, default=300, help="Wait a maximum of X seconds after a stream is finished to download live chat. Default: 300. This is useful if waiting for chat to end causes hanging.")
    
    parser.add_argument('--new-line', action='store_true', help="Console messages always print to new line. (Currently only ensured for stats output)")
    
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
               
    if options.get('wait_for_video', None) is not None:        
        options['wait_for_video'] = tuple(options.get('wait_for_video')[:2])
        
    if options.get('proxy', None) is not None:
        options['proxy'] = process_proxies(options.get('proxy', None))
        
    id = options.get('ID')
    resolution = options.get('resolution')
    
    # For testing
    
    #options['batch_size'] = 5
    #options['write_thumbnail'] = True
    #options['write_description'] = True
    #options['write_info_json'] = True
    
    
    main(id=id, resolution=resolution, options=options)