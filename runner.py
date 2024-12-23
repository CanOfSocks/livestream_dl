import argparse
try:
    import getUrls
    import download_Live
except ModuleNotFoundError as e:
    from . import getUrls
    from . import download_Live
import ast

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
    
    
    if options.get('json_file', None) is not None:
        import json
        with open(options.get('json_file'), 'r', encoding='utf-8') as file:
            info_dict = json.load(file)
    elif info_dict:
        pass
    else:
        info_dict, live_status = getUrls.get_Video_Info(id, cookies=options.get("cookies", None))
    download_Live.download_segments(info_dict, resolution, options, logger)
    
if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Download YouTube livestreams (https://github.com/CanOfSocks/livestream_dl)")

    parser.add_argument('ID', type=str, nargs='?', default=None, help='The video URL or ID')
    
    parser.add_argument('--resolution', type=str, default=None, dest='resolution', help="""Desired resolution. Can be best, audio_only or specific resolution.
                        Possible values best, 2160p60, 2160p, 1440p60, 1440p, 1080p60, premium, 1080p, 720p60, 720p, 480p, 360p, 240p, 144p.
                        '*' can be used as a wildcard e.g. 1080* for 1080p60 or 1080p.
                        Default: best""")
    
    parser.add_argument('--video-format', type=int, help="Specify specific video format. Resolution will be ignored if used")
    
    parser.add_argument('--audio-format', type=int, help="Specify specific audio format. Resolution will be ignored if used")
    
    parser.add_argument('--threads', type=int, default=1, help="Number of download threads per format. This will be 2x for an video and audio download. Default: 1")
    
    parser.add_argument('--batch-size', type=int, default=5, help="Number of segments before the temporary database is committed to disk. This is useful for reducing disk access instances. Default: 5")
    
    parser.add_argument('--segment-retries', type=int, default=10, help="Number of times to retry grabbing a segment. Default: 10")
    
    parser.add_argument('--no-merge', action='store_false', dest='merge', help="Don't merge video using ffmpeg")

    parser.add_argument('--merge', action='store_true', dest='merge', help="Merge video using ffmpeg, overrides --no-merge")
    
    parser.add_argument('--cookies', type=str, default=None, help="Path to cookies file")
    
    parser.add_argument('--output', type=str, default="%(fulltitle)s (%(id)s)", help="Path/file name for output files. Supports yt-dlp output formatting")

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
    
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level. Default is INFO.")
    
    parser.add_argument("--no-console", action="store_false", help="Do not log messages to the console.")
    
    parser.add_argument("--log-file", type=str, help="Path to the log file where messages will be saved.")

    # Parse the arguments
    args = parser.parse_args()
    

    # Access the 'ID' value
    options = vars(args)
    if options.get('ID', None) is None and options.get('json_file', None) is None:
        options['ID'] = str(input("Please enter a video URL: ")).strip()

    if options.get('resolution', None) is None and (options.get('video_format', None) is None or options.get('audio_format', None) is None):
        options['resolution'] = str(input("Please enter resolution: ")).strip()
               
    if options.get('wait_for_video', None) is not None:        
        options['wait_for_video'] = tuple(options.get('wait_for_video')[:2])
        
    id = options.get('ID')
    resolution = options.get('resolution')
    
    # For testing
    
    #options['batch_size'] = 5
    #options['write_thumbnail'] = True
    #options['write_description'] = True
    #options['write_info_json'] = True
    
    
    main(id=id, resolution=resolution, options=options)