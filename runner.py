import argparse
import getUrls
import download_Live
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
    


def main(id, resolution='best', options={}):
    if options.get('json_file', None) is not None:
        import json
        with open(options.get('json_file'), 'r', encoding='utf-8') as file:
            info_dict = json.load(file)
    else:
        info_dict, live_status = getUrls.get_Video_Info(id, cookies=options.get("cookies", None))
    download_Live.download_segments(info_dict, resolution, options)
    
if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Download an video by ID")

    # Add a required positional argument 'ID'
    parser.add_argument('ID', type=str, nargs='?', default=None, help='The video URL or ID')
    
    parser.add_argument('--resolution', type=str, default=None, dest='resolution', help="Desired resolution. Can be best, audio_only or specific resolution. Default: best")
    
    parser.add_argument('--video-format', type=int, help="Specify specific video format. Resolution will be ignored if used")
    
    parser.add_argument('--audio-format', type=int, help="Specify specific audio format. Resolution will be ignored if used")
    
    parser.add_argument('--threads', type=int, default=1, help="Number of download threads per format. This will be 2x for an video and audio download. Default: 1")
    
    parser.add_argument('--batch-size', type=int, default=5, help="Number of segments before the temporary database is committed to disk. This is useful for reducing disk access instances. Default: 5")
    
    parser.add_argument('--segment-retries', type=int, default=5, help="Number of times to retry grabbing a segment. Default: 5")
    
    parser.add_argument('--no-merge', action='store_false', dest='merge', help="Don't merge video")

    parser.add_argument('--merge', action='store_true', dest='merge', help="Merge video, overrides --no-merge")
    
    parser.add_argument('--cookies', type=str, default=None, help="Path to cookies file")

    parser.add_argument('--temp-folder', type=str, default=None, dest='temp_folder', help="Path for temporary files. Currently doesn't support templates like the output files")
    
    parser.add_argument('--output', type=str, default="%(fulltitle)s (%(id)s)", help="Path for output files")
    
    parser.add_argument('--write-thumbnail', action='store_true', help="Write thumbnail to file")
    
    parser.add_argument('--embed-thumbnail', action='store_true', help="Embed thumbnail into final file. Ignored if --no-merge is used")
    
    parser.add_argument('--write-info-json', action='store_true', help="Write info-json to file")
    
    parser.add_argument('--write-description', action='store_true', help="Write description to file")
    
    parser.add_argument('--keep-temp-files', action='store_true', help="Keep all temp files i.e. database and ts files")
    
    parser.add_argument('--keep-ts-files', action='store_true', help="Keep all ts files")
    
    parser.add_argument('--live-chat', action='store_true', help="Get Live chat")
    
    parser.add_argument('--keep-database-file', action='store_true', help="Keep database file. If using with --direct-to-ts, this keeps the state file")
    
    parser.add_argument('--recovery', action='store_true', help="Puts downloader into stream recovery mode")
    
    parser.add_argument('--database-in-memory', action='store_true', help="Keep stream segments in memory. Requires a lot of RAM (Not recommended)")
    
    parser.add_argument('--direct-to-ts', action='store_true', help="Write directly to ts file instead of database. May use more RAM if a segment is slow to download. This overwrites most database options")
    
    parser.add_argument("--wait-for-video", type=int, nargs="*", help="(min, max) Minimum and maximum interval to wait for a video")
    
    parser.add_argument('--json-file', type=str, default=None, help="Path to yt-dlp info.json file. Overrides ID and skips retrieving URLs")

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
    
    options['threads'] = 5
    options['batch_size'] = 5
    options['write_thumbnail'] = True
    options['write_description'] = True
    options['write_info_json'] = True
    
    
    main(id=id, resolution=resolution, options=options)