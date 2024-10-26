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
    info_dict, live_status = getUrls.get_Video_Info(id, cookies=options.get("cookies", None))
    #if live_status == 'is_live' or live_status == 'was_live':
    download_Live.download_segments(info_dict, resolution, options)
    
if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Download an video by ID")

    # Add a required positional argument 'ID'
    parser.add_argument('ID', type=str, help='The video ID (required)')
    
    parser.add_argument('--resolution', type=str, default='best', dest='resolution', help="Desired resolution. Can be best, audio_only or specific resolution.")
    
    parser.add_argument('--formats', type=int, nargs='*', dest='resolution', help="Specify specific formats to download. Overrides resolution. Use with --no-merge if not using a video and audio format")
    
    parser.add_argument('--threads', type=int, default=1, help="Number of download threads per format. This will be 2x for an video and audio download")
    
    parser.add_argument('--batch-size', type=int, default=1, help="Number of segments before the temporary database is committed to disk. This is useful for reducing disk access instances.")
    
    parser.add_argument('--no-merge', action='store_false', dest='merge', help="Don't merge video")

    parser.add_argument('--merge', action='store_true', dest='merge', help="Merge video, overrides --no-merge")
    
    parser.add_argument('--cookies', type=str, default=None, help="Path to cookies file")

    parser.add_argument('--temp-folder', type=str, default=None, dest='temp_folder', help="Path for temporary files")
    
    parser.add_argument('--output', type=str, default="%(title)s", help="Path for output files")
    
    parser.add_argument('--write-thumbnail', action='store_true', help="Write thumbnail to file")
    
    parser.add_argument('--embed-thumbnail', action='store_true', help="Embed thumbnail into final file. Ignored if --no-merge is used")
    
    parser.add_argument('--write-info-json', action='store_true', help="Write info-json to file")
    
    parser.add_argument('--write-description', action='store_true', help="Write description to file")
    
    parser.add_argument('--keep-temp-files', action='store_true', help="Keep all temp files i.e. database and ts files")
    
    parser.add_argument('--keep-ts-files', action='store_true', help="Keep all ts files")
    
    parser.add_argument('--keep-database-file', action='store_true', help="Keep database file")
    
    parser.add_argument('--database-in-memory', action='store_true', help="Keep stream segments in memory. Requires a lot of RAM (Not recommended)")

    # Parse the arguments
    args = parser.parse_args()
    

    # Access the 'ID' value
    id = args.ID
    resolution = args.resolution
    options = vars(args)
    
    # For testing
    
    args['threads'] = 5
    args['batch_size'] = 5
    args['write_thumbnail'] = True
    args['write_description'] = True
    args['write_info_json'] = True
    
    
    main(id=id, resolution=resolution, options=options)