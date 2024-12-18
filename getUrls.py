#!/usr/local/bin/python
import yt_dlp

class MyLogger:
    def __init__(self):
        pass

    def debug(self, msg):
        #print(msg)
        if str(msg).startswith('[DEBUG]'):
            pass
        elif str(msg).startswith('[wait] R'):
            print(msg, end='\r')
        else:
            print(msg)
    def info(self, msg):
        print(msg)

    def warning(self, msg):
        #
        if "private" in msg.lower() or "UNAVAILABLE" in msg.upper() or "unavailable" in str(msg) or "should already be available" in msg.lower() or "not available" in msg.lower():
            raise yt_dlp.utils.DownloadError("Private video. Sign in if you've been granted access to this video")
        else:
            print(msg)
        

    def error(self, msg):
        print(msg)
        pass
            
def get_Video_Info(id, wait=True, cookies=None):
    #url = "https://www.youtube.com/watch?v={0}".format(id)
    url = str(id)
    logger = MyLogger()
    
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
#        'extractor_args': 'youtube:player_client=web;skip=dash;formats=incomplete,duplicate',
        'logger': logger
    }
    
    if isinstance(wait, tuple):
        ydl_opts['wait_for_video'] = wait
    elif wait is True:
        ydl_opts['wait_for_video'] = (5,300)

    info_dict = {}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info_dict = ydl.extract_info(url, download=False)
            info_dict = ydl.sanitize_info(info_dict)
            # Check if the video is private
            if not (info_dict.get('live_status') == 'is_live' or info_dict.get('live_status') == 'post_live'):
                #print("Video has been processed, please use yt-dlp directly")
                raise ValueError("Video has been processed, please use yt-dlp directly")
        except yt_dlp.utils.DownloadError as e:
            # If an error occurs, we can assume the video is private or unavailable
            if 'video is private' in str(e) or "Private video. Sign in if you've been granted access to this video" in str(e):
                raise PermissionError("Video {0} is private, unable to get stream URLs".format(id))
            elif 'This live event will begin in' in str(e) or 'Premieres in' in str(e):
                raise ValueError("Video is not yet available. Consider using waiting option")
            #elif "This video is available to this channel's members on level" in str(e) or "members-only content" in str(e):
            elif " members " in str(e) or " members-only " in str(e):
                raise PermissionError("Video {0} is a membership video. Requires valid cookies".format(id))
            else:
                raise e
        
      
    return info_dict, info_dict.get('live_status')
