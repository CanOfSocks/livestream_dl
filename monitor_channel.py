from datetime import datetime, timedelta, timezone
from yt_dlp import YoutubeDL
import logging
import json

from YoutubeURL import YTDLPLogger

def withinFuture(releaseTime=None, lookahead=24):
    #Assume true if value missing
    #lookahead = getConfig.get_look_ahead()
    if(not releaseTime or not lookahead):
        return True
    release = datetime.fromtimestamp(releaseTime, timezone.utc)    
    limit = datetime.now(timezone.utc) + timedelta(hours=lookahead)
    if(release <= limit):
        return True
    else:
        return False

def get_upcoming_or_live_videos(channel_id, tab=None, options={}, logger: logging = None):
    logger = logger or logging.getLogger()
    #channel_id = str(channel_id)
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        #'force_generic_extractor': True,
        'sleep_interval': 1,
        'sleep_interval_requests': 1,
        'no_warnings': True,
        'cookiefile': options.get("cookies", None),
        'playlist_items': '1-{0}'.format(options.get("playlist_items", 50)),
        #'verbose': True
        #'match_filter': filters
        "logger": YTDLPLogger(logger=logger),
    }
    try:
        with YoutubeDL(ydl_opts) as ydl:
            if tab == "membership":
                if channel_id.startswith("UUMO"):
                    url = "https://www.youtube.com/playlist?list={0}".format(channel_id)
                elif channel_id.startswith("UC") or channel_id.startswith("UU"):
                    url = "https://www.youtube.com/playlist?list={0}".format("UUMO" + channel_id[2:])
                else:
                    ydl_opts.update({'playlist_items': '1:10'})
                    url = "https://www.youtube.com/channel/{0}/{1}".format(channel_id, tab)
                    
            elif tab == "streams":
                if channel_id.startswith("UU"):
                    url = "https://www.youtube.com/playlist?list={0}".format(channel_id)
                elif channel_id.startswith("UC"):
                    url = "https://www.youtube.com/playlist?list={0}".format("UU" + channel_id[2:])
                elif channel_id.startswith("UUMO"):
                    url = "https://www.youtube.com/playlist?list={0}".format("UU" + channel_id[4:])
                else:
                    ydl_opts.update({'playlist_items': '1:10'})
                    url = "https://www.youtube.com/channel/{0}/{1}".format(channel_id, tab)
                    
            else:
                ydl_opts.update({'playlist_items': '1:10'})
                url = "https://www.youtube.com/channel/{0}/{1}".format(channel_id, tab)
                
            info = ydl.extract_info(url, download=False)
            #logging.debug(json.dumps(info))
            upcoming_or_live_videos = []
            for video in info['entries']:
                if (video.get('live_status') == 'is_live' or video.get('live_status') == 'post_live' 
                    or (video.get('live_status') == 'is_upcoming' and withinFuture(video.get('release_timestamp', None), **({"lookahead": options["monitor_lookahead"]} if "monitor_lookahead" in options else {})))):

                    logger.debug("({1}) live_status = {0}".format(video.get('live_status'),video.get('id')))
                    logger.debug(json.dumps(video))
                    upcoming_or_live_videos.append(video.get('id'))


            return list(set(upcoming_or_live_videos))
    except Exception as e:
        logger.exception("An unexpected error occurred when trying to fetch videos")
        raise