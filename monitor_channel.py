from datetime import datetime, timedelta, timezone
from yt_dlp import YoutubeDL
import logging
import json

from YoutubeURL import YTDLPLogger, VERBOSE_LEVEL_NUM

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

def resolve_channel(url: str, logger: logging = None):
    logger = logger or logging.getLogger()
    try:
        channel_id = get_channel(channel_url=url, logger=logger)
        if channel_id and str(channel_id).startswith("UC"):
            return channel_id
        else:
            raise ValueError("Unable to find channel ID")
    except Exception as e:
        if "not currently live" in str(e):
            logger.log(VERBOSE_LEVEL_NUM, "Channel found, but not live: {0}. Waiting until a live stream is found to resolve channel ID".format(e))
            return None
        logger.warning("Unable to find channel ID with URL search using '{0}'. Attemptiong to use youtube search.")
        channel_id = get_by_name(channel_name=url, logger=logger)
        if channel_id and str(channel_id).startswith("UC"):
            return channel_id
        else:
            logger.error("Unable to find channel using search: {0}".format(url))
    return None

def get_channel(channel_url: str, logger: logging = None):
    logger = logger or logging.getLogger()
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'playlist_items': '1',
        "logger": YTDLPLogger(logger=logger),
    }
    
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(channel_url, download=False)
        return ydl.sanitize_info(info).get("channel_id", None)
        

def get_by_name(channel_name: str, logger: logging = None):
    # Search for the channel specifically
    # 'ytsearch1' finds the first result
    #search_query = f"ytsearch1:, channel"
    search_query = f"https://www.youtube.com/results?search_query={channel_name}&sp=EgIQAg%253D%253D"
    
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'playlist_items': '1',
        "logger": YTDLPLogger(logger=logger),
    }
    
    with YoutubeDL(ydl_opts) as ydl:
        results = ydl.extract_info(search_query, download=False)
        results = ydl.sanitize_info(results)
        if 'entries' in results and len(results['entries']) > 0:
            first_result = results['entries'][0]
            logger.info(f"Found Channel: {first_result.get('uploader')}")
            return first_result.get('channel_id')
                
                
    logger.error("No channel found with query: {0}".format(channel_name))
    return None