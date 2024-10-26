#!/usr/local/bin/python
import yt_dlp
import os
import json
import requests
import base64
import subprocess
from shutil import move

import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from json import load       

import YoutubeURL

class MyLogger:
    def __init__(self):
        self.private_video_detected = False

    def debug(self, msg):
        #print(msg)
        pass
    
    def info(self, msg):
        print(msg)

    def warning(self, msg):
        #print(msg)
        if "Private" in msg or "private" in msg or "UNAVAILABLE" in msg.upper() or "should already be available" in msg.upper():
            raise yt_dlp.utils.DownloadError("Private video. Sign in if you've been granted access to this video")
        

    def error(self, msg):
        print(msg)
        pass
            
def get_Video_Info(id, wait=True, cookies=None):
    #url = "https://www.youtube.com/watch?v={0}".format(id)
    url = str(id)
    logger = MyLogger()
    
    ydl_opts = {
        
        'retries': 25,
        'skip_download': True,
        'cookiefile': cookies,
#        'quiet': True,
        'no_warnings': True,
#        'extractor_args': 'youtube:player_client=web;skip=dash;formats=incomplete,duplicate',
#        'logger': logger
    }
    
    if wait == True:
        ydl_opts['wait_for_video'] = (1,300)

    info_dict = {}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info_dict = ydl.extract_info(url, download=False)
        info_dict = ydl.sanitize_info(info_dict)
        # Check if the video is private
        if not (info_dict.get('live_status') == 'is_live' or info_dict.get('live_status') == 'post_live'):
            print("Video has been processed, please use yt-dlp directly")
            raise ValueError("Video has been processed, please use yt-dlp directly")
        
      
    return info_dict, info_dict.get('live_status')
