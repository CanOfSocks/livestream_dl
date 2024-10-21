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

    def warning(self, msg):
        #print(msg)
        if "Private" in msg or "private" in msg or "UNAVAILABLE" in msg.upper() or "should already be available" in msg.upper():
            raise yt_dlp.utils.DownloadError("Private video. Sign in if you've been granted access to this video")
        

    def error(self, msg):
        print(msg)
        pass
            
def get_Video_Info(id):
    url = "https://www.youtube.com/watch?v={0}".format(id)
    
    logger = MyLogger()
    
    ydl_opts = {
        'retries': 25,
        'wait_for_video': (5, 1800),
        'skip_download': True,       
        'quiet': True,
        'no_warnings': True,
        'extractor_args': 'youtube:player_client=web;skip=dash;formats=incomplete,duplicate',
        'logger': logger
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info_dict = ydl.extract_info(url, download=False)
        # Check if the video is private
        if not (info_dict.get('live_status') == 'is_live' or info_dict.get('live_status') == 'post_live'):
            raise Exception("Video has been processed, please use yt-dlp directly")
        return info_dict
        

def get_image(url):
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Get the content of the response
        image_content = response.content

        # Convert the image content to a base64 string
        base64_image = base64.b64encode(image_content).decode()

        return f"data:image/jpeg;base64,{base64_image}"


print(YoutubeURL.Formats().getFormatURL(get_Video_Info('Ojz4jmLgNOI'), '1080p*'))       
    
