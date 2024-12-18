import yt_dlp
import sqlite3
import requests
from requests.adapters import HTTPAdapter, Retry
import random
from datetime import datetime
import time
import concurrent.futures
import json
import getUrls
import YoutubeURL

import subprocess
import os  

from urllib.parse import urlparse, parse_qs

import shutil

import logging

from headers import user_agents

kill_all = False

live_chat_result = None

logging.basicConfig(
    filename='output.log',   # File where the logs will be stored
    level=logging.INFO,      # Minimum level of messages to log (INFO or higher)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
)

# File name dictionary
file_names = {
    'databases': []
}

# Create runner function for each download format
def download_stream(info_dict, resolution, batch_size, max_workers, folder=None, file_name=None, keep_database=False, cookies=None, retries=5):
    try:
        downloader = DownloadStream(info_dict, resolution=resolution, batch_size=batch_size, max_workers=max_workers, folder=folder, file_name=file_name, cookies=cookies, fragment_retries=retries)        
        downloader.live_dl()
        file_name = downloader.combine_segments_to_file(downloader.merged_file_name)
        if not keep_database:
            downloader.delete_temp_database()
        elif downloader.temp_db_file != ':memory:':
            database_file = fileInfo(downloader.temp_db_file, file_type='database', format=downloader.format)
            file_names['databases'].append(database_file)
    finally:
        # Explicitly close connection
        downloader.close_connection()
        file = fileInfo(file_name, file_type=downloader.type, format=downloader.format)
    return file, downloader.type

# Create runner function for each download format
def download_stream_direct(info_dict, resolution, batch_size, max_workers, folder=None, file_name=None, keep_state=False, cookies=None, retries=5):
    try:
        downloader = DownloadStreamDirect(info_dict, resolution=resolution, batch_size=batch_size, max_workers=max_workers, folder=folder, file_name=file_name, cookies=cookies, fragment_retries=retries)        
        file_name = downloader.live_dl()
        if not keep_state:
            downloader.delete_state_file()
        else:
            database_file = fileInfo(downloader.state_file_name, file_type='database', format=downloader.format)
            file_names['databases'].append(database_file)
    finally:
        file = fileInfo(file_name, file_type=downloader.type, format=downloader.format)
    return file, downloader.type

def recover_stream(info_dict, resolution, batch_size=5, max_workers=5, folder=None, file_name=None, keep_database=False, cookies=None, retries=5):
    
    downloader = StreamRecovery(info_dict, resolution=resolution, batch_size=batch_size, max_workers=max_workers, folder=folder, file_name=file_name, cookies=cookies, fragment_retries=retries)        
    result = downloader.live_dl()
    downloader.save_stats()    
    if result:
        file_name = downloader.combine_segments_to_file(downloader.merged_file_name)
        if not keep_database:
            downloader.delete_temp_database()
        elif downloader.temp_db_file != ':memory:':
            database_file = fileInfo(downloader.temp_db_file, file_type='database', format=downloader.format)
            file_names['databases'].append(database_file)
    # Explicitly close connection
    downloader.close_connection()
    file = fileInfo(file_name, file_type=downloader.type, format=downloader.format)
    
    
    
    return file, downloader.type

# Multithreaded function to download new segments with delayed commit after a batch
def download_segments(info_dict, resolution='best', options={}):
    futures = set()
    #file_names = {}
       
    print(json.dumps(options, indent=4))
    outputFile = output_filename(info_dict=info_dict, outtmpl=options.get('output'))
    file_name = None
    
    # Requires testing
    if options.get('temp_folder') is not None and options.get('temp_folder') != os.path.dirname(outputFile):
        output_folder, file_name = os.path.split(outputFile)
        download_folder = output_filename(info_dict=info_dict, outtmpl=options.get('temp_folder'))
        options['temp_folder'] = download_folder
    else:
        download_folder, file_name = os.path.split(outputFile)
    options['filename'] = file_name
    if download_folder:    
        os.makedirs(download_folder, exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor() as executor:  
        try: 
            
            # Download auxiliary files (thumbnail, info,json etc)
            auxiliary_thread = executor.submit(download_auxiliary_files, info_dict=info_dict, options=options)
            futures.add(auxiliary_thread)
            
            live_chat_thread = None            
            if options.get('live_chat', False) is True:
                import threading
                live_chat_thread = threading.Thread(target=download_live_chat, args=(info_dict,options), daemon=True)
                live_chat_thread.start()
                #download_live_chat(info_dict=info_dict, options=options)
                #chat_thread = executor.submit(download_live_chat, info_dict=info_dict, options=options)
                #futures.add(chat_thread)
            
            format_parser = YoutubeURL.Formats()
            # For use of specificed format. Expects two values, but can work with more
            if options.get('video_format', None) is not None or options.get('audio_format', None) is not None:
                if options.get('recovery', False) is True:
                    if options.get('video_format', None) is not None:
                        if int(options.get('video_format')) not in format_parser.video.get('best'):    
                            raise ValueError("Video format not valid, please use one from {0}".format(format_parser.video))
                        else:
                            video_future = executor.submit(recover_stream, info_dict=info_dict, resolution=int(options.get('video_format')), batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                        keep_database=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), cookies=options.get('cookies'), retries=options.get('segment_retries'))
                            futures.add(video_future)
                    
                    if options.get('audio_format', None) is not None:
                        if int(options.get('audio_format')) not in format_parser.audio: 
                            raise ValueError("Audio format not valid, please use one from {0}".format(format_parser.audio))
                        else:
                            audio_future = executor.submit(recover_stream, info_dict=info_dict, resolution=int(options.get('audio_format')), batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                        keep_database=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), retries=options.get('segment_retries'))
                            futures.add(audio_future)  
                elif options.get('direct_to_ts', False) is True:
                    if options.get('video_format', None) is not None:
                        if int(options.get('video_format')) not in format_parser.video.get('best'):    
                            raise ValueError("Video format not valid, please use one from {0}".format(format_parser.video))
                        else:
                            video_future = executor.submit(download_stream_direct, info_dict=info_dict, resolution=int(options.get('video_format')), batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                        keep_state=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), cookies=options.get('cookies'), retries=options.get('segment_retries'))
                            futures.add(video_future)
                    
                    if options.get('audio_format', None) is not None:
                        if int(options.get('audio_format')) not in format_parser.audio: 
                            raise ValueError("Audio format not valid, please use one from {0}".format(format_parser.audio))
                        else:
                            audio_future = executor.submit(download_stream_direct, info_dict=info_dict, resolution=int(options.get('audio_format')), batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                        keep_state=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), retries=options.get('segment_retries'))
                            futures.add(audio_future)  
                else:
                    if options.get('video_format', None) is not None:
                        if int(options.get('video_format')) not in format_parser.video.get('best'):    
                            raise ValueError("Video format not valid, please use one from {0}".format(format_parser.video))
                        else:
                            video_future = executor.submit(download_stream, info_dict=info_dict, resolution=int(options.get('video_format')), batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                        keep_database=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), cookies=options.get('cookies'), retries=options.get('segment_retries'))
                            futures.add(video_future)
                    
                    if options.get('audio_format', None) is not None:
                        if int(options.get('audio_format')) not in format_parser.audio: 
                            raise ValueError("Audio format not valid, please use one from {0}".format(format_parser.audio))
                        else:
                            audio_future = executor.submit(download_stream, info_dict=info_dict, resolution=int(options.get('audio_format')), batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                        keep_database=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), retries=options.get('segment_retries'))
                            futures.add(audio_future)                        
                    
            elif resolution.lower() != "audio_only":                
                if YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution) is not None:
                    # Submit tasks for both video and audio downloads   
                    if options.get('recovery', False) is True:
                        video_future = executor.submit(recover_stream, info_dict=info_dict, resolution=resolution, batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                            keep_database=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), retries=options.get('segment_retries'))
                        audio_future = executor.submit(recover_stream, info_dict=info_dict, resolution="audio_only", batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                            keep_database=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), retries=options.get('segment_retries'))
                    elif options.get('direct_to_ts', False) is True:
                        video_future = executor.submit(download_stream_direct, info_dict=info_dict, resolution=resolution, batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                            keep_state=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), retries=options.get('segment_retries'))
                        audio_future = executor.submit(download_stream_direct, info_dict=info_dict, resolution="audio_only", batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                            keep_state=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), retries=options.get('segment_retries'))
                    else:
                        video_future = executor.submit(download_stream, info_dict=info_dict, resolution=resolution, batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                            keep_database=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), retries=options.get('segment_retries'))
                        audio_future = executor.submit(download_stream, info_dict=info_dict, resolution="audio_only", batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, 
                                            keep_database=(options.get("keep_temp_files", False) or options.get("keep_database_file", False)), retries=options.get('segment_retries'))
                    
                            # Wait for both downloads to finish
                    futures.add(video_future)
                    futures.add(audio_future)
                    
                else:
                    raise ValueError("Resolution is not valid or does not exist in stream")
                            
            elif resolution.lower() == "audio_only":
                if options.get('recovery', False) is True:
                    futures.add(executor.submit(recover_stream, info_dict=info_dict, resolution="audio_only", batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, retries=options.get('segment_retries')))
                elif options.get('direct_to_ts', False) is True:
                    futures.add(executor.submit(download_stream_direct, info_dict=info_dict, resolution="audio_only", batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, retries=options.get('segment_retries')))
                else:
                    futures.add(executor.submit(download_stream, info_dict=info_dict, resolution="audio_only", batch_size=options.get('batch_size',1), max_workers=options.get("threads", 1), folder=download_folder, file_name=file_name, retries=options.get('segment_retries')))
                
            while True:
                done, not_done = concurrent.futures.wait(futures, timeout=0.1, return_when=concurrent.futures.ALL_COMPLETED)
                # Continuously check for completion or interruption
                for future in done:
                    if future.exception() is not None:
                        raise future.exception()
                    
                    result, type = future.result()
                    logging.info("result of thread: {0}".format(result))
                    print("\033[31m{0}\033[0m".format(result))
                    
                    if type == 'auxiliary':
                        file_names.update(result)
                    elif str(type).lower() == 'video':
                        file_names['video'] = result
                    elif str(type).lower() == 'audio':
                        file_names['audio'] = result    
                    else:
                        file_names[result.type] = result
                    
                    futures.remove(future)
                    
                if len(not_done) <= 0:
                    break
                else:
                    time.sleep(0.9)
            
            if live_chat_thread is not None:
                print("Waiting for live chat to end")
                live_chat_thread.join()
                if live_chat_result is not None:
                    file_names.update(live_chat_result)
            
        except KeyboardInterrupt as e:
            global kill_all
            kill_all = True
            print("Keyboard interrupt detected")
            done, not_done = concurrent.futures.wait(futures, timeout=5, return_when=concurrent.futures.ALL_COMPLETED)
            if len(not_done) > 0:
                print("Cancelling remaining threads")
            for future in not_done:
                _ = future.cancel()
            done, not_done = concurrent.futures.wait(futures, timeout=5, return_when=concurrent.futures.ALL_COMPLETED)
            raise
            
    if options.get('merge', None) or not options.get('no_merge', False):
        create_mp4(file_names=file_names, info_dict=info_dict, options=options)
        
    if options.get('temp_folder', None) is not None:
        move_to_final(options=options, outputFile=outputFile, file_names=file_names)               
                
    #move_to_final(info_dict, options, file_names)
    
def output_filename(info_dict, outtmpl):
    outputFile = str(yt_dlp.YoutubeDL().prepare_filename(info_dict, outtmpl=outtmpl))
    return outputFile

def move_to_final(options, outputFile, file_names):
    if os.path.dirname(outputFile):
        os.makedirs(os.path.dirname(outputFile), exist_ok=True)
    try:
        if file_names.get('thumbnail'):
            thumbnail = file_names.get('thumbnail')
            thumb_output = "{0}.{1}".format(outputFile, thumbnail.ext)
            print("Moving {0} to {1}".format("{0}.{1}".format(thumbnail.path, thumbnail.ext), thumb_output))
            shutil.move("{0}.{1}".format(thumbnail.path, thumbnail.ext), thumb_output)
    except Exception as e:
        print("unable to move thumbnail: {0}".format(e))
    
    try:
        if file_names.get('info_json'):
            info_json = file_names.get('info_json')
            info_output = "{0}.{1}".format(outputFile, info_json.ext)
            print("Moving {0} to {1}".format("{0}.{1}".format(info_json.path, info_json.ext), info_output))
            shutil.move("{0}.{1}".format(info_json.path, info_json.ext), info_output)
    except Exception as e:
        print("unable to move info_json: {0}".format(e))
        
    try:
        if file_names.get('description'):
            description = file_names.get('description')
            description_output = "{0}.{1}".format(outputFile, description.ext)
            print("Moving {0} to {1}".format("{0}.{1}".format(description.path, description.ext), description_output))
            shutil.move("{0}.{1}".format(description.path, description.ext), description_output)
    except Exception as e:
        print("unable to move description: {0}".format(e))
    
    try:
        if file_names.get('video'):
            video = file_names.get('video')
            video_output = "{0}.{1}.{2}".format(outputFile, video.format, video.ext)
            print("Moving {0} to {1}".format("{0}.{1}".format(video.path, video.ext), video_output))
            shutil.move("{0}.{1}".format(video.path, video.ext), video_output)
    except Exception as e:
        print("unable to move video stream: {0}".format(e))
        
    try:
        if file_names.get('audio'):
            audio = file_names.get('audio')
            audio_output = "{0}.{1}.{2}".format(outputFile, audio.format, audio.ext)
            print("Moving {0} to {1}".format("{0}.{1}".format(audio.path, audio.ext), audio_output))
            shutil.move("{0}.{1}".format(audio.path, audio.ext), audio_output)
    except Exception as e:
        print("unable to move audio stream: {0}".format(e))
        
    try:
        if file_names.get('merged'):
            merged = file_names.get('merged')
            merged_output = "{0}.{1}".format(outputFile, merged.ext)
            print("Moving {0} to {1}".format("{0}.{1}".format(merged.path, merged.ext), merged_output))
            shutil.move("{0}.{1}".format(merged.path, merged.ext), merged_output)
    except Exception as e:
        print("unable to move merged video: {0}".format(e))
        
    try:
        if file_names.get('ffmpeg_cmd'):
            ffmpeg_cmd = file_names.get('ffmpeg_cmd')
            ffmpeg_cmd_output = "{0}.{1}".format(outputFile, ffmpeg_cmd.ext)
            print("Moving {0} to {1}".format("{0}.{1}".format(ffmpeg_cmd.path, ffmpeg_cmd.ext), ffmpeg_cmd_output))
            shutil.move("{0}.{1}".format(ffmpeg_cmd.path, ffmpeg_cmd.ext), ffmpeg_cmd_output)
    except Exception as e:
        print("unable to move merged video: {0}".format(e))
        
    try:
        if file_names.get('live_chat'):
            live_chat = file_names.get('live_chat')
            live_chat_output = "{0}.{1}".format(outputFile, live_chat.ext)
            print("Moving {0} to {1}".format("{0}.{1}".format(live_chat.path, live_chat.ext), live_chat_output))
            shutil.move("{0}.{1}".format(live_chat.path, live_chat.ext), live_chat_output)
    except Exception as e:
        print("unable to move live chat zip: {0}".format(e))
     
    try:
        if file_names.get('databases'):
            for file in file_names.get('databases'):
                db_output = "{0}.{1}.{2}".format(outputFile, file.format, file.ext)
                print("Moving {0} to {1}".format("{0}.{1}".format(file.path, file.ext), db_output))
                shutil.move("{0}.{1}".format(file.path, file.ext), db_output)
    except Exception as e:
        print("unable to move database files: {0}".format(e))
        
    try:
        os.rmdir(options.get('temp_folder'))
    except Exception as e:
        print("Error removing temp folder: {0}".format(e))
        
    print("Finished moving files from temporary directory to output destination")
    
def download_live_chat(info_dict, options):
    import yt_dlp
    import zipfile
    
    if options.get('filename') is not None:
        filename = options.get('filename')
    else:
        filename = info_dict.get('id')
    
    if options.get("temp_folder"):
        base_output = os.path.join(options.get("temp_folder"), filename)
    else:
        base_output = filename

    ydl_opts = {
        'skip_download': True,               # Skip downloading video/audio
        'quiet': True,
        'cookiefile': options.get('cookies'),
        #'live_from_start': True,
        'writesubtitles': True,              # Extract subtitles (live chat)
        'subtitlesformat': 'json',           # Set format to JSON
        'subtitleslangs': ['live_chat'],     # Only extract live chat subtitles
        'concurrent_fragment_downloads': 2,
        'outtmpl': base_output          # Save to a JSON file
        
    }
    livechat_filename = base_output + ".live_chat.json"
    zip_filename = base_output + ".live_chat.zip"
    
    print("Downloading live chat to: {0}".format(livechat_filename))
    # Run yt-dlp with the specified options
    # Don't except whole process on live chat fail
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            result = ydl.process_ie_result(info_dict)
    except Exception as e:
        print("\033[31m{0}\033[0m".format(e))
    time.sleep(1)
    if os.path.exists("{0}.part".format(livechat_filename)):
        shutil.move("{0}.part", livechat_filename)
    
    try:
        with zipfile.ZipFile(zip_filename, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9, allowZip64=True) as zipf:
            zipf.write(livechat_filename, arcname=os.path.basename(livechat_filename))
        os.remove(livechat_filename)
        live_chat = {
            'live_chat': fileInfo(path=base_output, ext='.live_chat.zip', file_type='live_chat')
        }
        global live_chat_result
        live_chat_result = live_chat
        return live_chat, 'live_chat'
    except Exception as e:
        print("\033[31m{0}\033[0m".format(e))
    

def download_auxiliary_files(info_dict, options, thumbnail=None):
    if options.get('filename') is not None:
        filename = options.get('filename')
    else:
        filename = info_dict.get('id')
    
    if options.get("temp_folder"):
        base_output = os.path.join(options.get("temp_folder"), filename)
    else:
        base_output = filename
    
    created_files = {}
    """
    try:
        if options.get('write_info_json'):
            info_file = "{0}.info.json".format(base_output)
            with open(info_file, 'w', encoding='utf-8') as f:
                f.write(json.dumps(info_dict, indent=4))
            created_files['info_json'] = fileInfo(base_output, ext='info.json', type='info_json')
    except Exception as e:
        print("\033[31m{0}\033[0m".format(e))
        
    try:
        if options.get('write_description'):
            desc_file = "{0}.description".format(base_output)
            with open(desc_file, 'w', encoding='utf-8') as f:
                f.write(info_dict.get('description', ""))    
            created_files['description'] = fileInfo(desc_file, ext='description', type='description')
    except Exception as e:
        print("\033[31m{0}\033[0m".format(e))
        
    try:
        if options.get('write_thumbnail') or options.get("embed_thumbnail") and info_dict.get('thumbnail'):
            # Filter URLs ending with '.jpg' and sort by preference in descending order
            jpg_urls = [item for item in info_dict['thumbnails'] if item['url'].endswith('.jpg')]
            highest_preference_jpg = max(jpg_urls, key=lambda x: x['preference']).get('url')
            print("Best url: {0}".format(highest_preference_jpg))
            thumb_file = "{0}.jpg".format(base_output)
            retry_strategy = Retry(
                    total=5,  # maximum number of retries
                    backoff_factor=1, 
                    status_forcelist=[204, 400, 401, 403, 404, 429, 500, 502, 503, 504],  # the HTTP status codes to retry on
                )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            # create a new session object
            session = requests.Session()
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            response = session.get(highest_preference_jpg, timeout=30)
            with open(thumb_file, 'wb') as f:
                f.write(response.content)
            created_files['thumbnail'] = fileInfo(base_output, ext='jpg', type='thumbnail')      
    except Exception as e:
        print("\033[31m{0}\033[0m".format(e))
    """
    ydl_opts = {
        'skip_download': True,
        'quiet': True,
#        'cookiefile': options.get('cookies', None),
        'writeinfojson': options.get('write_info_json', False),
        'writedescription': options.get('write_description', False),
        'writethumbnail': (options.get('write_thumbnail', False) or options.get("embed_thumbnail", False)),
        'outtmpl': base_output
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        #base_name = ydl.prepare_filename(info_dict)
        #result = ydl.download_with_info_file(info_dict)
        thumbnails = ydl._write_thumbnails('video', info_dict, ydl.prepare_filename(info_dict, 'thumbnail'))
        
        if thumbnails:
            thumb_base = str(os.path.splitext(thumbnails[0][0])[0])
            thumb_ext = str(os.path.splitext(thumbnails[0][0])[1])
            created_files['thumbnail'] = fileInfo(thumb_base, ext=thumb_ext, file_type='thumbnail')
        if ydl._write_info_json('video', info_dict, ydl.prepare_filename(info_dict, 'infojson')) or os.path.exists(ydl.prepare_filename(info_dict, 'infojson')):
            created_files['info_json'] = fileInfo(ydl.prepare_filename(info_dict), ext='info.json', file_type='info_json')
            
        if ydl._write_description('video', info_dict, ydl.prepare_filename(info_dict, 'description')) or os.path.exists(ydl.prepare_filename(info_dict, 'description')):
            created_files['description'] = fileInfo(ydl.prepare_filename(info_dict), ext='description', file_type='description')
        
    return created_files, 'auxiliary'
    
        
def create_mp4(file_names, info_dict, options):
    index = 0
    thumbnail = None
    video = None
    audio = None
    ext = options.get('ext', None)

    
    ffmpeg_builder = ['ffmpeg', '-y', 
                      '-hide_banner', '-nostdin', '-loglevel', 'error', '-stats'
                      ]
    
    if file_names.get('thumbnail') and options.get('embed_thumbnail', True):
        input = ['-i', "{0}.{1}".format(file_names.get('thumbnail').abs_path, file_names.get('thumbnail').ext), '-thread_queue_size', '1024']
        thumbnail = index
        index += 1
    
    # Add input files
    if file_names.get('video'):        
        input = ['-i', "{0}.{1}".format(file_names.get('video').abs_path, file_names.get('video').ext), '-thread_queue_size', '1024']
        ffmpeg_builder.extend(input)
        video = index
        index += 1
            
    if file_names.get('audio'):
        input = ['-i', "{0}.{1}".format(file_names.get('audio').abs_path, file_names.get('audio').ext), '-thread_queue_size', '1024']
        ffmpeg_builder.extend(input)
        audio = index
        index += 1
        if video is None and ext is None:
            ext = '.ogg'
    # Add faststart
    ffmpeg_builder.extend(['-movflags', 'faststart'])
    
    # Add mappings
    for i in range(0, index):
        input = ['-map', str(i)]
        ffmpeg_builder.extend(input)
        
    if thumbnail is not None:
        ffmpeg_builder.extend(['-disposition:v:{0}'.format(thumbnail), 'attached_pic'])
        
    #Add Copy codec
    ffmpeg_builder.extend(['-c', 'copy'])
        
    # Add metadata
    ffmpeg_builder.extend(['-metadata', "DATE={0}".format(info_dict.get("upload_date"))])
    ffmpeg_builder.extend(['-metadata', "COMMENT={0}\n{1}".format(info_dict.get("original_url"), info_dict.get("description"))])
    ffmpeg_builder.extend(['-metadata', "TITLE={0}".format(info_dict.get("fulltitle"))])
    ffmpeg_builder.extend(['-metadata', "ARTIST={0}".format(info_dict.get("channel"))])
    
    if options.get('filename') is not None:
        filename = options.get('filename')
    else:
        filename = info_dict.get('id')
    
    if options.get("temp_folder"):
        base_output = os.path.join(options.get("temp_folder"), filename)
    else:
        base_output = filename
    
    if ext is None:
        ext = info_dict.get('ext', '.mp4')
    if ext is not None and not str(ext).startswith("."):
        ext = "." + ext
    if not base_output.endswith(ext):
        base_output = base_output + ext  
        
    ffmpeg_builder.append(os.path.abspath(base_output))
    
    ffmpeg_command_file = "{0}.ffmpeg.txt".format(filename)
    file_names['ffmpeg_cmd'] =  fileInfo(write_ffmpeg_command(ffmpeg_builder, ffmpeg_command_file), file_type='ffmpeg_command')
        
    print("Executing ffmpeg. Outputting to {0}".format(ffmpeg_builder[-1]))
    result = subprocess.run(ffmpeg_builder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', check=True)
    #print(result.stdout)
    #print(result.stderr)
    
    file_names['merged'] = fileInfo(base_output, ext=ext, file_type='merged')
    
    # Remove temp video and audio files
    if not (options.get('keep_ts_files') or options.get('keep_temp_files')):
        if file_names.get('video'): 
            os.remove("{0}.{1}".format(file_names.get('video').path, file_names.get('video').ext))
            del file_names['video']
        if file_names.get('audio'): 
            os.remove("{0}.{1}".format(file_names.get('audio').path, file_names.get('audio').ext))
            del file_names['audio']
    
    return file_names
    #for file in file_names:
    #    os.remove(file)

def write_ffmpeg_command(command_array, filename):
    # Determine the platform
    
    with open(filename, 'w', encoding='utf-8') as f:
        if os.name == 'nt':  # Windows
            for arg in command_array:
                # Handle multiline and quotes for Windows
                if '\n' in arg:
                    arg = arg.replace('\n', '^ \n')  # Escape newlines
                f.write(f'"{arg}" ' if ' ' in arg else f'{arg} ')

        else:  # Assuming it's a Unix-like system (Linux, macOS)
            for arg in ffmpeg_args:
                # Write each argument for Linux
                f.write(f'"{arg}" ' if ' ' in arg else f'{arg} ')
            f.write("\n")  # Ensure the last command line ends properly

    return filename
    
class fileInfo:
    def __init__(self, path, ext=None, file_type=None, size=None, abs_path=None, format=None, cookies_path=None):       
        self.path = os.path.splitext(path)[0]
        
        self.basename = os.path.basename(path)
        
        if abs_path is None:
            self.abs_path = os.path.abspath(self.path)
        else:
            self.abs_path = abs_path
            
        self.cookies_path = cookies_path
        
        if ext is None:
            self.ext = os.path.splitext(path)[1]
        else:
            self.ext = ext
        
        self.ext = str(self.ext).lstrip('.')
            
        self.type = file_type
        
        if size is None and os.path.exists(self.path):
            self.size = os.path.getsize(self.path)
        else:
            self.size = None
            
        self.format = format
    
    def getPath(self):
        return "{0}.{1}".format(self.path, self.ext)
    
    def getName(self):
        return "{0}.{1}".format(self.basename, self.ext)
    
    def getAbsPath(self):
        return "{0}.{1}".format(self.abs_path, self.ext)

class DownloadStream:
    def __init__(self, info_dict, resolution='best', batch_size=10, max_workers=5, fragment_retries=5, folder=None, file_name=None, database_in_memory=False, cookies=None, recovery_thread_multiplier=2):        
        
        self.latest_sequence = -1
        self.already_downloaded = set()
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        self.resolution = resolution
        
        self.id = info_dict.get('id')
        self.live_status = info_dict.get('live_status')
        
        self.info_dict = info_dict
        self.stream_urls = []
        
        self.stream_url, self.format = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution, return_format=True) 
        
        if self.stream_url is None:
            raise ValueError("Stream URL not found for {0}, unable to continue".format(resolution))
        
        self.stream_urls.append(self.stream_url)
        # Extract and parse the query parameters into a dictionary
        parsed_url = urlparse(self.stream_url)        
        self.url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}

        print(json.dumps(self.url_params))
        
        self.database_in_memory = database_in_memory
        
        if file_name is None:
            file_name = self.id    
        
        self.file_base_name = file_name
        
        self.merged_file_name = "{0}.{1}.ts".format(file_name, self.format)     
        if self.database_in_memory:
            self.temp_db_file = ':memory:'
        else:
            self.temp_db_file = '{0}.{1}.temp'.format(file_name, self.format)
        
        self.folder = folder    
        if self.folder:
            os.makedirs(folder, exist_ok=True)
            self.merged_file_name = os.path.join(self.folder, self.merged_file_name)
            self.file_base_name = os.path.join(self.folder, self.file_base_name)
            if not self.database_in_memory:
                self.temp_db_file = os.path.join(self.folder, self.temp_db_file)

            
        self.fragment_retries = fragment_retries
        self.retry_strategy = Retry(
            total=fragment_retries,  # maximum number of retries
            backoff_factor=1, 
            status_forcelist=[204, 400, 401, 403, 404, 408, 429, 500, 502, 503, 504],  # the HTTP status codes to retry on
        )        
        
        self.is_403 = False
        self.is_private = False
        self.estimated_segment_duration = 0
        self.refresh_retries = 0
        
        self.recovery_thread_multiplier = recovery_thread_multiplier
        
        self.cookies = cookies
        
        self.type = None
        self.ext = None        
        
        self.update_latest_segment()
        self.url_checked = time.time()

        self.conn, self.cursor = self.create_db(self.temp_db_file)    
        
    def get_expire_time(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        # Get the 'expire' parameter
        expire_value = query_params.get('expire', [-1])[0]
        if expire_value >= 0:
            return int(expire_value)
        return expire_value

    def refresh_Check(self):    
        
        #print("Refresh check ({0})".format(self.format))  
        
        # By this stage, a stream would have a URL. Keep using it if the video becomes private or a membership      
        if (time.time() - self.url_checked >= 3600.0 or self.is_403) and not self.is_private:
            print("Refreshing URL for {0}".format(self.format))
            try:
                info_dict, live_status = getUrls.get_Video_Info(self.id, wait=False, cookies=self.cookies)
                
                # Check for new manifest, if it has, start a nested download session
                if self.detect_manifest_change(info_json=info_dict) is True:
                    return True
                
                stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=self.format, return_format=False)
                if stream_url is not None:
                    self.stream_url = stream_url
                    self.stream_urls.append(stream_url)
                    
                    filtered_array = [url for url in self.stream_urls if int(self.get_expire_time(url)) > time.time()]
                    self.stream_urls = filtered_array
                    
                if live_status is not None:
                    self.live_status = live_status
                
                if info_dict:
                    self.info_dict = info_dict    
                
            except PermissionError as e:
                print("Permission error: {0}".format(e))
                if "membership" in str(e) and not self.is_403:
                    print("{0} is now members only. Continuing until 403 errors")
                else:
                    self.is_private = True
            except ValueError as e:
                print("Value error: {0}".format(e))
                if self.get_expire_time(self.stream_url) < time.time():
                    raise TimeoutError("Video is processed and stream url for {0} has expired, unable to continue...".format(self.format))
            except Exception as e:
                print("Error: {0}".format(e))                     
            self.url_checked = time.time()
                
    def live_dl(self):
        
        print("\033[31mStarting download of live fragments ({0})\033[0m".format(self.format))
        self.already_downloaded = self.segment_exists_batch()
        wait = 0   
        self.cursor.execute('BEGIN TRANSACTION')
        uncommitted_inserts = 0     
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="{0}-{1}".format(self.id,self.format)) as executor:
            submitted_segments = set()
            future_to_seg = {}
            
            # Trackers for optimistic segment downloads 
            optimistic = True
            optimistic_seg = 0           

            while True:     
                self.check_kill()
                if self.refresh_Check() is True:
                    break
                        
                # Process completed segment downloads, wait up to 5 seconds for segments to complete before next loop
                done, not_done = concurrent.futures.wait(future_to_seg, timeout=0.1, return_when=concurrent.futures.ALL_COMPLETED)  # need to fully determine if timeout or ALL_COMPLETED takes priority             
                
                for future in done:
                    head_seg_num, segment_data, seg_num, status, headers = future.result()
                    
                    # Remove from submitted segments in case it neeeds to be regrabbed
                    submitted_segments.remove(seg_num)
                    
                    # If successful in downloading segments optimistically, continue doing so
                    if seg_num >= self.latest_sequence and (status is None or status != 200):
                        print("Unable to optimistically grab segment {1} for {0}".format(self.format, seg_num))
                        optimistic = False
                    else: 
                        optimistic = True
                    
                    if head_seg_num > self.latest_sequence:
                        print("More segments available: {0}, previously {1}".format(head_seg_num, self.latest_sequence))                    
                        self.latest_sequence = head_seg_num
                        
                    if headers is not None and headers.get("X-Head-Time-Sec", None) is not None:
                        self.estimated_segment_duration = int(headers.get("X-Head-Time-Sec"))/self.latest_sequence

                    if segment_data is not None:
                        # Insert segment data in the main thread (database interaction)
                        self.insert_single_segment(cursor=self.cursor, segment_order=seg_num, segment_data=segment_data)
                        uncommitted_inserts += 1
                        
                        # If finished threads exceeds batch size, commit the whole batch of threads at once. 
                        # Has risk of not committing if a thread has no segment data, but this would be corrected naturally in following loop(s)
                        if uncommitted_inserts >= max(self.batch_size, len(done)):
                            print("Writing segments to file...")
                            self.commit_batch(self.conn)
                            uncommitted_inserts = 0
                            self.cursor.execute('BEGIN TRANSACTION') 
                    
                    # Remove completed thread to free RAM
                    del future_to_seg[future]
                      
                segments_to_download = set(range(0, self.latest_sequence)) - self.already_downloaded    
                                        
                # If segments remain to download, don't bother updating and wait for segment download to refresh values.
                if len(segments_to_download) <= 0:
                    
                    # Only attempt to grab optimistic segment once to ensure it does not cause a loop at the end of a stream
                    if optimistic and optimistic_seg <= self.latest_sequence and (self.latest_sequence+1) not in submitted_segments:
                        optimistic_seg = (self.latest_sequence+1)
                        
                        # Wait estimated fragment time +0.1s to make sure it would exist
                        time.sleep(self.estimated_segment_duration + 0.1)
                        
                        print("Adding segment {1} optimistically ({0})".format(self.format, optimistic_seg))
                        segments_to_download.add(optimistic_seg)
                        
                    # If optimistic grab is not successful, revert back to using headers from base stream URL
                    else:
                        print("Checking for more segments available for {0}".format(self.format))
                        self.update_latest_segment()
                        segments_to_download = set(range(0, self.latest_sequence)) - self.already_downloaded                              
                        

                # If update has no segments and no segments are currently running, wait                              
                if len(segments_to_download) <= 0 and len(future_to_seg) <= 0:                 
                    wait += 1
                    print("No new fragments available for {0}, attempted {1} times...".format(self.format, wait))
                        
                    # If waited for new fragments hits 20 loops, assume stream is offline
                    if wait > 20:
                        print("Wait time for new fragment exceeded, ending download...")
                        break    
                    # If over 10 wait loops have been executed, get page for new URL and update status if necessary
                    elif wait > 10:
                        if self.is_private:
                            print("Video is private and no more segments are available. Ending...")
                            break
                        else:
                            print("No new fragments found... Getting new url")
                            info_dict = None
                            live_status = None
                            try:
                                info_dict, live_status = getUrls.get_Video_Info(self.id, wait=False, cookies=self.cookies)
                                
                            # If membership stream (without cookies) or privated, mark as end of stream as no more fragments can be grabbed
                            except PermissionError as e:
                                print(e)
                                self.is_private = True
                            except Exception as e:
                                logging.info("Error refreshing URL: {0}".format(e))
                                print("Error refreshing URL: {0}".format(e))
                            
                            # If status of downloader is not live, assume stream has ended
                            if self.live_status != 'is_live':
                                print("Livestream has ended, committing any remaining segments")
                                #self.catchup()
                                break
                            
                            # If live has changed, use new URL to get any fragments that may be missing
                            elif self.live_status == 'is_live' and live_status is not None and live_status != 'is_live':
                                print("Stream has finished ({0})".format(live_status))
                                self.live_status = live_status
                                stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=self.format, return_format=False) 
                                if stream_url is not None:
                                    self.stream_url = stream_url  
                                    self.refresh_retries = 0
                                #self.catchup()
                                break
                            
                            # If livestream is still live, use new url
                            elif live_status == 'is_live':
                                print("Updating url to new url")
                                stream_url = None
                                
                                # Check for new manifest, if it has, start a nested download session
                                if self.detect_manifest_change(info_json=info_dict) is True:
                                    break
                                else:
                                    stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=self.format, return_format=False)
                                if stream_url is not None:
                                    self.stream_url = stream_url  
                                    self.stream_urls.append(stream_url)
                                    self.refresh_retries = 0
                            
                            if info_dict:
                                self.info_dict = info_dict                                                   
                    time.sleep(10)
                    continue
                
                elif len(segments_to_download) > 0 and self.is_private and len(submitted_segments) > 0:
                    print("Video is private, waiting for remaining threads to finish before going to stream recovery")
                    time.sleep(5)
                    continue
                elif len(segments_to_download) > 0 and self.is_private:
                    print("Video is private and still has segments remaining, moving to stream recovery")
                    self.commit_batch(self.conn)
                    self.close_connection()
                    
                    for i in range(5, 0, -1):
                        print("Waiting {0} minutes before starting stream recovery to improve chances of success".format(i))
                        time.sleep(60)
                    print("Sending stream URLs of {0} to stream recovery: {1}".format(self.format, self.stream_urls))
                    downloader = StreamRecovery(info_dict=self.info_dict, resolution=self.format, batch_size=self.batch_size, max_workers=max((self.recovery_thread_multiplier*self.max_workers*int(len(self.stream_urls))),self.recovery_thread_multiplier), file_name=self.file_base_name, cookies=self.cookies, fragment_retries=self.fragment_retries, stream_urls=self.stream_urls)
                    downloader.live_dl()
                    downloader.close_connection()
                    return True
                else:
                    wait = 0
                    
                # Check if segments already exist within database (used to not create more connections). Needs fixing upstream
                existing = set()
                for seg_num in segments_to_download:
                    if self.segment_exists(self.cursor, seg_num):
                        self.already_downloaded.add(seg_num)
                        existing.add(seg_num)
                segments_to_download = segments_to_download - existing
                


                # Add new threads to existing future dictionary, done directly to almost half RAM usage from creating new threads
                
                for seg_num in segments_to_download:
                    if seg_num not in submitted_segments:
                        future_to_seg.update({
                            executor.submit(self.download_segment, "{0}&sq={1}".format(self.stream_url, seg_num), seg_num): seg_num
                        })
                        submitted_segments.add(seg_num)
                    # Have up to 2x max workers of threads submitted
                    if len(future_to_seg) > 2*self.max_workers:
                        break
                
            self.commit_batch(self.conn)
        self.commit_batch(self.conn)
        return True

    def update_latest_segment(self):
        # Kill if keyboard interrupt is detected
        self.check_kill()
        
        stream_url_info = self.get_Headers(self.stream_url)
        if stream_url_info is not None and stream_url_info.get("X-Head-Seqnum", None) is not None:
            self.latest_sequence = int(stream_url_info.get("X-Head-Seqnum"))
            print("Latest sequence: {0}".format(self.latest_sequence))
            
        if stream_url_info is not None and stream_url_info.get('Content-Type', None) is not None:
            self.type, self.ext = str(stream_url_info.get('Content-Type')).split('/')
    
    def get_Headers(self, url):
        try:
            # Send a GET request to a URL
            response = requests.get(url, timeout=30)
            # 200 and 204 responses appear to have valid headers so far
            if response.status_code == 200 or response.status_code == 204:
                self.is_403 = False
                # Print the response headers
                #print(json.dumps(dict(response.headers), indent=4))  
                return response.headers
            elif response.status_code == 403:
                print("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                return None
            else:
                print("Error retrieving headers: {0}".format(response.status_code))
                print(json.dumps(dict(response.headers), indent=4))
                return None
            
        except requests.exceptions.Timeout as e:
            logging.info("Timed out updating fragments: {0}".format(e))
            print(e)
            return None
    
    def detect_manifest_change(self, info_json):
        if YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.format, return_format=False) is not None:
            temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.format, return_format=False)
            parsed_url = urlparse(temp_stream_url)        
            temp_url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}
            if temp_url_params.get("id", None) is not None and temp_url_params.get("id") != self.url_params.get("id"):
                print("New manifest for format {0} detected, starting a new instance for the new manifest".format(self.format))
                self.commit_batch()
                download_stream(info_dict=info_json, resolution=self.format, batch_size=self.batch_size, max_workers=self.max_workers, file_name="{0}.{1}".format(self.file_base_name, str(temp_url_params.get("id")).split('.')[-1]), keep_database=False, reties=self.fragment_retries, cookies=self.cookies)
                return True
            else:
                return False
        elif YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.resolution, return_format=False) is not None:
            temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.resolution, return_format=False)
            parsed_url = urlparse(temp_stream_url)        
            temp_url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}
            if temp_url_params.get("id", None) is not None and temp_url_params.get("id") != self.url_params.get("id"):
                print("New manifest for resolution {0} detected, but not the same format as {1}, starting a new instance for the new manifest".format(self.resolution, self.format))
                self.commit_batch()
                download_stream(info_dict=info_json, resolution=self.resolution, batch_size=self.batch_size, max_workers=self.max_workers, file_name="{0}.{1}".format(self.file_base_name, str(temp_url_params.get("id")).split('.')[-1]), keep_database=False, reties=self.fragment_retries, cookies=self.cookies)
                return True
        elif self.resolution != "audio_only" and YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution="best", return_format=False) is not None:
            temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution="best", return_format=False)
            parsed_url = urlparse(temp_stream_url)        
            temp_url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}
            if temp_url_params.get("id", None) is not None and temp_url_params.get("id") != self.url_params.get("id"):
                print("New manifest has been found, but it is not the same format or resolution".format(self.resolution, self.format))
                self.commit_batch()
                download_stream(info_dict=info_json, resolution="best", batch_size=self.batch_size, max_workers=self.max_workers, file_name="{0}.{1}".format(self.file_base_name, str(temp_url_params.get("id")).split('.')[-1]), keep_database=False, reties=self.fragment_retries, cookies=self.cookies)
                return True
        return False

    def create_connection(self, file):
        conn = sqlite3.connect(file)
        cursor = conn.cursor()
        
        # Database connection optimisation. Benefits will need to be tested
        if not self.database_in_memory:
            # Set the journal mode to WAL
            cursor.execute('PRAGMA journal_mode = WAL;')        
            # Set the synchronous mode to NORMAL
            cursor.execute('PRAGMA synchronous = NORMAL;')
            # Increase page size to help with large blobs
            cursor.execute('pragma page_size = 32768;')
        
        return conn, cursor
    
    def create_db(self, temp_file):
        # Connect to SQLite database (or create it if it doesn't exist)
        conn, cursor = self.create_connection(temp_file)
        
        # Create the table where id represents the segment order
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS segments (
            id INTEGER PRIMARY KEY, 
            segment_data BLOB
        )
        ''')
        conn.commit()
        return conn, cursor

    # Function to check if a segment exists in the database
    def segment_exists(self, cursor, segment_order):
        cursor.execute('SELECT 1 FROM segments WHERE id = ?', (segment_order,))
        return cursor.fetchone() is not None
    
    def segment_exists_batch(self):
        """
        Queries the database to check if a batch of segment numbers are already downloaded.
        Returns a set of existing segment numbers.
        """
        query = "SELECT id FROM segments"
        self.cursor.execute(query)
        return set(row[0] for row in self.cursor.fetchall())

    # Function to download a single segment
    def download_segment(self, segment_url, segment_order):
        self.check_kill()
        time.sleep(120)
        try:
            # create an HTTP adapter with the retry strategy and mount it to the session
            adapter = HTTPAdapter(max_retries=self.retry_strategy)
            # create a new session object
            session = requests.Session()
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            response = session.get(segment_url, timeout=30)
            if response.status_code == 200:
                print("Downloaded segment {0} of {1} to memory...".format(segment_order, self.format))
                self.is_403 = False
                #return latest header number and segmqnt content
                return int(response.headers.get("X-Head-Seqnum", -1)), response.content, int(segment_order), response.status_code, response.headers  # Return segment order and data
            elif response.status_code == 403:
                print("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                return -1, None, segment_order, response.status_code, response.headers
            else:
                print("Error downloading segment {0}: {1}".format(segment_order, response.status_code))
                return -1, None, segment_order, response.status_code, response.headers
        except requests.exceptions.Timeout as e:
            logging.info("Fragment timeout {1}: {0}".format(e, segment_order))
            print(e)
            return -1, None, segment_order, None, None
        except requests.exceptions.RetryError as e:
            logging.info("Retries exceeded downloading fragment: {0}".format(e))
            print("Retries exceeded downloading fragment: {0}".format(e))
            if "(Caused by ResponseError('too many 204 error responses')" in str(e):
                return -1, bytes(), segment_order, 204, None
            elif "(Caused by ResponseError('too many 403 error responses')" in str(e):
                self.is_403 = True
                return -1, None, segment_order, 403, None
            else:
                return -1, None, segment_order, None, None
        except requests.exceptions.ChunkedEncodingError as e:
            logging.info("No data in request for fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("No data in request for fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, bytes(), segment_order, None, None
        except requests.exceptions.ConnectionError as e:
            logging.info("Connection error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("Connection error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.Timeout as e:
            logging.info("Timeout while retrieving downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("Timeout while retrieving downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.HTTPError as e:
            logging.info("HTTP error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("HTTP error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except Exception as e:
            logging.info("Unknown error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("Unknown error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
            
    # Function to insert a single segment without committing
    def insert_single_segment(self, cursor, segment_order, segment_data):

        cursor.execute('''
            INSERT INTO segments (id, segment_data) 
            VALUES (?, ?) 
            ON CONFLICT(id) 
            DO UPDATE SET segment_data = CASE 
                WHEN LENGTH(excluded.segment_data) > LENGTH(segments.segment_data) 
                THEN excluded.segment_data 
                ELSE segments.segment_data 
            END;
        ''', (segment_order, segment_data))


    # Function to commit after a batch of inserts
    def commit_batch(self, conn):
        conn.commit()
        
    def close_connection(self):
        self.conn.close()

    # Function to combine segments into a single file
    def combine_segments_to_file(self, output_file, cursor=None):
        if cursor is None:
            cursor = self.cursor
        
        print("Merging segments to {0}".format(output_file))
        with open(output_file, 'wb') as f:
            cursor.execute('SELECT segment_data FROM segments ORDER BY id')
            for segment in cursor:  # Cursor iterates over rows one by one
                segment_piece = segment[0]
                # Clean each segment if required as ffmpeg sometimes doesn't like the segments from YT
                cleaned_segment = self.remove_sidx(segment_piece)
                f.write(cleaned_segment)
        return output_file
    
    ### Via ytarchive            
    def get_atoms(self, data):
        """
        Get the name of top-level atoms along with their offset and length
        In our case, data should be the first 5kb - 8kb of a fragment

        :param data:
        """
        atoms = {}
        ofs = 0

        while True:
            # We should be fine and not run into errors, but I do dumb things
            try:
                alen = int(data[ofs:ofs + 4].hex(), 16)
                if alen > len(data):
                    break

                aname = data[ofs + 4:ofs + 8].decode()
                atoms[aname] = {"ofs": ofs, "len": alen}
                ofs += alen
            except Exception:
                break

            if ofs + 8 >= len(data):
                break

        return atoms

    ### Via ytarchive  
    def remove_sidx(self, data):
        """
        Remove the sidx atom from a chunk of data

        :param data:
        """
        atoms = self.get_atoms(data)
        if not "sidx" in atoms:
            return data

        sidx = atoms["sidx"]
        ofs = sidx["ofs"]
        rlen = sidx["ofs"] + sidx["len"]
        new_data = data[:ofs] + data[rlen:]

        return new_data
    
    def check_kill(self):
        # Kill if keyboard interrupt is detected
        if kill_all:
            print("Kill command detected, ending thread")
            raise KeyboardInterrupt("Kill command executed")
        
    def delete_temp_database(self):
        self.close_connection()
        os.remove(self.temp_db_file)
        
    def delete_ts_file(self):
        os.remove(self.merged_file_name)
        
    def remove_folder(self):
        if self.folder:
            self.delete_temp_database()
            self.delete_ts_file()
            os.remove(self.folder)
            
class DownloadStreamDirect:
    def __init__(self, info_dict, resolution='best', batch_size=10, max_workers=5, fragment_retries=5, folder=None, file_name=None, cookies=None):        
        
        self.latest_sequence = -1
        self.already_downloaded = set()
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        self.id = info_dict.get('id')
        self.live_status = info_dict.get('live_status')
        
        self.resolution=resolution
        
        self.stream_url, self.format = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution, return_format=True) 
        
        if self.stream_url is None:
            raise ValueError("Stream URL not found for {0}, unable to continue".format(resolution))
        
        if file_name is None:
            file_name = self.id    
            
        self.file_base_name = file_name
        
        self.merged_file_name = "{0}.{1}.ts".format(file_name, self.format)  
        
        self.state_file_name = "{0}.{1}.state".format(self.id, self.format)        
        self.state_file_backup = "{0}.bkup".format(self.state_file_name)
        
        self.folder = folder    
        if self.folder:
            os.makedirs(folder, exist_ok=True)
            self.file_base_name = os.path.join(self.folder, self.file_base_name)
            self.merged_file_name = os.path.join(self.folder, self.merged_file_name)
            self.state_file_name = os.path.join(self.folder, self.state_file_name)
            self.state_file_backup = os.path.join(self.folder, self.state_file_backup)
            
        self.state = {
            'last_written': -1,
            'file_size': 0
        }
        
        if os.path.exists(self.state_file_backup) and os.path.exists(self.merged_file_name):
            with open(self.state_file_backup, "r") as file:
                loaded_state = json.load(file)
            ts_size = os.path.getsize(self.merged_file_name)
            if ts_size >= loaded_state.get('file_size', 0) and loaded_state.get('last_written', None) is not None:
                self.state = loaded_state
            print(self.state)
        elif os.path.exists(self.state_file_name) and os.path.exists(self.merged_file_name):
            with open(self.state_file_name, "r") as file:
                loaded_state = json.load(file)
            ts_size = os.path.getsize(self.merged_file_name)
            if ts_size >= loaded_state.get('file_size', 0) and loaded_state.get('last_written', None) is not None:
                self.state = loaded_state
            print(self.state)
        
        self.fragment_retries=fragment_retries
        
        self.retry_strategy = Retry(
            total=fragment_retries,  # maximum number of retries
            backoff_factor=1, 
            status_forcelist=[204, 400, 401, 403, 404, 408, 429, 500, 502, 503, 504],  # the HTTP status codes to retry on
        )   
        
             
        
        self.is_403 = False
        self.is_private = False
        self.estimated_segment_duration = 0
        self.refresh_retries = 0
        
        self.cookies = cookies
        
        self.type = None
        self.ext = None        
        
        self.update_latest_segment()
        self.url_checked = time.time()   
        self.stream_urls = [self.stream_url]
        
    def get_expire_time(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        # Get the 'expire' parameter
        expire_value = query_params.get('expire', [None])[0]
        if expire_value is not None:
            return int(expire_value)
        return expire_value

    def refresh_Check(self):    
        
        #print("Refresh check ({0})".format(self.format))  
        
        # By this stage, a stream would have a URL. Keep using it if the video becomes private or a membership      
        if (time.time() - self.url_checked >= 3600.0 or self.is_403) and not self.is_private:
            print("Refreshing URL for {0}".format(self.format))
            try:
                info_dict, live_status = getUrls.get_Video_Info(self.id, wait=False, cookies=self.cookies)
                
                # Check for new manifest, if it has, start a nested download session
                if self.detect_manifest_change(info_json=info_dict) is True:
                    return True
                
                stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=self.format, return_format=False) 
                if stream_url is not None:
                    self.stream_url = stream_url
                    self.stream_urls.append(stream_url)
                    filtered_array = [url for url in self.stream_urls if int(self.get_expire_time(url)) < time.time()]
                    self.stream_urls = filtered_array
                if live_status is not None:
                    self.live_status = live_status
            except PermissionError as e:
                print(e)
                self.is_private = True
            except Exception as e:
                print(e)                       
            self.url_checked = time.time()
                
    def live_dl(self):
        
        print("\033[31mStarting download of live fragments ({0})\033[0m".format(self.format))
        wait = 0   
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="{0}-{1}".format(self.id,self.format)) as executor:
            submitted_segments = set()
            future_to_seg = {}
            
            downloaded_segments = {}
            
            # Trackers for optimistic segment downloads 
            optimistic = True
            optimistic_seg = 0
            
            while True:     
                self.check_kill()
                if self.refresh_Check() is True:
                    break
                        
                # Process completed segment downloads, wait up to 5 seconds for segments to complete before next loop
                done, _ = concurrent.futures.wait(future_to_seg, timeout=5, return_when=concurrent.futures.ALL_COMPLETED)  # need to fully determine if timeout or ALL_COMPLETED takes priority             
                
                for future in done:
                    head_seg_num, segment_data, seg_num, status, headers = future.result()
                    
                    # Remove from submitted segments in case it neeeds to be regrabbed
                    submitted_segments.remove(seg_num)
                    
                    # If successful in downloading segments optimistically, continue doing so
                    if seg_num >= self.latest_sequence and (status is None or status != 200):
                        print("Unable to optimistically grab segment {1} for {0}".format(self.format, seg_num))
                        optimistic = False
                    else: 
                        optimistic = True
                    
                    if head_seg_num > self.latest_sequence:
                        print("More segments available: {0}, previously {1}".format(head_seg_num, self.latest_sequence))                    
                        self.latest_sequence = head_seg_num
                        
                    if headers is not None and headers.get("X-Head-Time-Sec", None) is not None:
                        self.estimated_segment_duration = int(headers.get("X-Head-Time-Sec"))/self.latest_sequence

                    if segment_data is not None:
                        downloaded_segments[seg_num] = segment_data
                                            
                    # Remove completed thread to free RAM
                    del future_to_seg[future]
                
                if downloaded_segments.get(self.state.get('last_written') + 1, None) is not None:
                    # If filesize is 0 or the placeholder for not existing (-1), start in write mode
                    if self.state.get('file_size', 0) <= 0:
                        mode = 'wb'
                    else:
                        mode = "r+b"
                    with open(self.merged_file_name, mode) as file:
                        if mode != 'wb':
                            file.seek(self.state.get('file_size'))
                        # Look for written segment +1 for up to the downloaded dictionary times to write any downloaded segments to the ts file
                        for _ in range(0, len(downloaded_segments)):
                            segment = downloaded_segments.pop(self.state.get('last_written') + 1, None)
                            if segment is not None:
                                cleaned_segment = self.remove_sidx(segment)
                                file.write(cleaned_segment)
                                self.state['last_written'] = self.state.get('last_written') + 1
                            else:
                                continue
                        # Truncate file in case writing occurred before end of file
                        file.truncate()
                    # Small sleep to give best chance that file size will be updated
                    time.sleep(0.1)
                    
                    self.state['file_size'] = os.path.getsize(self.merged_file_name)
                    if os.path.exists(self.state_file_name):
                        shutil.move(self.state_file_name, self.state_file_backup)
                    with open(self.state_file_name, "w") as file:
                        json.dump(self.state, file, indent=4)
                        
                    print("Written {0} segments of {1} to file. Current file size is {2} bytes".format(self.state.get('last_written'),self.format, self.state.get('file_size')))
                    
                    # Cleanup of leftover segments
                    # Needs to be determined if these are accidental extras or missed segments
                    if len(downloaded_segments) > 0:
                        seg_keys = list(downloaded_segments.keys())
                        for seg_key in seg_keys:
                            if self.state.get('last_written') - int(seg_key) > self.max_workers*2:
                                print("Segment {0} of {1} has been detected as leftover, removing from dictionary".format(seg_key, self.format))
                                del downloaded_segments[seg_key]
                
                segments_to_download = set(range(self.state.get('last_written')+1, self.latest_sequence)) - submitted_segments 
                                               
                # If segments remain to download, don't bother updating and wait for segment download to refresh values.
                if len(segments_to_download) <= 0:
                    
                    # Only attempt to grab optimistic segment once to ensure it does not cause a loop at the end of a stream
                    if optimistic and optimistic_seg <= self.latest_sequence and (self.latest_sequence+1) not in submitted_segments:
                        optimistic_seg = (self.latest_sequence+1)
                        
                        # Wait estimated fragment time +0.1s to make sure it would exist
                        time.sleep(self.estimated_segment_duration + 0.1)
                        
                        print("Adding segment {1} optimistically ({0})".format(self.format, optimistic_seg))
                        segments_to_download.add(optimistic_seg)
                        
                    # If optimistic grab is not successful, revert back to using headers from base stream URL
                    else:
                        print("Checking for more segments available for {0}".format(self.format))
                        self.update_latest_segment()
                        segments_to_download = set(range(self.state.get('last_written')+1, self.latest_sequence)) - submitted_segments                             
                        
                # If update has no segments and no segments are currently running, wait                              
                if len(segments_to_download) <= 0 and len(future_to_seg) <= 0:                 
                    wait += 1
                    print("No new fragments available for {0}, attempted {1} times...".format(self.format, wait))
                        
                    # If waited for new fragments hits 20 loops, assume stream is offline
                    if wait > 20:
                        print("Wait time for new fragment exceeded, ending download...")
                        break    
                    # If over 10 wait loops have been executed, get page for new URL and update status if necessary
                    elif wait > 10:
                        if self.is_private:
                            print("Video is private and no more segments are available. Ending...")
                            break
                        else:
                            print("No new fragments found... Getting new url")
                            info_dict = None
                            live_status = None
                            try:
                                info_dict, live_status = getUrls.get_Video_Info(self.id, wait=False, cookies=self.cookies)
                                
                            # If membership stream (without cookies) or privated, mark as end of stream as no more fragments can be grabbed
                            except PermissionError as e:
                                print(e)
                                self.is_private = True
                            except Exception as e:
                                logging.info("Error refreshing URL: {0}".format(e))
                                print("Error refreshing URL: {0}".format(e))
                            
                            # If status of downloader is not live, assume stream has ended
                            if self.live_status != 'is_live':
                                print("Livestream has ended, committing any remaining segments")
                                #self.catchup()
                                break
                            
                            # If live has changed, use new URL to get any fragments that may be missing
                            elif self.live_status == 'is_live' and live_status is not None and live_status != 'is_live':
                                print("Stream has finished ({0})".format(live_status))
                                self.live_status = live_status
                                stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=self.format, return_format=False) 
                                if stream_url is not None:
                                    self.stream_url = stream_url  
                                    self.refresh_retries = 0
                                #self.catchup()
                                break
                            
                            # If livestream is still live, use new url
                            elif live_status == 'is_live':
                                print("Updating url to new url")
                                stream_url = None
                                # Check for new manifest, if it has, start a nested download session
                                if self.detect_manifest_change(info_json=info_dict) is True:
                                    break
                                else:
                                    stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=self.format, return_format=False)
                                if stream_url is not None:
                                    self.stream_url = stream_url  
                                    self.refresh_retries = 0
                        
                    time.sleep(5)
                    continue
                else:
                    wait = 0
                    
                # Add new threads to existing future dictionary, done directly to almost half RAM usage from creating new threads
                future_to_seg.update(
                    {
                        executor.submit(self.download_segment, "{0}&sq={1}".format(self.stream_url, seg_num), seg_num): seg_num
                        for seg_num in segments_to_download
                        if seg_num not in submitted_segments and not submitted_segments.add(seg_num)
                    }
                )
                
        return self.merged_file_name

    def update_latest_segment(self):
        # Kill if keyboard interrupt is detected
        self.check_kill()
        
        stream_url_info = self.get_Headers(self.stream_url)
        if stream_url_info is not None and stream_url_info.get("X-Head-Seqnum", None) is not None:
            self.latest_sequence = int(stream_url_info.get("X-Head-Seqnum"))
            print("Latest sequence: {0}".format(self.latest_sequence))
            
        if stream_url_info is not None and stream_url_info.get('Content-Type', None) is not None:
            self.type, self.ext = str(stream_url_info.get('Content-Type')).split('/')
    
    def get_Headers(self, url):
        try:
            # Send a GET request to a URL
            response = requests.get(url, timeout=30)
            # 200 and 204 responses appear to have valid headers so far
            if response.status_code == 200 or response.status_code == 204:
                self.is_403 = False
                # Print the response headers
                #print(json.dumps(dict(response.headers), indent=4))  
                return response.headers
            elif response.status_code == 403:
                print("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                return None
            else:
                print("Error retrieving headers: {0}".format(response.status_code))
                print(json.dumps(dict(response.headers), indent=4))
                return None
            
        except requests.exceptions.Timeout as e:
            logging.info("Timed out updating fragments: {0}".format(e))
            print(e)
            return None

    # Function to download a single segment
    def download_segment(self, segment_url, segment_order):
        self.check_kill()
        try:
            # create an HTTP adapter with the retry strategy and mount it to the session
            adapter = HTTPAdapter(max_retries=self.retry_strategy)
            # create a new session object
            session = requests.Session()
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            response = session.get(segment_url, timeout=30)
            if response.status_code == 200:
                print("Downloaded segment {0} of {1} to memory...".format(segment_order, self.format))
                self.is_403 = False
                #return latest header number and segmqnt content
                return int(response.headers.get("X-Head-Seqnum", -1)), response.content, int(segment_order), response.status_code, response.headers  # Return segment order and data
            elif response.status_code == 403:
                print("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                return -1, None, segment_order, response.status_code, response.headers
            else:
                print("Error downloading segment {0}: {1}".format(segment_order, response.status_code))
                return -1, None, segment_order, response.status_code, response.headers
        except requests.exceptions.Timeout as e:
            logging.info("Fragment timeout {1}: {0}".format(e, segment_order))
            print(e)
            return -1, None, segment_order, None, None
        except requests.exceptions.RetryError as e:
            logging.info("Retries exceeded downloading fragment: {0}".format(e))
            print("Retries exceeded downloading fragment: {0}".format(e))
            if "(Caused by ResponseError('too many 204 error responses')" in str(e):
                return -1, bytes(), segment_order, 204, None
            #elif "(Caused by ResponseError('too many 403 error responses')" in str(e):
            #    self.is_403 = True
            #    return -1, None, segment_order, 403, None
            else:
                return -1, None, segment_order, None, None
        except requests.exceptions.ChunkedEncodingError as e:
            logging.info("No data in request for fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("No data in request for fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, bytes(), segment_order, None, None
        except requests.exceptions.ConnectionError as e:
            logging.info("Connection error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("Connection error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.Timeout as e:
            logging.info("Timeout while retrieving downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("Timeout while retrieving downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.HTTPError as e:
            logging.info("HTTP error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("HTTP error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except Exception as e:
            logging.info("Unknown error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("Unknown error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
            
    def detect_manifest_change(self, info_json):
        if YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.format, return_format=False) is not None:
            temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.format, return_format=False)
            parsed_url = urlparse(temp_stream_url)        
            temp_url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}
            if temp_url_params.get("id", None) is not None and temp_url_params.get("id") != self.url_params.get("id"):
                print("New manifest for format {0} detected, starting a new instance for the new manifest".format(self.format))
                self.commit_batch()
                download_stream_direct(info_dict=info_json, resolution=self.format, batch_size=self.batch_size, max_workers=self.max_workers, file_name="{0}.{1}".format(self.file_base_name, str(temp_url_params.get("id")).split('.')[-1]), reties=self.fragment_retries, cookies=self.cookies)
                return True
            else:
                return False
        elif YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.resolution, return_format=False) is not None:
            temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.resolution, return_format=False)
            parsed_url = urlparse(temp_stream_url)        
            temp_url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}
            if temp_url_params.get("id", None) is not None and temp_url_params.get("id") != self.url_params.get("id"):
                print("New manifest for resolution {0} detected, but not the same format as {1}, starting a new instance for the new manifest".format(self.resolution, self.format))
                self.commit_batch()
                download_stream_direct(info_dict=info_json, resolution=self.resolution, batch_size=self.batch_size, max_workers=self.max_workers, file_name="{0}.{1}".format(self.file_base_name, str(temp_url_params.get("id")).split('.')[-1]), reties=self.fragment_retries, cookies=self.cookies)
                return True
        elif self.resolution != "audio_only" and YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution="best", return_format=False) is not None:
            temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution="best", return_format=False)
            parsed_url = urlparse(temp_stream_url)        
            temp_url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}
            if temp_url_params.get("id", None) is not None and temp_url_params.get("id") != self.url_params.get("id"):
                print("New manifest has been found, but it is not the same format or resolution".format(self.resolution, self.format))
                self.commit_batch()
                download_stream_direct(info_dict=info_json, resolution="best", batch_size=self.batch_size, max_workers=self.max_workers, file_name="{0}.{1}".format(self.file_base_name, str(temp_url_params.get("id")).split('.')[-1]), reties=self.fragment_retries, cookies=self.cookies)
                return True
        return False
    # Function to combine segments into a single file
    def combine_segments_to_file(self, output_file, cursor=None):
        if cursor is None:
            cursor = self.cursor
        
        print("Merging segments to {0}".format(output_file))
        with open(output_file, 'wb') as f:
            cursor.execute('SELECT segment_data FROM segments ORDER BY id')
            for segment in cursor:  # Cursor iterates over rows one by one
                segment_piece = segment[0]
                # Clean each segment if required as ffmpeg sometimes doesn't like the segments from YT
                cleaned_segment = self.remove_sidx(segment_piece)
                f.write(cleaned_segment)
        return output_file
    
    ### Via ytarchive            
    def get_atoms(self, data):
        """
        Get the name of top-level atoms along with their offset and length
        In our case, data should be the first 5kb - 8kb of a fragment

        :param data:
        """
        atoms = {}
        ofs = 0

        while True:
            # We should be fine and not run into errors, but I do dumb things
            try:
                alen = int(data[ofs:ofs + 4].hex(), 16)
                if alen > len(data):
                    break

                aname = data[ofs + 4:ofs + 8].decode()
                atoms[aname] = {"ofs": ofs, "len": alen}
                ofs += alen
            except Exception:
                break

            if ofs + 8 >= len(data):
                break

        return atoms

    ### Via ytarchive  
    def remove_sidx(self, data):
        """
        Remove the sidx atom from a chunk of data

        :param data:
        """
        atoms = self.get_atoms(data)
        if not "sidx" in atoms:
            return data

        sidx = atoms["sidx"]
        ofs = sidx["ofs"]
        rlen = sidx["ofs"] + sidx["len"]
        new_data = data[:ofs] + data[rlen:]

        return new_data
    
    def check_kill(self):
        # Kill if keyboard interrupt is detected
        if kill_all:
            print("Kill command detected, ending thread")
            raise KeyboardInterrupt("Kill command executed")
        
    def delete_state_file(self):
        if os.path.exists(self.state_file_name):
            os.remove(self.state_file_name)
        if os.path.exists(self.state_file_backup):
            os.remove(self.state_file_backup) 
        
    def delete_ts_file(self):
        os.remove(self.merged_file_name)
        
    def remove_folder(self):
        if self.folder:
            self.delete_temp_database()
            self.delete_ts_file()
            os.remove(self.folder)
            
class StreamRecovery:
    
    def __init__(self, info_dict={}, resolution='best', batch_size=10, max_workers=5, fragment_retries=5, folder=None, file_name=None, database_in_memory=False, cookies=None, recovery=False, segment_retry_time=30, stream_urls=[], live_status="is_live"):        
        from datetime import datetime
        self.latest_sequence = -1
        self.already_downloaded = set()
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # If info.json is defined, use the value within that, otherwise attempt to extracct ID from the first stream URL
        self.id = info_dict.get('id', self.get_id_from_url(stream_urls[0]) if stream_urls else None)

        # Use info.json if available, otherwise try using passed live_status value (default is_live)
        self.live_status = info_dict.get('live_status', live_status)
        
        #print("Stream recovery info dict: {0}".format(info_dict))
        #print("Stream recovery format: {0}".format(resolution))
        
        
        # If stream URLs are given, use them to get the format and also try to extract any URLs from the info.json too. If no stream URLs are passed, use the given resolution and the info.json only               
        if stream_urls:
            print("{0} stream urls available".format(len(stream_urls)))
            for url in stream_urls:
                self.format = self.get_format_from_url(url)
                if self.format is not None:
                    print("Stream recovery - Found format {0} from itags".format(self.format))
                    break            
            self.stream_urls = stream_urls          
        else:
            self.stream_urls, self.format = YoutubeURL.Formats().getAllFormatURL(info_json=info_dict, resolution=resolution, return_format=True) 
        
        print("Recovery - Resolution: {0}, Format: {1}".format(resolution, self.format))
        """        
        if stream_urls:
            if not self.stream_urls:
                self.stream_urls = []
            self.stream_urls = list(set(self.stream_urls) | set(stream_urls))
        """
        if self.stream_urls is None:
            raise ValueError("Stream URL not found for {0}, unable to continue".format(resolution))
        
        print("Number of stream URLs available: {0}".format(len(self.stream_urls)))
        self.stream_url = random.choice(self.stream_urls)
        
        self.database_in_memory = database_in_memory
        
        if file_name is None:
            file_name = self.id    
        
        self.file_base_name = file_name
        
        self.merged_file_name = "{0}.{1}.ts".format(file_name, self.format)     
        if self.database_in_memory:
            self.temp_db_file = ':memory:'
        else:
            self.temp_db_file = '{0}.{1}.temp'.format(file_name, self.format)
        
        self.folder = folder    
        if self.folder:
            os.makedirs(folder, exist_ok=True)
            self.merged_file_name = os.path.join(self.folder, self.merged_file_name)
            self.file_base_name = os.path.join(self.folder, self.file_base_name)
            if not self.database_in_memory:
                self.temp_db_file = os.path.join(self.folder, self.temp_db_file)
        
        self.retry_strategy = self.CustomRetry(
            total=3,  # maximum number of retries
            backoff_factor=1, 
            status_forcelist=[204, 400, 401, 403, 404, 408, 429, 500, 502, 503, 504],  # the HTTP status codes to retry on
            downloader_instance=self,
            retry_time_clamp=4
        )  
        
        self.fragment_retries = fragment_retries  
        self.segment_retry_time = segment_retry_time  
        
        self.is_403 = False
        self.is_401 = False
        self.is_private = False
        self.estimated_segment_duration = 0
        self.refresh_retries = 0
        self.recover = recovery
        
        self.sequential = False
        
        self.count_400s = 0
        
        self.sleep_time = 1
        
        self.cookies = cookies
        
        self.type = None
        self.ext = None        
        
        self.expires = None
        expires = []
        for url in self.stream_urls:
            expire_value = self.get_expire_time(url)
            if expire_value is not None:
                expires.append(int(expire_value))
        if expires:
            self.expires = int(max(expires))
            
        if time.time() > self.expires:
            
            print("\033[31mCurrent time is beyond highest expire time, unable to recover\033[0m".format(self.format))
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            format_exp = datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')
            raise TimeoutError("Current time {0} exceeds latest URL expiry time of {1}".format(now, format_exp))
            
        self.update_latest_segment()
        
        
        self.url_checked = time.time()

        self.conn, self.cursor = self.create_db(self.temp_db_file) 
        
        self.count_403s = {}        
        self.user_agent_403s = {}
        self.user_agent_full_403s = {}
        
    def get_expire_time(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        # Get the 'expire' parameter
        expire_value = query_params.get('expire', [-1])[0]
        if expire_value >= 0:
            return int(expire_value)
        return expire_value
    
    def get_format_from_url(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        print(query_params)
        # Get the 'expire' parameter
        print("Itags from url: {0}".format(query_params.get("itag", [None])))
        itag = query_params.get("itag", [None])[0]
        return str(itag).strip()
    
    def get_id_from_url(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        id = str(query_params.get("id", [None])[0])[:11].strip()
        return id
                
    def live_dl(self):
        #from itertools import groupby
        print("\033[31mStarting download of live fragments ({0})\033[0m".format(self.format))
        self.already_downloaded = self.segment_exists_batch()
        #wait = 0   
        self.cursor.execute('BEGIN TRANSACTION')
        uncommitted_inserts = 0     
        
        self.sleep_time = max(self.estimated_segment_duration, 0.1)
        
        # Track retries of all missing segments in database      
        self.segments_retries = {key: {'retries': 0, 'last_retry': 0, 'ideal_retry_time': random.uniform(max(self.segment_retry_time,900),max(self.segment_retry_time+300,1200))} for key in range(self.latest_sequence + 1) if key not in self.already_downloaded}
        segments_to_download = set(range(0, self.latest_sequence)) - self.already_downloaded  
        
        i = 0
        
        last_print = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="{0}-{1}".format(self.id,self.format)) as executor:
            submitted_segments = set()
            future_to_seg = {}
            
            # Trackers for optimistic segment downloads 
            if self.expires is not None:
                from datetime import datetime
                #print(datetime.fromtimestamp(int(self.expires)))
                print("Recovery mode active, URL expected to expire at {0}".format(datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')))
            else:
                print("Recovery mode active")
                       
            
            while True:     
                self.check_kill()                                        
                # Process completed segment downloads, wait up to 5 seconds for segments to complete before next loop
                done, not_done = concurrent.futures.wait(future_to_seg, timeout=0.1, return_when=concurrent.futures.ALL_COMPLETED)  # need to fully determine if timeout or ALL_COMPLETED takes priority             
                
                for future in done:
                    head_seg_num, segment_data, seg_num, status, headers = future.result()
                    
                    # Remove from submitted segments in case it neeeds to be regrabbed
                    if seg_num in submitted_segments:
                        submitted_segments.remove(seg_num)
                    
                    if head_seg_num > self.latest_sequence:
                        print("More segments available: {0}, previously {1}".format(head_seg_num, self.latest_sequence))        
                        self.segments_retries.update({key: {'retries': 0, 'last_retry': 0, 'ideal_retry_time': random.uniform(max(self.segment_retry_time,900),max(self.segment_retry_time+300,1200))} for key in range(self.latest_sequence, head_seg_num) if key not in self.already_downloaded})
                        self.latest_sequence = head_seg_num
                        
                    if headers is not None and headers.get("X-Head-Time-Sec", None) is not None:
                        self.estimated_segment_duration = int(headers.get("X-Head-Time-Sec"))/self.latest_sequence                       

                    if segment_data is not None:
                        # Insert segment data in the main thread (database interaction)
                        self.insert_single_segment(cursor=self.cursor, segment_order=seg_num, segment_data=segment_data)
                        uncommitted_inserts += 1
                        
                        # Assume segment will be added
                        if self.segments_retries.get(seg_num, None) is not None:
                            del self.segments_retries[seg_num]
                        
                        # If finished threads exceeds batch size, commit the whole batch of threads at once. 
                        # Has risk of not committing if a thread has no segment data, but this would be corrected naturally in following loop(s)
                        if uncommitted_inserts >= max(self.batch_size, len(done)):
                            print("Writing segments to file...")
                            self.commit_batch(self.conn)
                            uncommitted_inserts = 0
                            self.cursor.execute('BEGIN TRANSACTION') 
                    else:
                        if self.segments_retries.get(seg_num, None) is not None:
                            self.segments_retries[seg_num]['retries'] = self.segments_retries[seg_num]['retries'] + 1
                            self.segments_retries[seg_num]['last_retry'] = time.time()
                            if self.segments_retries[seg_num]['retries'] >= self.fragment_retries:
                                print("Segment {0} of {1} has exceeded maximum number of retries")
                    
                    # Remove completed thread to free RAM
                    del future_to_seg[future]
                      
                #segments_to_download = set(range(0, self.latest_sequence)) - self.already_downloaded    
                                       
                if len(self.segments_retries) <= 0:
                    print("All segment downloads complete, ending...")
                    break
                
                elif self.is_403 and self.expires is not None and time.time() > self.expires:
                    print("URL(s) have expired and failures being detected, ending...")
                    break
                
                elif all(value['retries'] > self.fragment_retries for value in self.segments_retries.values()):
                    print("All remaining segments have exceeded their retry count, ending...")
                    break
                
                elif self.is_401:
                    print("401s detected for {0}, sleeping for a minute")
                    time.sleep(60)
                    for url in self.stream_urls:
                        if self.live_status == 'post_live':
                            self.update_latest_segment(url="{0}&sq={1}".format(url, self.latest_sequence+1))
                        else:
                            self.update_latest_segment(url=url)
                # Request base url if receiving 403s
                elif self.is_403:
                    for url in self.stream_urls:
                        if self.live_status == 'post_live':
                            self.update_latest_segment(url="{0}&sq={1}".format(url, self.latest_sequence+1))
                        else:
                            self.update_latest_segment(url=url)
                    
                segments_to_download = set()
                potential_segments_to_download = set(self.segments_retries.keys()) - self.already_downloaded
                # Don't bother calculating if there are threads not done
                #if len(not_done) <= 0:      
                sorted_retries = -1
                if self.sequential:
                    # Create a dictionary sorted by number of retries, lowest first, then by segment number, lowest first
                    sorted_retries = dict(sorted(self.segments_retries.items(), key=lambda item: (item[1]['retries'], item[0])))
                else:
                    """
                    # Step 1: Sort the dictionary by `retries` first
                    sorted_retries = sorted(segments_retries.items(), key=lambda item: item[1]['retries'])

                    # Step 2: Group by `retries` and shuffle each group
                    grouped_and_shuffled = []
                    for _, group in groupby(sorted_retries, key=lambda item: item[1]['retries']):
                        group_list = list(group)
                        random.shuffle(group_list)  # Shuffle within the same `retries` count
                        grouped_and_shuffled.extend(group_list)

                    # Step 3: Convert back to a dictionary
                    sorted_retries = dict(grouped_and_shuffled)
                    """
                    current_time = time.time()
                    # Step 1: Separate items into two groups
                    priority_items = {
                        key: value for key, value in self.segments_retries.items()
                        if (current_time - value['last_retry']) > value['ideal_retry_time'] and value['retries'] > 0
                    }
                    non_priority_items = {
                        key: value for key, value in self.segments_retries.items()
                        if not ((current_time - value['last_retry']) > value['ideal_retry_time'] and value['retries'] > 0)
                    }

                    # Step 2: Sort each group (preserve keys)
                    priority_items_sorted = dict(sorted(priority_items.items(), key=lambda item: item[1]['retries']))
                    non_priority_items_sorted = dict(sorted(non_priority_items.items(), key=lambda item: item[1]['retries']))

                    # Combine the results
                    sorted_retries = priority_items_sorted | non_priority_items_sorted
                    
                    
                if sorted_retries != -1:
                    potential_segments_to_download = sorted_retries.keys()
                     
                #if len(not_done) < 1 or (len(not_done) < self.max_workers and not (self.is_403 or self.is_401)):
                if not not_done or len(not_done) < self.max_workers:
                #elif len(not_done) <= 0 and not self.is_401 and not self.is_403:
                    new_download = set()
                    number_to_add = self.max_workers - len(not_done)
                    """
                    if self.is_403:
                        number_to_add = self.max_workers - len(not_done)
                    else:
                        number_to_add = self.max_workers*2 - len(not_done)
                    """
                    for seg_num in potential_segments_to_download:
                        #print("{0}: {1} seconds since last retry".format(seg_num,time.time() - segments_retries[seg_num]['last_retry']))
                        if seg_num not in submitted_segments and self.segments_retries[seg_num]['retries'] < self.fragment_retries and time.time() - self.segments_retries[seg_num]['last_retry'] > self.segment_retry_time:                            
                            if seg_num in self.already_downloaded:
                                del self.segments_retries[seg_num]
                                continue
                            if self.segment_exists(self.cursor, seg_num):
                                self.already_downloaded.add(seg_num)
                                continue
                            new_download.add(seg_num)
                            print("Adding segment {0} of {2} with retries: {1}".format(seg_num, self.segments_retries[seg_num]['retries'], self.format))
                        if len(new_download) >= number_to_add:                            
                            break
                    segments_to_download = new_download
                    """
                    self.sleep_time = max(self.sleep_time/2, 1)    
                        
                elif (self.is_403 or self.is_401) and len(not_done) <= 0: 
                    new_download = set()
                    number_to_add = 1
                    for seg_num in segments_to_download:
                        if segments_retries[seg_num]['retries'] < self.fragment_retries and time.time() - segments_retries[seg_num]['last_retry'] > self.segment_retry_time:
                            new_download.add(seg_num)
                        if len(new_download) >= number_to_add:                            
                            break
                    segments_to_download = new_download                        
                    self.sleep_time = min(self.sleep_time*2, self.segment_retry_time)                
                    # Sleep to prevent 400 errors, may only be included if 400 errors occur   
                    print("Sleeping for {0}s before adding segment downloads".format(self.sleep_time))
                    #time.sleep(self.sleep_time) 
                
                if self.is_403 or self.is_401:
                    time.sleep(self.estimated_segment_duration)
                """    
                # New
                for seg_num in segments_to_download:
                    if seg_num not in submitted_segments:
                        future_to_seg[executor.submit(self.download_segment, "{0}&sq={1}".format(self.stream_urls[i % len(self.stream_urls)], seg_num), seg_num)] = seg_num
                        submitted_segments.add(seg_num)
                        i += 1
                        #time.sleep(0.25)
                '''
                # Old        
                for url in self.stream_urls:
                    future_to_seg.update(
                        {
                            executor.submit(self.download_segment, "{0}&sq={1}".format(url, seg_num), seg_num): seg_num
                            for seg_num in segments_to_download
                            if not submitted_segments.add(seg_num) and not time.sleep(0.25)
                        }
                    )
                '''
                if len(submitted_segments) == 0 and len(self.segments_retries) < 11 and time.time() - last_print > self.segment_retry_time:
                    print("{2} remaining segments for {1}: {0}".format(self.segments_retries, self.format, len(self.segments_retries)))
                    last_print = time.time()
                elif len(submitted_segments) == 0 and time.time() - last_print > 15:
                    print("{0} segments remain for {1}".format(len(self.segments_retries), self.format))
                    last_print = time.time()
                
            self.commit_batch(self.conn)
        self.commit_batch(self.conn)
        return len(self.segments_retries) <= 0

    def update_latest_segment(self, url=None):
        # Kill if keyboard interrupt is detected
        self.check_kill()
        
        # Remove expired URLs
        filtered_array = [url for url in self.stream_urls if int(self.get_expire_time(url)) > time.time()]
        
        if len(filtered_array) > 0:
            self.stream_urls = filtered_array
            expire_times = []
            for url in self.stream_urls:
                exp_time = self.get_expire_time(url)
                if exp_time:
                    expire_times.append(exp_time)
            self.expires = max(expire_times)
            
        if time.time() > self.expires:
            
            print("\033[31mCurrent time is beyond highest expire time, unable to recover\033[0m".format(self.format))
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            format_exp = datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')
            raise TimeoutError("Current time {0} exceeds latest URL expiry time of {1}".format(now, format_exp))
        
        if url is None:
            if len(self.stream_urls) > 1:
                url = random.choice(self.stream_urls)
            else:
                url = self.stream_urls[0]
        
        stream_url_info = self.get_Headers(url)
        if stream_url_info is not None and stream_url_info.get("X-Head-Seqnum", None) is not None:
            new_latest = int(stream_url_info.get("X-Head-Seqnum"))
            if new_latest > self.latest_sequence and self.latest_sequence > -1:
                self.segments_retries.update({key: {'retries': 0, 'last_retry': 0, 'ideal_retry_time': random.uniform(max(self.segment_retry_time,900),max(self.segment_retry_time+300,1200))} for key in range(self.latest_sequence, new_latest) if key not in self.already_downloaded})
            self.latest_sequence = new_latest
            print("Latest sequence: {0}".format(self.latest_sequence))
            
        if stream_url_info is not None and stream_url_info.get('Content-Type', None) is not None:
            self.type, self.ext = str(stream_url_info.get('Content-Type')).split('/')
            
        if stream_url_info is not None and stream_url_info.get("X-Head-Time-Sec", None) is not None:
            self.estimated_segment_duration = int(stream_url_info.get("X-Head-Time-Sec"))/max(self.latest_sequence,1)
    
    def get_Headers(self, url):
        try:
            # Send a GET request to a URL
            #response = requests.get(url, timeout=30)
            response = requests.get(url, timeout=30)
            #print("Print response: {0}".format(response.status_code))
            # 200 and 204 responses appear to have valid headers so far
            if response.status_code == 200 or response.status_code == 204:
                self.is_403 = False
                self.is_401 = False
                # Print the response headers
                #print(json.dumps(dict(response.headers), indent=4))  
            elif response.status_code == 403:
                self.is_403 = True
            elif response.status_code == 401:
                self.is_401 = True
            else:
                print("Error retrieving headers: {0}".format(response.status_code))
                print(json.dumps(dict(response.headers), indent=4))
            return response.headers
            
        except requests.exceptions.Timeout as e:
            logging.info("Timed out updating fragments: {0}".format(e))
            print(e)
            return None
    

    def create_connection(self, file):
        conn = sqlite3.connect(file)
        cursor = conn.cursor()
        
        # Database connection optimisation. Benefits will need to be tested
        if not self.database_in_memory:
            # Set the journal mode to WAL
            cursor.execute('PRAGMA journal_mode = WAL;')        
            # Set the synchronous mode to NORMAL
            cursor.execute('PRAGMA synchronous = NORMAL;')
            # Increase page size to help with large blobs
            cursor.execute('pragma page_size = 32768;')
        
        return conn, cursor
    
    def create_db(self, temp_file):
        # Connect to SQLite database (or create it if it doesn't exist)
        conn, cursor = self.create_connection(temp_file)
        
        # Create the table where id represents the segment order
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS segments (
            id INTEGER PRIMARY KEY, 
            segment_data BLOB
        )
        ''')
        conn.commit()
        return conn, cursor

    # Function to check if a segment exists in the database
    def segment_exists(self, cursor, segment_order):
        cursor.execute('SELECT 1 FROM segments WHERE id = ?', (segment_order,))
        return cursor.fetchone() is not None
    
    def segment_exists_batch(self):
        """
        Queries the database to check if a batch of segment numbers are already downloaded.
        Returns a set of existing segment numbers.
        """
        query = "SELECT id FROM segments"
        self.cursor.execute(query)
        return set(row[0] for row in self.cursor.fetchall())
    
    class CustomRetry(Retry):
        def __init__(self, *args, downloader_instance=None, retry_time_clamp=4, segment_number=None, **kwargs):
            super().__init__(*args, **kwargs)
            self.downloader_instance = downloader_instance  # Store the Downloader instance
            self.retry_time_clamp = retry_time_clamp
            self.segment_number = segment_number

        def increment(self, method=None, url=None, response=None, error=None, _pool=None, _stacktrace=None):
            # Check the response status code and set self.is_403 if it's 403
            if response and response.status == 403:
                if self.downloader_instance:  # Ensure the instance exists
                    self.downloader_instance.is_403 = True
                if self.segment_number is not None:
                    print("{0} encountered a 403")
                    
            return super().increment(method, url, response, error, _pool, _stacktrace)
        
        # Limit backoff to a maximum of 4 seconds
        def get_backoff_time(self):
            # Calculate the base backoff time using exponential backoff
            base_backoff = super().get_backoff_time()

            clamped_backoff = min(self.retry_time_clamp, base_backoff)
            return clamped_backoff

    class SessionWith403Counter(requests.Session):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.num_retries = 0  # Initialize counter for 403 responses

        def get_403_count(self):
            return self.num_retries  # Return the number of 403 responses
        

    # Function to download a single segment
    def download_segment(self, segment_url, segment_order):
        self.check_kill()

        # create an HTTP adapter with the retry strategy and mount it to the session
        adapter = HTTPAdapter(max_retries=self.retry_strategy)
        # create a new session object
        #session = requests.Session()
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        user_agent = random.choice(user_agents)
        headers = {
            "User-Agent": user_agent,
        }
        
        try:            
            response = session.get(segment_url, timeout=30, headers=headers)
            if response.status_code == 200:
                print("Downloaded segment {0} of {1} to memory...".format(segment_order, self.format))
                self.is_403 = False
                self.is_401 = False
                #return latest header number and segment content                
                return int(response.headers.get("X-Head-Seqnum", -1)), response.content, int(segment_order), response.status_code, response.headers  # Return segment order and data
            elif response.status_code == 403:
                print("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                return -1, None, segment_order, response.status_code, response.headers
            else:
                print("Error downloading segment {0}: {1}".format(segment_order, response.status_code))
                return -1, None, segment_order, response.status_code, response.headers
        except requests.exceptions.Timeout as e:
            logging.info("Fragment timeout {1}: {0}".format(e, segment_order))
            print(e)
            return -1, None, segment_order, None, None
        except requests.exceptions.RetryError as e:
            logging.info("Retries exceeded downloading fragment: {0}".format(e))
            print("Retries exceeded downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            if "(Caused by ResponseError('too many 204 error responses')" in str(e):
                self.is_403 = False
                self.is_401 = False
                return -1, bytes(), segment_order, 204, None
            elif "(Caused by ResponseError('too many 403 error responses')" in str(e):
                self.is_403 = True
                self.count_403s.update({segment_order: (self.count_403s.get(segment_order, 0) + 1)})
                self.user_agent_full_403s.update({user_agent: (self.user_agent_full_403s.get(user_agent, 0) + 1)})
                return -1, None, segment_order, 403, None
            elif "(Caused by ResponseError('too many 401 error responses')" in str(e):
                self.is_401 = True
                return -1, None, segment_order, 401, None
            else:
                return -1, None, segment_order, None, None
        except requests.exceptions.ChunkedEncodingError as e:
            logging.info("No data in request for fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("No data in request for fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, bytes(), segment_order, None, None
        except requests.exceptions.ConnectionError as e:
            logging.info("Connection error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("Connection error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.Timeout as e:
            logging.info("Timeout while retrieving downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("Timeout while retrieving downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.HTTPError as e:
            logging.info("HTTP error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("HTTP error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except Exception as e:
            logging.info("Unknown error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            print("Unknown error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
            
    # Function to insert a single segment without committing
    def insert_single_segment(self, cursor, segment_order, segment_data):

        cursor.execute('''
            INSERT INTO segments (id, segment_data) 
            VALUES (?, ?) 
            ON CONFLICT(id) 
            DO UPDATE SET segment_data = CASE 
                WHEN LENGTH(excluded.segment_data) > LENGTH(segments.segment_data) 
                THEN excluded.segment_data 
                ELSE segments.segment_data 
            END;
        ''', (segment_order, segment_data))


    # Function to commit after a batch of inserts
    def commit_batch(self, conn):
        conn.commit()
        
    def close_connection(self):
        self.conn.close()

    # Function to combine segments into a single file
    def combine_segments_to_file(self, output_file, cursor=None):
        if cursor is None:
            cursor = self.cursor
        
        print("Merging segments to {0}".format(output_file))
        with open(output_file, 'wb') as f:
            cursor.execute('SELECT segment_data FROM segments ORDER BY id')
            for segment in cursor:  # Cursor iterates over rows one by one
                segment_piece = segment[0]
                # Clean each segment if required as ffmpeg sometimes doesn't like the segments from YT
                cleaned_segment = self.remove_sidx(segment_piece)
                f.write(cleaned_segment)
        return output_file
    
    ### Via ytarchive            
    def get_atoms(self, data):
        """
        Get the name of top-level atoms along with their offset and length
        In our case, data should be the first 5kb - 8kb of a fragment

        :param data:
        """
        atoms = {}
        ofs = 0

        while True:
            # We should be fine and not run into errors, but I do dumb things
            try:
                alen = int(data[ofs:ofs + 4].hex(), 16)
                if alen > len(data):
                    break

                aname = data[ofs + 4:ofs + 8].decode()
                atoms[aname] = {"ofs": ofs, "len": alen}
                ofs += alen
            except Exception:
                break

            if ofs + 8 >= len(data):
                break

        return atoms

    ### Via ytarchive  
    def remove_sidx(self, data):
        """
        Remove the sidx atom from a chunk of data

        :param data:
        """
        atoms = self.get_atoms(data)
        if not "sidx" in atoms:
            return data

        sidx = atoms["sidx"]
        ofs = sidx["ofs"]
        rlen = sidx["ofs"] + sidx["len"]
        new_data = data[:ofs] + data[rlen:]

        return new_data
    
    def check_kill(self):
        # Kill if keyboard interrupt is detected
        if kill_all:
            print("Kill command detected, ending thread")
            raise KeyboardInterrupt("Kill command executed")
        
    def delete_temp_database(self):
        self.close_connection()
        os.remove(self.temp_db_file)
        
    def delete_ts_file(self):
        os.remove(self.merged_file_name)
        
    def remove_folder(self):
        if self.folder:
            self.delete_temp_database()
            self.delete_ts_file()
            os.remove(self.folder)
    def save_stats(self):
        # Stats files
        with open("{0}.{1}_seg_403s.json".format(self.file_base_name, self.format), 'w', encoding='utf-8') as outfile:
            json.dump(self.count_403s, outfile, indent=4)
        with open("{0}.{1}_usr_ag_403s.json".format(self.file_base_name, self.format), 'w', encoding='utf-8') as outfile:
            json.dump(self.user_agent_403s, outfile, indent=4)
        with open("{0}.{1}_usr_ag_full_403s.json".format(self.file_base_name, self.format), 'w', encoding='utf-8') as outfile:
            json.dump(self.user_agent_full_403s, outfile, indent=4)
 