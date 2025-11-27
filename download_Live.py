from __future__ import annotations
import yt_dlp
import sqlite3
import requests
from requests.adapters import HTTPAdapter, Retry
import random
from datetime import datetime
import time
import concurrent.futures
import json
from pathlib import Path
import errno

import copy

from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

try:
    import getUrls
    import YoutubeURL
    from headers import user_agents
except ModuleNotFoundError as e:
    from . import getUrls
    from . import YoutubeURL
    from .headers import user_agents

import subprocess
import os  

from urllib.parse import urlparse, parse_qs

import shutil

import logging
import logging.handlers

import re

import threading



class LiveStreamDownloader:

    def __init__(self, kill_all=threading.Event(), cleanup = threading.Event(), logger: logging = None):
        if logger:
            self.logger = logger
        else:
            # 1. Create a Named Logger instance and assign it to self.logger
            self.logger = logging.getLogger(self.__class__.__name__) 
            # Ensure it processes messages (important if you don't call setup_logging immediately)
        self.logger.propagate = False 

        # Global state converted to instance attributes
        self.kill_all = kill_all
        self.cleanup = cleanup
        self.live_chat_result = None
        self.chat_timeout = None
        # File name dictionary
        self.file_names = {
            'databases': [],
            "streams": {}
        }
        self.stats = {}

    # Create runner function for each download format
    def download_stream(self, info_dict, resolution, batch_size=5, max_workers=1, folder=None, file_name=None, keep_database=False, cookies=None, retries=5, yt_dlp_options=None, proxies=None, yt_dlp_sort=None, include_dash=False, include_m3u8=False, force_m3u8=False, manifest=0):
        download_params = locals().copy()
        download_params.update({"download_function": self.download_stream})
        file = None
        filetype = None
        with DownloadStream(info_dict, resolution=resolution, batch_size=batch_size, max_workers=max_workers, folder=folder, file_name=file_name, cookies=cookies, fragment_retries=retries, 
                                        yt_dlp_options=yt_dlp_options, proxies=proxies, yt_dlp_sort=yt_dlp_sort, include_dash=include_dash, include_m3u8=include_m3u8, force_m3u8=force_m3u8, 
                                        download_params=download_params, livestream_coordinator=self) as downloader:              
            downloader.live_dl()
            file_name = downloader.combine_segments_to_file(downloader.merged_file_name)
            if not keep_database:
                self.logger.info("Merging to ts complete, removing {0}".format(downloader.temp_db_file))
                downloader.delete_temp_database()
            elif downloader.temp_db_file != ':memory:':
                database_file = FileInfo(downloader.temp_db_file, file_type='database', format=downloader.format)
                self.file_names['databases'].append(database_file)

            file = FileInfo(file_name, file_type=downloader.type, format=downloader.format)
            filetype = downloader.type

        self.file_names.setdefault("streams", {}).setdefault(manifest, {}).update({
            str(filetype).lower(): file
        })
        
        return file, filetype

    # Create runner function for each download format
    def download_stream_direct(self, info_dict, resolution, batch_size, max_workers, folder=None, file_name=None, keep_state=False, cookies=None, retries=5, yt_dlp_options=None, proxies=None, yt_dlp_sort=None, include_dash=False, include_m3u8=False, force_m3u8=False, manifest=0):
        download_params = locals().copy()
        download_params.update({"download_function": self.download_stream_direct})
        file = None
        filetype = None

        with DownloadStreamDirect(info_dict, resolution=resolution, max_workers=max_workers, folder=folder, file_name=file_name, cookies=cookies, fragment_retries=retries, 
                                            yt_dlp_options=yt_dlp_options, proxies=proxies, yt_dlp_sort=yt_dlp_sort, include_dash=include_dash, include_m3u8=include_m3u8, force_m3u8=force_m3u8, 
                                            download_params=download_params, livestream_coordinator=self) as downloader:
            file_name = downloader.live_dl()
            file = FileInfo(file_name, file_type=downloader.type, format=downloader.format)
            filetype = downloader.type
            downloader.delete_state_file()

        self.file_names.setdefault("streams", {}).setdefault(manifest, {}).update({
            str(filetype).lower(): file
        })
        return file, filetype

    def recover_stream(self, info_dict, resolution, batch_size=5, max_workers=5, folder=None, file_name=None, keep_database=False, cookies=None, retries=5, yt_dlp_options=None, proxies=None, yt_dlp_sort=None, force_merge=False, recovery_failure_tolerance=0, manifest=0):

        file = None
        filetype = None

        with StreamRecovery(info_dict, resolution=resolution, batch_size=batch_size, max_workers=max_workers, folder=folder, file_name=file_name, cookies=cookies, fragment_retries=retries, 
                            proxies=proxies, yt_dlp_sort=yt_dlp_sort, livestream_coordinator=self) as downloader:
            
            result = downloader.live_dl()
            #downloader.save_stats()    
            if force_merge or result <= 0 or result <= recovery_failure_tolerance:
                if result > 0:
                    self.logger.warning("({2}) Stream recovery of format {0} has {1} outstanding segments which were not able to complete. Exitting".format(downloader.format, result, downloader.id))
                file_name = downloader.combine_segments_to_file(downloader.merged_file_name)
                if not keep_database:
                    self.logger.info("Merging to ts complete, removing {0}".format(downloader.temp_db_file))
                    downloader.delete_temp_database()
                elif downloader.temp_db_file != ':memory:':
                    database_file = FileInfo(downloader.temp_db_file, file_type='database', format=downloader.format)
                    self.file_names['databases'].append(database_file)
            else:
                raise getUrls.VideoDownloadError("({2}) Stream recovery of format {0} has {1} outstanding segments which were not able to complete. Exitting".format(downloader.format, result, downloader.id))
            file = FileInfo(file_name, file_type=downloader.type, format=downloader.format) 
            filetype = downloader.type  
            
        self.file_names.setdefault("streams", {}).setdefault(manifest, {}).update({
            str(filetype).lower(): file
        })
        return file, filetype

    def submit_download(self, executor, info_dict, resolution, options, download_folder, file_name, futures, is_audio=False):
        extra_kwargs = {}

        # Options only not used by recovery
        if not options.get('recovery', False):
            extra_kwargs.update({
                "include_dash": options.get("dash", False),
                "include_m3u8": options.get("m3u8", False), 
                "force_m3u8": options.get("force_m3u8", False),
            })

        # Select the function
        if options.get('recovery', False):
            func = self.recover_stream
            extra_kwargs.update({
                "force_merge": options.get('force_recovery_merge', False),
                "recovery_failure_tolerance": options.get('recovery_failure_tolerance', 0)
            })
            keep_key = "keep_database"
        elif options.get('direct_to_ts', False):
            func = self.download_stream_direct
            extra_kwargs.update({
                "keep_state": options.get("keep_temp_files", False) or options.get("keep_database_file", False),
            })
            keep_key = None
        else:
            func = self.download_stream
            extra_kwargs.update({
                "keep_database": options.get("keep_temp_files", False) or options.get("keep_database_file", False),
            })
            keep_key = "keep_database"

        kwargs = dict(
            info_dict=info_dict,
            resolution="audio_only" if is_audio else resolution,
            batch_size=options.get('batch_size', 1),
            max_workers=options.get("threads", 1),
            folder=download_folder,
            file_name=file_name,
            retries=options.get('segment_retries'),
            cookies=options.get('cookies'),
            yt_dlp_options=options.get('ytdlp_options', None),
            proxies=options.get("proxy", None),
            yt_dlp_sort=options.get('custom_sort', None),

        )

        if extra_kwargs:
            kwargs.update(extra_kwargs)

        self.logger.debug("Starting executor with: {0}".format(json.dumps(kwargs)))

        # Submit to executor
        future = executor.submit(func, **kwargs)
        futures.add(future)
        return future


    # Multithreaded function to download new segments with delayed commit after a batch
    def download_segments(self, info_dict, resolution='best', options={}, thread_event: threading.Event=None):
        futures = set()
        #file_names = {}

        if thread_event is not None:        
            self.kill_all = thread_event
            
        setup_logging(log_level=options.get('log_level', "INFO"), console=(not options.get('no_console', False)), file=options.get('log_file', None), file_options=options.get("log_file_options",{}), logger_name=str(self.__class__.__name__))
        self.stats['id'] = info_dict.get('id', None)
        self.logger.debug(json.dumps(options, indent=4))
        outputFile = self.output_filename(info_dict=info_dict, outtmpl=options.get('output'))
        file_name = None
        # Requires testing
        if options.get('temp_folder') is not None and options.get('temp_folder') != os.path.dirname(outputFile):
            output_folder, file_name = os.path.split(outputFile)
            download_folder = self.output_filename(info_dict=info_dict, outtmpl=options.get('temp_folder'))
            options['temp_folder'] = download_folder
        else:
            download_folder, file_name = os.path.split(outputFile)
        options['filename'] = file_name
        if download_folder:    
            os.makedirs(download_folder, exist_ok=True)
            

        with concurrent.futures.ThreadPoolExecutor() as executor:  
            try: 
                
                # Download auxiliary files (thumbnail, info,json etc)
                auxiliary_thread = executor.submit(self.download_auxiliary_files, info_dict=info_dict, options=options)
                futures.add(auxiliary_thread)
                
                live_chat_thread = None            
                if options.get('live_chat', False) is True:
                    live_chat_thread = threading.Thread(target=self.download_live_chat, args=(info_dict,options), daemon=True)
                    live_chat_thread.start()
                    #download_live_chat(info_dict=info_dict, options=options)
                    #chat_thread = executor.submit(download_live_chat, info_dict=info_dict, options=options)
                    #futures.add(chat_thread)
                
                format_check = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution, sort=options.get('custom_sort', None), include_dash=(options.get("dash", False) and not options.get('recovery', False)), include_m3u8=options.get("m3u8", False), force_m3u8=options.get("force_m3u8", False))
                if not format_check:
                    raise ValueError("Resolution is not valid or does not exist in stream")
                # For use of specificed format. Expects two values, but can work with more
                # Video + Audio
                if resolution.lower() != "audio_only":
                    
                    #Video
                    self.submit_download(executor, info_dict, resolution, options, download_folder, file_name, futures, is_audio=False)
                    if format_check.protocol != "m3u8_native":
                        #Audio
                        self.submit_download(executor, info_dict, resolution, options, download_folder, file_name, futures, is_audio=True)

                    #futures.add(video_future)
                    #futures.add(audio_future)
                # Audio-only
                else:
                    self.submit_download(executor, info_dict, resolution, options, download_folder, file_name, futures, is_audio=True)
                    #futures.add(audio_future)
                    
                while True:
                    if self.kill_all.is_set():
                        raise KeyboardInterrupt("Thread kill event is set, ending...")
                    done, not_done = concurrent.futures.wait(futures, timeout=1, return_when=concurrent.futures.ALL_COMPLETED)
                    # Continuously check for completion or interruption
                    for future in done:
                        
                        #if future.exception() is not None:
                        #    if type == 'auxiliary': 
                                #logging.error(str(future.exception()))
                        #   else:
                        #        raise future.exception()
                        
                        result, type = future.result()
                        self.logger.info("\033[31m{0}\033[0m".format(result))
                        
                        if type == 'auxiliary':
                            self.file_names.update(result)
                        elif str(type).lower() == 'video':
                            #self.file_names['video'] = result
                            pass
                        elif str(type).lower() == 'audio':
                            #self.file_names['audio'] = result  
                            pass  
                        else:
                            self.file_names[str(type)] = result
                        
                        futures.discard(future)
                        
                    if len(not_done) <= 0:
                        break
                    #else:
                    #    time.sleep(0.9)
                    self.print_stats(options=options)
                
                if live_chat_thread is not None:
                    self.logger.info("Waiting for live chat to end")
                    chat_timeout = time.time()
                    live_chat_thread.join()
                
            except KeyboardInterrupt as e:
                self.kill_all.set()
                self.logger.debug("Keyboard interrupt detected")
                if len(not_done) > 0:
                    self.logger.debug("Cancelling remaining threads")
                for future in not_done:
                    _ = future.cancel()
                done, not_done = concurrent.futures.wait(futures, timeout=5, return_when=concurrent.futures.ALL_COMPLETED)
                executor.shutdown(wait=False,cancel_futures=True)
                self.logger.debug("Shutdown all threads")
                raise
                
        
        self.create_mp4(file_names=self.file_names, info_dict=info_dict, options=options)
            
        
        self.move_to_final(options=options, output_file=outputFile, file_names=self.file_names)
                    
        #move_to_final(info_dict, options, file_names)
        
    def output_filename(self, info_dict, outtmpl):
        outputFile = str(yt_dlp.YoutubeDL().prepare_filename(info_dict, outtmpl=outtmpl))
        return outputFile
    """
    def move_to_final(options, outputFile, file_names):
        logging.debug("Tracked Files: {0}".format(json.dumps(file_names, indent=4)))
        if os.path.dirname(outputFile):
            os.makedirs(os.path.dirname(outputFile), exist_ok=True)
        try:
            if file_names.get('thumbnail'):
                # Remove thumbnail if write_thumbnail isn't True as it may have only been requested for embedding      
                if options.get('write_thumbnail', False):
                    thumbnail = file_names.get('thumbnail')
                    thumb_output = "{0}{1}".format(outputFile, thumbnail.suffix)
                    if str(thumbnail).strip() != str(thumb_output).strip():
                        logging.debug("Moving {0} to {1}".format(thumbnail.absolute(), thumb_output))                
                        shutil.move(thumbnail.absolute(), thumb_output)
                else:
                    logging.info("Removing {0}".format(file_names.get('thumbnail').absolute()))
                    file_names.get('thumbnail').unlink(missing_ok=True)
                    file_names.pop('thumbnail',None)

        except Exception as e:
            logging.exception("unable to move thumbnail: {0}".format(e))
        
        try:
            if file_names.get('info_json'):
                info_json = file_names.get('info_json')
                info_output = "{0}{1}".format(outputFile, '.info.json')
                if str(info_json).strip() != str(info_output).strip():
                    logging.info("Moving {0} to {1}".format(info_json.absolute(), info_output))            
                    shutil.move(info_json.absolute(), info_output)
        except Exception as e:
            logging.exception("unable to move info_json: {0}".format(e))
            
        try:
            if file_names.get('description'):
                description = file_names.get('description')
                description_output = "{0}{1}".format(outputFile, description.suffix)            
                if str(description).strip() != str(description_output).strip():
                    logging.info("Moving {0} to {1}".format(description.absolute(), description_output))
                    shutil.move(description.absolute(), description_output)
        except Exception as e:
            logging.exception("unable to move description: {0}".format(e))
        
        try:
            if file_names.get('video'):
                video = file_names.get('video')
                video_output = "{0}.{1}{2}".format(outputFile, video._format, video.suffix)
                if str(video).strip() != str(video_output).strip():
                    logging.info("Moving {0} to {1}".format(video.absolute(), video_output))
                    shutil.move(video.absolute(), video_output)
        except Exception as e:
            logging.exception("unable to move video stream: {0}".format(e))
            
        try:
            if file_names.get('audio'):
                audio = file_names.get('audio')
                audio_output = "{0}.{1}{2}".format(outputFile, audio._format, audio.suffix)
                if str(audio).strip() != str(audio_output).strip():
                    logging.info("Moving {0} to {1}".format(audio.absolute(), audio_output))
                    shutil.move(audio.absolute(), audio_output)
        except Exception as e:
            logging.exception("unable to move audio stream: {0}".format(e))
            
        try:
            if file_names.get('merged'):
                merged = file_names.get('merged')
                merged_output = "{0}{1}".format(outputFile, merged.suffix)
                if str(merged).strip() != str(merged_output).strip():
                    logging.info("Moving {0} to {1}".format(merged.absolute(), merged_output))
                    shutil.move(merged.absolute(), merged_output)
        except Exception as e:
            logging.exception("unable to move merged video: {0}".format(e))
                    
        try:
            if file_names.get('live_chat'):
                live_chat = file_names.get('live_chat')
                live_chat_output = "{0}{1}".format(outputFile, ".live_chat.zip")
                if str(live_chat).strip() != str(live_chat_output).strip():
                    logging.info("Moving {0} to {1}".format(live_chat.absolute(), live_chat_output))
                    shutil.move(live_chat.absolute(), live_chat_output)
        except Exception as e:
            logging.exception("unable to move live chat zip: {0}".format(e))
        
        try:
            if file_names.get('databases'):
                for file in file_names.get('databases'):
                    db_output = "{0}.{1}{2}".format(outputFile, file.format, file.suffix)
                    if str(file).strip() != str(db_output).strip():
                        logging.info("Moving {0} to {1}".format(file.absolute(), db_output))
                        shutil.move(file.absolute(), db_output)
        except Exception as e:
            logging.exception("unable to move database files: {0}".format(e))
        try:
            if file_names.get('ffmpeg_cmd') and file_names.get('ffmpeg_cmd').exists():
                if options.get('write_ffmpeg_command', False):
                    ffmpeg_command = file_names.get('ffmpeg_cmd')
                    ffmpeg_command_output = "{0}{1}".format(outputFile, ".ffmpeg.txt")
                    if str(ffmpeg_command).strip() != str(ffmpeg_command_output).strip():
                        logging.info("Moving {0} to {1}".format(ffmpeg_command.absolute(), ffmpeg_command_output))
                        shutil.move(ffmpeg_command.absolute(), ffmpeg_command_output)
                else:
                    file_names.get('ffmpeg_cmd').unlink()
        except Exception as e:
            logging.exception("unable to move ffmpeg command file: {0}".format(e))
            
        try:
            os.rmdir(options.get('temp_folder'))
        except OSError as e:
            if e.errno == errno.ENOTEMPTY:
                logging.warning(f"Error: Directory not empty: {e.filename}")
                # Optional: handle non-empty directory (e.g., log, retry, or remove contents)
            else:
                logging.exception("Error removing temp folder: {0}".format(e))
        except Exception as e:
            logging.exception("Error removing temp folder: {0}".format(e))
            
        logging.info("Finished moving files from temporary directory to output destination")
    """

    def move_to_final(self, options, output_file, file_names):
        def maybe_move(key, dest_func, file_names_dict=file_names, option_flag=None):
            """
            key: key in file_names
            dest_func: func -> pathlib -> string dest path
            delete_if_false: if True, delete when option_flag not set
            option_flag: name of boolean option (write_thumbnail etc)
            """
            f = file_names_dict.get(key)
            if not f:
                return
            try:
                # deletion case (thumbnail / ffmpeg)
                if option_flag is not None and not options.get(option_flag, False):
                    self.logger.info(f"Removing {f.absolute()}")
                    f.unlink(missing_ok=True)
                    file_names_dict.pop(key, None)
                    return

                dest = dest_func(f)
                if str(f).strip() != str(dest).strip():
                    self.logger.info(f"Moving {f.absolute()} → {dest}")
                    shutil.move(f.absolute(), dest)
                else:
                    self.logger.debug(f"{f.absolute()} is already in final destination")
            except Exception as e:
                self.logger.exception(f"unable to move {key}: {e}")

        # ensure output dir exists
        out_dir = os.path.dirname(output_file)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        # === individual file handlers ===

        maybe_move('thumbnail',
                lambda f: f"{output_file}{f.suffix}",
                option_flag='write_thumbnail')

        maybe_move('info_json',
                lambda f: f"{output_file}.info.json",
                )

        maybe_move('description',
                lambda f: f"{output_file}{f.suffix}",
                )
        
        stream_manifests = list(self.file_names["streams"].items())
        for manifest, stream in stream_manifests:
            stream_output_file = output_file
            if len(stream_manifests) > 1:
                stream_output_file = f"{output_file}.{manifest}"
            maybe_move('video',
                    lambda f: f"{stream_output_file}.{f._format}{f.suffix}",
                    file_names_dict=stream)

            maybe_move('audio',
                    lambda f: f"{stream_output_file}.{f._format}{f.suffix}",
                    file_names_dict=stream)

            maybe_move('merged',
                    lambda f: f"{stream_output_file}{f.suffix}",
                    file_names_dict=stream)

        maybe_move('live_chat',
                lambda f: f"{output_file}.live_chat.zip",
                )

        # special: databases = list
        try:
            for f in file_names.get('databases', []):
                dest = f"{output_file}.{f._format}{f.suffix}"
                if str(f).strip() != str(dest).strip():
                    self.logger.info(f"Moving {f.absolute()} → {dest}")
                    shutil.move(f.absolute(), dest)
        except Exception as e:
            self.logger.exception(f"unable to move database files: {e}")

        # special: ffmpeg_cmd
        maybe_move('ffmpeg_cmd',
                lambda f: f"{output_file}.ffmpeg.txt",
                option_flag='write_ffmpeg_command')

        # remove temp folder
        """
        if options.get('temp_folder', None) is not None:
            try:
                os.rmdir(options.get('temp_folder'))
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    self.logger.warning(f"Error: Directory not empty: {e.filename}")
                else:
                    self.logger.exception(f"Error removing temp folder: {e}")
            except Exception as e:
                self.logger.exception(f"Error removing temp folder: {e}")
        """
        self.logger.info("Finished moving files from temporary directory to output destination")

    def download_live_chat(self, info_dict, options):
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

        class YTDLP_Chat_logger():
            def __init__(self, logger):
                self.logger = logger

            def debug(self, msg):
                self.logger.debug("(yt-dlp chat): {0}".format(msg))

            def info(self, msg):
                self.logger.info("(yt-dlp chat): {0}".format(msg))

            def warning(self, msg):
                self.logger.warning("(yt-dlp chat): {0}".format(msg))

            def error(self, msg):
                self.logger.error("(yt-dlp chat): {0}".format(msg))

        ydl_opts = {
            'skip_download': True,               # Skip downloading video/audio
            'logger': YTDLP_Chat_logger(logger=self.logger),
            'quiet': True,
            'cookiefile': options.get('cookies', None),
            'retries': 25,
            'concurrent_fragment_downloads': 3,
            #'live_from_start': True,
            'writesubtitles': True,              # Extract subtitles (live chat)
            'subtitlesformat': 'json',           # Set format to JSON
            'subtitleslangs': ['live_chat'],     # Only extract live chat subtitles
            'concurrent_fragment_downloads': 2,
            'outtmpl': base_output          # Save to a JSON file        
        }
        print(options.get('ytdlp_options', {}))
        ydl_opts.update(options.get('ytdlp_options', {}))
        
        livechat_filename = base_output + ".live_chat.json"
        zip_filename = base_output + ".live_chat.zip"
        
        self.logger.info("Downloading live chat to: {0}".format(livechat_filename))
        # Run yt-dlp with the specified options
        # Don't except whole process on live chat fail
        
        try:
            import chat_downloader
            from chat_downloader import ChatDownloader
            try:
                # URL of the video or stream chat
                chat_url = 'https://www.youtube.com/watch?v={0}'.format(info_dict.get('id'))
                self.logger.debug("Attempting to download with chat downloader")
                # Initialize the ChatDownloader
                chat_download = ChatDownloader(cookies=options.get('cookies', None), proxy=next(iter((options.get('proxy', None) or {}).values()), None))

                # Open a JSON file to save the chat

                # Download the chat
                chat = chat_download.get_chat(chat_url, output=livechat_filename, overwrite=False)

                # Process chat messages for the duration of the timeout
                for message in chat:
                    if self.kill_all.is_set():
                        self.logger.debug("Killing live chat downloader")
                        chat_download.close()
                        break
                    if self.chat_timeout is not None and time.time() - self.chat_timeout >= options.get('stop_chat_when_done', 300):
                        self.logger.warning("Stopping chat download for {0}, timeout ({1}s) exceeded".format(options.get('id', "N/A"), options.get('stop_chat_when_done', 300)))
                        chat_download.close()
                        break
                chat_download.close()   

            # Temporary fallback due to know issue of chat-downloader not working after youtube changes
            except chat_downloader.errors.ParsingError as e:
                self.logger.exception("Unable to parse live chat using chat-downloader, using yt-dlp")
                if options.get('proxy', None) is not None:
                    ydl_opts['proxy'] = next(iter((options.get('proxy', None) or {}).values()), None)
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        result = ydl.process_ie_result(info_dict)
                        #result = ydl.download_with_info_file(info_dict)
                        #result = ydl._writesubtitles(info_dict, )
                except Exception as e:
                    self.logger.exception("\033[31m{0}\033[0m".format(e))
            
        except ImportError as e:
            self.logger.warning("Unable to import chat-downloader, using yt-dlp")
            if options.get('proxy', None) is not None:
                ydl_opts['proxy'] = next(iter((options.get('proxy', None) or {}).values()), None)
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    result = ydl.process_ie_result(info_dict)
                    #result = ydl.download_with_info_file(info_dict)
                    #result = ydl._writesubtitles()
            except Exception as e:
                self.logger.exception("\033[31m{0}\033[0m".format(e)) 
        
            
        except Exception as e:
            self.logger.exception("\033[31m{0}\033[0m".format(e))
        time.sleep(1)
        part_file = "{0}.part".format(livechat_filename)
        if os.path.exists(part_file):
            # Append part to 
            chunk_size = 1024*1024*10  # number of characters per chunk, up to 10M characters
            with open(part_file, "r", encoding="utf-8") as fa, \
                open(livechat_filename, "a", encoding="utf-8") as fb:
                while chunk := fa.read(chunk_size):
                    fb.write(chunk)
            os.remove("{0}.part".format(livechat_filename))

        
        try:
            with zipfile.ZipFile(zip_filename, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9, allowZip64=True) as zipf:
                zipf.write(livechat_filename, arcname=os.path.basename(livechat_filename))
            os.remove(livechat_filename)
            live_chat = {
                'live_chat': FileInfo(zip_filename, file_type='live_chat')
            }
            self.file_names.update(live_chat)
            return live_chat, 'live_chat'
        except Exception as e:
            self.logger.exception("\033[31m{0}\033[0m".format(e))
        
    def replace_ip_in_json(self, file_name):
        import re
        pattern = re.compile(r'((?:[0-9]{1,3}\.){3}[0-9]{1,3})|((?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4})')

        with open(file_name, 'r', encoding="utf-8") as file:
            content = file.read()

        modified_content = re.sub(pattern, '0.0.0.0', content)

        with open(file_name, 'w', encoding="utf-8") as file:
            file.write(modified_content)

    def remove_urls_from_json(self, file_name):
        with open(file_name, 'r', encoding="utf-8") as file:
            data = json.load(file)
            
        if data.get('formats', None) is not None:
            for format in data['formats']:
                if format.get('url') is not None:
                    format['url'] = "https://www.youtube.com/watch?v={0}".format(data.get('id', ""))
                    
                if format.get('manifest_url') is not None:
                    format['manifest_url'] = "https://www.youtube.com/watch?v={0}".format(data.get('id', ""))
                
                format.pop('fragment_base_url', None)
                format.pop('fragments', None)
                    
        if data.get('thumbnails', None) is not None:
            for thumbnail in data['thumbnails']:
                if thumbnail.get('url', None) is not None:
                    parsed_url = urlparse(thumbnail.get('url', ""))
                    thumbnail['url'] = "{0}://{1}{2}".format(parsed_url.scheme, parsed_url.netloc, parsed_url.path)
                    
        if data.get('url', None) is not None:
            data['url'] = "https://www.youtube.com/watch?v={0}".format(data.get('id', ""))

        if data.get('manifest_url', None) is not None:
            data['manifest_url'] = "https://www.youtube.com/watch?v={0}".format(data.get('id', ""))
            
        data['removed_urls'] = True
        
        with open(file_name, "w", encoding='utf-8') as file:
            json.dump(data, file)
            
    def download_auxiliary_files(self, info_dict, options):
        if options.get('filename') is not None:
            filename = options.get('filename')
        else:
            filename = info_dict.get('id')
        
        if options.get("temp_folder"):
            base_output = os.path.join(options.get("temp_folder"), filename)
        else:
            base_output = filename
        
        created_files = {}

        ydl_opts = {
            'skip_download': True,
            'quiet': True,
    #        'cookiefile': options.get('cookies', None),
            'writeinfojson': options.get('write_info_json', False),
            'writedescription': options.get('write_description', False),
            'writethumbnail': (options.get('write_thumbnail', False) or options.get("embed_thumbnail", False)),
            'outtmpl': base_output,
            'retries': 10,
            
        }
        if options.get('proxy', None) is not None:
            ydl_opts['proxy'] = next(iter((options.get('proxy', None) or {}).values()), None)
            
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            #base_name = ydl.prepare_filename(info_dict)
            #result = ydl.download_with_info_file(info_dict)
            
            if ydl._write_info_json('video', info_dict, ydl.prepare_filename(info_dict, 'infojson')) or os.path.exists(ydl.prepare_filename(info_dict, 'infojson')):
                created_files['info_json'] = FileInfo(ydl.prepare_filename(info_dict, 'infojson'), file_type='info_json')
                try:
                    if options.get('remove_ip_from_json'):
                        self.replace_ip_in_json(created_files['info_json'].absolute())
                    if options.get('clean_urls'):
                        self.remove_urls_from_json(created_files['info_json'].absolute())
                except Exception as e:
                    self.logger.exception(str(e))
                
            if ydl._write_description('video', info_dict, ydl.prepare_filename(info_dict, 'description')) or os.path.exists(ydl.prepare_filename(info_dict, 'description')):
                created_files['description'] = FileInfo(ydl.prepare_filename(info_dict, 'description'), file_type='description')
                
            thumbnails = ydl._write_thumbnails('video', info_dict, ydl.prepare_filename(info_dict, 'thumbnail'))
            
            if thumbnails:            
                created_files['thumbnail'] = FileInfo(thumbnails[0][0], file_type='thumbnail')
                
            
        return created_files, 'auxiliary'
        
            
    def create_mp4(self, file_names, info_dict, options):
        #self.logger.debug("Files: {0}".format(json.dumps(file_names)))
        stream_manifests = list(self.file_names["streams"].items())
        for manifest, stream in stream_manifests:
            index = 0
            thumbnail = None
            video = None
            audio = None
            ext = options.get('ext', None)

            ffmpeg_builder = ['ffmpeg', '-y', 
                            '-hide_banner', '-nostdin', '-loglevel', 'error', '-stats'
                            ]
            
            if file_names.get('thumbnail', None) and options.get('embed_thumbnail', True):
                if file_names.get('thumbnail').exists():
                    if str(file_names.get('thumbnail').suffix).lower() == '.webp':
                        self.logger.info("{0} is a webp file, converting to png".format(file_names.get('thumbnail').name))
                        png_thumbnail = file_names.get('thumbnail').with_suffix(".png")
                        thumbnail_conversion = ["ffmpeg", "-y", "-i", str(file_names.get('thumbnail').absolute()), str(png_thumbnail.absolute())]
                        try:
                            result = subprocess.run(thumbnail_conversion, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', check=True)
                        except subprocess.CalledProcessError as e:
                            self.logger.error(e.stderr)
                            self.logger.fatal(e)
                            raise e
                        # Remove webp thumbnail
                        self.logger.debug("Replacing thumbnail with .png version")
                        file_names.pop('thumbnail').unlink(missing_ok=True)                
                        file_names['thumbnail'] = FileInfo(png_thumbnail, file_type='thumbnail')
                    
                    input = ['-i', str(file_names.get('thumbnail').absolute()), '-thread_queue_size', '1024']
                    ffmpeg_builder.extend(input)
                    thumbnail = index
                    index += 1
                else:
                    self.logger.error("Thumbnail file: {0} is missing, continuing without embedding".format(file_names.get('thumbnail').absolute()))
            
            # Add input files
            if stream.get('video', None):        
                input = ['-i', str(stream.get('video').absolute()), '-thread_queue_size', '1024']
                ffmpeg_builder.extend(input)
                video = index
                index += 1
                    
            if stream.get('audio', None):
                input = ['-i', str(stream.get('audio').absolute()), '-thread_queue_size', '1024']
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

            if len(stream_manifests) > 1:
                base_output = f"{base_output}.{manifest}" 
            
            if ext is None:
                self.logger.debug("No extension detected, switching to yt-dlp decided video (defaulting to MP4)")
                ext = info_dict.get('ext', '.mp4')
            if ext is not None and not str(ext).startswith("."):
                ext = "." + str(ext)
            if not base_output.endswith(ext):
                base_output = base_output + ext  
                
            ffmpeg_builder.append(os.path.abspath(base_output))
            
            if options.get('write_ffmpeg_command', True):
                ffmpeg_command_file = "{0}.ffmpeg.txt".format(filename)
                file_names["streams"][manifest]['ffmpeg_cmd'] =  FileInfo(self.write_ffmpeg_command(ffmpeg_builder, ffmpeg_command_file), file_type='ffmpeg_command')

            if not (options.get('merge', True)):    
                return file_names
            
            self.logger.debug("FFmpeg command: {0}".format(' '.join(ffmpeg_builder)))
            self.logger.info("Executing ffmpeg. Outputting to {0}".format(ffmpeg_builder[-1]))
            try:
                result = subprocess.run(ffmpeg_builder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', check=True)
                self.logger.debug("FFmpeg STDOUT: {0}".format(result.stdout))
                self.logger.debug("FFmpeg STDERR: {0}".format(result.stderr))
            except subprocess.CalledProcessError as e:
                self.logger.error(e.stderr)
                self.logger.fatal(e)
                raise e
            #print(result.stdout)
            #print(result.stderr)
            
            
            
            file_names["streams"][manifest]['merged'] = FileInfo(base_output, file_type='merged')
            self.logger.info("Successfully merged files into: {0}".format(file_names["streams"][manifest].get('merged').absolute()))
            
            
            # Remove temp video and audio files
            if not (options.get('keep_ts_files') or options.get('keep_temp_files')):
                if file_names["streams"][manifest].get('video'): 
                    self.logger.info("Removing {0}".format(file_names["streams"][manifest].get('video').absolute()))
                    file_names["streams"][manifest].get('video').unlink(missing_ok=True)
                    file_names["streams"][manifest].pop('video',None)
                if file_names["streams"][manifest].get('audio'): 
                    self.logger.info("Removing {0}".format(file_names["streams"][manifest].get('audio').absolute()))
                    file_names["streams"][manifest].get('audio').unlink(missing_ok=True)
                    file_names["streams"][manifest].pop('audio',None)       
        
        return file_names
        #for file in file_names:
        #    os.remove(file)

    def write_ffmpeg_command(self, command_array, filename):
        import shlex
        # Determine the platform
        """
        Builds a platform-compatible FFmpeg command with proper quoting.

        Args:
            command_array (list): List of arguments to append to the command.
            filename: Filename to write command to 

        Returns:
            str: A properly quoted FFmpeg command.
        """
        if os.name == "nt":  # Windows
            # Handle special quoting and escaping for Windows
            quoted_args = []
            for arg in command_array:
                if "\n" in arg:
                    # Replace newlines with literal \n
                    arg = arg.replace("\n", "\\n")
                # Escape double quotes and wrap in double quotes if necessary
                if " " in arg or any(ch in arg for ch in ('&', '^', '%', '$', '#', '"')):
                    arg = '"{0}"'.format(arg.replace("\"", "\\\""))
                quoted_args.append(arg)
            command_string = "{0}".format(' '.join(quoted_args))
        else:  # POSIX (Linux/macOS)
            # Use shlex.quote for safe quoting
            #quoted_args = [shlex.quote(arg) for arg in arguments]
            command_string = shlex.join(command_array)

        

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(command_string + "\n")

        return filename

    def convert_bytes(self, bytes):
        # List of units in order
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
        
        # Start with bytes and convert to larger units
        unit_index = 0
        while bytes >= 1024 and unit_index < len(units) - 1:
            bytes /= 1024
            unit_index += 1
        
        # Format and return the result
        return f"{bytes:.2f} {units[unit_index]}"

    def print_stats(self, options):
        if options.get('stats_as_json', False):
            print(json.dumps(self.stats), end="\r")
            return
        
        # If not info log level or below, don't print stats
        if not options.get("log_level", None) in ["DEBUG", "INFO"]:
            return
        
        if not (self.stats.get('video', None) or self.stats.get('audio', None)):
            return
        
        print("{0}:".format(self.stats.get('id')), end=" ")
        
        if self.stats.get('video'):
            print("Video: {0}/{1} segments".format(self.stats.get('video', {}).get('downloaded_segments', 0), self.stats.get('video', {}).get('latest_sequence', 0)), end="")
            if self.stats.get('video', {}).get('status', None):
                print(" ({0})".format(self.stats.get('video', {}).get('status', "").capitalize()), end="")
            print(", ", end="")
        if self.stats.get('audio'):
            print("Audio: {0}/{1} segments".format(self.stats.get('audio', {}).get('downloaded_segments', 0), self.stats.get('audio', {}).get('latest_sequence', 0)), end="")
            if self.stats.get('video', {}).get('status', None):
                print(" ({0})".format(self.stats.get('audio', {}).get('status', "").capitalize()), end="")
            print(", ", end="")
        if self.stats.get('video', {}).get('current_filesize', None) or self.stats.get('audio', {}).get('current_filesize', None):
            current_size = self.stats.get('video', {}).get('current_filesize', 0) + self.stats.get('audio', {}).get('current_filesize', 0)
            current_size_string = self.convert_bytes(current_size)
            print("~{0} downloaded".format(current_size_string), end=" ")
        if options.get("new_line", False):
            print()
        else:
            print("\r",end="")
        
    def add_url_param(self, url: str, key, value) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query[key] = [value]  # add or replace parameter

        new_query = urlencode(query, doseq=True)
        new_url = parsed._replace(query=new_query)
        return str(urlunparse(new_url))  
        
class FileInfo(Path):
    _file_type = None  # Class attribute for storing the file type    
    _format = None

    def __new__(cls, *args, file_type=None, format=None, **kwargs):
        # Call the parent's constructor
        instance = super().__new__(cls, *args, **kwargs)
        # Set the file_type attribute if provided
        instance._file_type = file_type
        instance._format = format
        return instance

    @property
    def file_type(self):
        # Getter for file_type
        return self._file_type

    @file_type.setter
    def file_type(self, value):
        # Setter for file_type
        self._file_type = value

    def __repr__(self):
        # Custom string representation
        return f"{super().__repr__()} (file_type={self._file_type})"
    
    def to_dict(self):
        return {
            "filename": str(self),
            "filetype": str(self._file_type),
            "format": str(self._format)
        }

class DownloadStream:
    def __init__(self, info_dict, resolution='best', batch_size=10, max_workers=5, fragment_retries=5, folder=None, file_name=None, database_in_memory=False, cookies=None, recovery_thread_multiplier=2, yt_dlp_options=None, proxies=None, yt_dlp_sort=None, include_dash=False, include_m3u8=False, force_m3u8=False, download_params = {}, livestream_coordinator: LiveStreamDownloader = None):        
        self.livestream_coordinator = livestream_coordinator
        if self.livestream_coordinator:
            self.logger = self.livestream_coordinator.logger
            self.kill_all = self.livestream_coordinator.kill_all
        else:
            self.logger = logging.getLogger()
            self.kill_all = threading.Event()

        
        self.params = download_params or locals().copy()
        self.conn = None
        self.latest_sequence = -1
        self.already_downloaded = set()
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.yt_dlp_options = yt_dlp_options

        self.include_dash = include_dash
        self.include_m3u8 = include_m3u8
        self.force_m3u8 = force_m3u8
        
        self.resolution = resolution
        self.yt_dlp_sort = yt_dlp_sort
        
        self.id = info_dict.get('id')
        self.live_status = info_dict.get('live_status')
        
        self.info_dict = info_dict
        self.stream_urls = []
        
        self.stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution, sort=self.yt_dlp_sort, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8) 
        
        self.format = self.stream_url.format_id

        if self.stream_url is None:
            raise ValueError("Stream URL not found for {0}, unable to continue".format(resolution))
        
        self.stream_urls.append(self.stream_url)
        # Extract and parse the query parameters into a dictionary
        #parsed_url = urlparse(self.stream_url)        
        #self.url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}

        self.logger.debug("{0} stream URL parameters: {1}".format(self.id,json.dumps(self.stream_url.url_parameters)))
        
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
            status_forcelist=[204, 400, 401, 403, 404, 408, 413, 429, 500, 502, 503, 504],  # the HTTP status codes to retry on
            backoff_max=4
        )        
        
        self.is_403 = False
        self.is_private = False
        self.estimated_segment_duration = 0
        self.refresh_retries = 0
        
        self.recovery_thread_multiplier = recovery_thread_multiplier
        
        self.cookies = cookies
        self.type = None
        self.ext = None     

        self.following_manifest_thread = None
        
        self.proxies = proxies   
        
        self.update_latest_segment()
        self.url_checked = time.time()
        
        self.conn, self.cursor = self.create_db(self.temp_db_file)    
        
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type] = {}

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
        return False
        
    def get_expire_time(self, url: YoutubeURL.YoutubeURL):
        return url.expire

    def refresh_Check(self):    
        
        #print("Refresh check ({0})".format(self.format)) 
        filtered_array = [url for url in self.stream_urls if int(self.get_expire_time(url)) >= time.time()]
        self.stream_urls = filtered_array  
        
        # By this stage, a stream would have a URL. Keep using it if the video becomes private or a membership      
        if (time.time() - self.url_checked >= 3600.0 or (time.time() - self.url_checked >= 30.0 and self.is_403) or len(self.stream_urls) <= 0) and not self.is_private:
            return self.refresh_url()
                
    def live_dl(self):
        
        self.logger.info("\033[31mStarting download of live fragments ({0})\033[0m".format(self.format))
        self.already_downloaded = self.segment_exists_batch()
        latest_downloaded_segment = -1
        wait = 0   
        self.cursor.execute('BEGIN TRANSACTION')
        uncommitted_inserts = 0     
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]['status'] = "recording"
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="{0}-{1}".format(self.id,self.format)) as executor:
            submitted_segments = set()
            future_to_seg = {}
            
            # Trackers for optimistic segment downloads 
            optimistic_fails_max = 10
            optimistic_fails = 0
            optimistic_seg = 0           
            latest_downloaded_segment = -1

            segments_to_download = set()

            segment_retries = {}

            while True:     
                self.check_kill(executor)
                if self.refresh_Check() is True:
                    break
                
                if self.livestream_coordinator and self.livestream_coordinator.stats.get(self.type, None) is None:
                    self.livestream_coordinator.stats[self.type] = {}
                # Process completed segment downloads, wait up to 5 seconds for segments to complete before next loop
                done, not_done = concurrent.futures.wait(future_to_seg, timeout=0.1, return_when=concurrent.futures.ALL_COMPLETED)  # need to fully determine if timeout or ALL_COMPLETED takes priority             
                
                for future in done:
                    head_seg_num, segment_data, seg_num, status, headers = future.result()
                    
                    self.logger.debug("\033[92mFormat: {3}, Segnum: {0}, Status: {1}, Data: {2}\033[0m".format(
                            seg_num, status, "None" if segment_data is None else f"{len(segment_data)} bytes", self.format
                        ))

                    if seg_num >= optimistic_seg and (status is None or status != 200):
                        optimistic_fails += 1
                        self.logger.debug("Unable to optimistically grab segment {1} for {0}. Up to {2} attempts".format(self.format, seg_num, optimistic_fails))
                        
                    elif seg_num >= optimistic_seg and status == 200:
                        optimistic_fails = 0
                        if seg_num >= latest_downloaded_segment:
                            latest_downloaded_segment = seg_num
                    
                    if head_seg_num > self.latest_sequence:
                        self.logger.debug("More segments available: {0}, previously {1}".format(head_seg_num, self.latest_sequence))                    
                        self.latest_sequence = head_seg_num
                        if self.livestream_coordinator:
                            self.livestream_coordinator.stats[self.type]["latest_sequence"] = self.latest_sequence
                        
                    if headers is not None and headers.get("X-Head-Time-Sec", None) is not None:
                        self.estimated_segment_duration = int(headers.get("X-Head-Time-Sec"))/self.latest_sequence
                    
                    #if headers and headers.get('X-Bandwidth-Est', None):
                    #    stats[self.type]["estimated_size"] = int(headers.get('X-Bandwidth-Est', 0))

                    if segment_data is not None:
                        # Insert segment data in the main thread (database interaction)
                        self.insert_single_segment(cursor=self.cursor, segment_order=seg_num, segment_data=segment_data)
                        uncommitted_inserts += 1
                        
                        # If finished threads exceeds batch size, commit the whole batch of threads at once. 
                        # Has risk of not committing if a thread has no segment data, but this would be corrected naturally in following loop(s)
                        if uncommitted_inserts >= max(self.batch_size, len(done)):
                            self.logger.debug("Writing segments to file...")
                            self.commit_batch(self.conn)
                            uncommitted_inserts = 0
                            self.cursor.execute('BEGIN TRANSACTION') 
                            
                        if self.livestream_coordinator:
                            self.livestream_coordinator.stats[self.type]["downloaded_segments"] = len(self.already_downloaded)
                        segment_retries.pop(seg_num, None)

                        if status == 200 and seg_num > latest_downloaded_segment:
                            latest_downloaded_segment = seg_num
                    elif status is None or status != 200:
                        segment_retries[seg_num] = segment_retries.get(seg_num, 0) + 1
                        self.logger.debug("Unable to download {0} ({1}). Currently at {2} retries".format(seg_num, self.format, segment_retries.get(seg_num, "UNKNOWN")))
                        
                    
                    # Remove from submitted segments in case it neeeds to be regrabbed
                    submitted_segments.discard(seg_num)

                    # Remove completed thread to free RAM
                    future_to_seg.pop(future,None)
                    
                segments_to_download = set(range(0, max(self.latest_sequence + 1, latest_downloaded_segment + 1))) - self.already_downloaded - set(k for k, v in segment_retries.items() if v > self.fragment_retries)  

                optimistic_seg = max(self.latest_sequence, latest_downloaded_segment) + 1  
                                        
                # If segments remain to download, don't bother updating and wait for segment download to refresh values.
                """
                if optimistic_fails < optimistic_fails_max and optimistic_seg not in submitted_segments and optimistic_seg not in self.already_downloaded and optimistic_seg not in segments_to_download:
                        
                        
                        
                        #logging.debug("\033[93mAdding segment {1} optimistically ({0}). Currently at {2} fails\033[0m".format(self.format, optimistic_seg, optimistic_fails))
                        logging.debug("\033[93mAdding segment {1} optimistically ({0}). Currently at {2} fails\033[0m".format(self.format, optimistic_seg, optimistic_fails))
                        segments_to_download.discard(optimistic_seg)
                        segments_to_download = {optimistic_seg, *segments_to_download}       
                """
                # If update has no segments and no segments are currently running, wait                              
                if len(segments_to_download) <= 0 and len(future_to_seg) <= 0:                 
                    wait += 1
                    self.logger.debug("No new fragments available for {0}, attempted {1} times...".format(self.format, wait))
                        
                    # If waited for new fragments hits 20 loops, assume stream is offline
                    if wait > 20:
                        self.logger.debug("Wait time for new fragment exceeded, ending download...")
                        break    
                    # If over 10 wait loops have been executed, get page for new URL and update status if necessary
                    elif wait > 10:
                        if self.is_private:
                            self.logger.debug("Video is private and no more segments are available. Ending...")
                            break
                        else:
                            refresh = self.refresh_url()
                            if refresh is False:
                                break       
                            elif refresh is True:
                                self.logger.info("Video finished downloading via new manifest")
                                break
                    time.sleep(10)
                    self.update_latest_segment()
                    continue
                
                elif len(segments_to_download) > 0 and self.is_private and len(future_to_seg) > 0:
                    self.logger.debug("Video is private, waiting for remaining threads to finish before going to stream recovery")
                    time.sleep(5)
                    continue
                elif len(segments_to_download) > 0 and self.is_private:
                    if self.stream_url.protocol == "https":
                        self.logger.debug("Video is private and still has segments remaining, moving to stream recovery")
                        self.commit_batch(self.conn)
                        self.close_connection()
                        
                        for i in range(5, 0, -1):
                            self.logger.debug("Waiting {0} minutes before starting stream recovery to improve chances of success".format(i))
                            time.sleep(60)
                        self.logger.warning("Sending stream URLs of {0} to stream recovery: {1}".format(self.format, self.stream_urls))
                        if self.livestream_coordinator:
                            try:
                                new_params = copy.deepcopy(self.params)
                                new_params.update({
                                    "info_dict": copy.deepcopy(self.info_dict),
                                    "resolution": str(self.format),
                                    "file_name": f"{self.file_base_name}",
                                    "max_workers": max((self.recovery_thread_multiplier*self.max_workers*int(len(self.stream_urls))),self.recovery_thread_multiplier)
                                })
                                new_params.pop("include_dash", None)
                                new_params.pop("include_m3u8", None)
                                new_params.pop("force_m3u8", None)
                                self.following_manifest_thread = threading.Thread(
                                    target=self.livestream_coordinator.recover_stream,
                                    kwargs=new_params,
                                    daemon=True
                                )
                                self.following_manifest_thread.start()
                            except Exception as e:
                                self.logger.exception("An error occurred while trying to recover the stream")
                        else:
                            try:
                                downloader = StreamRecovery(info_dict=self.info_dict, resolution=str(self.format), batch_size=self.batch_size, max_workers=max((self.recovery_thread_multiplier*self.max_workers*int(len(self.stream_urls))),self.recovery_thread_multiplier), file_name=self.file_base_name, cookies=self.cookies, fragment_retries=self.fragment_retries, stream_urls=self.stream_urls, proxies=self.proxies)
                                downloader.live_dl()
                                downloader.close_connection()
                            except Exception as e:
                                self.logger.exception("An error occurred while trying to recover the stream")
                        time.sleep(1)
                        self.conn, self.cursor = self.create_connection(self.temp_db_file)
                        return True
                    else:
                        self.logger.warning("{0} - Stream is now private and segments remain. Current stream protocol does not support stream recovery, ending...")
                        break
                
                elif segment_retries and all(v > self.fragment_retries for v in segment_retries.values()):
                    self.logger.warning("All remaining segments have exceeded the retry threshold, attempting URL refresh...")
                    refresh = self.refresh_url()
                    if self.refresh_url() is True:
                        self.logger.info("Video finished downloading via new manifest")
                        break
                    elif self.is_private or refresh is False:
                        self.logger.warning("Failed to refresh URL or stream is private, ending...")
                        break
                    else:
                        segment_retries = {}
                else:
                    wait = 0
                
                # Check if segments already exist within database (used to not create more connections). Needs fixing upstream
                for seg_num in segments_to_download:
                    if self.segment_exists(self.cursor, seg_num):
                        self.already_downloaded.add(seg_num)
                # Check if optimistic segment has already downloaded or not
                if optimistic_seg not in self.already_downloaded and self.segment_exists(self.cursor, optimistic_seg):
                    self.already_downloaded.add(optimistic_seg)

                segments_to_download = segments_to_download - self.already_downloaded

                #print("Segments to download: {0}".format(segments_to_download))
                #print("remaining threads: {0}".format(future_to_seg))
                
                # Add optimistic segment if conditions are right
                # Only attempt to grab optimistic segment a number of times to ensure it does not cause a loop at the end of a stream
                if (self.max_workers > 1 or not segments_to_download) and optimistic_fails < optimistic_fails_max and optimistic_seg not in self.already_downloaded and optimistic_seg not in submitted_segments:
                    # Wait estimated fragment time +0.1s to make sure it would exist. Wait a minimum of 2s if no segments are to be submitted
                    if not segments_to_download:
                        time.sleep(max(self.estimated_segment_duration, 2) + 0.1)
                    self.logger.debug("\033[93mAdding segment {1} optimistically ({0}). Currently at {2} fails\033[0m".format(self.format, optimistic_seg, optimistic_fails))
                    future_to_seg.update({
                        executor.submit(self.download_segment, self.stream_url.segment(optimistic_seg), optimistic_seg): optimistic_seg
                    })
                    submitted_segments.add(optimistic_seg)

                    # Ensure wait isn't triggered while optimistic segments is enabled
                    wait = 0
                
                
                # Add new threads to existing future dictionary, done directly to almost half RAM usage from creating new threads
                for seg_num in segments_to_download:
                    if seg_num not in submitted_segments:
                        future_to_seg.update({
                            executor.submit(self.download_segment, self.stream_url.segment(seg_num), seg_num): seg_num
                        })
                        submitted_segments.add(seg_num)
                    # Have up to 2x max workers of threads submitted
                    if len(future_to_seg) > 2*self.max_workers:
                        break
                
            self.commit_batch(self.conn)
        self.commit_batch(self.conn)
        if self.following_manifest_thread is not None:
            self.following_manifest_thread.join()
        return True

    def update_latest_segment(self):
        # Kill if keyboard interrupt is detected
        self.check_kill()
        
        stream_url_info = self.get_Headers(self.stream_url)
        if stream_url_info is not None and stream_url_info.get("X-Head-Seqnum", None) is not None:
            self.latest_sequence = int(stream_url_info.get("X-Head-Seqnum"))
            self.logger.debug("Latest sequence: {0}".format(self.latest_sequence))
            
        if stream_url_info is not None and stream_url_info.get('Content-Type', None) is not None:
            self.type, self.ext = str(stream_url_info.get('Content-Type')).split('/')
    
    def get_Headers(self, url):
        try:
            # Send a GET request to a URL
            response = requests.get(url, timeout=30, proxies=self.proxies)
            # 200 and 204 responses appear to have valid headers so far
            if response.status_code == 200 or response.status_code == 204:
                self.is_403 = False
                # Print the response headers
                #print(json.dumps(dict(response.headers), indent=4))  
                return response.headers
            elif response.status_code == 403:
                self.logger.warning("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                return None
            else:
                self.logger.debug("Error retrieving headers: {0}".format(response.status_code))
                self.logger.debug(json.dumps(dict(response.headers), indent=4))
                return None
            
        except requests.exceptions.Timeout as e:
            self.logger.info("Timed out updating fragments: {0}".format(e))
            #print(e)
            return None
        
        except Exception as e:
            self.logger.exception("\033[31m{0}\033[0m".format(e))
            return None
    
    def detect_manifest_change(self, info_json, follow_manifest=True):
        resolution = "Unknown"
        try:
            resolution = "(bv/ba/best)[format_id~='^{0}(?:-.*)?$'][protocol={1}]".format(self.stream_url.itag, self.stream_url.protocol)
            self.logger.debug("Searching for new manifest of same format {0}".format(resolution))
            temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=resolution, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8)
            if temp_stream_url is not None:
                #resolution = r"(format_id~='^({0}(?:\D*(?:[^0-9].*)?)?)$')[protocol={1}]".format(str(self.format).split('-', 1)[0], self.stream_url.protocol)
                
                #parsed_url = urlparse(temp_stream_url)        
                #temp_url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}
                #if temp_url_params.get("id", None) is not None and temp_url_params.get("id") != self.url_params.get("id"):
                if temp_stream_url.itag is not None and temp_stream_url.protocol == self.stream_url.protocol and temp_stream_url.itag == self.stream_url.itag and temp_stream_url.manifest != self.stream_url.manifest:
                    self.logger.warning("({1}) New manifest for format {0} detected, starting a new instance for the new manifest".format(self.format, self.id))
                    self.commit_batch(self.conn)
                    if follow_manifest:
                        new_params = copy.deepcopy(self.params)
                        new_params.update({
                            "info_dict": copy.deepcopy(info_json),
                            "resolution": str(self.format),
                            "file_name": f"{self.file_base_name}.{temp_stream_url.manifest}",
                            "manifest": temp_stream_url.manifest,
                        })
                        if new_params.get("download_function", None) is not None: 
                            self.following_manifest_thread = threading.Thread(
                                target=new_params.get("download_function"),
                                kwargs=new_params,
                                daemon=True
                            )
                            self.following_manifest_thread.start()
                        else:
                            download_Instance = self.__class__(**new_params)
                            self.following_manifest_thread = threading.Thread(
                                target=download_Instance.live_dl,
                                daemon=True
                            )
                            self.following_manifest_thread.start()
                    return True
                else:
                    return False
        except yt_dlp.utils.ExtractorError as e:
            self.logger.warning("Unable to find stream of same format ({0}) for {1}".format(resolution, self.id))
            
        try:
            if YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.resolution, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8) is not None:
                self.logger.debug("Searching for new manifest of same resolution {0}".format(resolution))
                temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.resolution, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8)
                if temp_stream_url.itag is not None and temp_stream_url.itag != self.stream_url.itag:
                    self.logger.warning("({2}) New manifest for resolution {0} detected, but not the same format as {1}, starting a new instance for the new manifest".format(self.resolution, self.format, self.id))
                    self.commit_batch(self.conn)
                    if follow_manifest:
                        new_params = copy.deepcopy(self.params)
                        new_params.update({
                            "info_dict": copy.deepcopy(info_json),
                            "resolution": self.resolution,
                            "file_name": f"{self.file_base_name}.{temp_stream_url.manifest}",
                            "manifest": self.stream_url.itag if self.stream_url.manifest == temp_stream_url.manifest else self.stream_url.manifest,
                        })
                        if new_params.get("download_function", None) is not None: 
                            self.following_manifest_thread = threading.Thread(
                                target=new_params.get("download_function"),
                                kwargs=new_params,
                                daemon=True
                            )
                            self.following_manifest_thread.start()
                        else:
                            download_Instance = self.__class__(**new_params)
                            self.following_manifest_thread = threading.Thread(
                                target=download_Instance.live_dl,
                                daemon=True
                            )
                            self.following_manifest_thread.start()
                    return True
                else:
                    return False
        except yt_dlp.utils.ExtractorError as e:
            self.logger.warning("Unable to find stream of same resolution ({0}) for {1}".format(self.resolution, self.id))

        try:
            if self.resolution != "audio_only" and YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution="best", include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8) is not None:
                self.logger.debug("Searching for new best stream")
                temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution="best", include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8)
                if temp_stream_url.itag is not None and temp_stream_url.itag != self.stream_url.itag:
                    self.logger.warning("({2}) New manifest has been found, but it is not the same format or resolution".format(self.resolution, self.format, self.id))
                    self.commit_batch(self.conn)
                    if follow_manifest:
                        new_params = copy.deepcopy(self.params)
                        new_params.update({
                            "info_dict": copy.deepcopy(info_json),
                            "resolution": "best",
                            "file_name": f"{self.file_base_name}.{temp_stream_url.manifest}",
                            "manifest": self.stream_url.itag if self.stream_url.manifest == temp_stream_url.manifest else self.stream_url.manifest,
                        })
                        if new_params.get("download_function", None) is not None: 
                            self.following_manifest_thread = threading.Thread(
                                target=new_params.get("download_function"),
                                kwargs=new_params,
                                daemon=True
                            )
                            self.following_manifest_thread.start()
                        else:
                            download_Instance = self.__class__(**new_params)
                            self.following_manifest_thread = threading.Thread(
                                target=download_Instance.live_dl,
                                daemon=True
                            )
                            self.following_manifest_thread.start()
                    return True
                else:
                    return False
        except yt_dlp.utils.ExtractorError as e:
            self.logger.warning("Unable to find any stream for {1} when attempting to find 'best' stream".format(self.resolution, self.id))
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
        #time.sleep(120)
        try:
            # create an HTTP adapter with the retry strategy and mount it to the session
            adapter = HTTPAdapter(max_retries=self.retry_strategy)
            # create a new session object
            session = requests.Session()
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            response = session.get(segment_url, timeout=30, proxies=self.proxies)
            if response.status_code == 200:
                self.logger.debug("Downloaded segment {0} of {1} to memory...".format(segment_order, self.format))
                self.is_403 = False
                #return latest header number and segmqnt content
                return int(response.headers.get("X-Head-Seqnum", -1)), response.content, int(segment_order), response.status_code, response.headers  # Return segment order and data
            elif response.status_code == 403:
                self.logger.debug("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                return int(response.headers.get("X-Head-Seqnum", -1)), None, segment_order, response.status_code, response.headers
            else:
                self.logger.debug("Error downloading segment {0}: {1}".format(segment_order, response.status_code))
                return int(response.headers.get("X-Head-Seqnum", -1)), None, segment_order, response.status_code, response.headers
        except requests.exceptions.Timeout as e:
            self.logger.warning("Fragment timeout {1}: {0}".format(e, segment_order))
            return -1, None, segment_order, None, None
        except requests.exceptions.RetryError as e:
            self.logger.debug("Retries exceeded downloading fragment: {0}".format(e))
            match = re.search(r"too many (\d{3}) error responses", str(e))

            if match:
                status_code = int(match.group(1))
                if status_code == 403:
                    self.is_403 = True
                    return -1, None, segment_order, 403, None
                elif status_code == 204:
                    return -1, bytes(), segment_order, 204, None
                else:
                    return -1, None, segment_order, status_code, None
            else:
                return -1, None, segment_order, None, None
        except requests.exceptions.ChunkedEncodingError as e:
            self.logger.debug("No data in request for fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, bytes(), segment_order, None, None
        except requests.exceptions.ConnectionError as e:
            self.logger.debug("Connection error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.Timeout as e:
            self.logger.debug("Timeout while retrieving downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.HTTPError as e:
            self.logger.debug("HTTP error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except Exception as e:
            self.logger.warning("Unknown error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
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
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]["current_filesize"] = os.path.getsize(self.temp_db_file)
        
    def close_connection(self):
        if self.conn:
            self.conn.close()

    def combine_segments_to_file(self, output_file, cursor=None):
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]['status'] = "merging"
        if cursor is None:
            cursor = self.cursor
        
        self.logger.debug("Merging segments to {0}".format(output_file))
        with open(output_file, 'wb') as f:
            cursor.execute('SELECT segment_data FROM segments ORDER BY id')
            first = True
            for segment in cursor:  # Cursor iterates over rows one by one
                segment_piece = segment[0]
                # Clean each segment if required as ffmpeg sometimes doesn't like the segments from YT
                if str(self.ext).lower().endswith("mp4") or not str(self.ext):
                    segment_piece = self.clean_segments(segment_piece, first)
                first = False
                f.write(segment_piece)
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]['status'] = "merged"
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
            try:
                if ofs + 8 > len(data):
                    break

                alen = int(data[ofs:ofs + 4].hex(), 16)
                if alen > len(data) or alen < 8:
                    break

                aname = data[ofs + 4:ofs + 8].decode()
                atoms[aname] = {"ofs": ofs, "len": alen}
                ofs += alen
            except Exception:
                break

        return atoms

    def remove_atoms(self, data, atom_list):
        """
        Remove specified atoms from a chunk of data

        :param data: The byte data containing atoms
        :param atom_list: List of atom names to remove
        """
        atoms = self.get_atoms(data)
        atoms_to_remove = [atoms[name] for name in atom_list if name in atoms]
        
        # Sort by offset in descending order to avoid shifting issues
        atoms_to_remove.sort(key=lambda x: x["ofs"], reverse=True)
        
        for atom in atoms_to_remove:
            ofs = atom["ofs"]
            rlen = ofs + atom["len"]
            data = data[:ofs] + data[rlen:]
        
        return data
    
    def clean_segments(self, data, first=True):
        bad_atoms = ["sidx"]
        if first is False:
            bad_atoms.append("ftyp")

        return self.remove_atoms(data=data, atom_list=bad_atoms)
    
    def check_kill(self, executor: concurrent.futures.ThreadPoolExecutor=None):
        # Kill if keyboard interrupt is detected
        if self.kill_all.is_set():
            self.logger.debug("Kill command detected, ending thread")
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
            self.close_connection()
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

    def refresh_url(self, follow_manifest=True):
        self.logger.info("Refreshing URL for {0}".format(self.format))
        if self.following_manifest_thread is None:
            try:
                info_dict, live_status = getUrls.get_Video_Info(self.id, wait=False, cookies=self.cookies, additional_options=self.yt_dlp_options, include_dash=self.include_dash, include_m3u8=(self.include_m3u8 or self.force_m3u8))
                
                # Check for new manifest, if it has, start a nested download session
                if self.detect_manifest_change(info_json=info_dict, follow_manifest=follow_manifest) is True:
                    return True
                
                #resolution = "(format_id^={0})[protocol={1}]".format(str(self.format).rsplit('-', 1)[0], self.stream_url.protocol)
                #resolution = r"(format_id~='^({0}(?:\D*(?:[^0-9].*)?)?)$')[protocol={1}]".format(str(self.format).split('-', 1)[0], self.stream_url.protocol)
                resolution = "(bv/ba/best)[format_id~='^{0}(?:-.*)?$'][protocol={1}]".format(self.stream_url.itag, self.stream_url.protocol)
                #stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=str(self.format), sort=self.yt_dlp_sort, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8)
                stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution, sort=self.yt_dlp_sort, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8) 
                if stream_url is not None:
                    self.stream_url = stream_url
                    self.stream_urls.append(stream_url)
                    
                    filtered_array = [url for url in self.stream_urls if int(self.get_expire_time(url)) > time.time()]
                    self.stream_urls = filtered_array
                    self.refresh_retries = 0
                else:
                    self.logger.warning("Unable to refresh URLs for {0} on format {2} ({1})".format(self.id, self.format, resolution))
                    
                if live_status is not None:
                    self.live_status = live_status
                
                if info_dict:
                    self.info_dict = info_dict    
                
            except getUrls.VideoInaccessibleError as e:
                self.logger.warning("Video Inaccessible error: {0}".format(e))
                if "membership" in str(e) and not self.is_403:
                    self.logger.warning("{0} is now members only. Continuing until 403 errors")
                else:
                    self.is_private = True
            except getUrls.VideoUnavailableError as e:
                self.logger.critical("Video Unavailable error: {0}".format(e))
                if self.get_expire_time(self.stream_url) < time.time():
                    raise TimeoutError("Video is unavailable and stream url for {0} has expired, unable to continue...".format(self.format))
            except getUrls.VideoProcessedError as e:
                # Livestream has been processed
                self.logger.exception("Error refreshing URL: {0}".format(e))
                self.logger.info("Livestream has ended and processed.")
                return False
            except getUrls.LivestreamError:
                self.logger.debug("Livestream has ended.")
                self.live_status = "was_live"
                return False 
            except Exception as e:
                self.logger.exception("Error: {0}".format(e))                     
        self.url_checked = time.time()

        if self.live_status != 'is_live':
            self.logger.debug("Livestream has ended.")
            #self.catchup()
            return False 

class DownloadStreamDirect(DownloadStream):
    def __init__(self, info_dict, resolution='best', batch_size=10, max_workers=5, fragment_retries=5,
                folder=None, file_name=None, cookies=None, yt_dlp_options=None, proxies=None,
                yt_dlp_sort=None, include_dash=False, include_m3u8=False, force_m3u8=False, download_params = {}, livestream_coordinator: LiveStreamDownloader=None):
        params = download_params or locals().copy()
        # Initialize base class, but use in-memory DB (unused)
        super().__init__(
            info_dict=info_dict,
            resolution=resolution,
            batch_size=batch_size,
            max_workers=max_workers,
            fragment_retries=fragment_retries,
            folder=folder,
            file_name=file_name,
            database_in_memory=True,
            cookies=cookies,
            yt_dlp_options=yt_dlp_options,
            proxies=proxies,
            yt_dlp_sort=yt_dlp_sort,
            include_dash=include_dash,
            include_m3u8=include_m3u8,
            force_m3u8=force_m3u8,
            download_params=params,
            livestream_coordinator=livestream_coordinator,
        )
        # Close the unused in-memory DB connection
        if self.conn:
            self.close_connection()
        self.conn = None
        self.cursor = None

        # State tracking for direct writes
        self.state_file_name = f"{self.file_base_name}.{self.format}.state"
        self.state_file_backup = f"{self.file_base_name}.{self.format}.state.bkup"

        if self.folder:
            self.state_file_name = os.path.join(self.folder, os.path.basename(self.state_file_name))
            self.state_file_backup = os.path.join(self.folder, os.path.basename(self.state_file_backup))

        self.state = {
            'last_written': -1,
            'file_size': 0
        }

        # Attempt to restore existing state
        self._load_existing_state()
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type] = {}
        self.logger.debug(f"DownloadStreamDirect initialized for {self.id} ({self.format})")

    def _load_existing_state(self):
        """Restore download progress if a state file exists"""
        for path in [self.state_file_backup, self.state_file_name]:
            if os.path.exists(path) and os.path.exists(self.merged_file_name):
                try:
                    with open(path, "r") as file:
                        loaded = json.load(file)
                    ts_size = os.path.getsize(self.merged_file_name)
                    if ts_size >= loaded.get('file_size', 0) and loaded.get('last_written', None) is not None:
                        self.state = loaded
                        self.logger.debug(f"Resumed state: {self.state}")
                        return
                except Exception as e:
                    self.logger.warning(f"Failed to load state file {path}: {e}")

    def _save_state(self):
        """Safely write the current state to disk"""
        try:
            if os.path.exists(self.state_file_name):
                shutil.move(self.state_file_name, self.state_file_backup)
            with open(self.state_file_name, "w") as f:
                json.dump(self.state, f, indent=4)
        except Exception as e:
            self.logger.warning(f"Failed to save state file: {e}")
    
    def live_dl(self):
        self.logger.info(f"\033[31mStarting download of live fragments ({self.format}) [Direct Mode]\033[0m")
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]['status'] = "recording"
            self.livestream_coordinator.stats[self.type]["downloaded_segments"] = self.state['last_written']
            self.livestream_coordinator.stats[self.type]["current_filesize"] = self.state['file_size']

        submitted_segments = set()
        downloaded_segments = {}
        future_to_seg = {}
        optimistic_seg = 0
        optimistic = True
        wait = 0

        

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers,
                                                thread_name_prefix=f"{self.id}-{self.format}") as executor:
            
            # Trackers for optimistic segment downloads 
            optimistic_fails_max = 10
            optimistic_fails = 0
            optimistic_seg = 0  
            # Add range of up to head segment +1
            segments_to_download = list()   
            segment_retries = {}      

            while True:
                self.check_kill(executor)
                if self.refresh_Check() is True:
                    break

                done, _ = concurrent.futures.wait(future_to_seg, timeout=1, return_when=concurrent.futures.ALL_COMPLETED)

                for future in done:
                    head_seg_num, segment_data, seg_num, status, headers = future.result()
                    submitted_segments.discard(seg_num)
                    future_to_seg.pop(future, None)
                    self.logger.debug("\033[92mFormat: {3}, Segnum: {0}, Status: {1}, Data: {2}\033[0m".format(
                            seg_num, status, "None" if segment_data is None else f"{len(segment_data)} bytes", self.format
                        ))

                    if seg_num >= optimistic_seg and (status is None or status != 200):
                        optimistic_fails += 1
                        self.logger.debug("Unable to optimistically grab segment {1} for {0}. Up to {2} attempts".format(self.format, seg_num, optimistic_fails))
                        
                    elif seg_num >= optimistic_seg and status == 200:
                        optimistic_fails = 0
                    
                    if head_seg_num > self.latest_sequence:
                        self.logger.debug("More segments available: {0}, previously {1}".format(head_seg_num, self.latest_sequence))                    
                        self.latest_sequence = head_seg_num
                        if self.livestream_coordinator:
                            self.livestream_coordinator.stats[self.type]["latest_sequence"] = self.latest_sequence
                        
                    if headers is not None and headers.get("X-Head-Time-Sec", None) is not None:
                        self.estimated_segment_duration = int(headers.get("X-Head-Time-Sec"))/self.latest_sequence

                    if segment_data is not None:
                        downloaded_segments[seg_num] = segment_data
                        segment_retries.pop(seg_num, None)
                    elif status is None or status != 200:
                        segment_retries[seg_num] = segment_retries.get(seg_num, 0) + 1
                        self.logger.debug("Unable to download {0} ({1}). Currently at {2} retries".format(seg_num, self.format, segment_retries.get(seg_num, "UNKNOWN")))

                # Write contiguous downloaded segments
                # Check if there is at least one segment to write
                if downloaded_segments.get(self.state['last_written'] + 1, None) is not None:
                    # If segments exist, open the file *once*
                    mode = 'wb' if self.state['file_size'] == 0 else 'r+b'
                    with open(self.merged_file_name, mode) as f:
                        # Seek to the end of the file *once* (if not a new file)
                        if mode != 'wb':
                            f.seek(self.state['file_size'])

                        # Loop and write all available consecutive segments
                        while downloaded_segments.get(self.state['last_written'] + 1, None) is not None:
                            seg_num = self.state['last_written'] + 1
                            segment = downloaded_segments.pop(seg_num)
                            cleaned = self.clean_segments(segment)
                            
                            f.write(cleaned)
                            f.truncate()  # Truncates the file at the current position (after the write)
                            
                            self.state['last_written'] = seg_num
                            
                            # Optimization: Use f.tell() instead of os.path.getsize()
                            # f.tell() returns the current file position, which is the new
                            # file size after writing and truncating. This is much faster.
                            self.state['file_size'] = f.tell()                           
                            if self.livestream_coordinator:
                                self.livestream_coordinator.stats[self.type]["downloaded_segments"] = self.state['last_written']
                                self.livestream_coordinator.stats[self.type]["current_filesize"] = self.state['file_size']
                            self.logger.debug(f"Written segment {seg_num} ({self.format}), file size: {self.state['file_size']} bytes")
                    self._save_state()

                # Remove any potential stray segments 
                downloaded_segments = dict((k, v) for k, v in downloaded_segments.items() if k >= self.state.get('last_written',0))

                # Determine segments to download. Sort into a list as direct to ts relies on segments to be written in order
                segments_to_download = sorted(set(range(self.state['last_written'] + 1, self.latest_sequence + 1)) - submitted_segments - set(downloaded_segments.keys()) - set(k for k, v in segment_retries.items() if v > self.fragment_retries))

                optimistic_seg = max(self.latest_sequence, self.state.get('last_written',0)) + 1  
                                        
                
                if optimistic_fails < optimistic_fails_max and optimistic_seg not in submitted_segments and optimistic_seg not in self.already_downloaded and len(segments_to_download) <= 2 * self.max_workers:
                    # Wait estimated fragment time +0.1s to make sure it would exist. Wait a minimum of 2s
                    if not segments_to_download:
                        time.sleep(max(self.estimated_segment_duration, 2) + 0.1)
                    
                    self.logger.debug("\033[93mAdding segment {1} optimistically ({0}). Currently at {2} fails\033[0m".format(self.format, optimistic_seg, optimistic_fails))
                    segments_to_download.append(optimistic_seg)
                                    
                # If update has no segments and no segments are currently running, wait                              
                if not segments_to_download and not future_to_seg:                 
                    wait += 1
                    self.logger.debug("No new fragments available for {0}, attempted {1} times...".format(self.format, wait))
                        
                    # If waited for new fragments hits 20 loops, assume stream is offline
                    if wait > 20:
                        self.logger.debug("Wait time for new fragment exceeded, ending download...")
                        break    
                    # If over 10 wait loops have been executed, get page for new URL and update status if necessary
                    elif wait > 10:
                        if self.is_private:
                            self.logger.debug("Video is private and no more segments are available. Ending...")
                            break
                        else:
                            refresh = self.refresh_url(follow_manifest=False)
                            if refresh is False:
                                break       
                            elif refresh is True:
                                self.logger.warning("Video has new manifest. This cannot be handled by current implementation of Direct to .ts implementation")
                                break
                    time.sleep(10)
                    # Check for header updates
                    self.update_latest_segment()
                    continue
                
                elif len(segments_to_download) > 0 and self.is_private and len(future_to_seg) > 0:
                    self.logger.debug("Video is private, waiting for remaining threads to finish before ending")
                    time.sleep(5)
                    continue

                elif len(segments_to_download) > 0 and self.is_private:
                    self.logger.warning("{0} - Stream is now private and segments remain. Current stream protocol does not support stream recovery, ending...")
                    break

                elif segment_retries and all(v > self.fragment_retries for v in segment_retries.values()):
                    self.logger.warning("All remaining segments have exceeded the retry threshold, attempting URL refresh...")
                    if self.refresh_url(follow_manifest=False) is True:
                        self.logger.warning("Video has new manifest. This cannot be handled by current implementation of Direct to .ts implementation")
                        break
                    elif self.is_private or self.refresh_url(follow_manifest=False) is False:
                        self.logger.warning("Failed to refresh URL or stream is private, ending...")
                        break
                    else:
                        segment_retries = {}
                else:
                    wait = 0

                for seg_num in segments_to_download:
                    if seg_num not in submitted_segments:
                        future_to_seg[executor.submit(self.download_segment, self.stream_url.segment(seg_num), seg_num)] = seg_num
                        submitted_segments.add(seg_num)
                    if len(future_to_seg) > 2 * self.max_workers:
                        break
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]['status'] = "merged"
        self.logger.info(f"Completed direct download for {self.format}")
        return self.merged_file_name
        
    def delete_state_file(self):
        """Remove saved state files"""
        for path in [self.state_file_name, self.state_file_backup]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                self.logger.warning(f"Failed to delete state file {path}: {e}")

    def remove_folder(self):
        """Remove folder and associated files"""
        if self.folder:
            self.delete_state_file()
            self.delete_ts_file()
            try:
                os.rmdir(self.folder)
            except Exception:
                pass

# Gemini super class version - remains untested with youtube changes
class StreamRecovery(DownloadStream):
    
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
            if response and response.status and self.segment_number is not None:
                self.logger.debug("{0} encountered a {1} code".format(self.segment_number, response.status))
                    
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
        

    def __init__(self, info_dict={}, resolution='best', batch_size=10, max_workers=5, fragment_retries=5, folder=None, file_name=None, database_in_memory=False, cookies=None, recovery=False, segment_retry_time=30, stream_urls=[], live_status="is_live", proxies=None, yt_dlp_sort=None, livestream_coordinator: LiveStreamDownloader=None):        
        from datetime import datetime
        # Call the base class __init__.
        # This will perform all common setup: file paths, proxy, cookies,
        # initial DB creation, etc.
        # We pass flags that are specific to StreamRecovery's logic (e.g., no DASH).
        super().__init__(
            info_dict=info_dict,
            resolution=resolution,
            batch_size=batch_size,
            max_workers=max_workers,
            fragment_retries=fragment_retries,
            folder=folder,
            file_name=file_name,
            database_in_memory=database_in_memory,
            cookies=cookies,
            recovery_thread_multiplier=2, # Default value, not used by StreamRecovery
            yt_dlp_options=None, # StreamRecovery doesn't use this
            proxies=proxies,
            yt_dlp_sort=yt_dlp_sort,
            include_dash=False, # StreamRecovery logic specifically excludes DASH
            include_m3u8=False,
            force_m3u8=False,
            livestream_coordinator=livestream_coordinator,        
        )      
        self.expires = time.time()
        # --- Start of StreamRecovery-specific __init__ logic ---
        # The following logic overrides or extends the base class setup.

        # Override ID and live_status with fallback logic
        self.id = info_dict.get('id', self.get_id_from_url(stream_urls[0]) if stream_urls else self.id)
        self.live_status = info_dict.get('live_status', live_status)
        self.yt_dlp_sort = yt_dlp_sort
            
        # Override stream_urls and format
        # StreamRecovery can be passed URLs directly or finds *all* of them,
        # unlike DownloadStream which finds just one.
        if stream_urls:
            self.logger.debug("{0} stream urls available".format(len(stream_urls)))
            for url in stream_urls:
                self.format = url.format_id
                if self.format is not None:
                    self.logger.debug("Stream recovery - Found format {0} from itags".format(self.format))
                    break            
            self.stream_urls = stream_urls          
        else:
            self.stream_urls = YoutubeURL.Formats().getFormatURL(
                info_json=info_dict, 
                resolution=resolution,  
                sort=self.yt_dlp_sort, 
                get_all=True, # Key difference: get all URLs
                include_dash=False,
                include_m3u8=False
            )

        if not self.stream_urls:
            raise ValueError("No compatible stream URLs not found for {0}, unable to continue".format(resolution))
            
        self.format = str(self.stream_urls[0].itag)            
        
        self.logger.debug("Recovery - Resolution: {0}, Format: {1}".format(resolution, self.format))
        
        self.logger.debug("Number of stream URLs available: {0}".format(len(self.stream_urls)))
        
        # Override stream_url with a random choice
        self.stream_url = random.choice(self.stream_urls)
        self.format = self.stream_url.format_id
        
        # The base __init__ already set file names based on its format detection.
        # We must re-set them using the format this class detected, which may differ.
        original_db_file = self.temp_db_file
        self.merged_file_name = "{0}.{1}.ts".format(self.file_base_name, self.format)     
        if self.database_in_memory:
            self.temp_db_file = ':memory:'
        else:
            self.temp_db_file = '{0}.{1}.temp'.format(self.file_base_name, self.format)
        
        if self.folder:
            self.merged_file_name = os.path.join(self.folder, os.path.basename(self.merged_file_name))
            if not self.database_in_memory:
                self.temp_db_file = os.path.join(self.folder, os.path.basename(self.temp_db_file))

        # If the determined format (and thus DB file) is different from the
        # one the base class created, we must close the old DB connection
        # and create a new one pointing to the correct file.
        if original_db_file != self.temp_db_file:
            self.close_connection()
            self.conn, self.cursor = self.create_db(self.temp_db_file) 
        
        # Override retry_strategy with the custom one for recovery
        self.retry_strategy = self.CustomRetry(
            total=3,  # maximum number of retries
            backoff_factor=1, 
            status_forcelist=[204, 400, 401, 403, 404, 408, 429, 500, 502, 503, 504],
            downloader_instance=self,
            backoff_max=4
        )  
        
        self.fragment_retries = fragment_retries  
        self.segment_retry_time = segment_retry_time  
        
        # Set StreamRecovery-specific properties
        self.is_401 = False
        self.recover = recovery
        self.sequential = False
        self.count_400s = 0
        self.sleep_time = 1
        
        # Override expires logic to check all available URLs
        expires = [
            int(self.get_expire_time(url))
            for url in self.stream_urls
            if self.get_expire_time(url) is not None
        ]

        if expires:
            self.expires = max(expires)
            
        if self.expires and time.time() > self.expires:
            self.logger.error("\033[31mCurrent time is beyond highest expire time, unable to recover\033[0m".format(self.format))
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            format_exp = datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')
            raise TimeoutError("Current time {0} exceeds latest URL expiry time of {1}".format(now, format_exp))
        
        # The base __init__ already called update_latest_segment(),
        # but we must call it again because self.stream_url and self.stream_urls
        # have been overridden.
        self.update_latest_segment()
        
        self.url_checked = time.time()

        # (Re-)populate already_downloaded from the correct DB
        self.already_downloaded = self.segment_exists_batch() 
        
        # Set more StreamRecovery-specific properties
        self.count_403s = {}        
        self.user_agent_403s = {}
        self.user_agent_full_403s = {}
        
        # Ensure stats are set for the correct type
        if self.type and self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type] = {}
    
    def get_format_from_url(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        self.logger.debug(query_params)
        # Get the 'expire' parameter
        self.logger.debug("Itags from url: {0}".format(query_params.get("itag", [None])))
        itag = query_params.get("itag", [None])[0]
        return str(itag).strip()
    
    def get_id_from_url(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        id = str(query_params.get("id", [None])[0])[:11].strip()
        return id
                
    def live_dl(self):
        # This method is completely different from the base class.
        # It's designed to recover missing segments, not optimistically
        # download a live edge.
        self.logger.info("\033[31mStarting download of live fragments ({0})\033[0m".format(self.format))
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]['status'] = "recording"
        self.already_downloaded = self.segment_exists_batch()
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
            
            if self.expires is not None:
                from datetime import datetime
                self.logger.debug("Recovery mode active, URL expected to expire at {0}".format(datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')))
            else:
                self.logger.debug("Recovery mode active")
                    
            
            while True:     
                self.check_kill(executor)     
                if self.livestream_coordinator and self.livestream_coordinator.stats.get(self.type, None) is None:
                    self.livestream_coordinator.stats[self.type] = {}     

                self.check_Expiry()

                if (not self.stream_urls) or (self.expires and time.time() > self.expires):
                    self.logger.fatal("\033[31mCurrent time is beyond highest expire time and no valid URLs remain, unable to recover\033[0m".format(self.format))
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    format_exp = datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')
                    self.commit_batch()
                    raise TimeoutError("Current time {0} exceeds latest URL expiry time of {1}".format(now, format_exp)) 
                
                done, not_done = concurrent.futures.wait(future_to_seg, timeout=0.1, return_when=concurrent.futures.ALL_COMPLETED)
                
                for future in done:
                    head_seg_num, segment_data, seg_num, status, headers = future.result()

                    self.logger.debug("\033[92mFormat: {3}, Segnum: {0}, Status: {1}, Data: {2}\033[0m".format(
                            seg_num, status, "None" if segment_data is None else f"{len(segment_data)} bytes", self.format
                        ))
                    
                    if seg_num in submitted_segments:
                        submitted_segments.discard(seg_num)
                    
                    if head_seg_num > self.latest_sequence:
                        self.logger.debug("More segments available: {0}, previously {1}".format(head_seg_num, self.latest_sequence))        
                        self.segments_retries.update({key: {'retries': 0, 'last_retry': 0, 'ideal_retry_time': random.uniform(max(self.segment_retry_time,900),max(self.segment_retry_time+300,1200))} for key in range(self.latest_sequence, head_seg_num) if key not in self.already_downloaded})
                        self.latest_sequence = head_seg_num
                        
                        
                    if headers is not None and headers.get("X-Head-Time-Sec", None) is not None:
                        self.estimated_segment_duration = int(headers.get("X-Head-Time-Sec"))/self.latest_sequence  
                        
                    if segment_data is not None:
                        self.insert_single_segment(cursor=self.cursor, segment_order=seg_num, segment_data=segment_data)
                        uncommitted_inserts += 1
                        
                        if self.segments_retries.get(seg_num, None) is not None:
                            self.segments_retries.pop(seg_num,None)
                        
                        if uncommitted_inserts >= max(self.batch_size, len(done)):
                            self.logger.debug("Writing segments to file...")
                            self.commit_batch(self.conn)
                            uncommitted_inserts = 0
                            self.cursor.execute('BEGIN TRANSACTION') 
                    else:
                        if self.segments_retries.get(seg_num, None) is not None:
                            self.segments_retries[seg_num]['retries'] = self.segments_retries[seg_num]['retries'] + 1
                            self.segments_retries[seg_num]['last_retry'] = time.time()
                            if self.segments_retries[seg_num]['retries'] >= self.fragment_retries:
                                self.logger.debug("Segment {0} of {1} has exceeded maximum number of retries".format(seg_num, self.latest_sequence))
                                
                    future_to_seg.pop(future,None)

                    if self.livestream_coordinator:
                        self.livestream_coordinator.stats[self.type]["latest_sequence"] = self.latest_sequence
                    
                        self.livestream_coordinator.stats[self.type]["downloaded_segments"] = self.latest_sequence - len(self.segments_retries)
                                            
                    
                if len(self.segments_retries) <= 0:
                    self.logger.info("All segment downloads complete, ending...")
                    break

                elif all(value['retries'] > self.fragment_retries for value in self.segments_retries.values()):
                    self.logger.error("All remaining segments have exceeded their retry count, ending...")
                    break
                
                elif self.is_403 and self.expires is not None and time.time() > self.expires:
                    self.logger.fatal("URL(s) have expired and failures being detected, ending...")
                    break               
                
                elif self.is_401:
                    self.logger.debug("401s detected for {0}, sleeping for a minute")
                    time.sleep(60)
                    for url in self.stream_urls:
                        if self.live_status == 'post_live':
                            self.update_latest_segment(url=self.stream_url.segment(self.latest_sequence+1))
                        else:
                            self.update_latest_segment(url=url)
                
                elif self.is_403:
                    for url in self.stream_urls:
                        if self.live_status == 'post_live':
                            self.update_latest_segment(url=self.stream_url.segment(self.latest_sequence+1))
                        else:
                            self.update_latest_segment(url=url)
                    
                segments_to_download = set()
                potential_segments_to_download = set(self.segments_retries.keys()) - self.already_downloaded
                
                sorted_retries = -1
                if self.sequential:
                    sorted_retries = dict(sorted(self.segments_retries.items(), key=lambda item: (item[1]['retries'], item[0])))
                else:
                    current_time = time.time()
                    priority_items = {
                        key: value for key, value in self.segments_retries.items()
                        if (current_time - value['last_retry']) > value['ideal_retry_time'] and value['retries'] > 0
                    }
                    non_priority_items = {
                        key: value for key, value in self.segments_retries.items()
                        if not ((current_time - value['last_retry']) > value['ideal_retry_time'] and value['retries'] > 0)
                    }
                    priority_items_sorted = dict(sorted(priority_items.items(), key=lambda item: item[1]['retries']))
                    non_priority_items_sorted = dict(sorted(non_priority_items.items(), key=lambda item: item[1]['retries']))
                    sorted_retries = priority_items_sorted | non_priority_items_sorted
                    
                    
                if sorted_retries != -1:
                    potential_segments_to_download = sorted_retries.keys()
                    
                if not not_done or len(not_done) < self.max_workers:
                    new_download = set()
                    number_to_add = self.max_workers - len(not_done)

                    for seg_num in potential_segments_to_download:
                        if seg_num not in submitted_segments and self.segments_retries[seg_num]['retries'] <= self.fragment_retries and time.time() - self.segments_retries[seg_num]['last_retry'] > self.segment_retry_time:                            
                            if seg_num in self.already_downloaded:
                                self.segments_retries.pop(seg_num,None)
                                continue
                            if self.segment_exists(self.cursor, seg_num):
                                self.already_downloaded.add(seg_num)
                                continue
                            new_download.add(seg_num)
                            self.logger.debug("Adding segment {0} of {2} with retries: {1}".format(seg_num, self.segments_retries[seg_num]['retries'], self.format))
                        if len(new_download) >= number_to_add:                            
                            break
                    segments_to_download = new_download
                    
                for seg_num in segments_to_download:
                    if seg_num not in submitted_segments:
                        # Round-robin through available stream URLs
                        future_to_seg[executor.submit(self.download_segment, self.stream_urls[i % len(self.stream_urls)].segment(seg_num), seg_num)] = seg_num
                        submitted_segments.add(seg_num)
                        i += 1
                
                if len(submitted_segments) == 0 and len(self.segments_retries) < 11 and time.time() - last_print > self.segment_retry_time:
                    self.logger.debug("{2} remaining segments for {1}: {0}".format(self.segments_retries, self.format, len(self.segments_retries)))
                    last_print = time.time()
                elif len(submitted_segments) == 0 and time.time() - last_print > self.segment_retry_time + 5:
                    self.logger.debug("{0} segments remain for {1}".format(len(self.segments_retries), self.format))
                    last_print = time.time()
                
            self.commit_batch(self.conn)
        self.commit_batch(self.conn)
        return len(self.segments_retries)
    
    def check_Expiry(self): 
        #print("Refresh check ({0})".format(self.format)) 
        filtered_array = [url for url in self.stream_urls if int(self.get_expire_time(url)) >= time.time()]
        self.stream_urls = filtered_array  

        expires = [
            int(self.get_expire_time(url))
            for url in self.stream_urls
            if self.get_expire_time(url) is not None
        ]

        if expires:
            self.expires = max(expires)


    def update_latest_segment(self, url=None):
        from datetime import datetime
        # Overrides base method to handle multiple, expiring URLs
        self.check_kill()
        
        # Remove expired URLs
        self.check_Expiry()  

        if (not self.stream_urls) or (self.expires and time.time() > self.expires):
            self.logger.fatal("\033[31mCurrent time is beyond highest expire time and no valid URLs remain, unable to recover\033[0m".format(self.format))
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            format_exp = datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')
            self.commit_batch()
            raise TimeoutError("Current time {0} exceeds latest URL expiry time of {1}".format(now, format_exp))         
        
        if url is None:
            url = random.choice(self.stream_urls)
        
        stream_url_info = self.get_Headers(url)
        if stream_url_info is not None and stream_url_info.get("X-Head-Seqnum", None) is not None:
            new_latest = int(stream_url_info.get("X-Head-Seqnum"))
            if new_latest > self.latest_sequence and self.latest_sequence > -1:
                self.segments_retries.update({key: {'retries': 0, 'last_retry': 0, 'ideal_retry_time': random.uniform(max(self.segment_retry_time,900),max(self.segment_retry_time+300,1200))} for key in range(self.latest_sequence, new_latest) if key not in self.already_downloaded})
            self.latest_sequence = new_latest
            self.logger.debug("Latest sequence: {0}".format(self.latest_sequence))
            
        if stream_url_info is not None and stream_url_info.get('Content-Type', None) is not None:
            self.type, self.ext = str(stream_url_info.get('Content-Type')).split('/')
            
        if stream_url_info is not None and stream_url_info.get("X-Head-Time-Sec", None) is not None:
            self.estimated_segment_duration = int(stream_url_info.get("X-Head-Time-Sec"))/max(self.latest_sequence,1)
        
        if self.livestream_coordinator:
            if self.livestream_coordinator.stats.get(self.type, None):    
                self.livestream_coordinator.stats[self.type]["latest_sequence"] = self.latest_sequence
            else:
                self.livestream_coordinator.stats[self.type] = {}
                self.livestream_coordinator.stats[self.type]["latest_sequence"] = self.latest_sequence
    
    def get_Headers(self, url):
        # Overrides base method to add 401 handling
        try:
            response = requests.get(url, timeout=30, proxies=self.proxies)
            if response.status_code == 200 or response.status_code == 204:
                self.is_403 = False
                self.is_401 = False
            elif response.status_code == 403:
                self.is_403 = True
            elif response.status_code == 401:
                self.is_401 = True # <-- Specific to StreamRecovery
            else:
                self.logger.warning("Error retrieving headers: {0}".format(response.status_code))
                self.logger.debug(json.dumps(dict(response.headers), indent=4))
            return response.headers
            
        except requests.exceptions.Timeout as e:
            self.logger.debug("Timed out updating fragments: {0}".format(e))
            return None
        
        except Exception as e:
            self.logger.exception("\033[31m{0}\033[0m".format(e))
            return None

    def download_segment(self, segment_url, segment_order):
        # Overrides base method to add User-Agent rotation
        # and more detailed exception handling for recovery
        self.check_kill()

        adapter = HTTPAdapter(max_retries=self.retry_strategy)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Key difference: Use random User-Agent
        user_agent = random.choice(user_agents)
        headers = {
            "User-Agent": user_agent,
        }
        
        try:            
            response = session.get(segment_url, timeout=30, headers=headers, proxies=self.proxies)
            if response.status_code == 200:
                self.logger.debug("Downloaded segment {0} of {1} to memory...".format(segment_order, self.format))
                self.is_403 = False
                self.is_401 = False
                return int(response.headers.get("X-Head-Seqnum", -1)), response.content, int(segment_order), response.status_code, response.headers
            elif response.status_code == 403:
                self.logger.debug("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                return -1, None, segment_order, response.status_code, response.headers
            else:
                self.logger.debug("Error downloading segment {0}: {1}".format(segment_order, response.status_code))
                return -1, None, segment_order, response.status_code, response.headers
        except requests.exceptions.Timeout as e:
            self.logger.debug(e)
            return -1, None, segment_order, None, None
        except requests.exceptions.RetryError as e:
            # Key difference: More detailed handling
            self.logger.debug("Retries exceeded downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
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
            self.logger.debug("No data in request for fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, bytes(), segment_order, None, None
        except requests.exceptions.ConnectionError as e:
            self.logger.debug("Connection error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.Timeout as e:
            self.logger.warning("Timeout while retrieving downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.HTTPError as e:
            self.logger.warning("HTTP error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except Exception as e:
            self.logger.exception("Unknown error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
            
    def save_stats(self):
        # Stats files
        with open("{0}.{1}_seg_403s.json".format(self.file_base_name, self.format), 'w', encoding='utf-8') as outfile:
            json.dump(self.count_403s, outfile, indent=4)
        with open("{0}.{1}_usr_ag_403s.json".format(self.file_base_name, self.format), 'w', encoding='utf-8') as outfile:
            json.dump(self.user_agent_403s, outfile, indent=4)
        with open("{0}.{1}_usr_ag_full_403s.json".format(self.file_base_name, self.format), 'w', encoding='utf-8') as outfile:
            json.dump(self.user_agent_full_403s, outfile, indent=4)
'''
class StreamRecovery:
    
    def __init__(self, info_dict={}, resolution='best', batch_size=10, max_workers=5, fragment_retries=5, folder=None, file_name=None, database_in_memory=False, cookies=None, recovery=False, segment_retry_time=30, stream_urls=[], live_status="is_live", proxies=None, yt_dlp_sort=None):        
        self.conn = None
        from datetime import datetime
        self.latest_sequence = -1
        self.already_downloaded = set()
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # If info.json is defined, use the value within that, otherwise attempt to extracct ID from the first stream URL
        self.id = info_dict.get('id', self.get_id_from_url(stream_urls[0]) if stream_urls else None)

        # Use info.json if available, otherwise try using passed live_status value (default is_live)
        self.live_status = info_dict.get('live_status', live_status)
        
        self.yt_dlp_sort = yt_dlp_sort
        
        #print("Stream recovery info dict: {0}".format(info_dict))
        #print("Stream recovery format: {0}".format(resolution))
        
        
        # If stream URLs are given, use them to get the format and also try to extract any URLs from the info.json too. If no stream URLs are passed, use the given resolution and the info.json only               
        if stream_urls:
            logging.debug("{0} stream urls available".format(len(stream_urls)))
            for url in stream_urls:
                self.format = self.get_format_from_url(url)
                if self.format is not None:
                    logging.debug("Stream recovery - Found format {0} from itags".format(self.format))
                    break            
            self.stream_urls = stream_urls          
        else:
            self.stream_urls, self.format = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution, return_format=True, sort=self.yt_dlp_sort, get_all=True, include_dash=False)
        
        logging.debug("Recovery - Resolution: {0}, Format: {1}".format(resolution, self.format))
        """        
        if stream_urls:
            if not self.stream_urls:
                self.stream_urls = []
            self.stream_urls = list(set(self.stream_urls) | set(stream_urls))
        """
        if self.stream_urls is None:
            raise ValueError("Stream URL not found for {0}, unable to continue".format(resolution))
        
        logging.debug("Number of stream URLs available: {0}".format(len(self.stream_urls)))
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
            backoff_max=4
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
            
            logging.error("\033[31mCurrent time is beyond highest expire time, unable to recover\033[0m".format(self.format))
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            format_exp = datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')
            raise TimeoutError("Current time {0} exceeds latest URL expiry time of {1}".format(now, format_exp))
        
        self.proxies = proxies
        
        self.update_latest_segment()
        
        
        self.url_checked = time.time()

        self.conn, self.cursor = self.create_db(self.temp_db_file) 
        
        self.count_403s = {}        
        self.user_agent_403s = {}
        self.user_agent_full_403s = {}
        stats[self.type] = {}

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
        return False
        
    def get_expire_time(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        # Get the 'expire' parameter
        expire_value = query_params.get('expire', [-1])[0]
        if expire_value is not None:
            return int(expire_value)
        return expire_value
    
    def get_format_from_url(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        logging.debug(query_params)
        # Get the 'expire' parameter
        logging.debug("Itags from url: {0}".format(query_params.get("itag", [None])))
        itag = query_params.get("itag", [None])[0]
        return str(itag).strip()
    
    def get_id_from_url(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        id = str(query_params.get("id", [None])[0])[:11].strip()
        return id
                
    def live_dl(self):
        #from itertools import groupby
        logging.info("\033[31mStarting download of live fragments ({0})\033[0m".format(self.format))
        stats[self.type]['status'] = "recording"
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
                logging.debug("Recovery mode active, URL expected to expire at {0}".format(datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')))
            else:
                logging.debug("Recovery mode active")
                       
            
            while True:     
                self.check_kill()     
                if stats.get(self.type, None) is None:
                    stats[self.type] = {}                                   
                # Process completed segment downloads, wait up to 5 seconds for segments to complete before next loop
                done, not_done = concurrent.futures.wait(future_to_seg, timeout=0.1, return_when=concurrent.futures.ALL_COMPLETED)  # need to fully determine if timeout or ALL_COMPLETED takes priority             
                
                for future in done:
                    head_seg_num, segment_data, seg_num, status, headers = future.result()
                    
                    # Remove from submitted segments in case it neeeds to be regrabbed
                    if seg_num in submitted_segments:
                        submitted_segments.discard(seg_num)
                    
                    if head_seg_num > self.latest_sequence:
                        logging.debug("More segments available: {0}, previously {1}".format(head_seg_num, self.latest_sequence))        
                        self.segments_retries.update({key: {'retries': 0, 'last_retry': 0, 'ideal_retry_time': random.uniform(max(self.segment_retry_time,900),max(self.segment_retry_time+300,1200))} for key in range(self.latest_sequence, head_seg_num) if key not in self.already_downloaded})
                        self.latest_sequence = head_seg_num
                        
                        
                    if headers is not None and headers.get("X-Head-Time-Sec", None) is not None:
                        self.estimated_segment_duration = int(headers.get("X-Head-Time-Sec"))/self.latest_sequence  
                        
                    #if headers and headers.get('X-Bandwidth-Est'):
                    #    stats[self.type]["estimated_size"] = int(headers.get('X-Bandwidth-Est'))

                    if segment_data is not None:
                        # Insert segment data in the main thread (database interaction)
                        self.insert_single_segment(cursor=self.cursor, segment_order=seg_num, segment_data=segment_data)
                        uncommitted_inserts += 1
                        
                        # Assume segment will be added
                        if self.segments_retries.get(seg_num, None) is not None:
                            self.segments_retries.pop(seg_num,None)
                        
                        # If finished threads exceeds batch size, commit the whole batch of threads at once. 
                        # Has risk of not committing if a thread has no segment data, but this would be corrected naturally in following loop(s)
                        if uncommitted_inserts >= max(self.batch_size, len(done)):
                            logging.debug("Writing segments to file...")
                            self.commit_batch(self.conn)
                            uncommitted_inserts = 0
                            self.cursor.execute('BEGIN TRANSACTION') 
                    else:
                        if self.segments_retries.get(seg_num, None) is not None:
                            self.segments_retries[seg_num]['retries'] = self.segments_retries[seg_num]['retries'] + 1
                            self.segments_retries[seg_num]['last_retry'] = time.time()
                            if self.segments_retries[seg_num]['retries'] >= self.fragment_retries:
                                logging.debug("Segment {0} of {1} has exceeded maximum number of retries".format(seg_num, self.latest_sequence))
                                
                    
                    stats[self.type]["latest_sequence"] = self.latest_sequence
                    # Remove completed thread to free RAM
                    future_to_seg.pop(future,None)
                    stats[self.type]["downloaded_segments"] = self.latest_sequence - len(self.segments_retries)
                      
                #segments_to_download = set(range(0, self.latest_sequence)) - self.already_downloaded    
                                       
                if len(self.segments_retries) <= 0:
                    logging.info("All segment downloads complete, ending...")
                    break

                elif all(value['retries'] > self.fragment_retries for value in self.segments_retries.values()):
                    logging.error("All remaining segments have exceeded their retry count, ending...")
                    break
                
                elif self.is_403 and self.expires is not None and time.time() > self.expires:
                    logging.fatal("URL(s) have expired and failures being detected, ending...")
                    break               
                
                elif self.is_401:
                    logging.debug("401s detected for {0}, sleeping for a minute")
                    time.sleep(60)
                    for url in self.stream_urls:
                        if self.live_status == 'post_live':
                            self.update_latest_segment(url=add_url_param(self.stream_url, "sq", self.latest_sequence+1))
                        else:
                            self.update_latest_segment(url=url)
                # Request base url if receiving 403s
                elif self.is_403:
                    for url in self.stream_urls:
                        if self.live_status == 'post_live':
                            self.update_latest_segment(url=add_url_param(self.stream_url, "sq", self.latest_sequence+1))
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
                        if seg_num not in submitted_segments and self.segments_retries[seg_num]['retries'] <= self.fragment_retries and time.time() - self.segments_retries[seg_num]['last_retry'] > self.segment_retry_time:                            
                            if seg_num in self.already_downloaded:
                                self.segments_retries.pop(seg_num,None)
                                continue
                            if self.segment_exists(self.cursor, seg_num):
                                self.already_downloaded.add(seg_num)
                                continue
                            new_download.add(seg_num)
                            logging.debug("Adding segment {0} of {2} with retries: {1}".format(seg_num, self.segments_retries[seg_num]['retries'], self.format))
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
                        future_to_seg[executor.submit(self.download_segment, add_url_param(self.stream_urls[i % len(self.stream_urls)], "sq", seg_num), seg_num)] = seg_num
                        submitted_segments.add(seg_num)
                        i += 1
                        #time.sleep(0.25)
                """
                # Old        
                for url in self.stream_urls:
                    future_to_seg.update(
                        {
                            executor.submit(self.download_segment, add_url_param(self.stream_url, "sq", seg_num), seg_num): seg_num
                            for seg_num in segments_to_download
                            if not submitted_segments.add(seg_num) and not time.sleep(0.25)
                        }
                    )
                """
                if len(submitted_segments) == 0 and len(self.segments_retries) < 11 and time.time() - last_print > self.segment_retry_time:
                    logging.debug("{2} remaining segments for {1}: {0}".format(self.segments_retries, self.format, len(self.segments_retries)))
                    last_print = time.time()
                elif len(submitted_segments) == 0 and time.time() - last_print > self.segment_retry_time + 5:
                    logging.debug("{0} segments remain for {1}".format(len(self.segments_retries), self.format))
                    last_print = time.time()
                
            self.commit_batch(self.conn)
        self.commit_batch(self.conn)
        return len(self.segments_retries)

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
            
            logging.fatal("\033[31mCurrent time is beyond highest expire time, unable to recover\033[0m".format(self.format))
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
            logging.debug("Latest sequence: {0}".format(self.latest_sequence))
            
        if stream_url_info is not None and stream_url_info.get('Content-Type', None) is not None:
            self.type, self.ext = str(stream_url_info.get('Content-Type')).split('/')
            
        if stream_url_info is not None and stream_url_info.get("X-Head-Time-Sec", None) is not None:
            self.estimated_segment_duration = int(stream_url_info.get("X-Head-Time-Sec"))/max(self.latest_sequence,1)
        
        if stats.get(self.type, None):    
            stats[self.type]["latest_sequence"] = self.latest_sequence
        else:
            stats[self.type] = {}
            stats[self.type]["latest_sequence"] = self.latest_sequence
    
    def get_Headers(self, url):
        try:
            # Send a GET request to a URL
            #response = requests.get(url, timeout=30)
            response = requests.get(url, timeout=30, proxies=self.proxies)
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
                logging.warning("Error retrieving headers: {0}".format(response.status_code))
                logging.debug(json.dumps(dict(response.headers), indent=4))
            return response.headers
            
        except requests.exceptions.Timeout as e:
            logging.debug("Timed out updating fragments: {0}".format(e))
            return None
        
        except Exception as e:
            logging.exception("\033[31m{0}\033[0m".format(e))
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
        cursor.execute(\'''
        CREATE TABLE IF NOT EXISTS segments (
            id INTEGER PRIMARY KEY, 
            segment_data BLOB
        )
        \''')
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
            if response and response.status and self.segment_number is not None:
                logging.debug("{0} encountered a {1} code".format(self.segment_number, response.status))
                    
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
            response = session.get(segment_url, timeout=30, headers=headers, proxies=self.proxies)
            if response.status_code == 200:
                logging.debug("Downloaded segment {0} of {1} to memory...".format(segment_order, self.format))
                self.is_403 = False
                self.is_401 = False
                #return latest header number and segment content                
                return int(response.headers.get("X-Head-Seqnum", -1)), response.content, int(segment_order), response.status_code, response.headers  # Return segment order and data
            elif response.status_code == 403:
                logging.debug("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                return -1, None, segment_order, response.status_code, response.headers
            else:
                logging.debug("Error downloading segment {0}: {1}".format(segment_order, response.status_code))
                return -1, None, segment_order, response.status_code, response.headers
        except requests.exceptions.Timeout as e:
            logging.debug(e)
            return -1, None, segment_order, None, None
        except requests.exceptions.RetryError as e:
            logging.debug("Retries exceeded downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
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
            logging.debug("No data in request for fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, bytes(), segment_order, None, None
        except requests.exceptions.ConnectionError as e:
            logging.debug("Connection error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.Timeout as e:
            logging.warning("Timeout while retrieving downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except requests.exceptions.HTTPError as e:
            logging.warning("HTTP error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
        except Exception as e:
            logging.exception("Unknown error downloading fragment {1} of {2}: {0}".format(e, segment_order, self.format))
            return -1, None, segment_order, None, None
            
    # Function to insert a single segment without committing
    def insert_single_segment(self, cursor, segment_order, segment_data):

        cursor.execute(\'''
            INSERT INTO segments (id, segment_data) 
            VALUES (?, ?) 
            ON CONFLICT(id) 
            DO UPDATE SET segment_data = CASE 
                WHEN LENGTH(excluded.segment_data) > LENGTH(segments.segment_data) 
                THEN excluded.segment_data 
                ELSE segments.segment_data 
            END;
        \''', (segment_order, segment_data))


    # Function to commit after a batch of inserts
    def commit_batch(self, conn):
        conn.commit()
        
    def close_connection(self):
        if self.conn:
            self.conn.close()

    # Function to combine segments into a single file
    def combine_segments_to_file(self, output_file, cursor=None):
        stats[self.type]['status'] = "merging"
        if cursor is None:
            cursor = self.cursor
        
        logging.debug("Merging segments to {0}".format(output_file))
        with open(output_file, 'wb') as f:
            cursor.execute('SELECT segment_data FROM segments ORDER BY id')
            first = True
            for segment in cursor:  # Cursor iterates over rows one by one
                segment_piece = segment[0]
                # Clean each segment if required as ffmpeg sometimes doesn't like the segments from YT
                if str(self.ext).lower().endswith("mp4") or not str(self.ext):
                    segment_piece = self.clean_segments(segment_piece, first)
                first = False
                f.write(segment_piece)
        stats[self.type]['status'] = "merged"
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
            try:
                if ofs + 8 > len(data):
                    break

                alen = int(data[ofs:ofs + 4].hex(), 16)
                if alen > len(data) or alen < 8:
                    break

                aname = data[ofs + 4:ofs + 8].decode()
                atoms[aname] = {"ofs": ofs, "len": alen}
                ofs += alen
            except Exception:
                break

        return atoms

    def remove_atoms(self, data, atom_list):
        """
        Remove specified atoms from a chunk of data

        :param data: The byte data containing atoms
        :param atom_list: List of atom names to remove
        """
        atoms = self.get_atoms(data)
        atoms_to_remove = [atoms[name] for name in atom_list if name in atoms]
        
        # Sort by offset in descending order to avoid shifting issues
        atoms_to_remove.sort(key=lambda x: x["ofs"], reverse=True)
        
        for atom in atoms_to_remove:
            ofs = atom["ofs"]
            rlen = ofs + atom["len"]
            data = data[:ofs] + data[rlen:]
        
        return data
    
    def clean_segments(self, data, first=True):
        bad_atoms = ["sidx"]
        if first is False:
            bad_atoms.append("ftyp")

        return self.remove_atoms(data=data, atom_list=bad_atoms)
    
    def check_kill(self):
        # Kill if keyboard interrupt is detected
        if kill_all:
            logging.debug("Kill command detected, ending thread")
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
 
    '''

import os
import logging
import logging.handlers

def setup_logging(
    log_level="INFO",
    console=True,
    file: str | None = None,
    force=False,
    file_options: dict = None,
    logger: logging.Logger | None = None,
    logger_name: str | None = None
) -> logging.Logger:
    """
    Configure logging.

    :param log_level: logging level name ("DEBUG", "INFO", etc.)
    :param console: whether to log to console (StreamHandler)
    :param file: path to log file; if provided, file logging is enabled
    :param force: if True, remove existing handlers on the logger
    :param file_options: dict for file handler options (e.g. rotation: maxBytes, when, interval, backupCount)
    :param logger: optionally pass an existing logger object
    :param logger_name: if provided, get/create a named logger; otherwise root logger
    :return: the configured logger
    """
    file_options = file_options or {}

    def disable_quick_edit():
        import ctypes
        kernel32 = ctypes.windll.kernel32
        hStdin = kernel32.GetStdHandle(-10)  # STD_INPUT_HANDLE
        mode = ctypes.c_ulong()
        kernel32.GetConsoleMode(hStdin, ctypes.byref(mode))
        mode.value &= ~0x40  # Remove ENABLE_QUICK_EDIT_MODE
        kernel32.SetConsoleMode(hStdin, mode)

    # On Windows, disable quick edit so console input isn't paused by click:
    if os.name == "nt":
        disable_quick_edit()

    # Get the logger: named if logger_name, else root
    if logger is None:
        logger = logging.getLogger(logger_name)
        #if logger_name:
        #    logger = logging.getLogger(logger_name)
        #else:
        #    logger = logging.getLogger()  # root logger

    # If force, clear handlers; else, if it already has handlers, do nothing
    if force:
        # remove all handlers safely
        for h in list(logger.handlers):
            logger.removeHandler(h)
    else:
        # If logger already configured, skip
        if logger.handlers:
            return logger

    # Convert level string to logging level
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    # Build formatter, with logger_name if given
    if logger_name:
        fmt_str = f'[%(levelname)s] [{logger_name}] %(asctime)s - %(message)s'
    else:
        fmt_str = '[%(levelname)s] %(asctime)s - %(message)s'
    formatter = logging.Formatter(fmt_str, datefmt='%Y-%m-%d %H:%M:%S')

    # Optionally disable propagation so logs don't bubble up in unwanted ways
    # If this is a named logger (i.e. not root), you may want to stop propagation
    if logger_name:
        logger.propagate = False

    # Console handler
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # File handler
    if file:
        # Decide type of file handler
        if file_options.get("maxBytes"):
            handler = logging.handlers.RotatingFileHandler(
                file,
                mode='a',
                maxBytes=file_options.get("maxBytes", 10 * 1024 * 1024),
                backupCount=file_options.get("backupCount", 5),
                encoding='utf-8'
            )
        elif file_options.get("when"):
            handler = logging.handlers.TimedRotatingFileHandler(
                file,
                when=file_options.get("when", "midnight"),
                interval=file_options.get("interval", 1),
                backupCount=file_options.get("backupCount", 7),
                encoding='utf-8'
            )
        else:
            handler = logging.FileHandler(file, mode='a', encoding='utf-8')

        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


