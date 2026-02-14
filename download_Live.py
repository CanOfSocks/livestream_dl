from __future__ import annotations

import yt_dlp.YoutubeDL
import yt_dlp.utils
import sqlite3
#import requests
#from requests.adapters import HTTPAdapter, Retry
import random
import time
import concurrent.futures
import json
from pathlib import Path
import copy


from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

try:
    import getUrls
    import YoutubeURL
    import setup_logger
    #from headers import user_agents
except ModuleNotFoundError as e:
    from . import getUrls
    from . import YoutubeURL
    from . import setup_logger
    #from .headers import user_agents

import os

from urllib.parse import urlparse, parse_qs

import shutil

import logging

from typing import Optional
#import re

import threading

import httpx

import struct


#import setup_logger

# Backward compatibility
setup_logging = setup_logger.setup_logging


class LiveStreamDownloader:

    def __init__(self, kill_all: threading.Event=threading.Event(), cleanup = threading.Event(), logger: logging = None, kill_this: threading.Event = None,):
        if logger:
            self.logger = logger
        else:
            # 1. Create a Named Logger instance and assign it to self.logger
            self.logger = logging.getLogger(self.__class__.__name__) 
            # Ensure it processes messages (important if you don't call setup_logging immediately)
        self.logger.propagate = False 
        self.kill_this = kill_this or threading.Event()

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

        self.refresh_json = {}
        self.live_status = ""
        self.lock: threading.Lock = threading.Lock()

    # Create runner function for each download format
    def download_stream(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, manifest=0, **kwargs):
        options = options or {}
        try:
            # Update download params for recursive/following threads
            # We copy options to avoid mutating the original dict passed in
            current_options = options.copy()
            current_options["download_function"] = self.download_stream
            
            file = None
            filetype = None
            
            with DownloadStream(info_dict, stream_url=stream_url, options=current_options, livestream_coordinator=self, **kwargs) as downloader:       
                self.stats["status"] = "Recording"       
                downloader.live_dl()
                file_name = downloader.combine_segments_to_file(downloader.merged_file_name)
                
                keep_database = current_options.get('keep_database', False)
                if not keep_database:
                    self.logger.info("Merging to ts complete, removing {0}".format(downloader.temp_db_file))
                    downloader.delete_temp_database()
                elif downloader.temp_db_file != ':memory:':
                    database_file = FileInfo(downloader.temp_db_file, file_type='database', format=downloader.format)
                    self.file_names['databases'].append(database_file)

                file = FileInfo(file_name, file_type=downloader.type, format=downloader.format, vcodec=downloader.stream_url.vcodec, acodec=downloader.stream_url.acodec, language=downloader.stream_url.language, protocol=downloader.stream_url.protocol)
                filetype = downloader.type        

            self.file_names.setdefault("streams", {}).setdefault(manifest, {}).update({
                str(filetype).lower(): file
            })
            
            return file, filetype
        except Exception as e:
            self.logger.exception("Unexpected error occurred while downloading stream")
            raise

    # Create runner function for each download format
    def download_stream_direct(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, manifest=0, **kwargs):
        options = options or {}
        try:
            current_options = options.copy()
            current_options["download_function"] = self.download_stream_direct
            
            file = None
            filetype = None

            with DownloadStreamDirect(info_dict, stream_url=stream_url, options=current_options, livestream_coordinator=self, **kwargs) as downloader:
                self.stats["status"] = "Recording"
                file_name = downloader.live_dl()
                file = FileInfo(file_name, file_type=downloader.type, format=downloader.format, vcodec=downloader.stream_url.vcodec, acodec=downloader.stream_url.acodec, language=downloader.stream_url.language, protocol=downloader.stream_url.protocol)
                filetype = downloader.type
                downloader.delete_state_file()

            self.file_names.setdefault("streams", {}).setdefault(manifest, {}).update({
                str(filetype).lower(): file
            })
            return file, filetype
        except Exception as e:
            self.logger.exception("Unexpected error occurred while downloading stream")
            raise

    def recover_stream(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, manifest=0, **kwargs):
        options = options or {}
        try:
            current_options = options.copy()
            current_options["download_function"] = self.recover_stream
            file = None
            filetype = None
            force_merge = current_options.get('force_recovery_merge', False)
            recovery_failure_tolerance = current_options.get('recovery_failure_tolerance', 0)
            no_merge = current_options.get('no_merge', False)
            keep_database = current_options.get('keep_database', False)

            with StreamRecovery(info_dict, stream_url=stream_url, options=current_options, livestream_coordinator=self, **kwargs) as downloader:
                self.stats["status"] = "Recording"
                result = downloader.live_dl()
                #downloader.save_stats()    
                if (force_merge or result <= 0 or result <= recovery_failure_tolerance) and (not no_merge):
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
                file = FileInfo(file_name, file_type=downloader.type, format=downloader.format, vcodec=downloader.stream_url.vcodec, acodec=downloader.stream_url.acodec, language=downloader.stream_url.language, protocol=downloader.stream_url.protocol)
                filetype = downloader.type  
                
            self.file_names.setdefault("streams", {}).setdefault(manifest, {}).update({
                str(filetype).lower(): file
            })
            return file, filetype
        except Exception as e:
            self.logger.exception("Unexpected error occurred while downloading stream")
            raise

    def submit_download(self, executor, info_dict: dict, resolution, options: dict, download_folder, file_name, futures, is_audio=False):
        
        # Prepare the options dictionary that will be passed to the runner functions
        # This maps the external 'options' keys to the internal keys expected by DownloadStream classes
        run_options = {
            "batch_size": options.get('batch_size', 1),
            "max_workers": options.get("threads", 1),
            "folder": download_folder,
            "file_name": file_name,
            "fragment_retries": options.get('segment_retries'),
            "cookies": options.get('cookies'),
            "yt_dlp_options": options.get('ytdlp_options', None),
            "proxies": options.get("proxy", None),
            "yt_dlp_sort": options.get('custom_sort', None),
            "is_audio": is_audio,
            "wait_limit": options.get("wait_limit", 0)
        }

        if is_audio and options.get("audio_format"):
            run_options["resolution"] = options.get("audio_format") # Override resolution key if needed internally
        elif options.get("video_format"):
            run_options["resolution"] = options.get("video_format")

        # Specific Logic for Logic Selection
        if options.get('recovery', False):
            func = self.recover_stream
            run_options.update({
                "force_merge": options.get('force_recovery_merge', False),
                "recovery_failure_tolerance": options.get('recovery_failure_tolerance', 0),
                "keep_database": options.get("keep_temp_files", False) or options.get("keep_database_file", False),
                "recovery": True,
                # Recovery specific
                "stream_urls": options.get('stream_urls', []),
                "no_merge": options.get('no_merge', False)
            })
        elif options.get('direct_to_ts', False):
            func = self.download_stream_direct
            run_options.update({
                "keep_state": options.get("keep_temp_files", False) or options.get("keep_database_file", False),
                # Standard options
                "include_dash": options.get("dash", False),
                "include_m3u8": options.get("m3u8", False),
                "force_m3u8": options.get("force_m3u8", False),
            })
        else:
            func = self.download_stream
            run_options.update({
                "keep_database": options.get("keep_temp_files", False) or options.get("keep_database_file", False),
                # Standard options
                "include_dash": options.get("dash", False),
                "include_m3u8": options.get("m3u8", False),
                "force_m3u8": options.get("force_m3u8", False),
            })

        # Add any extra values from the original options that might be needed by custom logic
        # avoiding overwriting keys we just set if possible, or allowing override if intended
        for key, value in options.items():
            run_options.setdefault(key, value)

        self.logger.debug("Starting executor with options: {0}".format(json.dumps(str(run_options))))

        # Submit to executor
        # We now pass 'run_options' as the 'options' argument to the runner function
        future = executor.submit(func, info_dict=info_dict, resolution="audio_only" if is_audio else resolution, options=run_options)
        futures.add(future)
        return future


    # Multithreaded function to download new segments with delayed commit after a batch
    def download_segments(self, info_dict: dict, resolution='best', options: dict={}, thread_event: threading.Event=None):
        futures = set()
        #file_names = {}

        if thread_event is not None:        
            self.kill_all = thread_event
            
        #setup_logging(log_level=options.get('log_level', "INFO"), console=(not options.get('no_console', False)), file=options.get('log_file', None), file_options=options.get("log_file_options",{}), logger_name=str(self.__class__.__name__))
        
        #setup_logging(log_level=options.get('ytdlp_log_level') or self.logger.getEffectiveLevel(), console=(not options.get('no_console', False)), file=options.get('log_file', None), file_options=options.get("log_file_options",{}), logger_name="yt-dlp", video_id=info_dict.get("id", "ID N/A"), metadata={"log_type", "default"})
        
        self.stats['id'] = info_dict.get('id', None)
        self.stats["status"] = "Starting"
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
            
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor: 
            done = not_done = set() 
            try: 
                
                # Download auxiliary files (thumbnail, info,json etc)

                base_output = None
                if options.get('filename') is not None:
                    filename = options.get('filename')
                else:
                    filename = info_dict.get('id')
                
                if download_folder:
                    base_output = os.path.join(download_folder, filename)
                else:
                    base_output = filename

                options["download_folder"] = download_folder
                format_handler = YoutubeURL.Formats()
                format_extractor_options = {}
                if options.get("ext", None):
                    format_extractor_options.update({"merge_output_format": options.get("ext", "").strip(" .")})
                formats_info = format_handler.getFormats(info_json=info_dict, resolution=resolution, sort=options.get('custom_sort', None), include_dash=(options.get("dash", False) and not options.get('recovery', False)), include_m3u8=options.get("m3u8", False), force_m3u8=options.get("force_m3u8", False), base_path=base_output, ydl_options=format_extractor_options)

                stream_urls = []
                #with open("data.json", "w", encoding="utf-8") as json_file:
                #    json.dump(formats_info, json_file, indent=4)
                
                for format_info in formats_info.get('requested_formats') or [formats_info]:
                    format_obj: YoutubeURL.YoutubeURL = None
                    if format_info.get('protocol', "") == "http_dash_segments":
                        format_obj = YoutubeURL.YoutubeURL(format_info.get('fragment_base_url'), format_info.get('protocol'), format_id=format_info.get('format_id'), logger=self.logger, vcodec=format_info.get('vcodec', None), acodec=format_info.get('acodec', None), format_note=format_info.get("format_note"), language=format_info.get('language', None), ext=format_info.get('ext', None))
                        
                    elif format_info.get('protocol', "") == "m3u8_native":      
                        format_obj = YoutubeURL.YoutubeURL(url=format_handler.getM3u8Url(format_info.get('url')), protocol=format_info.get('protocol'), format_id=format_info.get('format_id'), logger=self.logger, vcodec=format_info.get('vcodec', None), acodec=format_info.get('acodec', None), format_note=format_info.get("format_note"), language=format_info.get('language', None), ext=format_info.get('ext', None))
                        if not format_info.get('format_id', None):
                            format['format_id'] = str(format_obj.itag).strip() 
                    else:
                        format_obj = YoutubeURL.YoutubeURL(format_info.get('url'), format_info.get('protocol'), format_id=format_info.get('format_id'), logger=self.logger, vcodec=format_info.get('vcodec', None), acodec=format_info.get('acodec', None), format_note=format_info.get("format_note"), language=format_info.get('language', None), ext=format_info.get('ext', None))

                    if format_obj:
                        stream_urls.append(format_obj)
                

                auxiliary_thread = executor.submit(self.download_auxiliary_files, info_dict=formats_info, options=options)
                futures.add(auxiliary_thread)

                if not stream_urls:
                    raise ValueError("Resolution is not valid or does not exist in stream")
                
                for stream_url in stream_urls:
                    options["keep_database"] = options.get("keep_temp_files", False) or options.get("keep_database_file", False)
                    options["keep_state"] = options.get("keep_temp_files", False) or options.get("keep_database_file", False)
                    if options.get('recovery', False):
                        options["stream_urls"] = options.get('stream_urls', []),
                        futures.add(executor.submit(self.recover_stream, info_dict=info_dict, stream_url=stream_url, options=options, manifest=stream_url.manifest))
                    elif options.get('direct_to_ts', False):
                        futures.add(executor.submit(self.download_stream_direct, info_dict=info_dict, stream_url=stream_url, options=options, manifest=stream_url.manifest))
                    else:
                        futures.add(executor.submit(self.download_stream, info_dict=info_dict, stream_url=stream_url, options=options, manifest=stream_url.manifest))

                info_dict = formats_info

                live_chat_thread = None
                if options.get('live_chat', False) is True:
                    
                    live_chat_thread = threading.Thread(target=self.download_live_chat, args=(info_dict,options), daemon=True)
                    live_chat_thread.start()

                while True:
                    if self.kill_all.is_set() or self.kill_this.is_set():
                        raise KeyboardInterrupt("Thread kill event is set, ending...")
                    done, not_done = concurrent.futures.wait(futures, timeout=1, return_when=concurrent.futures.FIRST_COMPLETED)
                    # Continuously check for completion or interruption
                    for future in done:
                        try:
                            #if future.exception() is not None:
                            #    if type == 'auxiliary': 
                                    #logging.error(str(future.exception()))
                            #   else:
                            #        raise future.exception()
                            
                            result, type = future.result()
                            self.logger.log(setup_logger.VERBOSE_LEVEL_NUM, "\033[93m{0}\033[0m".format(result))
                            
                            if type == 'auxiliary':
                                self.file_names.update(result)

                                # Remove parts that would not be needed by stream downloaders
                                self.remove_format_segment_playlist_from_info_dict(info_dict=info_dict)
                                info_dict.pop("thumbnails", None)
                                # Remove no longer required parts for the rest of the downloader
                                #info_dict = self.trim_info_json(info_dict=info_dict, keys_to_keep={"id", "ext", "upload_date", "original_url", "description", "fulltitle", "channel"})
                                

                            elif str(type).lower() == 'video':
                                #self.file_names['video'] = result
                                pass
                            elif str(type).lower() == 'audio':
                                #self.file_names['audio'] = result  
                                pass
                            elif str(type).lower() == 'live_chat':
                                pass
                            else:
                                self.file_names[str(type)] = result
                        except Exception as e:
                            self.logger.exception(e)
                        futures.discard(future)
                        
                    if len(not_done) <= 0:
                        break
                    #else:
                    #    time.sleep(0.9)
                    self.print_stats(options=options)

                if live_chat_thread and live_chat_thread.is_alive():
                    self.logger.info("Media downloads finished, waiting for live chat to end...")
                    if options.get('stop_chat_when_done', None):
                        self.chat_timeout = time.time()
                    while live_chat_thread.is_alive() and not (self.kill_all.is_set() or self.kill_this.is_set()):
                        time.sleep(0.1)
                
            except KeyboardInterrupt as e:
                self.kill_this.set()
                self.logger.debug("Keyboard interrupt detected")
                if len(not_done) > 0:
                    self.logger.debug("Cancelling remaining threads")
                for future in not_done:
                    _ = future.cancel()
                done, not_done = concurrent.futures.wait(futures, timeout=5, return_when=concurrent.futures.ALL_COMPLETED)
                executor.shutdown(wait=False,cancel_futures=True)
                self.logger.debug("Shutdown all threads")
                self.stats["status"] = "Cancelled"
                raise        
                
        self.stats["status"] = "Muxing"
        self.create_mp4(file_names=self.file_names, info_dict=info_dict, options=options)
            
        self.stats["status"] = "Moving"
        self.move_to_final(options=options, output_file=outputFile, file_names=self.file_names)
        self.stats["status"] = "Finished"            
        #move_to_final(info_dict, options, file_names)
        
    def output_filename(self, info_dict, outtmpl):
        outputFile = str(yt_dlp.YoutubeDL().prepare_filename(info_dict, outtmpl=outtmpl)).replace("%", "％") # Replace normal percent sign with unicode version to prevent ffmpeg errors
        return outputFile
    

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
                    self.logger.info(f"Moving {f.absolute()} -> {dest}")
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
                    self.logger.info(f"Moving {f.absolute()} -> {dest}")
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
        
        if info_dict.get("status", "") == "post_live":
            self.logger.warning("Stream is post live, unlikely to retrieve live chat")
        if options.get('filename') is not None:
            filename = options.get('filename')
        else:
            filename = info_dict.get('id')
        
        if options.get("temp_folder"):
            base_output = os.path.join(options.get("temp_folder"), filename)
        else:
            base_output = filename

        class YTDLP_Chat_logger(YoutubeURL.YTDLPLogger):        
            def __init__(self, logger: logging = logging.getLogger()):
                super().__init__(logger=logger)

            def prefix(self, msg: str):
                if msg.startswith("[live-chat] "):
                    return msg
                else:
                    return "[live-chat] {0}".format(msg)
            def debug(self, msg):
                super().debug(self.prefix(msg))
            def info(self, msg):
                super().info(self.prefix(msg))
            def warning(self, msg):
                super().warning(self.prefix(msg))
            def error(self, msg):
                super().error(self.prefix(msg))
        
        logger = self.logger
        
        ydl_opts = {
            'skip_download': True,               # Skip downloading video/audio
            'logger': YTDLP_Chat_logger(logger=logger),
            #'quiet': True,
            'cookiefile': options.get('cookies', None),
            'retries': 25,
            'concurrent_fragment_downloads': 3,
            #'live_from_start': True,
            'writesubtitles': True,              # Extract subtitles (live chat)
            'subtitlesformat': 'json',           # Set format to JSON
            'subtitleslangs': ['live_chat'],     # Only extract live chat subtitles
            'outtmpl': base_output          # Save to a JSON file        
        }
        self.logger.debug(options.get('ytdlp_options', {}))
        ydl_opts.update(options.get('ytdlp_options', {}))
        
        livechat_filename = base_output + ".live_chat.json"
        
        
        self.logger.info("Downloading live chat to: {0}".format(livechat_filename))
        # Run yt-dlp with the specified options
        # Don't except whole process on live chat fail
        class ChatDownloaderParsingError(ValueError):
            pass
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
                    if self.kill_all.is_set() or self.kill_this.is_set():
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
                raise ChatDownloaderParsingError(e)
                """
                
                if options.get('proxy', None) is not None:
                    ydl_opts['proxy'] = next(iter((options.get('proxy', None) or {}).values()), None)
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        result = ydl.process_ie_result(info_dict)
                        try:
                            if result.get('requested_subtitles', {}).get('live_chat', {}).get('filepath', None):
                                livechat_filename = result.get('requested_subtitles', {}).get('live_chat', {}).get('filepath', None)
                        except:
                            self.logger.exception("Unable to find live chat path")
                        
                        
                        #result = ydl.download_with_info_file(info_dict)
                        #result = ydl._writesubtitles(info_dict, )
                except Exception as e:
                    self.logger.exception("\033[31m{0}\033[0m".format(e))
                """
            
        except (ImportError,ChatDownloaderParsingError) as e:
            if isinstance(e, ImportError):
                self.logger.warning("Unable to import chat-downloader, using yt-dlp")

            if options.get('proxy', None) is not None:
                ydl_opts['proxy'] = next(iter((options.get('proxy', None) or {}).values()), None)
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    result = ydl.process_ie_result(info_dict)
                    try:
                        
                        #if result.get('requested_subtitles', {}):
                        #    self.logger.warning(json.dumps(result.get('requested_subtitles')))
                        if result.get('requested_subtitles', {}).get('live_chat', {}).get('filepath', None):
                            livechat_filename = result.get('requested_subtitles', {}).get('live_chat', {}).get('filepath')
                    except Exception as e:
                        self.logger.warning("Unable to find live chat path, remaining with existing path")
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
            import zipfile
            zip_filename = base_output + ".live_chat.zip"
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

        logger = self.logger

        class YTDLP_Auxiliary_logger(YoutubeURL.YTDLPLogger):        
            def __init__(self, logger: logging = logging.getLogger()):
                super().__init__(logger=logger)

            def prefix(self, msg: str):
                if msg.startswith("[auxiliary] "):
                    return msg
                else:
                    return "[auxiliary] {0}".format(msg)
            def debug(self, msg):
                super().debug(self.prefix(msg))
            def info(self, msg):
                super().info(self.prefix(msg))
            def warning(self, msg):
                super().warning(self.prefix(msg))
            def error(self, msg):
                super().error(self.prefix(msg))

        ydl_opts = {
            'skip_download': True,
            'quiet': True,
    #        'cookiefile': options.get('cookies', None),
            'writeinfojson': options.get('write_info_json', False),
            'writedescription': options.get('write_description', False),
            'writethumbnail': (options.get('write_thumbnail', False) or options.get("embed_thumbnail", False)),
            'outtmpl': base_output,
            'retries': 10,
            'logger': YTDLP_Auxiliary_logger(logger=logger),
        }
        if options.get('proxy', None) is not None:
            ydl_opts['proxy'] = next(iter((options.get('proxy', None) or {}).values()), None)
            
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            #base_name = ydl.prepare_filename(info_dict)
            #result = ydl.download_with_info_file(info_dict)
            info = ydl.process_ie_result(info_dict)
            #with open("aux_data.json", "w", encoding="utf-8") as json_file:
            #    json.dump(info, json_file, indent=4)
            
            # Check if info.json has been 
            info_dict_file = next(
                (item for item in info.get("requested_downloads", []) if item.get("infojson_filename", None) and os.path.exists(item["infojson_filename"])), 
                {}
            )
            if info_dict_file.get("infojson_filename"):
                
                created_files['info_json'] = FileInfo(info_dict_file.get("infojson_filename"), file_type='info_json')
                try:
                    if options.get('remove_ip_from_json'):
                        self.replace_ip_in_json(created_files['info_json'].absolute())
                    if options.get('clean_urls'):
                        self.remove_urls_from_json(created_files['info_json'].absolute())
                except Exception as e:
                    self.logger.exception("Unable to find info.json file")
            elif ydl_opts.get("writeinfojson", False):
                self.logger.error("Unable to write info.json")
                    
            if ydl_opts.get("writedescription", False):
                if info_dict.get("description"):
                    try:
                        description_file = ydl.prepare_filename(info, 'description')
                        with open(description_file, "w", encoding="utf-8") as desc_file:
                            desc_file.write(info_dict["description"])
                        created_files['description'] = FileInfo(description_file, file_type='description')
                    except Exception as e:
                        self.logger.exception("Unable to write description file")
                else:
                    self.logger.info("No description in info.json")
                
            #thumbnails = ydl._write_thumbnails('video', info_dict, ydl.prepare_filename(info_dict, 'thumbnail'))
            try:
                thumbnail = next((-i for i, t in enumerate(info['thumbnails'][::-1], 1) if t.get('filepath')), {})            
                thumbnail_filename = info.get('thumbnails', [])[thumbnail].get('filepath', None)
                if thumbnail is None or not os.path.exists(thumbnail_filename):
                    if ydl_opts.get("writethumbnail", False):
                        self.logger.error("Unable to download thumbnail")
                else:
                    created_files['thumbnail'] = FileInfo(thumbnail_filename, file_type='thumbnail')       
            except Exception as e:
                self.logger.exception("Unable to download thumbnail")
                               
            
        return created_files, 'auxiliary'
        
    def create_mp4(self, file_names: dict, info_dict: dict, options: dict):
        self.logger.log(setup_logger.VERBOSE_LEVEL_NUM, "Files: {0}\n".format(json.dumps(self.file_names, default=lambda o: o.to_dict())))
        import subprocess
        import mimetypes
        from yt_dlp.postprocessor.ffmpeg import FFmpegMetadataPP, FFmpegPostProcessorError, PostProcessor
        import shlex
        from sys import platform as sys_platform
        class LiveStreamDLMerger(FFmpegMetadataPP):
            def __init__(self, downloader, ffmpeg_command_file: FileInfo=None):
                self.ffmpeg_command_file = ffmpeg_command_file
                super().__init__(downloader=downloader)
            
            @PostProcessor._restrict_to()
            def run(self, info):
                super().run(info)

            def string_to_safe_shell_file(self, command_string, file_path):
                # 1. Split the string into a list, respecting quotes
                # Note: posix=False helps keep Windows-style backslashes intact
                args = shlex.split(command_string, posix=(sys_platform != 'win32'))

                # 2. Re-build it using the OS-specific logic
                if sys_platform == 'win32':
                    safe_cmd = subprocess.list2cmdline(args)
                else:
                    safe_cmd = shlex.join(args)

                # 3. Write to file
                with open(file_path, 'w', encoding="utf-8") as f:
                    f.write(safe_cmd + "\n")

            def write_debug(self, msg):
                # Intercept the specific debug message containing the command
                if msg.startswith('ffmpeg command line: ') and self.ffmpeg_command_file:
                    # Extract the command or just save the whole line
                    command_string = msg.replace('ffmpeg command line: ', '')
                    #self.string_to_safe_shell_file(command_string=command_string, file_path=str(ffmpeg_command_file.absolute()))
                    with open(str(ffmpeg_command_file.absolute()), 'w', encoding="utf-8") as f:
                        f.write(command_string + "\n")
                
                # Always call the original write_debug so you don't break logging
                super().write_debug(msg)

        class YTDLP_FFmpeg_logger(YoutubeURL.YTDLPLogger):        
            def __init__(self, logger: logging = logging.getLogger()):
                super().__init__(logger=logger)

            def prefix(self, msg: str):
                if msg.startswith("[ffmpeg] "):
                    return msg
                else:
                    return "[ffmpeg] {0}".format(msg)
            def debug(self, msg):
                super().debug(self.prefix(msg))
            def info(self, msg):
                super().info(self.prefix(msg))
            def warning(self, msg):
                super().warning(self.prefix(msg))
            def error(self, msg):
                super().error(self.prefix(msg))


        #self.logger.debug("Files: {0}".format(json.dumps(file_names)))
        stream_manifests = list(self.file_names["streams"].items())
        input_args = ['-thread_queue_size', '1024', "-seekable", "0"]
        for manifest, stream in stream_manifests:
            index = 0
            thumbnail = None
            video = None
            audio = None

            info_dict["requested_formats"] = []
            files = []

            # Determine output path
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

            ext = options.get("ext") or info_dict.get('ext', '.mp4')               
            ext = "." + str(ext).strip(" .")

            if not base_output.endswith(ext):
                base_output = base_output + ext  

            ydl_opts = {
                'skip_download': True,
                'quiet': True,
                'outtmpl': base_output,
                'logger': YTDLP_FFmpeg_logger(logger=self.logger),
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                
                ffmpeg_command_file = FileInfo("{0}.ffmpeg.txt".format(filename), file_type='ffmpeg_command')
                file_names["streams"][manifest]['ffmpeg_cmd'] = ffmpeg_command_file
                livestream_merger = LiveStreamDLMerger(downloader=ydl, ffmpeg_command_file=ffmpeg_command_file)
                
                # Create a mapping for the indices we need to track
                stream_indices = {"video": video, "audio": audio}

                for key in ["video", "audio"]:
                    file_info: FileInfo = stream.get(key)
                    if not file_info:
                        continue

                    # 1. Update info_dict
                    info_dict["requested_formats"].append({
                        "file_path": str(file_info.absolute()),
                        "format": file_info._format,
                        "acodec": file_info._acodec or 'none',
                        "vcodec": file_info._vcodec or 'none',
                        "language": file_info._language,
                        "protocol": file_info._protocol,
                    })

                    # 2. Add to ffmpeg files
                    files.append(str(file_info.absolute()))

                    # 3. Track the index and increment
                    stream_indices[key] = index
                    index += 1

                # Extract the values back if you need them as standalone variables
                video, audio = stream_indices["video"], stream_indices["audio"]


                # From FFmpegMergerPP
                args = ['-c', 'copy']
                audio_streams = 0
                video_streams = 0
                for (i, fmt) in enumerate(info_dict['requested_formats']):
                    if fmt.get('acodec') != 'none':
                        args.extend(['-map', f'{i}:a:0'])
                        aac_fixup = fmt['protocol'].startswith('m3u8') and self.get_audio_codec(fmt['filepath']) == 'aac'
                        if aac_fixup:
                            args.extend([f'-bsf:a:{audio_streams}', 'aac_adtstoasc'])
                        if fmt.get("format"):
                            args.extend([f'-metadata:s:a:{audio_streams}', f'comment={fmt.get("format")}'])
                        audio_streams += 1
                        
                    if fmt.get('vcodec') != 'none':
                        args.extend(['-map', f'{i}:v:0'])
                        if fmt.get("format"):
                            args.extend([f'-metadata:s:v:{video_streams}', f'comment={fmt.get("format")}'])
                        video_streams += 1
                    index = i + 1

                if file_names.get('thumbnail', None) and options.get('embed_thumbnail', True):
                    if file_names.get('thumbnail').exists():
                        # Use "guess_file_type" if function exists (added in 3.13), otherwise fall back to depreciated version
                        guess = getattr(mimetypes, 'guess_file_type', mimetypes.guess_type)

                        mime_type, _ = guess(file_names.get('thumbnail'))

                        # If not jpeg or png, convert to png
                        if not str(mime_type) in ("image/jpeg", "image/png"):
                            self.logger.info("{0} is not a JPG or PNG file, converting to png".format(file_names.get('thumbnail').name))
                            png_thumbnail = file_names.get('thumbnail').with_suffix(".png")

                            thumbnail_conversion = ['ffmpeg', '-y', 
                                    '-hide_banner', '-nostdin', '-loglevel', 'error', '-stats',
                                    "-i", str(file_names.get('thumbnail').absolute()), "-c", "png", "-compression_level", "9", "-pred", "mixed", str(png_thumbnail.absolute())
                                ]
                            try:                            
                                result = subprocess.run(thumbnail_conversion, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', check=True)
                                self.logger.debug("Replacing thumbnail with .png version")
                                file_names.pop('thumbnail').unlink(missing_ok=True)                
                                file_names['thumbnail'] = FileInfo(png_thumbnail, file_type='thumbnail')
                                del result
                            except subprocess.CalledProcessError as e:
                                self.logger.error(e.stderr)
                                self.logger.critical(e)
                    
                        thumbnail = index    
                        if not ext.lower() == ".mkv": # Don't add input file for mkv, use attach later
                            files.append(str(file_names.get('thumbnail').absolute()))  
                            args.extend(['-map', f'{index}'])                 
                            index += 1
                    else:
                        self.logger.error("Thumbnail file: {0} is missing, continuing without embedding".format(file_names.get('thumbnail').absolute()))

                # Add faststart
                #ffmpeg_builder.extend(['-movflags', 'faststart'])

                # From FFmpegMetadataPP
                if livestream_merger._add_metadata:
                    args.extend(item for pair in livestream_merger._get_metadata_opts(info_dict) for item in pair)


                attachments = 0
                
                if livestream_merger._add_infojson:
                    if info_dict['ext'] in ('mkv', 'mka') and file_names.get("info_json"):
                        if file_names.get("info_json").exists():
                            args.extend([
                                '-attach', str(file_names.get('thumbnail').absolute()),
                                f'-metadata:s:t:{attachments}', 'mimetype=application/json',
                                f'-metadata:s:t:{attachments}', 'filename=info.json',
                                ])                        
                            attachments += 1
                        else:
                            self.logger.error("Unable to find info.json file {0}".format(file_names.get("info_json")))               
                
                if thumbnail is not None:
                    if ext.lower() == ".mkv": # If file will be mkv, attach file instead
                        # Use "guess_file_type" if function exists (added in 3.13), otherwise fall back to depreciated version
                        guess = getattr(mimetypes, 'guess_file_type', mimetypes.guess_type)
                        mime_type, _ = guess(file_names.get('thumbnail'))  
                        args.extend(['-attach', str(file_names.get('thumbnail').absolute()), f"-metadata:s:t:{attachments}", "filename=cover{0}".format(file_names.get('thumbnail').suffix), f"-metadata:s:t:{attachments}", "mimetype={0}".format(mime_type or "application/octet-stream")])
                        attachments += 1
                    else: # For other formats, attach using disposition instead
                        args.extend(['-disposition:{0}'.format(thumbnail), 'attached_pic'])
                    
                merged_file = FileInfo(base_output, file_type='merged')
                try:                    
                    livestream_merger.real_run_ffmpeg(
                        [(path, input_args) for path in files],
                        [(str(merged_file.absolute()), args)])
                except subprocess.CalledProcessError as e:
                    self.logger.error(e.stderr)
                    self.logger.critical(e)
                    raise e
                
                except FFmpegPostProcessorError as e:
                    self.logger.exception(e)
                    raise e
            
                file_names["streams"][manifest]['merged'] = merged_file
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
            del ydl
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
    
    def universal_sanitize(self, text):
        if not text: return ""
        
        # Map dangerous ASCII to safe "Fullwidth" Unicode equivalents
        # Shells (Windows/Linux) treat these as normal text characters.
        replacements = {
            '&': '＆',  # Fullwidth Ampersand (Fixes your 'pp' crash)
            '|': '｜',  # Fullwidth Pipe
            '<': '＜',  # Fullwidth Less-Than
            '>': '＞',  # Fullwidth Greater-Than
            '"': '＂',  # Fullwidth Quote
            '^': '＾',  # Fullwidth Caret
            ';': '；',  # Fullwidth Semicolon
            '$': '＄',  # Fullwidth Dollar Sign
        }
        
        for char, replacement in replacements.items():
            text = text.replace(char, replacement)
    
        # Hard truncation to stay safe within the 8,191 limit
        # We leave room for the rest of the ffmpeg command.
        return (text[:6144] + '...') if len(text) > 6144 else text
        #return text

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
            # \033[K clears the line after printing the JSON
            print(f"\r{json.dumps(self.stats)}\033[K", end="", flush=True)
            return

        if self.logger.getEffectiveLevel() > logging.INFO:
            #print("Log level too high for printing: {0}".format(self.logger.getEffectiveLevel()))
            return

        if not (self.stats.get('video') or self.stats.get('audio')):
            #print("No stats available")
            #print(json.dumps(self.stats))
            return

        # Build the output parts in a list
        parts = [f"{self.stats.get('id')}:"]

        if self.stats.get('video'):
            v = self.stats.get('video', {})
            v_str = f"Video: {v.get('downloaded_segments', 0)}/{v.get('latest_sequence', 0)}"
            if v.get('status'):
                v_str += f" ({v.get('status').capitalize()})"
            parts.append(v_str)

        if self.stats.get('audio'):
            a = self.stats.get('audio', {})
            a_str = f"Audio: {a.get('downloaded_segments', 0)}/{a.get('latest_sequence', 0)}"
            # Note: Fixed a likely typo in your original code where you checked 
            # video status while printing audio stats
            if a.get('status'):
                a_str += f" ({a.get('status').capitalize()})"
            parts.append(a_str)

        if self.stats.get('video', {}).get('current_filesize') or self.stats.get('audio', {}).get('current_filesize'):
            size = self.stats.get('video', {}).get('current_filesize', 0) + self.stats.get('audio', {}).get('current_filesize', 0)
            parts.append(f"~{self.convert_bytes(size)} downloaded")

        # Join everything with commas or spaces
        full_line = " ".join(parts)

        if options.get("new_line", False):
            print(full_line)
        else:
            # \r moves to start, \033[K clears anything left over from the previous longer line
            print(f"\r{full_line}\033[K", end="", flush=True)
        
    def add_url_param(self, url: str, key, value) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query[key] = [value]  # add or replace parameter

        new_query = urlencode(query, doseq=True)
        new_url = parsed._replace(query=new_query)
        return str(urlunparse(new_url))  
    
    def trim_info_json(self, info_dict: dict, keys_to_keep: set):
        return {k: info_dict[k] for k in info_dict.keys() & keys_to_keep}

    def remove_format_segment_playlist_from_info_dict(self, info_dict: dict):
        for stream_format in info_dict.get('formats', []):
            try:
                stream_format.pop('fragments', None)
            except:
                pass

    def refresh_info_json(self, update_threshold: int, id, cookies=None, additional_options=None, include_dash=False, include_m3u8=False):
        # Check if time difference is greater than the threshold. If doesn't exist, subtraction of zero will always be true
        if time.time() - self.refresh_json.get("refresh_time", 0.0) > update_threshold:
            self.refresh_json, self.live_status = getUrls.get_Video_Info(id=id, wait=False, cookies=cookies, additional_options=additional_options, include_dash=include_dash, include_m3u8=include_m3u8, clean_info_dict=True)

            # Remove unnecessary items for info.json used purely for url refresh
            self.remove_format_segment_playlist_from_info_dict(self.refresh_json)

            self.refresh_json.pop("thumbnails", None)
            self.refresh_json.pop("tags", None)
            self.refresh_json.pop("description", None)

            # Add refresh time for reference
            self.refresh_json["refresh_time"] = time.time()                   

            return self.refresh_json, self.live_status
        else:
            return self.refresh_json, self.live_status
        
class FileInfo(Path):
    _file_type: str = None
    _format: str = None
    _acodec: str = None
    _vcodec: str = None
    _language: str = None
    _protocol: str = None

    def __new__(cls, *args, **kwargs):
        # 1. Pop custom arguments so they don't go to Path.__new__
        file_type = kwargs.pop("file_type", None)
        format = kwargs.pop("format", None)
        acodec = kwargs.pop("acodec", None)
        vcodec = kwargs.pop("vcodec", None)
        language = kwargs.pop("language", None)
        protocol = kwargs.pop("protocol", None)
        # 2. Create the instance using Path's machinery
        instance = super().__new__(cls, *args, **kwargs)
        
        # 3. Store the values on the instance
        instance._file_type = file_type
        instance._format = format
        instance._acodec = acodec
        instance._vcodec = vcodec
        instance._language = language
        instance._protocol = protocol
        return instance

    def __init__(self, *args, **kwargs):
        # 4. Overriding __init__ is CRITICAL.
        # We must NOT pass file_type or format to super().__init__
        kwargs.pop("file_type", None)
        kwargs.pop("format", None)
        super().__init__(*args, **kwargs)

    @property
    def file_type(self):
        return self._file_type

    @file_type.setter
    def file_type(self, value):
        self._file_type = value

    def __repr__(self):
        return f"{super().__repr__()} (file_type={self._file_type})"
    
    def to_dict(self):
        return {
            "filename": str(self),
            "filetype": str(self._file_type),
            "format": str(self._format)
        }

class DownloadStream:
    
    timeout_config = httpx.Timeout(10.0, connect=5.0)
    ATOM_BASED_EXTENSIONS = {".mp4", ".m4a", ".mov", ".m4v", ".f4v", ".3gp"}

    def __init__(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, livestream_coordinator: LiveStreamDownloader = None, **kwargs):        
        self.livestream_coordinator = livestream_coordinator
        if self.livestream_coordinator:
            self.logger = self.livestream_coordinator.logger
            self.kill_all = self.livestream_coordinator.kill_all
            self.kill_this = self.livestream_coordinator.kill_this
        else:
            self.logger = logging.getLogger()
            self.kill_all = threading.Event()
            self.kill_this = threading.Event()

        self.pending_segments: dict[int, bytes] = {}
        self.sqlite_thread = None
        
        self.options = options or {}
        # Use options dict for assignments with defaults
        self.params = self.options.get('download_params') or self.options.copy() 
        self.conn = None
        self.latest_sequence = -1
        self.already_downloaded = set()
        
        self.batch_size = self.options.get('batch_size', 10)
        self.max_workers = self.options.get('max_workers', 5)
        self.yt_dlp_options = self.options.get('yt_dlp_options')

        self.include_dash = self.options.get('include_dash', False)
        self.include_m3u8 = self.options.get('include_m3u8', False)
        self.force_m3u8 = self.options.get('force_m3u8', False)
        
        self.resolution = options.get("resolution", "best")
        self.yt_dlp_sort = self.options.get('yt_dlp_sort')
        
        self.id = info_dict.get('id')
        self.live_status = info_dict.get('live_status')
        
        self.info_dict = info_dict
        self.stream_urls = []

        self.wait_limit = self.options.get("wait_limit", 0)

        self.type = None

        """
        if resolution == "audio_only" or self.options.get("is_audio", False):
            self.type = "audio"
        else:
            self.type = "video"
        """
        #self.stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution, sort=self.yt_dlp_sort, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8, stream_type=self.type) 
        self.stream_url = stream_url

        if self.stream_url is None:
            raise ValueError("Stream URL not found for {0}, unable to continue".format(self.resolution))
        
        if stream_url.vcodec:
            self.type = "video"
        else:
            self.type = "audio"
        
        self.format = self.stream_url.format_id
        #print(self.stream_url)
        self.stream_urls.append(self.stream_url)
        # Extract and parse the query parameters into a dictionary
        #parsed_url = urlparse(self.stream_url)        
        #self.url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}

        self.logger.debug("{0} stream URL parameters: {1}".format(self.id,json.dumps(self.stream_url.url_parameters)))
        
        self.database_in_memory = self.options.get('database_in_memory', False)
        
        file_name = self.options.get('filename', None)
        if file_name is None:
            file_name = self.id    
        
        self.file_base_name = file_name
        
        self.merged_file_name = "{0}.{1}.ts".format(file_name, self.format)     
        if self.database_in_memory:
            self.temp_db_file = ':memory:'
        else:
            self.temp_db_file = '{0}.{1}.temp'.format(file_name, self.format)
        
        self.folder = self.options.get('download_folder')
        if self.folder:
            os.makedirs(self.folder, exist_ok=True)
            self.merged_file_name = os.path.join(self.folder, self.merged_file_name)
            self.file_base_name = os.path.join(self.folder, self.file_base_name)
            if not self.database_in_memory:
                self.temp_db_file = os.path.join(self.folder, self.temp_db_file)

            
        self.fragment_retries = self.options.get('fragment_retries', 5)
        
        self.is_403 = False
        self.is_private = False
        self.estimated_segment_duration = 0
        self.refresh_retries = 0
        
        self.recovery_thread_multiplier = self.options.get('recovery_thread_multiplier', 2)
        
        self.cookies = self.options.get('cookies')
        #self.type = None
        self.ext = self.stream_url.ext     

        self.following_manifest_thread = None
        
        self.proxies, self.mounts = self.process_proxies_for_httpx(proxies=self.options.get('proxies'))

        current_level = self.logger.getEffectiveLevel()            

        # Set log level of httpx to one level above other loggers
        if current_level == logging.NOTSET:
            target_level = logging.WARNING # Default fallback
        else:
            target_level = min(current_level + 10, logging.ERROR)

        logging.getLogger("httpx").setLevel(target_level)
        logging.getLogger("httpcore").setLevel(target_level)
        
        self.update_latest_segment()
        self.url_checked = time.time()
        
        self.conn = self.create_db(self.temp_db_file)    
        
        if self.livestream_coordinator:
            self.livestream_coordinator.stats.setdefault(self.type, {})["latest_sequence"] = self.latest_sequence

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
        
        self.logger.info("\033[33mStarting download of live fragments ({0} - {1})\033[0m".format(self.format, self.stream_url.fomat_note))
        self.already_downloaded = self.segment_exists_batch()
        latest_downloaded_segment = -1
        wait = 0   
        self.conn.execute('BEGIN TRANSACTION')
        uncommitted_inserts = 0     
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]['status'] = "recording"

        # Connection Limits
        # We use a semaphore to limit concurrency if needed, though 'active_tasks' len check does this too.
        limits = httpx.Limits(max_keepalive_connections=self.max_workers+1, max_connections=self.max_workers+1, keepalive_expiry=30)
        with (concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="{0}-{1}".format(self.id,self.format)) as executor,
            httpx.Client(timeout=self.timeout_config, limits=limits, proxy=self.proxies, mounts=self.mounts, http1=True, http2=self.http2_available(), follow_redirects=True, headers=self.info_dict.get("http_headers", None)) as client):
            submitted_segments = set()
            future_to_seg = {}
            
            # Trackers for optimistic segment downloads 
            optimistic_fails_max = 10
            optimistic_fails = 0
            optimistic_seg = 0           
            latest_downloaded_segment = -1

            segments_to_download = set()

            segment_retries = {}

            thread_windows_size = self.max_workers*2

            while True:     
                self.check_kill(executor)
                if self.refresh_Check() is True:
                    break
                
                if self.livestream_coordinator and self.livestream_coordinator.stats.get(self.type, None) is None:
                    self.livestream_coordinator.stats[self.type] = {}

                self.commit_segments()
                # Process completed segment downloads, wait up to 5 seconds for segments to complete before next loop
                done, not_done = concurrent.futures.wait(future_to_seg, timeout=1.0, return_when=concurrent.futures.FIRST_COMPLETED)  # need to fully determine if timeout or ALL_COMPLETED takes priority             
                
                for future in done:
                    seg_num = None
                    try:
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
                            #self.insert_single_segment(segment_order=seg_num, segment_data=segment_data)
                            #uncommitted_inserts += 1                               
                            existing = self.pending_segments.get(seg_num, None)
                            if existing is None or len(segment_data) > len(existing):
                                self.pending_segments[seg_num] = segment_data                    
                                
                            if self.livestream_coordinator:
                                self.livestream_coordinator.stats[self.type]["downloaded_segments"] = len(self.already_downloaded) + len(self.pending_segments)                          

                            if status == 200 and seg_num > latest_downloaded_segment:
                                latest_downloaded_segment = seg_num
                        elif status is None or status != 200:
                            segment_retries[seg_num] = segment_retries.get(seg_num, 0) + 1
                            self.logger.debug("Unable to download {0} ({1}). Currently at {2} retries".format(seg_num, self.format, segment_retries.get(seg_num, "UNKNOWN")))
                    
                    except httpx.ConnectTimeout:
                        self.logger.warning('({0}) Experienced connection timeout while fetching segment {1}'.format(self.format, future_to_seg.get(future,"(Unknown)")))
                        
                    except Exception as e:
                        self.logger.exception("An unknown error occurred")
                        seg_num = seg_num or future_to_seg.get(future,None)
                        if seg_num is not None:
                            segment_retries[seg_num] = segment_retries.get(seg_num, 0) + 1
                    # Remove from submitted segments in case it neeeds to be regrabbed

                    future_segnum = future_to_seg.pop(future,None)

                    submitted_segments.discard(seg_num if seg_num is not None else future_segnum)

                    # Remove completed thread to free RAM
                    
                optimistic_seg = max(self.latest_sequence, latest_downloaded_segment) + 1

                # Check if optimistic segment has already downloaded or not
                if optimistic_seg not in self.already_downloaded and self.segment_exists(optimistic_seg):
                    self.already_downloaded.add(optimistic_seg)
                    
                segments_to_download = set(range(0, max(self.latest_sequence + 1, latest_downloaded_segment + 1))) - self.already_downloaded - set(self.pending_segments.keys()) - set(k for k, v in segment_retries.items() if v > self.fragment_retries)  

                  
                                        
                # If segments remain to download, don't bother updating and wait for segment download to refresh values.
                """
                if optimistic_fails < optimistic_fails_max and optimistic_seg not in submitted_segments and optimistic_seg not in self.already_downloaded and optimistic_seg not in segments_to_download:
                        
                        
                        
                        #logging.debug("\033[93mAdding segment {1} optimistically ({0}). Currently at {2} fails\033[0m".format(self.format, optimistic_seg, optimistic_fails))
                        logging.debug("\033[93mAdding segment {1} optimistically ({0}). Currently at {2} fails\033[0m".format(self.format, optimistic_seg, optimistic_fails))
                        segments_to_download.discard(optimistic_seg)
                        segments_to_download = {optimistic_seg, *segments_to_download}       
                """

                # Add optimistic segment if conditions are right
                # Only attempt to grab optimistic segment a number of times to ensure it does not cause a loop at the end of a stream
                if (self.max_workers > 1 or not segments_to_download) and optimistic_fails < optimistic_fails_max and optimistic_seg not in self.already_downloaded and optimistic_seg not in self.pending_segments and optimistic_seg not in submitted_segments:
                    # Wait estimated fragment time +0.1s to make sure it would exist. Wait a minimum of 2s if no segments are to be submitted
                    if not segments_to_download:
                        time.sleep(max(self.estimated_segment_duration, 2) + 0.1)
                    self.logger.debug("\033[93mAdding segment {1} optimistically ({0}). Currently at {2} fails\033[0m".format(self.format, optimistic_seg, optimistic_fails))
                    future_to_seg.update({
                        executor.submit(self.download_segment, self.stream_url.segment(optimistic_seg), optimistic_seg, client): optimistic_seg
                    })
                    submitted_segments.add(optimistic_seg)

                    # Ensure wait isn't triggered while optimistic segments is enabled
                    wait = 0
                # If update has no segments and no segments are currently running, wait                              
                if len(segments_to_download) <= 0 and len(future_to_seg) <= 0:                 
                    
                    self.logger.debug("No new fragments available for {0}, attempted {1} times...".format(self.format, wait))
                        
                    # If waited for more than (non-zero) wait limit break. Zero wait limit is intended to keep checking until stream ends
                    if (self.wait_limit or 0) > 0 and wait > self.wait_limit:
                        self.logger.debug("Wait time for new fragment exceeded, ending download...")
                        break    
                    # If over 10 wait loops have been executed, get page for new URL and update status if necessary
                    elif wait % 10 == 0 and wait > 0:
                        temp_stream_url = self.stream_url
                        refresh = self.refresh_url()
                        if self.is_private:
                            self.logger.debug("Video is private and no more segments are available. Ending...")
                            break
                        elif refresh is False:
                            break                              
                        elif refresh is True:
                            self.logger.info("Video finished downloading via new manifest")
                            break
                        elif temp_stream_url != self.stream_url:
                            self.logger.info("({0}) New stream URL detecting, resetting segment retry log")
                            segment_retries.clear()
                    time.sleep(10)
                    self.update_latest_segment(client=client)
                    wait += 1
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
                                # Construct the options dictionary for the recovery call
                                recovery_options = {
                                    "batch_size": self.batch_size,
                                    "max_workers": max((self.recovery_thread_multiplier * self.max_workers * int(len(self.stream_urls))), self.recovery_thread_multiplier),
                                    "file_name": self.file_base_name,
                                    "cookies": self.cookies,
                                    "fragment_retries": self.fragment_retries,
                                    "proxies": self.proxies,
                                    "no_merge": True
                                }
                                self.livestream_coordinator.recover_stream(
                                    info_dict=self.info_dict, 
                                    resolution=str(self.format), 
                                    options=recovery_options,
                                    stream_urls=self.stream_urls,
                                    live_status=self.live_status,
                                )
                                #self.livestream_coordinator.recover_stream(info_dict=self.info_dict, resolution=str(self.format), batch_size=self.batch_size, max_workers=max((self.recovery_thread_multiplier*self.max_workers*int(len(self.stream_urls))),self.recovery_thread_multiplier), file_name=self.file_base_name, cookies=self.cookies, retries=self.fragment_retries, stream_urls=self.stream_urls, proxies=self.proxies, no_merge=True)
                            except Exception as e:
                                self.logger.exception("An error occurred while trying to recover the stream")
                        else:
                            try:
                                recovery_options = {
                                    "batch_size": self.batch_size,
                                    "max_workers": max((self.recovery_thread_multiplier * self.max_workers * int(len(self.stream_urls))), self.recovery_thread_multiplier),
                                    "file_name": self.file_base_name,
                                    "cookies": self.cookies,
                                    "fragment_retries": self.fragment_retries,
                                    "proxies": self.proxies
                                }
                                with StreamRecovery(
                                    info_dict=self.info_dict, 
                                    stream_url=self.stream_url, 
                                    options=recovery_options,
                                    livestream_coordinator=self.livestream_coordinator,
                                    stream_urls=self.stream_urls,
                                    live_status=self.live_status,
                                ) as downloader:
                                    downloader.live_dl()
                                    downloader.close_connection()
                                #with StreamRecovery(info_dict=self.info_dict, resolution=str(self.format), batch_size=self.batch_size, max_workers=max((self.recovery_thread_multiplier*self.max_workers*int(len(self.stream_urls))),self.recovery_thread_multiplier), 
                                #                    file_name=self.file_base_name, cookies=self.cookies, fragment_retries=self.fragment_retries, stream_urls=self.stream_urls, proxies=self.proxies) as downloader:
                                #    downloader.live_dl()
                                #    downloader.close_connection()
                            except Exception as e:
                                self.logger.exception("An error occurred while trying to recover the stream")
                        time.sleep(1)
                        self.conn = self.create_connection(self.temp_db_file)
                        return True
                        
                    else:
                        self.logger.warning("{0} - Stream is now private and segments remain. Current stream protocol does not support stream recovery, ending...")
                        break
                
                elif segment_retries and all(v > self.fragment_retries for v in segment_retries.values()):
                    
                    self.logger.warning("All remaining segments have exceeded the retry threshold, attempting URL refresh...")
                    temp_stream_url = self.stream_url
                    refresh = self.refresh_url()
                    if self.refresh_url() is True:
                        self.logger.info("Video finished downloading via new manifest")
                        break
                    elif self.is_private or refresh is False:
                        # If stream URL has changed, refresh retry count and continue
                        if temp_stream_url != self.stream_url:
                            self.logger.info("({0}) New stream URL detecting, resetting segment retry log")
                            segment_retries.clear()
                            continue
                        self.logger.warning("Failed to refresh URL or stream is private, ending...")
                        break
                    
                    else:
                        segment_retries.clear()
                else:
                    wait = 0
                    
                

                #segments_to_download = segments_to_download - self.already_downloaded

                #print("Segments to download: {0}".format(segments_to_download))
                #print("remaining threads: {0}".format(future_to_seg))

                
                
                # Get current state of futures to determine if more threads should be added this loop
                if len(not_done) <= 0 and len(done) > 0:
                    thread_windows_size = 3*thread_windows_size
                else:
                    thread_windows_size = max(thread_windows_size//2, 2*self.max_workers)
                
                #self.logger.debug("{0} segments in thread queue, set window size to {1}".format(len(not_done), thread_windows_size))

                # Add new threads to existing future dictionary, done directly to almost half RAM usage from creating new threads
                for seg_num in segments_to_download:
                    # Have up to 2x max workers of threads submitted
                    if len(future_to_seg) > max(10,2*self.max_workers, thread_windows_size):
                        break
                    if seg_num not in submitted_segments and seg_num not in self.pending_segments:
                        # Check if segment already exist within database (used to not create more connections). Needs fixing upstream
                        if self.segment_exists(seg_num):
                            self.already_downloaded.add(seg_num)
                            segment_retries.pop(seg_num,None)
                            continue
                        future_to_seg.update({
                            executor.submit(self.download_segment, self.stream_url.segment(seg_num), seg_num, client): seg_num
                        })
                        submitted_segments.add(seg_num)
                    
                
            self.commit_batch(self.conn)
        self.commit_batch(self.conn)
        if self.following_manifest_thread is not None:
            self.following_manifest_thread.join()
        return True           
    
    def download_segment(self, segment_url, segment_order, client: httpx.Client=None, immediate_403s=False):
        total_retries = self.fragment_retries
        backoff_factor = 1
        backoff_max = 4
        # 30-second hard limit for the entire download process per attempt
        MAX_DURATION = 30.0 
        status_forcelist = {204, 400, 401, 403, 404, 408, 413, 429, 500, 502, 503, 504}

        for attempt in range(total_retries + 1):
            if client is None or client.is_closed:
                # Set a base timeout of 30s for connect/read as well
                httpx.Client(timeout=self.timeout_config, proxy=self.proxies, mounts=self.mounts, http1=True, http2=self.http2_available(), follow_redirects=True, headers=self.info_dict.get("http_headers", None))
            
            start_time = time.time()
            try:
                self.check_kill() 

                with client.stream("GET", segment_url) as response:
                    status = response.status_code
                    headers = response.headers
                    head_seq = int(headers.get("X-Head-Seqnum", -1))

                    if status == 403 and immediate_403s:
                        self.is_403 = True

                    if status in status_forcelist and attempt < total_retries:
                        raise httpx.HTTPStatusError("Retryable Status", request=response.request, response=response)

                    # Handle auth flags
                    if status == 403:
                        self.is_403 = True
                        self.logger.warning("({0}) Encountered 403 status when downloading segment {1}".format(self.format, segment_order))
                    elif status == 401:
                        self.is_401 = True
                        self.logger.warning("({0}) Encountered 401 status when downloading segment {1}. This can be cause by using too many threads.".format(self.format, segment_order))
                    elif status in {200, 204}:
                        self.is_403 = self.is_401 = False

                    # Process successful stream
                    if status == 200:
                        buffer = bytearray()
                        # Use 64kb chunks to regularly check progress
                        for chunk in response.iter_bytes(chunk_size=(64 * 1024)):
                            # Hard limit check: Total elapsed time
                            if (time.time() - start_time) > MAX_DURATION:
                                raise httpx.TimeoutException("Download exceeded hard limit of {0}".format(MAX_DURATION))
                            
                            buffer.extend(chunk)
                        
                        return head_seq, bytes(buffer), int(segment_order), status, headers
                    
                    elif status == 204:
                        return head_seq, bytes(), int(segment_order), status, headers
                    else:
                        self.logger.warning("({0}) Unexpected status returned {1}".format(self.format, status))
                        return head_seq, None, int(segment_order), status, headers
                    
            except httpx.ConnectTimeout as e:
                if attempt >= total_retries:
                    raise                
                sleep_time = min(backoff_max, backoff_factor * (2 ** attempt))
                time.sleep(sleep_time)

            except (httpx.RequestError, httpx.HTTPStatusError, httpx.TimeoutException) as e:
                if attempt >= total_retries:
                    self.logger.warning("({0}) Unexpected error downloading segment {1} file after {2} attempts {3}".format(self.format, segment_order, attempt, e))
                    # Return standard error format
                    status_err = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
                    return -1, None, segment_order, status_err, None
                
                sleep_time = min(backoff_max, backoff_factor * (2 ** attempt))
                time.sleep(sleep_time)
                
            except Exception as e:
                self.logger.exception(f"Unknown error: {e}")
                if attempt >= total_retries:                    
                    return -1, None, segment_order, None, None
                
                sleep_time = min(backoff_max, backoff_factor * (2 ** attempt))
                time.sleep(sleep_time)

    
    def process_proxies_for_httpx(self, proxies) -> tuple[Optional[str], Optional[dict]]:
        if not proxies:
            return None, None
        if isinstance(proxies, str):
            return proxies.strip(), None
        elif isinstance(proxies, dict):
            proxy_mounts = {
                protocol: httpx.HTTPTransport(proxy=url) 
                for protocol, url in proxies.items()
            }
            return None, proxy_mounts
        
        return None, None

    def http2_available(self):
        HAS_HTTP2 = False
        try:
            import h2
            HAS_HTTP2 = True
        except ImportError:
            HAS_HTTP2 = False
        return HAS_HTTP2

    def update_latest_segment(self, client: httpx.Client = None):
        # Kill if keyboard interrupt is detected
        self.check_kill()
        
        stream_url_info = self.get_Headers(url=self.stream_url, client=client)
        if stream_url_info is not None and stream_url_info.get("X-Head-Seqnum", None) is not None:
            self.latest_sequence = int(stream_url_info.get("X-Head-Seqnum"))
            self.logger.debug("Latest sequence: {0}".format(self.latest_sequence))
            
        if stream_url_info is not None and stream_url_info.get('Content-Type', None) is not None:
            file_type, ext = str(stream_url_info.get('Content-Type')).split('/')
            if not self.ext and file_type.strip().lower() in ["video", "audio"]:
                #self.type = self.type.strip().lower()
                self.ext = ext

        if self.livestream_coordinator:
            self.livestream_coordinator.stats.setdefault(self.type, {})["latest_sequence"] = self.latest_sequence
    
    def get_Headers(self, url, client: httpx.Client=None):
        if client is None or client.is_closed:
            client = httpx.Client(timeout=self.timeout_config, proxy=self.proxies, mounts=self.mounts, http1=True, http2=self.http2_available(), follow_redirects=True, headers=self.info_dict.get("http_headers", None))
        try:
            # Send a GET request to a URL
            #response = requests.get(url, timeout=30, proxies=self.proxies)
            response = client.get(str(url))
            # 200 and 204 responses appear to have valid headers so far
            if response.status_code == 200 or response.status_code == 204:
                self.is_403 = self.is_401 = False

                # Print the response headers
                #print(json.dumps(dict(response.headers), indent=4))  
                
            elif response.status_code == 403:
                self.logger.warning("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                
            elif response.status_code == 401:
                self.is_401 = True
            else:
                self.logger.debug("Error retrieving headers: {0}".format(response.status_code))
                self.logger.debug(json.dumps(dict(response.headers), indent=4))
            return response.headers
            
        except httpx.TimeoutException as e:
            self.logger.info("Timed out updating fragments: {0}".format(e))
            #print(e)
            return None
        
        except Exception as e:
            self.logger.exception("\033[31m{0}\033[0m".format(e))
            return None
        
        return None
    
    def detect_manifest_change(self, info_json, follow_manifest=True):
        resolution = "Unknown"
        try:
            resolution = "(bv/ba/best)[format_id~='^{0}(?:-.*)?$'][protocol={1}]".format(self.stream_url.itag, self.stream_url.protocol)
            self.logger.debug("Searching for new manifest of same format {0}".format(resolution))
            temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=resolution, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8, stream_type=self.type)
            if temp_stream_url is not None:
                #resolution = r"(format_id~='^({0}(?:\D*(?:[^0-9].*)?)?)$')[protocol={1}]".format(str(self.format).split('-', 1)[0], self.stream_url.protocol)
                
                #parsed_url = urlparse(temp_stream_url)        
                #temp_url_params = {k: v if len(v) > 1 else v[0] for k, v in parse_qs(parsed_url.query).items()}
                #if temp_url_params.get("id", None) is not None and temp_url_params.get("id") != self.url_params.get("id"):
                if temp_stream_url.itag is not None and temp_stream_url.protocol == self.stream_url.protocol and temp_stream_url.itag == self.stream_url.itag and temp_stream_url.manifest != self.stream_url.manifest:
                    self.logger.warning("({1}) New manifest for format {0} detected, starting a new instance for the new manifest".format(self.format, self.id))
                    self.commit_batch(self.conn)
                    if follow_manifest:
                        #new_params = copy.copy(self.params)
                        new_options = copy.copy(self.options)
                        new_options.update({
                            "file_name": f"{self.file_base_name}.{temp_stream_url.manifest}",
                        })
                        new_params = {
                            "info_dict": info_json,
                            "stream_url": temp_stream_url,                            
                            "manifest": temp_stream_url.manifest,
                            "options": new_options,
                        }
                        if new_options.get("download_function", None) is not None: 
                            self.following_manifest_thread = threading.Thread(
                                target=new_options.get("download_function"),
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
                        new_options = None
                    return True
                else:
                    return False
        except yt_dlp.utils.ExtractorError as e:
            self.logger.warning("Unable to find stream of same format ({0}) for {1}".format(resolution, self.id))
            
        try:
            if YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.resolution, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8, stream_type=self.type) is not None:
                self.logger.debug("Searching for new manifest of same resolution {0}".format(resolution))
                temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution=self.resolution, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8, stream_type=self.type)
                if temp_stream_url.itag is not None and temp_stream_url.itag != self.stream_url.itag:
                    self.logger.warning("({2}) New manifest for resolution {0} detected, but not the same format as {1}, starting a new instance for the new manifest".format(self.resolution, self.format, self.id))
                    self.commit_batch(self.conn)
                    if follow_manifest:
                        #new_params = copy.copy(self.params)
                        new_options = copy.copy(self.options)
                        new_options.update({
                            "file_name": f"{self.file_base_name}.{temp_stream_url.manifest}",
                        })
                        new_params.update({
                            "info_dict": info_json,
                            "stream_url": temp_stream_url,
                            #"file_name": f"{self.file_base_name}.{temp_stream_url.manifest}",
                            "manifest": self.stream_url.itag if self.stream_url.manifest == temp_stream_url.manifest else self.stream_url.manifest,
                            "options": new_options,
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
                        new_options = None
                    return True
                else:
                    return False
        except yt_dlp.utils.ExtractorError as e:
            self.logger.warning("Unable to find stream of same resolution ({0}) for {1}".format(self.resolution, self.id))

        try:
            if self.resolution != "audio_only" and YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution="best", include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8, stream_type=self.type) is not None:
                self.logger.debug("Searching for new best stream")
                temp_stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_json, resolution="best", include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8, stream_type=self.type)
                if temp_stream_url.itag is not None and temp_stream_url.itag != self.stream_url.itag:
                    self.logger.warning("({2}) New manifest has been found, but it is not the same format or resolution".format(self.resolution, self.format, self.id))
                    self.commit_batch(self.conn)
                    if follow_manifest:
                        #new_params = copy.copy(self.params)
                        new_options = copy.copy(self.options)
                        new_options.update({
                            "file_name": f"{self.file_base_name}.{temp_stream_url.manifest}",
                            "resolution": "best",
                        })
                        new_params.update({
                            "info_dict": info_json,
                            "stream_url": temp_stream_url,                            
                            "options": new_options,
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
                        new_options = None
                    return True
                else:
                    return False
        except yt_dlp.utils.ExtractorError as e:
            self.logger.warning("Unable to find any stream for {1} when attempting to find 'best' stream".format(self.resolution, self.id))
        return False

    def create_connection(self, file):
        conn = sqlite3.connect(file, timeout=30)

        # Database connection optimization (when not in memory)
        if not self.database_in_memory:
            conn.execute('PRAGMA journal_mode = WAL;')
            conn.execute('PRAGMA synchronous = NORMAL;')
            conn.execute('PRAGMA page_size = 32768;')
            conn.execute('PRAGMA cache_size = -64000;')
            # Optionally commit immediately to persist the PRAGMA settings
            conn.commit()

        return conn

    
    def create_db(self, temp_file):
        # Connect to SQLite database (or create it if it doesn't exist)
        self.sqlite_thread = threading.get_ident()
        conn = self.create_connection(temp_file)  # should return a Connection object

        # Create the table (id = segment order, segment_data as BLOB)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS segments (
                id INTEGER PRIMARY KEY,
                segment_data BLOB
            )
        ''')
        conn.commit()

        return conn
    
    def commit_segments(self, force=False):
        if not self.pending_segments:
            if force:
                self.conn.commit()
                if self.livestream_coordinator:
                    self.livestream_coordinator.stats[self.type]["current_filesize"] = os.path.getsize(self.temp_db_file)
        # If finished threads exceeds batch size, commit the whole batch of threads at once. 
        # Has risk of not committing if a thread has no segment data, but this would be corrected naturally in following loop(s)
        elif force or len(self.pending_segments) >= self.batch_size:
            self.logger.debug("Writing segments to file...")
            try:
                # 1. SORT the batch by the segment ID (index 0 of the tuple)
                # This keeps the B-Tree insertions linear and avoids page splits.
                batch = sorted(self.pending_segments.items(), key=lambda kv: kv[0])

                # 2. Use your specific UPSERT logic within executemany
                sql = '''
                    INSERT INTO segments (id, segment_data) 
                    VALUES (?, ?) 
                    ON CONFLICT(id) 
                    DO UPDATE SET segment_data = CASE 
                        WHEN LENGTH(excluded.segment_data) > LENGTH(segments.segment_data) 
                        THEN excluded.segment_data 
                        ELSE segments.segment_data 
                    END;
                '''
                
                self.conn.executemany(sql, batch)
                # Micro-optimisation - unlikely to make a difference, but won't hurt
                del batch
                
                self.conn.commit()

                # 3. Re-open transaction for the next batch (WAL mode optimization)
                self.conn.execute('BEGIN TRANSACTION')
                
                # 4. Clear the buffer immediately to free RAM (important for video BLOBs)
                self.pending_segments.clear()
                
            except Exception as e:
                self.logger.exception(f"Batch insert failed:")
                self.conn.rollback()

            if self.livestream_coordinator:
                self.livestream_coordinator.stats[self.type]["current_filesize"] = os.path.getsize(self.temp_db_file)

    # Function to check if a segment exists in the database
    def segment_exists(self, segment_order):
        cursor = self.conn.execute('SELECT 1 FROM segments WHERE id = ?', (segment_order,))
        return cursor.fetchone() is not None
    
    def segment_exists_batch(self) -> set:
        """
        Queries the database to check if a batch of segment numbers are already downloaded.
        Returns a set of existing segment numbers.
        """
        query = "SELECT id FROM segments"
        cur = self.conn.execute(query)
        rows = cur.fetchall()
        return set(row[0] for row in rows)

    
    # Function to insert a single segment without committing
    def insert_single_segment(self, segment_order, segment_data):
        self.conn.execute('''
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
    def commit_batch(self, conn=None):
        self.conn.commit()
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]["current_filesize"] = os.path.getsize(self.temp_db_file)
        
    def close_connection(self):
        if self.conn:
            self.conn.close()

    def combine_segments_to_file(self, output_file):
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]['status'] = "merging"

        self.logger.debug(f"Merging segments to {output_file}")
        
        should_clean = self.should_clean(self.ext)

        with open(output_file, 'wb', buffering=1048576) as f:
            
            cursor = self.conn.execute('SELECT segment_data FROM segments ORDER BY id')
            chunk_size = max(self.batch_size*2, 3) # Number of rows to pull into RAM at once
            is_first = True
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                for (segment_data,) in rows:
                    # 1. clean_segments returns a bytearray
                    # 2. f.write consumes the bytearray directly
                    if should_clean:
                        piece = self.clean_segments(segment_data, first=is_first)
                        f.write(piece)
                    else:
                        f.write(segment_data)
                    
                    is_first = False

        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]['status'] = "merged"

        return output_file
    
    def should_clean(self, ext = None) -> bool:
        if not ext:
            return True
        ext = str(ext).lower()
        if not ext.startswith("."): 
            ext = f".{ext}"
        
        return (ext in self.ATOM_BASED_EXTENSIONS)
    
    ### Via ytarchive            
    def get_atoms(self, data):
        """
        Faster parsing using struct and avoiding unnecessary string conversions.
        """
        atoms = {}
        ofs = 0
        data_len = len(data)
        
        # Use memoryview to slice without copying
        view = memoryview(data)

        while ofs + 8 <= data_len:
            try:
                # '>I' reads 4 bytes as a Big-Endian Unsigned Int (the standard MP4 length format)
                # This is significantly faster than .hex() -> int()
                alen = struct.unpack_from('>I', view, ofs)[0]
                
                # Basic MP4 validation: length can't be smaller than the header itself (8 bytes)
                if alen < 8 or ofs + alen > data_len:
                    break

                # Decode the 4-character atom name (e.g., 'moov')
                aname = view[ofs + 4:ofs + 8].tobytes().decode(errors='ignore')
                atoms[aname] = {"ofs": ofs, "len": alen}
                ofs += alen
            except (struct.error, UnicodeDecodeError):
                break

        return atoms
    
    def remove_atoms(self, data, atom_list):
        """
        Removes atoms in-place using a bytearray to avoid memory re-allocations.
        """
        atoms = self.get_atoms(data)
        # Convert input (bytes) to bytearray so it's mutable
        data_buffer = bytearray(data)
        
        # Sort by offset descending so deleting an atom doesn't shift the 
        # offsets of the atoms we still need to delete.
        to_remove = [atoms[name] for name in atom_list if name in atoms]
        if not to_remove:
            return data_buffer

        to_remove.sort(key=lambda x: x["ofs"], reverse=True)
        
        for atom in to_remove:
            # del on bytearray shifts memory in C, no Python-level loops
            del data_buffer[atom["ofs"] : atom["ofs"] + atom["len"]]
        
        return data_buffer

    def clean_segments(self, data, first=True):
        # sidx is metadata usually used for DASH/HLS seeking, dead weight in a merged file.
        bad_atoms = ["sidx"]
        
        if not first:
            # ftyp and moov are headers. We only want them from the first segment.
            bad_atoms.extend(["ftyp", "moov"])

        return self.remove_atoms(data=data, atom_list=bad_atoms)
    
    def check_kill(self, executor: concurrent.futures.ThreadPoolExecutor = None, tasks=None):
        """
        Checks kill flags. If set, cancels threads or async tasks and shuts down.
        """
        if self.kill_all.is_set() or self.kill_this.is_set():
            try:
                self.logger.debug("Kill command detected, ending...")
                
                # Shutdown ThreadPool (Old way)
                if executor is not None:
                    executor.shutdown(wait=True, cancel_futures=True)
                    
                # Cancel Async Tasks (New way)
                if tasks is not None:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                if self.sqlite_thread is None or self.sqlite_thread == threading.get_ident():
                    self.close_connection()
            except Exception as e:
                self.logger.exception("Unable to perform cleanup")
            # Raising KeyboardInterrupt will bubble up to asyncio.run() 
            # which handles the final cleanup of the event loop.
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
                # Attempt to use coordinator check if available to reduce the overall stream information extration between video and audio streams
                if self.livestream_coordinator:
                    if self.livestream_coordinator.lock.acquire(timeout=5.0):  # wait up to 5 seconds for lock, otherwise try next loop. If unable to aquire, return False before other fields try to update
                        try:
                            # use existing info.json if coordinator was refreshed within last 15 mins, or within the time since the last refresh, whatever is smaller
                            # 15 minutes should be enough time to be efficient with natural jitter of the each download stream for a majority of livestreams
                            info_dict, live_status = self.livestream_coordinator.refresh_info_json(update_threshold=min(900.0, time.time()-self.url_checked), id=self.id, cookies=self.cookies, additional_options=self.yt_dlp_options, include_dash=self.include_dash, include_m3u8=(self.include_m3u8 or self.force_m3u8))
                        finally:
                            self.livestream_coordinator.lock.release()
                    else:
                        return None
                else:
                    info_dict, live_status = getUrls.get_Video_Info(self.id, wait=False, cookies=self.cookies, additional_options=self.yt_dlp_options, include_dash=self.include_dash, include_m3u8=(self.include_m3u8 or self.force_m3u8))
                
                # Check for new manifest, if it has, start a nested download session
                if self.detect_manifest_change(info_json=info_dict, follow_manifest=follow_manifest) is True:
                    return True
                
                #resolution = "(format_id^={0})[protocol={1}]".format(str(self.format).rsplit('-', 1)[0], self.stream_url.protocol)
                #resolution = r"(format_id~='^({0}(?:\D*(?:[^0-9].*)?)?)$')[protocol={1}]".format(str(self.format).split('-', 1)[0], self.stream_url.protocol)
                resolution = "(bv/ba/best)[format_id~='^{0}(?:-.*)?$'][protocol={1}]".format(self.stream_url.itag, self.stream_url.protocol)
                #stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=str(self.format), sort=self.yt_dlp_sort, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8)
                stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution, sort=self.yt_dlp_sort, include_dash=self.include_dash, include_m3u8=self.include_m3u8, force_m3u8=self.force_m3u8, stream_type=self.type) 
                if stream_url is not None:
                    self.stream_url = stream_url
                    self.stream_urls.append(stream_url)
                    
                    filtered_array = [url for url in self.stream_urls if int(self.get_expire_time(url)) > time.time()]
                    self.stream_urls = filtered_array
                    self.refresh_retries = 0
                    self.logger.log(setup_logger.VERBOSE_LEVEL_NUM, "Refreshed stream URL with {0} - {1}".format(stream_url.format_id, stream_url.fomat_note))
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
                self.live_status = "was_live"
                self.url_checked = time.time()
                return False
            except getUrls.LivestreamError:
                self.logger.debug("Livestream has ended.")
                self.live_status = "was_live"
                self.url_checked = time.time()
                return False 
            except Exception as e:
                self.logger.exception("Error: {0}".format(e))                     
        self.url_checked = time.time()

        if self.live_status != 'is_live':
            self.logger.debug("Livestream has ended.")
            #self.catchup()
            return False 
        
        return None

class DownloadStreamDirect(DownloadStream):
    def __init__(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, livestream_coordinator: LiveStreamDownloader=None):
        options = options or {}
        # Set database_in_memory to True for Direct download, overriding default or provided option
        options['database_in_memory'] = True
        
        # Initialize base class
        super().__init__(
            info_dict=info_dict,
            stream_url=stream_url,
            options=options,
            livestream_coordinator=livestream_coordinator
        )
        
        # Close the unused in-memory DB connection opened by super().__init__
        if self.conn:
            self.close_connection()
        self.conn = None

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
        self.logger.info(f"\033[93mStarting download of live fragments ({self.format} - {self.stream_url.fomat_note}) [Direct Mode]\033[0m")
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
        
        limits = httpx.Limits(max_keepalive_connections=self.max_workers+1, max_connections=self.max_workers+1, keepalive_expiry=30)
        with (concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix=f"{self.id}-{self.format}") as executor,
            httpx.Client(timeout=self.timeout_config, limits=limits, proxy=self.proxies, mounts=self.mounts, http1=True, http2=self.http2_available(), follow_redirects=True, headers=self.info_dict.get("http_headers", None)) as client):
            
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

                done, _ = concurrent.futures.wait(future_to_seg, timeout=1, return_when=concurrent.futures.FIRST_COMPLETED)

                for future in done:
                    seg_num = None
                    try:
                        head_seg_num, segment_data, seg_num, status, headers = future.result()
                        #print("Finished: {0}".format(seg_num))
                        #submitted_segments.discard(seg_num)
                        #future_to_seg.pop(future, None)
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
                            #print("Unable to download {0} ({1}). Currently at {2} retries".format(seg_num, self.format, segment_retries.get(seg_num, "UNKNOWN")))
                            self.logger.debug("Unable to download {0} ({1}). Currently at {2} retries".format(seg_num, self.format, segment_retries.get(seg_num, "UNKNOWN")))

                    except httpx.ConnectTimeout:
                        self.logger.warning('({0}) Experienced connection timeout while fetching segment {1}'.format(self.format, seg_num or future_to_seg.get(future,"(Unknown)"))) 

                    except Exception as e:
                        self.logger.exception("An unknown error occurred")
                        seg_num = seg_num or future_to_seg.get(future,None)
                        if seg_num is not None:
                            segment_retries[seg_num] = segment_retries.get(seg_num, 0) + 1
                    # Remove from submitted segments in case it neeeds to be regrabbed

                    future_segnum = future_to_seg.pop(future,None)

                    submitted_segments.discard(seg_num if seg_num is not None else future_segnum)

                # Write contiguous downloaded segments
                # Check if there is at least one segment to write
                
                #print("[{2}] Last written: {0} - Downloaded: {0} - Next ready: {3}".format(self.state['last_written'], downloaded_segments.keys(), self.format, downloaded_segments.get(self.state['last_written'] + 1, None) is not None))
                if downloaded_segments.get(self.state['last_written'] + 1, None) is not None:
                    # If segments exist, open the file *once*
                    mode = 'wb' if self.state['file_size'] == 0 else 'r+b'
                    isFirst = self.state['file_size'] == 0
                    should_clean = self.should_clean(self.ext)
                    with open(self.merged_file_name, mode) as f:
                        # Seek to the end of the file *once* (if not a new file)
                        if mode != 'wb':
                            f.seek(self.state['file_size'])

                        # Loop and write all available consecutive segments
                        while downloaded_segments.get(self.state['last_written'] + 1, None) is not None:
                            seg_num = self.state['last_written'] + 1
                            segment = downloaded_segments.pop(seg_num)
                            
                            if should_clean:
                                cleaned = self.clean_segments(data=segment, first=isFirst)                                
                                f.write(cleaned)
                            else:
                                f.write(segment)
                            f.truncate()  # Truncates the file at the current position (after the write)
                            
                            self.state['last_written'] = seg_num
                            
                            # Optimization: Use f.tell() instead of os.path.getsize()
                            # f.tell() returns the current file position, which is the new
                            # file size after writing and truncating. This is much faster.
                            self.state['file_size'] = f.tell()                         
                            
                            self.logger.debug(f"Written segment {seg_num} ({self.format}), file size: {self.state['file_size']} bytes")
                    self._save_state()
                    if self.livestream_coordinator:
                        self.livestream_coordinator.stats[self.type]["downloaded_segments"] = self.state.get('last_written', 0)
                        self.livestream_coordinator.stats[self.type]["current_filesize"] = self.state.get('file_size', 0) 
                    
                elif segment_retries.get(self.state.get('last_written', 0) + 1, 0) > self.fragment_retries:
                    self.logger.warning("({1}) Segment {0} has exceeded maximum segment retries, advancing count to save data...".format(self.state.get('last_written', 0), self.format))
                    self.state['last_written'] = self.state.get('last_written', 0) + 1

                # Remove any potential stray segments 
                downloaded_segments = dict((k, v) for k, v in downloaded_segments.items() if k >= self.state.get('last_written',0))

                # Determine segments to download. Sort into a list as direct to ts relies on segments to be written in order
                segments_to_download = sorted(set(range(self.state.get('last_written',-1) + 1, self.latest_sequence + 1)) - submitted_segments - set(downloaded_segments.keys()) - set(self.pending_segments.keys()) - set(k for k, v in segment_retries.items() if v > self.fragment_retries))

                optimistic_seg = max(self.latest_sequence, self.state.get('last_written',0)) + 1  
                                        
                
                if optimistic_fails < optimistic_fails_max and optimistic_seg not in submitted_segments and optimistic_seg not in self.already_downloaded and len(segments_to_download) <= 2 * self.max_workers:
                    # Wait estimated fragment time +0.1s to make sure it would exist. Wait a minimum of 2s
                    if not segments_to_download:
                        time.sleep(max(self.estimated_segment_duration, 2) + 0.1)
                    
                    self.logger.debug("\033[93mAdding segment {1} optimistically ({0}). Currently at {2} fails\033[0m".format(self.format, optimistic_seg, optimistic_fails))
                    segments_to_download.append(optimistic_seg)
                                    
                # If update has no segments and no segments are currently running, wait                              
                if len(segments_to_download) <= 0 and len(future_to_seg) <= 0:                 
                    
                    self.logger.debug("No new fragments available for {0}, attempted {1} times...".format(self.format, wait))
                        
                    # If waited for new fragments hits 20 loops, assume stream is offline
                    if wait > self.wait_limit and wait > self.wait_limit:
                        self.logger.debug("Wait time for new fragment exceeded, ending download...")
                        break    
                    # If over 10 wait loops have been executed, get page for new URL and update status if necessary
                    elif wait % 10 == 0 and wait > 0:
                        temp_stream_url = self.stream_url
                        refresh = self.refresh_url()
                        if self.is_private:
                            self.logger.debug("Video is private and no more segments are available. Ending...")
                            break
                        elif refresh is False:
                            break                              
                        elif refresh is True:
                            self.logger.info("Video finished downloading via new manifest")
                            break
                        elif temp_stream_url != self.stream_url:
                            self.logger.info("({0}) New stream URL detecting, resetting segment retry log")
                            segment_retries.clear()
                    time.sleep(10)
                    self.update_latest_segment(client=client)
                    wait += 1
                    continue
                    """
                elif len(segments_to_download) > 0 and self.is_private and len(future_to_seg) > 0:
                    self.logger.debug("Video is private, waiting for remaining threads to finish before ending")
                    time.sleep(5)
                    continue

                elif len(segments_to_download) > 0 and self.is_private:
                    self.logger.warning("{0} - Stream is now private and segments remain. Current stream protocol does not support stream recovery, ending...")
                    break
                """
                elif segment_retries and all(v > self.fragment_retries for v in segment_retries.values()):
                    self.logger.warning("All remaining segments have exceeded the retry threshold, attempting URL refresh...")
                    if self.refresh_url(follow_manifest=False) is True:
                        self.logger.warning("Video has new manifest. This cannot be handled by current implementation of Direct to .ts implementation")
                        break
                    elif self.is_private or self.refresh_url(follow_manifest=False) is False:
                        self.logger.warning("Failed to refresh URL or stream is private, ending...")
                        break
                    else:
                        segment_retries.clear()
                else:
                    wait = 0

                for seg_num in segments_to_download:
                    # Have up to 2x max workers of threads submitted
                    if len(future_to_seg) > max(10,2*self.max_workers):
                        break
                    if seg_num not in submitted_segments and seg_num > self.state.get('last_written', 0):
                        future_to_seg.update({
                            executor.submit(self.download_segment, self.stream_url.segment(seg_num), seg_num, client): seg_num
                        })
                        submitted_segments.add(seg_num)
                """
                for seg_num in segments_to_download:
                    if len(future_to_seg) > 2 * self.max_workers:
                        break
                    if seg_num not in submitted_segments:
                        future_to_seg[executor.submit(self.download_segment, self.stream_url.segment(seg_num), seg_num, client)] = seg_num
                        submitted_segments.add(seg_num)
                """
                    
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
    """
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
    """    

    def __init__(self, info_dict, stream_url: YoutubeURL.YoutubeURL, options=None, stream_urls: list[YoutubeURL.YoutubeURL]=[], live_status="is_live", livestream_coordinator: LiveStreamDownloader=None, **kwargs):        
        from datetime import datetime
        self.options = options or {}
        self.expires = time.time()
        
        self.livestream_coordinator = livestream_coordinator
        if self.livestream_coordinator:
            self.logger = self.livestream_coordinator.logger
            self.kill_all = self.livestream_coordinator.kill_all
            self.kill_this = self.livestream_coordinator.kill_this
        else:
            self.logger = logging.getLogger()
            self.kill_all = threading.Event()
            self.kill_this = threading.Event()
            
        self.pending_segments: dict[int, bytes] = {}
        self.sqlite_thread = None
        self.conn = None
        self.latest_sequence = -1
        self.already_downloaded = set()
        
        self.batch_size = self.options.get('batch_size', 10)
        self.max_workers = self.options.get('max_workers', 5)
        self.proxies, self.mounts = self.process_proxies_for_httpx(proxies=self.options.get('proxies'))
        
        self.resolution = options.get("resolution", "best")
        self.yt_dlp_sort = self.options.get('yt_dlp_sort')

        if stream_urls:
            self.logger.debug("{0} stream urls available".format(len(stream_urls)))
            for url in stream_urls:
                self.format = url.format_id
                if self.format is not None:
                    self.logger.debug("Stream recovery - Found format {0} from itags".format(self.format))
                    break            
            self.stream_urls = stream_urls          
        else:
            if stream_url.vcodec:
                self.type = "video"
            else:
                self.type = "audio"
            self.stream_urls = YoutubeURL.Formats().getFormatURL(
                info_json=info_dict, 
                resolution=self.resolution,  
                sort=self.yt_dlp_sort, 
                get_all=True, 
                include_dash=False,
                include_m3u8=False, 
                stream_type=self.type
            )



        if not self.stream_urls:
            raise ValueError("No compatible stream URLs not found for {0}, unable to continue".format(self.resolution))
        
        self.id = info_dict.get('id', self.stream_urls[0].id)
        
        self.info_dict = info_dict

        self.live_status = info_dict.get('live_status', live_status)
            
        self.stream_url: YoutubeURL.YoutubeURL = random.choice(self.stream_urls)
        if stream_url.vcodec:
            self.type = "video"
        else:
            self.type = "audio"
        self.format = self.stream_url.format_id           
        
        self.logger.debug("Recovery - Resolution: {0}, Format: {1}".format(self.resolution, self.format))
        
        self.logger.debug("Number of stream URLs available: {0}".format(len(self.stream_urls)))
        
        self.database_in_memory = self.options.get('database_in_memory', False)
        
        file_name = self.options.get('filename')
        if file_name is None:
            file_name = self.id    
        
        self.file_base_name = file_name
        
        self.merged_file_name = "{0}.{1}.ts".format(file_name, self.format)     
        if self.database_in_memory:
            self.temp_db_file = ':memory:'
        else:
            self.temp_db_file = '{0}.{1}.temp'.format(file_name, self.format)
        
        self.folder = self.options.get('folder')
        if self.folder:
            os.makedirs(self.folder, exist_ok=True)
            self.merged_file_name = os.path.join(self.folder, self.merged_file_name)
            self.file_base_name = os.path.join(self.folder, self.file_base_name)
            if not self.database_in_memory:
                self.temp_db_file = os.path.join(self.folder, self.temp_db_file)

        self.conn = self.create_db(self.temp_db_file) 
        
        self.fragment_retries = self.options.get('fragment_retries', 5)  
        self.segment_retry_time = self.options.get('segment_retry_time', 30)

        self.is_403 = False
        self.is_private = False
        self.estimated_segment_duration = 0

        self.type = None
        self.ext = self.stream_url.ext  
        
        # Set StreamRecovery-specific properties
        self.is_401 = False
        self.recover = self.options.get('recovery', False)
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
            self.logger.error("\033[31m ({0}) Current time is beyond highest expire time, unable to recover\033[0m".format(self.format))
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            format_exp = datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')
            raise TimeoutError("Current time {0} exceeds latest URL expiry time of {1}".format(now, format_exp))
        
        current_level = self.logger.getEffectiveLevel()            

        # Set log level of httpx to one level above other loggers
        if current_level == logging.NOTSET:
            target_level = logging.WARNING # Default fallback
        else:
            target_level = min(current_level + 10, logging.ERROR)

        logging.getLogger("httpx").setLevel(target_level)
        logging.getLogger("httpcore").setLevel(target_level)
        
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
    
                
    def live_dl(self):
        # This method is completely different from the base class.
        # It's designed to recover missing segments, not optimistically
        # download a live edge.
        self.logger.info("\033[93mStarting download of live fragments ({0} - {1})\033[0m".format(self.format, self.stream_url.fomat_note))
        if self.livestream_coordinator:
            self.livestream_coordinator.stats[self.type]['status'] = "recording"
        self.already_downloaded = self.segment_exists_batch()
        self.conn.execute('BEGIN TRANSACTION')
        uncommitted_inserts = 0     
        
        self.sleep_time = max(self.estimated_segment_duration, 0.1)
        
        # Track retries of all missing segments in database      
        self.segments_retries = {key: {'retries': 0, 'last_retry': 0, 'ideal_retry_time': random.uniform(max(self.segment_retry_time,900),max(self.segment_retry_time+300,1200))} for key in range(self.latest_sequence + 1) if key not in self.already_downloaded}
        segment_retries = {}
        #segments_to_download = set(range(0, self.latest_sequence)) - self.already_downloaded  
        
        i = 0
        
        last_print = time.time()
        limits = httpx.Limits(max_keepalive_connections=self.max_workers+1, max_connections=self.max_workers+1, keepalive_expiry=30)
        with (concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="{0}-{1}".format(self.id,self.format)) as executor,
              httpx.Client(timeout=self.timeout_config, limits=limits, proxy=self.proxies, mounts=self.mounts, http1=True, http2=self.http2_available(), follow_redirects=True, headers=self.info_dict.get("http_headers", None)) as client):
            submitted_segments = set()
            future_to_seg = {}
            
            if self.expires is not None:
                from datetime import datetime
                self.logger.debug("Recovery mode active, URL expected to expire at {0}".format(datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')))
            else:
                self.logger.debug("Recovery mode active")
                    
            thread_windows_size = 2*self.max_workers
            while True:     
                self.check_kill(executor)     
                if self.livestream_coordinator and self.livestream_coordinator.stats.get(self.type, None) is None:
                    self.livestream_coordinator.stats[self.type] = {}     

                self.check_Expiry()

                if (not self.stream_urls) or (self.expires and time.time() > self.expires):
                    self.logger.critical("\033[31mCurrent time is beyond highest expire time and no valid URLs remain, unable to recover\033[0m".format(self.format))
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    format_exp = datetime.fromtimestamp(int(self.expires)).strftime('%Y-%m-%d %H:%M:%S')
                    self.commit_batch()
                    raise TimeoutError("Current time {0} exceeds latest URL expiry time of {1}".format(now, format_exp)) 
                
                done, not_done = concurrent.futures.wait(future_to_seg, timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED)
                
                for future in done:
                    head_seg_num, segment_data, seg_num, status, headers = future.result()
                    try:
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
                            # self.insert_single_segment(segment_order=seg_num, segment_data=segment_data)
                            # uncommitted_inserts += 1        
                            
                            existing = self.pending_segments.get(seg_num, None)
                            if existing is None or len(segment_data) > len(existing):
                                self.pending_segments[seg_num] = segment_data               
                            
                            self.segments_retries.pop(seg_num,None)
                            
                            
                        else:                        
                            self.segments_retries.setdefault(seg_num, {})['retries'] = self.segments_retries.get(seg_num,{}).get('retries',0) + 1
                            self.segments_retries.setdefault(seg_num, {})['last_retry'] = time.time()
                            self.segments_retries.setdefault(seg_num, {}).setdefault('ideal_retry_time', random.uniform(max(self.segment_retry_time,900),max(self.segment_retry_time+300,1200)))

                            if self.segments_retries.get(seg_num, {}).get('retries',0) >= self.fragment_retries:
                                self.logger.debug("Segment {0} of {1} has exceeded maximum number of retries".format(seg_num, self.latest_sequence))
                    except httpx.ConnectTimeout:
                        self.logger.warning('({0}) Experienced connection timeout while fetching segment {1}'.format(self.format, future_to_seg.get(future,"(Unknown)")))      

                    except Exception as e:
                        self.logger.exception("An unknown error occurred")
                        seg_num = seg_num or future_to_seg.get(future,None)
                        if seg_num is not None:
                            self.segments_retries.setdefault(seg_num, {})['retries'] = self.segments_retries.get(seg_num,{}).get('retries',0) + 1
                            self.segments_retries.setdefault(seg_num, {})['last_retry'] = time.time()
                            self.segments_retries.setdefault(seg_num, {}).setdefault('ideal_retry_time', random.uniform(max(self.segment_retry_time,900),max(self.segment_retry_time+300,1200)))

                            if self.segments_retries.get(seg_num, {}).get('retries',0) >= self.fragment_retries:
                                self.logger.debug("Segment {0} of {1} has exceeded maximum number of retries".format(seg_num, self.latest_sequence))

                    future_to_seg.pop(future,None)

                if self.livestream_coordinator:
                    self.livestream_coordinator.stats[self.type]["latest_sequence"] = self.latest_sequence
                
                    self.livestream_coordinator.stats[self.type]["downloaded_segments"] = self.latest_sequence - len(self.segments_retries)

                #if uncommitted_inserts >= max(self.batch_size, len(done)):
                #    self.logger.debug("Writing segments to file...")
                #    self.commit_batch(self.conn)
                #    uncommitted_inserts = 0
                #    self.conn.execute('BEGIN TRANSACTION')    

                self.commit_segments()                         
                    
                if len(self.segments_retries) <= 0:
                    self.logger.info("All segment downloads complete, ending...")
                    break

                elif all(value['retries'] > self.fragment_retries for value in self.segments_retries.values()):
                    self.logger.error("All remaining segments have exceeded their retry count, ending...")
                    break
                
                elif self.is_403 and self.expires is not None and time.time() > self.expires:
                    self.logger.critical("URL(s) have expired and failures being detected, ending...")
                    break               
                
                elif self.is_401:
                    self.logger.debug("401s detected for {0}, sleeping for a minute")
                    time.sleep(60)
                    for url in self.stream_urls:
                        if self.live_status == 'post_live':
                            self.update_latest_segment(url=self.stream_url.segment(self.latest_sequence+1), client=client)
                        else:
                            self.update_latest_segment(url=url, client=client)
                
                elif self.is_403:
                    for url in self.stream_urls:
                        if self.live_status == 'post_live':
                            self.update_latest_segment(url=self.stream_url.segment(self.latest_sequence+1), client=client)
                        else:
                            self.update_latest_segment(url=url, client=client)
                    
                
                potential_segments_to_download = set(self.segments_retries.keys()) - self.already_downloaded
                
                if self.sequential:
                    sorted_retries = dict(sorted(self.segments_retries.items(), key=lambda item: (item[1]['retries'], item[0])))
                else:
                    """
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
                    """
                    current_time = time.time()
                    priority_items = {}
                    non_priority_items = {}

                    for key, value in self.segments_retries.items():
                        # .get() allows us to provide safe defaults if keys are missing
                        last_retry = value.get('last_retry', 0)
                        ideal_time = value.get('ideal_retry_time', 0)
                        retries = value.get('retries', 0)

                        # Check priority condition
                        if (current_time - last_retry) > ideal_time and retries > 0:
                            priority_items[key] = value
                        else:
                            non_priority_items[key] = value

                    # Sorting logic remains consistent
                    priority_items_sorted = dict(sorted(priority_items.items(), key=lambda item: item[1].get('retries', 0)))
                    non_priority_items_sorted = dict(sorted(non_priority_items.items(), key=lambda item: item[1].get('retries', 0)))

                    sorted_retries = priority_items_sorted | non_priority_items_sorted
                
                if sorted_retries:
                    potential_segments_to_download = sorted_retries.keys()
                    
                """
                if not not_done or len(not_done) < self.max_workers:
                    new_download = set()
                    number_to_add = self.max_workers - len(not_done)

                    for seg_num in potential_segments_to_download:
                        if seg_num not in submitted_segments :                            
                            if seg_num in self.already_downloaded:
                                self.segments_retries.pop(seg_num,None)
                                continue
                            if self.segment_exists(seg_num):
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
                """

                if len(not_done) <= 0 and len(done) > 0:
                    thread_windows_size = 3*thread_windows_size
                else:
                    thread_windows_size = max(thread_windows_size//2, 2*self.max_workers)

                # Add new threads to existing future dictionary, done directly to almost half RAM usage from creating new threads
                for seg_num in potential_segments_to_download:
                    # Have up to 2x max workers of threads submitted
                    if len(future_to_seg) > max(10,2*self.max_workers, thread_windows_size):
                        break
                    if seg_num not in submitted_segments and self.segments_retries.get(seg_num, {}).get('retries', 0) <= self.fragment_retries and time.time() - self.segments_retries.get(seg_num, {}).get('retries', 0) > self.segment_retry_time and seg_num not in self.pending_segments:
                        # Check if segment already exist within database (used to not create more connections). Needs fixing upstream
                        if self.segment_exists(seg_num):
                            self.already_downloaded.add(seg_num)
                            self.segments_retries.pop(seg_num,None)
                            continue
                        future_to_seg.update({
                            executor.submit(self.download_segment, self.stream_urls[i % len(self.stream_urls)].segment(seg_num), seg_num, client, True): seg_num
                        })
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


    def update_latest_segment(self, url=None, client=None):
        from datetime import datetime
        # Overrides base method to handle multiple, expiring URLs
        self.check_kill()
        
        # Remove expired URLs
        self.check_Expiry()  

        if (not self.stream_urls) or (self.expires and time.time() > self.expires):
            self.logger.critical("\033[31mCurrent time is beyond highest expire time and no valid URLs remain, unable to recover\033[0m".format(self.format))
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            format_exp = datetime.fromtimestamp(self.expires).strftime('%Y-%m-%d %H:%M:%S')
            self.commit_batch(self.conn)
            raise TimeoutError("Current time {0} exceeds latest URL expiry time of {1}".format(now, format_exp))         
        
        if url is None:
            url = random.choice(self.stream_urls)
        
        stream_url_info = self.get_Headers(url, client)
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
            self.livestream_coordinator.stats.setdefault(self.type, {})["latest_sequence"] = self.latest_sequence
    """
    def get_Headers(self, url, client=None):
        # Overrides base method to add 401 handling
        if client is None:
            client = httpx.Client(timeout=10, proxy=self.process_proxies_for_httpx(self.proxies), http1=True, http2=self.http2_available(), follow_redirects=True)
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
            
        except httpx.TimeoutException as e:
            self.logger.debug("Timed out updating fragments: {0}".format(e))
            return None
        
        except Exception as e:
            self.logger.exception("\033[31m{0}\033[0m".format(e))
            return None
            """
    """
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
        """
            
    def save_stats(self):
        # Stats files
        with open("{0}.{1}_seg_403s.json".format(self.file_base_name, self.format), 'w', encoding='utf-8') as outfile:
            json.dump(self.count_403s, outfile, indent=4)
        with open("{0}.{1}_usr_ag_403s.json".format(self.file_base_name, self.format), 'w', encoding='utf-8') as outfile:
            json.dump(self.user_agent_403s, outfile, indent=4)
        with open("{0}.{1}_usr_ag_full_403s.json".format(self.file_base_name, self.format), 'w', encoding='utf-8') as outfile:
            json.dump(self.user_agent_full_403s, outfile, indent=4)







