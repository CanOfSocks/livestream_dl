import yt_dlp
import sqlite3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

import time
import concurrent.futures
import json
import getUrls
import YoutubeURL

import subprocess
import os  

import signal

import logging

kill_all = False

logging.basicConfig(
    filename='output.log',   # File where the logs will be stored
    level=logging.INFO,      # Minimum level of messages to log (INFO or higher)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
)

# Create runner function for each download format
def download_stream(info_dict, resolution, batch_size, max_workers):
    try:
        downloader = DownloadStream(info_dict, resolution=resolution, batch_size=batch_size, max_workers=max_workers)
        #file_name = downloader.catchup()
        #time.sleep(0.5)
        print(downloader.stream_url)
        print(downloader.get_Headers("{0}&sq={1}".format(downloader.stream_url, 1)))
        file_name = downloader.live_dl()
        downloader.combine_segments_to_file(file_name)
    finally:
        # Explicitly close connection
        downloader.close_connection()
    return file_name

# Multithreaded function to download new segments with delayed commit after a batch
def download_segments(info_dict, resolution='best', batch_size=10, max_workers=5):
    try:    
        # For use of specificed format. Expects two values, but can work with more
        if isinstance(resolution, tuple) or isinstance(resolution, list) or isinstance(resolution, set):
            if len(resolution) == 1:
                download_stream(info_dict=info_dict, resolution=resolution[0], batch_size=batch_size, max_workers=max_workers)
            else:
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                    file_names = [] 
                    futures = []
                    for format in resolution:
                        new_future = executor.submit(download_stream, info_dict=info_dict, resolution=format, batch_size=batch_size, max_workers=max_workers)
                        futures.append(new_future)
                    
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()  # This will raise an exception if the future failed
                        logging.info("Result of thread: {0}".format(result))
                        print("\033[31m{0}\033[0m".format(result))
                        file_names.append(result)
                    create_mp4(file_names, info_dict)
        elif resolution.lower() != "audio_only":
            file_names = [] 

            # Use ThreadPoolExecutor to run downloads concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                # Submit tasks for both video and audio downloads
                video_future = executor.submit(download_stream, info_dict=info_dict, resolution=resolution, batch_size=batch_size, max_workers=max_workers)
                audio_future = executor.submit(download_stream, info_dict=info_dict, resolution="audio_only", batch_size=batch_size, max_workers=max_workers)
                
                        # Wait for both downloads to finish
                futures = [video_future, audio_future]
                
                #done, not_done = concurrent.futures.wait(futures, timeout=0.1, return_when=concurrent.futures.ALL_COMPLETED)
                
                #while 
                # Continuously check for completion or interruption
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()  # This will raise an exception if the future failed
                    logging.info("result of thread: {0}".format(result))
                    print("\033[31m{0}\033[0m".format(result))
                    file_names.append(result)

            create_mp4(file_names, info_dict)
            
        else:
            download_stream(info_dict=info_dict, resolution=resolution, batch_size=batch_size, max_workers=max_workers)
    except KeyboardInterrupt:
        global kill_all
        kill_all = True
        for future in futures:
            future.cancel()
            executor.shutdown(wait=False)
        
def create_mp4(file_names, info_dict):
    ffmpeg_builder = ['ffmpeg', '-y', 
                      '-hide_banner', '-nostdin', '-loglevel', 'fatal', '-stats'
                      ]
    
    # Add input files
    for file in file_names:
        input = ['-i', file]
        ffmpeg_builder.extend(input)
    
    # Add faststart
    ffmpeg_builder.extend(['-movflags', 'faststart'])
    
    # Add mappings
    for i in range(0, len(file_names)):
        input = ['-map', str(i)]
        ffmpeg_builder.extend(input)
        
    #Add Copy codec
    ffmpeg_builder.extend(['-c', 'copy'])
        
    # Add metadata
    ffmpeg_builder.extend(['-metadata', "DATE={0}".format(info_dict.get("upload_date"))])
    ffmpeg_builder.extend(['-metadata', "COMMENT={0}\n{1}".format(info_dict.get("original_url"), info_dict.get("description"))])
    ffmpeg_builder.extend(['-metadata', "TITLE={0}".format(info_dict.get("fulltitle"))])
    ffmpeg_builder.extend(['-metadata', "ARTIST={0}".format(info_dict.get("channel"))])
    
    options = {
        'skip_download': True,
        
        # Template, needs to have option
        'outtmpl': '%(fulltitle)s (%(id)s)',     
    }
    with yt_dlp.YoutubeDL(options) as ydl:
        outputFile = str(ydl.prepare_filename(info_dict))
    
    if not outputFile.endswith('.mp4'):
        outputFile = outputFile + '.mp4'  
        
    ffmpeg_builder.append(os.path.abspath(outputFile))
    
    
    with open("{0}.ffmpeg.txt".format(info_dict.get('id')), 'w', encoding='utf-8') as f:
        f.write(" ".join(ffmpeg_builder))   
        
    print("Executing ffmpeg...")
    try:
        result = subprocess.run(ffmpeg_builder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
    except subprocess.CalledProcessError as e:
        print(e)
        print(e.stderr)

    #for file in file_names:
    #    os.remove(file)
    

class DownloadStream:
    def __init__(self, info_dict, resolution='best', batch_size=10, max_workers=5, fragment_retries=10, force_merge=False, database_in_memory=False):        
        
        self.latest_sequence = -1
        self.already_downloaded = set()
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        self.id = info_dict.get('id')
        self.live_status = info_dict.get('live_status')
        
        self.stream_url, self.format = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution, return_format=True) 
        
        self.database_in_memory = database_in_memory
        
        self.file_name = "{0}.{1}.ts".format(self.id,self.format)     
        if self.database_in_memory:
            self.temp_file = ':memory:'
        else:
            self.temp_file = '{0}.{1}.temp'.format(self.id,self.format)
        
        self.retry_strategy = Retry(
            total=fragment_retries,  # maximum number of retries
            backoff_factor=1, 
            status_forcelist=[204, 400, 401, 403, 404, 429, 500, 502, 503, 504],  # the HTTP status codes to retry on
            
        )
        
        self.is_403 = False
        self.estimated_segment_duration = 0
        
        self.update_latest_segment()
        self.url_checked = time.time()
        # Force merge needs testing
        if force_merge and os.path.exists(self.temp_file) and not self.database_in_memory:
            self.conn, self.cursor = self.create_connection(self.temp_file)
            self.combine_segments_to_file(self.file_name, cursor=self.cursor)
            return os.path.abspath(self.file_name)  
        else:
            self.conn, self.cursor = self.create_db(self.temp_file)
        
        
    def catchup(self):      
        # Get list of segments that don't exist within temp database
        
        self.already_downloaded = self.segment_exists_batch()
        
        segments_to_download = set(range(0, self.latest_sequence)) - self.already_downloaded
                    
        if len(segments_to_download) <= 0:
            return 
        #try:
        # Use transaction for saving in chunks to reduce I/O
        self.cursor.execute('BEGIN TRANSACTION')
        uncommitted_inserts = 0
        # Multithreading for downloading segments
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_seg = {
                executor.submit(self.download_segment, "{0}&sq={1}".format(self.stream_url, seg_num), seg_num): seg_num
                for seg_num in segments_to_download
            }
            
            while True:
                done, not_done = concurrent.futures.wait(future_to_seg, timeout=5, return_when=concurrent.futures.ALL_COMPLETED)
                for future in done:
                    head_seg_num, segment_data, seg_num = future.result()
                    
                    self.refresh_Check()
                    
                    if head_seg_num > self.latest_sequence:
                        print("More segments available: {0}, previously {1}".format(head_seg_num, self.latest_sequence))
                        self.latest_sequence = head_seg_num

                    if segment_data is not None:
                        # Insert segment data in the main thread (database interaction)
                        self.insert_single_segment(cursor=self.cursor, segment_order=seg_num, segment_data=segment_data)
                        uncommitted_inserts += 1
                        if uncommitted_inserts >= max(self.batch_size,len(done)):
                            print("Writing segments to file...")
                            self.commit_batch(self.conn)
                            uncommitted_inserts = 0
                            self.cursor.execute('BEGIN TRANSACTION')
                            
                    # Remove future from dictionary to free memory
                    del future_to_seg[future]                
                    #print("Remaining futures ({1}): {0}".format(len(future_to_seg), self.format))
                # Loop until all threads have finished
                if len(not_done) == 0:
                    break
            
        #finally:
        
            self.commit_batch(self.conn)
        self.commit_batch(self.conn)
        print("\033[31mCatchup for {0} completed\033[0m".format(self.format))
        return os.path.abspath(self.file_name)
    
    def refresh_Check(self):    
        #print("Refresh check ({0})".format(self.format))        
        if time.time() - self.url_checked >= 3600.0 or self.is_403:
            print("Refreshing URL for {0}".format(self.format))
            info_dict, live_status = getUrls.get_Video_Info(self.id, wait=False)
            stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=self.format, return_format=False) 
            if stream_url is not None:
                self.stream_url = stream_url
            if live_status is not None:
                self.live_status = live_status
            
            self.url_checked = time.time()
                
    def live_dl(self):
        
        print("\033[31mStarting download of live fragments ({0})\033[0m".format(self.format))
        self.already_downloaded = self.segment_exists_batch()
        wait = 0   
        self.cursor.execute('BEGIN TRANSACTION')
        uncommitted_inserts = 0     
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            submitted_segments = set()
            future_to_seg = {}
            
            # Trackers for optimistic segment downloads 
            optimistic = True
            optimistic_seg = 0
            while True:               
                                               
                segments_to_download = set(range(0, self.latest_sequence)) - self.already_downloaded    
                
                self.refresh_Check()
                
                # If segments remain to download, don't bother updating and wait for segment download to refresh values.
                if len(segments_to_download) <= 0:
                    
                    # Only attempt to grab optimistic segment once to ensure it does not cause a loop at the end of a stream
                    if optimistic and optimistic_seg <= self.latest_sequence and (self.latest_sequence+1) not in submitted_segments:
                        optimistic_seg = (self.latest_sequence+1)
                        time.sleep(self.estimated_segment_duration)
                        
                        print("Adding segment {1} optimistically ({0})".format(self.format, optimistic_seg))
                        segments_to_download.add(optimistic_seg)
                        
                    # If optimistic grab is not successful, revert back to using headers from base stream URL
                    else:
                        print("Checking for more segments available for {0}".format(self.format))
                        self.update_latest_segment()
                        segments_to_download = set(range(0, self.latest_sequence)) - self.already_downloaded                              
                        
                # If update has found no segments, wait.                                
                if len(segments_to_download) <= 0:                    
                    wait += 1
                    print("No new fragments available for {0}, attempted {1} times...".format(self.format, wait))
                    
                    # If waited for new fragments hits 20 loops, assume stream is offline
                    if wait > 20:
                        print("Wait time for new fragment exceeded, running final check for missed fragments...")
                        self.catchup()
                        print("Collected all available fragments, exitting...")
                        break    
                    # If over 10 wait loops have been executed, get page for new URL and update status if necessary
                    elif wait > 10:
                        print("No new fragments found... Getting new url")
                        info_dict, live_status = getUrls.get_Video_Info(self.id, wait=False)
                        
                        # If status of downloader is not live, assume stream has ended
                        if self.live_status != 'is_live':
                            print("Livestream has ended, collecting any missing with existing url")
                            #self.catchup()
                            break
                        
                        # If live has changed, use new URL to get any fragments that may be missing
                        elif self.live_status == 'is_live' and live_status != 'is_live':
                            print("Stream has finished ({0}), getting any remaining segments with new url if available".format(live_status))
                            self.live_status = live_status
                            stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=self.format, return_format=False) 
                            if stream_url is not None:
                                self.stream_url = stream_url  
                            #self.catchup()
                            break
                        
                        # If livestream is still live, use new url
                        elif live_status == 'is_live':
                            print("Updating url to new url")
                            self.stream_url = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=self.format, return_format=False)
                            continue   
                    
                    time.sleep(5)
                    continue
                else:
                    wait = 0
                    
                # Check if segments already exist within database (used to not create more connections). Needs fixing upstream
                remove_set = set()
                for seg_num in segments_to_download:
                    if self.segment_exists(self.cursor, seg_num):
                        self.already_downloaded.add(seg_num)
                        remove_set.add(seg_num)
                segments_to_download = segments_to_download - remove_set

                # Add new threads to existing future dictionary, done directly to almost half RAM usage from creating new threads
                future_to_seg.update(
                    {
                        executor.submit(self.download_segment, "{0}&sq={1}".format(self.stream_url, seg_num), seg_num): seg_num
                        for seg_num in segments_to_download
                        if seg_num not in submitted_segments and not submitted_segments.add(seg_num)
                    }
                )

                # Merge the new futures into the existing `future_to_seg`, remove existing to free up space

                
                # Process completed segment downloads, wait up to 5 seconds for segments to complete before next loop
                done, not_done = concurrent.futures.wait(future_to_seg, timeout=5, return_when=concurrent.futures.ALL_COMPLETED)  # need to fully determine if timeout or ALL_COMPLETED takes priority             
                
                for future in done:
                    head_seg_num, segment_data, seg_num, status, headers = future.result()
                    
                    # Remove from submitted segments in case it neeeds to be regrabbed
                    submitted_segments.remove(seg_num)
                    
                    # If successful in downloading segments optimistically, continue doing so
                    if seg_num > self.latest_sequence and status != 200:
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
                    #print("Remaining futures ({2}) for live: {0}, finished {1}".format(len(not_done), len(done), self.format))
                    
                    # Remove completed threads to free RAM
                    del future_to_seg[future]
                # IDK, it seems to do better if something exists outside the for loop
                pass
            self.commit_batch(self.conn)
        self.commit_batch(self.conn)
        return os.path.abspath(self.file_name)    
           
    def get_live_segment(self, seg_num):
        # Create new connection and cursor to check sqlite database within thread
        single_conn, single_cursor = self.create_connection(self.temp_file)
        if self.segment_exists(cursor=single_cursor, segment_order=seg_num):
            self.already_downloaded.add(seg_num)
            return -1, None, seg_num
        else:
            return self.download_segment("{0}&sq={1}".format(self.stream_url, seg_num), seg_num)
    
    def update_latest_segment(self):
        # Kill if keyboard interrupt is detected
        if kill_all:
            raise KeyboardInterrupt
        stream_url_info = self.get_Headers(self.stream_url)
        if stream_url_info is not None and stream_url_info.get("X-Head-Seqnum", None) is not None:
            self.latest_sequence = int(stream_url_info.get("X-Head-Seqnum"))
            print("Latest sequence: {0}".format(self.latest_sequence))
    
    def get_Headers(self, url):
        try:
            # Send a GET request to a URL
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
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
        except requests.exceptions.RetryError as e:
            logging.info("Exceeded timeouts updating fragments: {0}".format(e))
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

    # Function to download a single segment
    def download_segment(self, segment_url, segment_order):
        # Kill if keyboard interrupt is detected
        if kill_all:
            raise KeyboardInterrupt
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
            elif response.status_code == 204:
                print("Segment {0} has no data")
                self.is_403 = False
                return -1, bytes(), segment_order, response.status_code, response.headers
            elif response.status_code == 403:
                print("Received 403 error, marking for URL refresh...")
                self.is_403 = True
                return -1, None, segment_order, response.status_code, response.headers
            else:
                print("Error downloading segment {0}: {1}".format(segment_order, response.status_code))
                return -1, None, segment_order, response.status_code
        except requests.exceptions.Timeout as e:
            logging.info("Fragment timeout {1}: {0}".format(e, segment_order))
            print(e)
            return -1, None, segment_order, response.status_code, response.headers
        except requests.exceptions.RetryError as e:
            if response.status_code == 204:
                self.is_403 = False
                return -1, bytes(), segment_order, response.status_code, response.headers
            logging.info("Timed out updating fragments: {0}".format(e))
            print(e)
            return -1, None, segment_order, response.status_code, response.headers

    # Function to insert a single segment without committing
    def insert_single_segment(self, cursor, segment_order, segment_data):
        cursor.execute('''
        INSERT OR IGNORE INTO segments (id, segment_data) VALUES (?, ?)
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