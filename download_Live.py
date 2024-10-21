import yt_dlp
import sqlite3
import requests
import time
import concurrent.futures
import json
import getUrls
import YoutubeURL

import threading

import subprocess
import os            

# Multithreaded function to download new segments with delayed commit after a batch
def download_segments(info_dict, resolution='best', batch_size=10, max_workers=5):
    
    if resolution.lower() != "audio_only":        
        
        file_names = []
        # Create runner function for each download format
        def download_stream(info_dict, resolution, batch_size, max_workers):
            file_name = DownloadStream(info_dict, resolution=resolution, batch_size=batch_size, max_workers=max_workers).catchup()
            file_names.append(file_name)
        # Create threads for both downloads        
        video_thread = threading.Thread(target=download_stream, args=(info_dict, resolution, batch_size, max_workers), daemon=True)
        audio_thread = threading.Thread(target=download_stream, args=(info_dict, "audio_only", batch_size, max_workers), daemon=True)

        # Start both threads
        video_thread.start()
        audio_thread.start()

        # Wait for both threads to finish
        video_thread.join()
        audio_thread.join()

        create_mp4(file_names, info_dict)
        
    else:
        DownloadStream(info_dict, resolution, batch_size, max_workers).catchup()
        
def create_mp4(file_names, info_dict):
    ffmpeg_builder = ['ffmpeg', '-y', 
                      #'-hide_banner', '-nostdin', '-loglevel', 'fatal', '-stats'
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
        print(result.stdout)
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        print(e)
        print(e.stderr)
    except Exception as e:
        print(e)
    #for file in file_names:
    #    os.remove(file)
    

class DownloadStream:
    def __init__(self, info_dict, resolution='best', batch_size=10, max_workers=5):
        self.latest_sequence = -1
        self.already_downloaded = set()
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        self.id = info_dict.get('id')
        
        self.stream_url, self.format = YoutubeURL.Formats().getFormatURL(info_json=info_dict, resolution=resolution, return_format=True)
        
        print("Getting stream headers")
        stream_url_info = self.get_Headers(self.stream_url)
        if stream_url_info.get("X-Head-Seqnum", -1) is not None:
            self.latest_sequence = int(stream_url_info.get("X-Head-Seqnum"))
            print("Latest sequence: {0}".format(self.latest_sequence))
        
        self.create_db(self.id, self.format)
        
        
    def catchup(self):      
        # Get list of segments that don't exist within temp database
        segments_to_download = []
        self.already_downloaded = self.segment_exists_batch()
        print(self.already_downloaded)
        for seg_num in range(0, self.latest_sequence):
            if seg_num not in self.already_downloaded:
                segments_to_download.append(seg_num)
                    
        if len(segments_to_download) <= 0:
            return 
        
        # Use transaction for saving in chunks to reduce I/O
        self.cursor.execute('BEGIN TRANSACTION')
        uncommitted_inserts = 0
        # Multithreading for downloading segments
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_seg = {
                executor.submit(self.download_segment, "{0}&sq={1}".format(self.stream_url, seg_num), seg_num): seg_num
                for seg_num in segments_to_download
            }

            for future in concurrent.futures.as_completed(future_to_seg):
                head_seg_num, segment_data, seg_num = future.result()
                
                if head_seg_num > self.latest_sequence:
                    print("More segments available: {0}, previously {1}".format(head_seg_num, self.latest_sequence))
                    self.latest_sequence = head_seg_num

                if segment_data is not None:
                    # Insert segment data in the main thread (database interaction)
                    self.insert_single_segment(cursor=self.cursor, segment_order=seg_num, segment_data=segment_data)
                    uncommitted_inserts += 1
                    if uncommitted_inserts >= self.batch_size*self.max_workers:
                        print("Writing segments to file...")
                        self.commit_batch(self.conn)
                        uncommitted_inserts = 0
                        self.cursor.execute('BEGIN TRANSACTION')
                                
            
        #finally:
            self.commit_batch(self.conn)
                
            # To be removed once segment refresh is implemented
            file_name = "{0}.{1}.ts".format(self.id,self.format)
            self.combine_segments_to_file(self.cursor,file_name)
            return os.path.abspath(file_name)
    
    def get_Headers(self, url):
        # Send a GET request to a URL
        response = requests.get(url)
        if response.status_code == 200:
            # Print the response headers
            print(json.dumps(dict(response.headers), indent=4))  
            return response.headers
        else:
            print("Error retrieving headers: {0}".format(response.status_code))
            print(json.dumps(dict(response.headers), indent=4))
    

    def create_db(self, id, format=None):
        # Connect to SQLite database (or create it if it doesn't exist)
        self.conn = sqlite3.connect('{0}.{1}.temp'.format(id,format))
        self.cursor = self.conn.cursor()

        # Create the table where id represents the segment order
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS segments (
            id INTEGER PRIMARY KEY, 
            segment_data BLOB
        )
        ''')
        self.conn.commit()
        return self.conn, self.cursor

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
        response = requests.get(segment_url)  # Download segment from URL
        if response.status_code == 200:
            print("Downloaded segment {0} to memory...".format(segment_order))
            
            #return latest header number and segmqnt content
            return int(response.headers.get("X-Head-Seqnum", -1)), response.content, int(segment_order)  # Return segment order and data
        else:
            print("Error downloading segment {0}: {1}".format(segment_order, response.status_code))
            return -1, None, segment_order

    # Function to insert a single segment without committing
    def insert_single_segment(self, cursor, segment_order, segment_data):
        cursor.execute('''
        INSERT INTO segments (id, segment_data) VALUES (?, ?)
        ''', (segment_order, segment_data))

    # Function to commit after a batch of inserts
    def commit_batch(self, conn):
        conn.commit()

    # Function to combine segments into a single file
    def combine_segments_to_file(self, cursor, output_file):
        
        print("Writing segments to {0}".format(output_file))
        with open(output_file, 'wb') as f:
            cursor.execute('SELECT segment_data FROM segments ORDER BY id')
            for segment in cursor:  # Cursor iterates over rows one by one
                f.write(segment[0])