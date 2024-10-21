import yt_dlp
import sqlite3
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import getUrls
import YoutubeURL


            

# Multithreaded function to download new segments with delayed commit after a batch
def download_segments(info_dict, resolution='best', batch_size=10, max_workers=5):
    DownloadStream(info_dict, resolution, batch_size, max_workers).catchup()

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
        
        try:
            uncommitted_inserts = 0
            # Use transaction for saving in chunks to reduce I/O
            self.cursor.execute('BEGIN TRANSACTION')  

            for seg_num in range(0, self.latest_sequence):            
                # Check if download is present within set
                if seg_num not in self.already_downloaded:
                    #Check if segments exists within temp database, add to set if it does
                    if self.segment_exists(cursor=self.cursor, segment_order=seg_num):
                        print("Segment {0} found in sql file".format(seg_num))
                        self.already_downloaded.add(seg_num)
                        continue
                    else:
                        url = "{0}&sq={1}".format(self.stream_url, seg_num)
                        head_seg_num, segment_data = self.download_segment(url, seg_num)
                        
                        if head_seg_num > self.latest_sequence:
                            print("More sements available: {0}, previously {1}".format(head_seg_num,self.latest_sequence))
                            self.latest_sequence = head_seg_num
                        
                        if segment_data is not None:
                            self.insert_single_segment(cursor=self.cursor, segment_order=seg_num, segment_data=segment_data)
                            uncommitted_inserts = uncommitted_inserts + 1

                        if uncommitted_inserts >= self.batch_size:
                            print("Writing segments to file...")
                            self.commit_batch(self.conn)
                            uncommitted_inserts = 0
                            self.cursor.execute('BEGIN TRANSACTION')
                                
            
        finally:
            self.commit_batch(self.conn)
                
            # To be removed once segment refresh is implemented
            self.combine_segments_to_file(self.cursor,"{0}.{1}.ts".format(self.id,self.format))
    
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

    # Function to download a single segment
    def download_segment(self, segment_url, segment_order):
        response = requests.get(segment_url)  # Download segment from URL
        if response.status_code == 200:
            print("Downloaded segment {0} to memory...".format(segment_order))
            
            #return latest header number and segmqnt content
            return int(response.headers.get("X-Head-Seqnum", -1)), response.content  # Return segment order and data
        else:
            return None, None

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