import yt_dlp
import sqlite3
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import getUrls
import YoutubeURL

def get_Headers(url):
    # Send a GET request to a URL
    response = requests.get(url)
    if response.status_code == 200:
        # Print the response headers
        print(json.dumps(dict(response.headers), indent=4))  
        return response.headers
    else:
        print("Error retrieving segment: {0}".format(response.status_code))
        print(json.dumps(dict(response.headers), indent=4))
    

def create_db(id, format=None):
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('{0}.{1}.temp'.format(id,format))
    cursor = conn.cursor()

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
def segment_exists(cursor, segment_order):
    cursor.execute('SELECT 1 FROM segments WHERE id = ?', (segment_order,))
    return cursor.fetchone() is not None

# Function to download a single segment
def download_segment(segment_url, segment_order):
    response = requests.get(segment_url)  # Download segment from URL
    if response.status_code == 200:
        print("Downloaded segment {0} to memory...".format(segment_order))
        
        #return latest header number and segmqnt content
        return int(response.headers.get("X-Head-Seqnum", -1)), response.content  # Return segment order and data
    else:
        return None, segment_order

# Function to insert a single segment without committing
def insert_single_segment(cursor, segment_order, segment_data):
    cursor.execute('''
    INSERT INTO segments (id, segment_data) VALUES (?, ?)
    ''', (segment_order, segment_data))

# Function to commit after a batch of inserts
def commit_batch(conn):
    conn.commit()

# Function to combine segments into a single file
def combine_segments_to_file(cursor, output_file):
    
    print("Writing segments to {0}".format(output_file))
    with open(output_file, 'wb') as f:
        cursor.execute('SELECT segment_data FROM segments ORDER BY id')
        for segment in cursor:  # Cursor iterates over rows one by one
            f.write(segment[0])
            

# Multithreaded function to download new segments with delayed commit after a batch
def download_segments(id, resolution='best', batch_size=10, max_workers=5):
    latest_sequence = -1
    already_downloaded = set()
    uncommitted_inserts = 0
    
    stream_url, format = YoutubeURL.Formats().getFormatURL(getUrls.get_Video_Info(id), resolution, return_format=True)
    
    
    stream_url_info = get_Headers(stream_url)
    if stream_url_info.get("X-Head-Seqnum", -1) is not None:
        latest_sequence = int(stream_url_info.get("X-Head-Seqnum"))
        print("Latest sequence: {0}".format(latest_sequence))
    
    conn, cursor = create_db(id, format)
    
    try:
        # Use transaction for saving in chunks to reduce I/O
        cursor.execute('BEGIN TRANSACTION')  

        for seg_num in range(0, latest_sequence):            
            # Check if download is present within set
            if seg_num not in already_downloaded:
                #Check if segments exists within temp database, add to set if it does
                if segment_exists(cursor=cursor, segment_order=seg_num):
                    print("Segment {0} found in sql file".format(seg_num))
                    already_downloaded.add(seg_num)
                    continue
                else:
                    url = "{0}&sq={1}".format(stream_url, seg_num)
                    head_seg_num, segment_data = download_segment(url, seg_num)
                    
                    if head_seg_num > latest_sequence:
                        print("More sements available: {0}, previously {1}".format(head_seg_num,latest_sequence))
                        latest_sequence = head_seg_num
                    
                    if segment_data is not None:
                        insert_single_segment(cursor=cursor, segment_order=seg_num, segment_data=segment_data)
                        uncommitted_inserts = uncommitted_inserts + 1

                    if uncommitted_inserts >= batch_size:
                        print("Writing segments to file...")
                        commit_batch(conn)
                        uncommitted_inserts = 0
                        cursor.execute('BEGIN TRANSACTION')
                            
        
    finally:
        commit_batch(conn)
            
    combine_segments_to_file(cursor,"{0}.{1}.ts".format(id,format))
    return    


download_segments('jGsi2eAU7MM')