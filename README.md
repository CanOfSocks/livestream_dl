# livestream_dl
Garbage youtube livestream downloader combining the principles of [ytarchive](https://github.com/Kethsar/ytarchive "Kethsar/ytarchive") and [ytarchiverawgo]([https://github.com/Kethsar/ytarchive](https://github.com/HoloArchivists/ytarchive-raw-go) "HoloArchivists/ytarchive-raw-go"). This focuses on using yt-dlp for more frequent updates for stream information extraction to handle changes YouTube implements.
This project aims to combine the features of live recording and stream recovery when a stream becomes unavailable.

# Requirements
- [python](https://www.python.org/) 3.12.5+ (May work on earlier version, but a dependency requires at least 3.9+)
- [ffmpeg](https://ffmpeg.org/)
- [yt-dlp](https://github.com/yt-dlp/yt-dlp)
- Packages within [requirements.txt](https://github.com/CanOfSocks/livestream_dl/blob/main/requirements.txt)

## Modification of yt-dlp
For the downloader to work, the [YouTube extractor from yt-dlp](https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/extractor/youtube.py) must be modified to save formats that would usually be discarded.

To do this comment/remove the [following lines](https://github.com/yt-dlp/yt-dlp/blob/cfa76f35d25eaf993437df8b833befbbf9255331/yt_dlp/extractor/youtube.py#L4031-L4032):
```python
            if fmt.get('targetDurationSec'):
                continue
```

As of December 2024, the following sed command works on Linux, ***be aware the location of the file may change and future versions of yt-dlp may change the extractor logic***:
```bash
sed -i '/if fmt.get('\'targetDurationSec\''):$/,/    continue$/s/^/#/' "$(pip show yt-dlp | grep Location | awk '{print $2}')/yt_dlp/extractor/youtube.py"
```

# Usage
To use, execute `runner.py` with python with any additional options.
```
usage: runner.py [-h] [--resolution RESOLUTION] [--video-format VIDEO_FORMAT] [--audio-format AUDIO_FORMAT] [--threads THREADS] [--batch-size BATCH_SIZE] [--segment-retries SEGMENT_RETRIES] [--no-merge] [--merge] [--cookies COOKIES] [--output OUTPUT]
                 [--temp-folder TEMP_FOLDER] [--write-thumbnail] [--embed-thumbnail] [--write-info-json] [--write-description] [--keep-temp-files] [--keep-ts-files] [--live-chat] [--keep-database-file] [--recovery] [--database-in-memory] [--direct-to-ts]        
                 [--wait-for-video [WAIT_FOR_VIDEO ...]] [--json-file JSON_FILE]
                 [ID]

Download YouTube livestreams (https://github.com/CanOfSocks/livestream_dl)

positional arguments:
  ID                    The video URL or ID

options:
  -h, --help            show this help message and exit
  --resolution RESOLUTION
                        Desired resolution. Can be best, audio_only or specific resolution. Possible values best, 2160p60, 2160p, 1440p60, 1440p, 1080p60, premium, 1080p, 720p60, 720p, 480p, 360p, 240p, 144p. '*' can be used as a wildcard e.g. 1080* for
                        1080p60 or 1080p. Default: best
  --video-format VIDEO_FORMAT
                        Specify specific video format. Resolution will be ignored if used
  --audio-format AUDIO_FORMAT
                        Specify specific audio format. Resolution will be ignored if used
  --threads THREADS     Number of download threads per format. This will be 2x for an video and audio download. Default: 1
  --batch-size BATCH_SIZE
                        Number of segments before the temporary database is committed to disk. This is useful for reducing disk access instances. Default: 5
  --segment-retries SEGMENT_RETRIES
                        Number of times to retry grabbing a segment. Default: 10
  --no-merge            Don't merge video using ffmpeg
  --merge               Merge video using ffmpeg, overrides --no-merge
  --cookies COOKIES     Path to cookies file
  --output OUTPUT       Path/file name for output files. Supports yt-dlp output formatting
  --temp-folder TEMP_FOLDER
                        Path for temporary files. Supports yt-dlp output formatting
  --write-thumbnail     Write thumbnail to file
  --embed-thumbnail     Embed thumbnail into final file. Ignored if --no-merge is used
  --write-info-json     Write info.json to file
  --write-description   Write description to file
  --keep-temp-files     Keep all temp files i.e. database and/or ts files
  --keep-ts-files       Keep all ts files
  --live-chat           Get Live chat
  --keep-database-file  Keep database file. If using with --direct-to-ts, this keeps the state file
  --recovery            Puts downloader into stream recovery mode
  --database-in-memory  Keep stream segments database in memory. Requires a lot of RAM (Not recommended)
  --direct-to-ts        Write directly to ts file instead of database. May use more RAM if a segment is slow to download. This overwrites most database options
  --wait-for-video [WAIT_FOR_VIDEO ...]
                        (min, max) Minimum and maximum interval to wait for a video
  --json-file JSON_FILE
                        Path to existing yt-dlp info.json file. Overrides ID and skips retrieving URLs
```

# Downloader methods
There are two download methods available for livestream downloading, using a SQLite database and writing directly to a ts file. For stream recovery, only the SQLite option is available. Merging to a video or audio file (.mp4, .mkv, .ogg etc) will require an extra write of information. The default method is the SQLite method.

## Direct writing to a .ts file
Writing directly to a ts file helps reduce the number of writes to a disk during the recording as it is only necessary to write to the final ts file. This may reduce disk wear over a long period of time of downloading many streams.
As segments are downloaded, the sequence number of the segment is used to decide which segment will be appended to the ts file next. If the "next" segment is not available to be written to the disk, other segments will be stored in RAM until the respective previous segment has been written to the disk. This performs in a similar way to [ytarchive](https://github.com/Kethsar/ytarchive "Kethsar/ytarchive").
To reduce file opening and closing actions on the file system, if multiple sequential segments can be written at once, they will be written into the file at once.
A state file is saved each time segments are written to the disk, saving the latest segment and size of the file, so the downloader doesn't need to re-download segments should the downloading session stop for any reason.
Segment downloads are not guaranteed to finish in a sequential order if more than 1 thread is used. If a segment is particularly slow at downloading, this may increase ram usage significantly while subsequent segments are saved in RAM.

For regular livestream recording that doesn't experience frequent segment download errors, using the direct writing method will work well.

## SQLite
The SQLite downloader method is used to improve handling of non-sequential successful segment downloads. This works by creating and using a basic table of the segment number as an ID and a blob to store the segment data. Once all segments are downloaded, a query is executed to sort all of the downloaded segments into the correct order and saved to a .ts file. This helps significantly to manage downloaded segments when failures occur often, such as unavailable stream recovery. By writing to the database and a final .ts file, this will require 2 full writes of the downloaded data, which may increase wear on flash-based systems. For most users, this increased wear will not be significant in the long-term.
This improves over saving individual segment files like [ytarchiverawgo]([https://github.com/Kethsar/ytarchive](https://github.com/HoloArchivists/ytarchive-raw-go) "HoloArchivists/ytarchive-raw-go") as all the downloaded segments are encapsilated into a single file, making it easier for the file system to handle.
In the SQLite method, the existence of a downloaded segment is checked before it is downloaded. This allows failed segments to be "looped back to" later without causing other slowdowns, and ensures some information is saved for a segment (even if it is empty, as is the case sometimes).


# Known Issues
## Concurrent futures
The downloader functions use the [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html) module to make managing the thread pool and handling thread results easier. This module allows for easy submission of new threads and controlling how many active threads are active at once. However, once threads have been submitted and/or started, it becomes difficult to control the execution of threads until they have finished. While efforts have been made to make stopping threads easier, the nature of concurrent futures may cause delays in the graceful stopping of the downloader (such as a keyboard interrupt), especially if a thread has gotten stuck at some point.

# Issues and contibuting
If you encounter replicatable issues with the downloader, please submit an issue.

If you would like to contribute, please submit a pull request.

# To-Do
While some components have been marked as added, testing of full functionalility may be required
- [ ] Feature parity with all relevant options of [ytarchive](https://github.com/Kethsar/ytarchive "Kethsar/ytarchive") and [ytarchive-raw-go]([https://github.com/Kethsar/ytarchive](https://github.com/HoloArchivists/ytarchive-raw-go) "HoloArchivists/ytarchive-raw-go")
- [ ] Implement proper log levels instead of printing all information to console
- [ ] Fall to stream recovery if stream goes private during recording
- [ ] Overhaul file tracking and management to be more robust
- [ ] Improve ffmpeg command execution
- [ ] Implement proxy/IP pool options - This will need to be implmented with contributions as I have no way of testing these
