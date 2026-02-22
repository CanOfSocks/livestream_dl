
# livestream_dl

A robust YouTube livestream downloader that combines the reliability of [yt-dlp](https://github.com/yt-dlp/yt-dlp) with the fragment-based downloading principles of [ytarchive](https://github.com/Kethsar/ytarchive).

This tool focuses on using `yt-dlp` for stream information extraction, ensuring compatibility with YouTube's changing infrastructure. It is designed to handle live recording with the ability to recover streams if they become unavailable (e.g., go private) during the download process.

## Key Features

* **Robust Downloading:** Uses `yt-dlp` for extraction, minimizing breakage from YouTube updates.
* **Stream Recovery:** capable of recovering streams that go private during recording (requires specific configuration).
* **Dual Write Modes:** Supports both direct `.ts` file writing (low disk IO) and SQLite-backed downloading (safer for unstable connections).
* **Live Chat:** Integrated support for downloading live chat (via `yt-dlp` or `chat-downloader`) and bundling it into a ZIP file.
* **Channel Monitoring:** Automated monitoring of channels for upcoming or live streams.
* **Protocol Fallbacks:** Optional support for DASH and HLS (m3u8) protocols to avoid modifying `yt-dlp` source code.

---

## Requirements

* [Python](https://www.python.org/) 3.12+ (Developed on 3.13)
* [FFmpeg](https://ffmpeg.org/) (Required for merging video/audio)
* [yt-dlp](https://github.com/yt-dlp/yt-dlp) (Must be installed via pip)
* [deno](https://deno.com/) Required for yt-dlp
* Dependencies listed in `requirements.txt`

### Optional
* [chat-downloader](https://github.com/xenova/chat-downloader) - For robust chat downloading with resume capabilities.

## Installation
### Clone Repository (**Recommended**)
1.  Clone the repository.
2.  Install dependencies:
    ```bash
    pip install -U -r requirements.txt
    ```
3.  Ensure `ffmpeg` is in your system PATH.
4.  Ensure `deno` is in your system PATH.
5.  Refer to [Usage](https://github.com/CanOfSocks/livestream_dl#usage) for options.

### Executable (.exe) file (Alpha)
1.  Download latest .exe from the [releases](https://github.com/CanOfSocks/livestream_dl/releases) tab.
2.  Ensure `ffmpeg` is in your system PATH or the "current" folder.
3.  Ensure `deno` is in your system PATH or the "current" folder.
4.  Launch with `livestream_dl.exe` (replacing `python runner.py` at [Usage](https://github.com/CanOfSocks/livestream_dl#usage).

### Docker (Alpha)
Run a premade container([ghcr.io/canofsocks/livestream_dl:latest](ghcr.io/canofsocks/livestream_dl:latest)). Includes [FFmpeg](https://ffmpeg.org/) and [deno](https://deno.com/). Example usage:
```bash
docker run -it -v "${PWD}/downloads:/app/downloads" ghcr.io/canofsocks/livestream_dl:latest python /app/runner.py --threads 4 --dash --m3u8 --write-thumbnail --embed-thumbnail --wait-for-video "60:600" --clean-info-json --remove-ip-from-json --live-chat --resolution "best" --write-info-json --log-level "INFO" -- "[VIDEO_ID]"
```
Refer to [Usage](https://github.com/CanOfSocks/livestream_dl#usage) for options.

---

## Important: Modification of yt-dlp (No longer required with yt-dlp releases after 2026.2.21

**Note:** This step is required **only** if you want to use the default adaptive stream URLs which support private stream recovery. If you prefer not to modify files, you can use the `--dash` or `--m3u8` flags (see Options below), though these have limitations regarding recovery.

To enable adaptive stream URLs that allow for private stream recovery, the `yt-dlp` youtube extractor must be modified to save formats that are usually discarded.

1.  Find the location of `yt-dlp`:
    ```bash
    pip show yt-dlp
    ```
2.  Open `yt_dlp/extractor/youtube/_video.py`.
3.  a) Replace `if fmt_stream.get('targetDurationSec'):` with `if fmt_stream.get('targetDurationSec') and not 'live_adaptive' in format_types:` (experimental patch to hopefully push to core yt-dlp), or
    b) Comment out or remove the following lines (approx line 3467):
    ```python
    if fmt_stream.get('targetDurationSec'):
        continue
    ```

**Linux `sed` command (As of Nov 2025):**
```bash
sed -i "/if[[:space:]]\+fmt_stream\.get('targetDurationSec'):/,/^[[:space:]]*continue/s/^[[:space:]]*/&#/" "$(pip show yt-dlp | awk '/Location/ {print $2}')/yt_dlp/extractor/youtube/_video.py"

```

---

## Usage

Execute `runner.py` with Python.

```bash
python runner.py [OPTIONS] [VIDEO_URL_OR_ID]

```

### Examples

**Basic download:**

```bash
python runner.py --threads 4 --dash --m3u8 --write-thumbnail --embed-thumbnail --wait-for-video "60:600" --clean-info-json --remove-ip-from-json --live-chat --resolution "best" --write-info-json --log-level "INFO" -- "[VIDEO_ID]"

```

**Download with live chat and embed thumbnail:**

```bash
python runner.py --live-chat --threads 4 --embed-thumbnail VIDEO_ID

```

**Monitor a channel for streams:**

```bash
python runner.py --monitor-channel --threads 4 --dash --m3u8 --wait-for-video 60 CHANNEL_ID

```

---

## Command Line Options

### General Configuration

| Option | Default | Description |
| --- | --- | --- |
| `ID` | `None` | The video URL or ID (Positional argument). |
| `--help`, `-h` | N/A | Show the help message and exit. |
| `--cookies` | `None` | Path to a Netscape-formatted cookies file (required for age-gated or members-only content). |
| `--temp-folder` | `None` | Path for temporary files (database, segments). Supports `yt-dlp` output formatting. |
| `--output` | `%(fulltitle)s (%(id)s)` | Path/filename for output files. Supports [yt-dlp output formatting](https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#output-template). A good example is: `"[%(release_date,upload_date)s] %(fulltitle).120B [%(channel)s] (%(id)s)"` |
| `--ext` | `None` | Force the extension of the final video file (e.g., `.mp4`, `.mkv`). |
| `--json-file` | `None` | Path to an existing `yt-dlp` info.json file. Overrides `ID` and skips retrieving URLs from the web. |
| `--write-ffmpeg-command` | `False` | Writes the final FFmpeg command used to merge the file to a `.txt` file for debugging or manual merging. |

### Download & Formatting

| Option | Default | Description |
| --- | --- | --- |
| `--resolution` | Input Prompt | Desired resolution. Can be `bv+ba/best`, `best`, or a custom [yt-dlp format filter](https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#format-selection).  <br>-A recommended value is `bv+ba/best`.  <br>-If unspecified, a prompt will appear.|
| `--custom-sort` | `None` | Custom sorting algorithm for formats based on [yt-dlp sorting syntax](https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#sorting-formats). |
| `--threads` | `2` | Number of download threads *per format*. Total threads = this value × 2 (for video+audio). |
| `--batch-size` | `5` | Number of segments downloaded before the temporary database is committed to disk (reduces disk IO). |
| `--segment-retries` | `10` | Number of times to retry downloading a specific segment before failing. |
| `--dash` | `False` | Use DASH URLs as fallback. Does **not** require `yt-dlp` modification but prevents stream recovery if DASH URLs are used. |
| `--m3u8` | `False` | Use HLS (m3u8) URLs as fallback. Combined audio/video streams (halves requests). Does **not** require `yt-dlp` modification but prevents recovery if m3u8 URLs are used. |
| `--force-m3u8` | `False` | Forces the use of m3u8 stream URLs. |
| `--wait-for-video` | `None` | Wait time (int) or range (`min:max`) to wait for a video to start or become available. Refer to [yt-dlp's documentation](https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#general-options) |
| `--ytdlp-options` | `None` | JSON string of additional `yt-dlp` options (overwrites conflicts). E.g `'{"extractor_args":{"youtubepot-bgutilhttp":{"base_url":["http://bgutil-provider:4416"]}}}'` to use Potoken retrieval. Refer to full options for more examples. |

### Processing & Output

| Option | Default | Description |
| --- | --- | --- |
| `--no-merge` | `False` | Do not merge video and audio using FFmpeg after download. |
| `--merge` | `False` | Force merging video using FFmpeg (overrides `--no-merge`). |
| `--write-thumbnail` | `False` | Save the video thumbnail to a separate file. |
| `--embed-thumbnail` | `False` | Embed the thumbnail into the final video file (ignored if `--no-merge` is active). |
| `--write-info-json` | `False` | Write the video metadata to an `info.json` file. |
| `--write-description` | `False` | Write the video description to a separate text file. |
| `--keep-temp-files` | `False` | Keep all temporary files (database and TS files) after finishing. |
| `--keep-ts-files` | `False` | Keep the intermediate TS files but delete the database. |
| `--live-chat` | `False` | Download live chat (requires `yt-dlp` or `chat-downloader`). |
| `--stop-chat-when-done` | `300` | Max seconds to wait for chat download to finish after stream ends. |
| `--disable-graceful-shutdown` | `False` | Disable graceful shutdown for downloader. Useful for testing when you don't want merging/muxing to be triggered on a keyboard interrupt. |

### Database & Storage Modes

| Option | Default | Description |
| --- | --- | --- |
| `--database-in-memory` | `False` | Keep the segment database in RAM. High memory usage; not recommended for long streams. |
| `--direct-to-ts` | `False` | Write directly to a `.ts` file instead of using a SQLite database. See "Downloader methods" below. |
| `--keep-database-file` | `False` | Keep the database file after completion. If using `--direct-to-ts`, keeps the state file. |

### Recovery & Resilience

| Option | Default | Description |
| --- | --- | --- |
| `--recovery` | `False` | Puts the downloader directly into stream recovery mode. |
| `--force-recover-merge` | `False` | Forces merging to the final file even if not all segments were successfully recovered. |
| `--recovery-failure-tolerance` | `0` | Max number of fragments allowed to fail during recovery without throwing a final error. |
| `--wait-limit` | `1800` | Max wait intervals (~10s each) for new segments after a download has started. If `0`, waits until status changes to `post_live` or `was_live`. Defaults to ~5 hours to avoid URL expiry errors. |

### Network & Logging

| Option | Default | Description |
| --- | --- | --- |
| `--proxy` | `None` | Proxy URL (string) or JSON string for multiple methods. First proxy used for `yt-dlp`/chat. |
| `--ipv4` | `False` | Force IPv4 only. |
| `--ipv6` | `False` | Force IPv6 only. |
| `--log-level` | `INFO` | Logging level: `DEBUG`, `VERBOSE`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. `Verbose` logging is a custom level that includes the `INFO` logs of `yt-dlp`. |
| `--no-console` | `False` | Disable printing log messages to the console. |
| `--log-file` | `None` | Path to a file where log messages will be saved. |
| `--stats-as-json` | `False` | Prints download statistics as a JSON string (bypasses log level). |
| `--new-line` | `False` | Ensures console messages always print to a new line (useful for some terminals). |
| `--redact-ips` | `False` | Redact IP addresses from logs. May be imperfect, so check logs if necessary. |

### Channel Monitoring

| Option | Default | Description |
| --- | --- | --- |
| `--monitor-channel` | `False` | Enables channel monitoring mode. Use the Channel ID in the `ID` argument. This can be found from the "Copy Channel ID" button:    <img width="300" alt="image" src="https://github.com/user-attachments/assets/9b4f3b2b-5947-45ff-8ca7-49126442fb41" />    <img width=300 alt="image" src="https://github.com/user-attachments/assets/64801bfe-3e59-4570-8953-b4c7742e1f6c" /> |
| `--members-only` | `False` | Monitor the 'Members Only' playlist instead of public streams. Requires cookies. |
| `--upcoming-lookahead` | `24` | Maximum time (in hours) to look ahead for upcoming videos to schedule. |
| `--playlist-items` | `50` | Maximum number of playlist items to check when monitoring. |

### Metadata Privacy

| Option | Default | Description |
| --- | --- | --- |
| `--remove-ip-from-json` | `False` | Replaces IP addresses in `info.json` with `0.0.0.0`. |
| `--clean-urls` | `False` | Removes potentially identifiable stream URLs from `info.json`. |
| `--clean-info-json` | `False` | Enables `yt-dlp`'s internal `clean-info-json` option. |

### Full options

```

usage: runner.py [-h] [--resolution RESOLUTION] [--custom-sort CUSTOM_SORT] [--threads THREADS] [--batch-size BATCH_SIZE] [--segment-retries SEGMENT_RETRIES] [--no-merge] [--merge] [--cookies COOKIES] [--output OUTPUT] [--ext EXT] [--temp-folder TEMP_FOLDER] [--write-thumbnail]
                 [--embed-thumbnail] [--write-info-json] [--write-description] [--keep-temp-files] [--keep-ts-files] [--live-chat] [--keep-database-file] [--recovery] [--force-recover-merge] [--recovery-failure-tolerance RECOVERY_FAILURE_TOLERANCE] [--wait-limit WAIT_LIMIT]       
                 [--database-in-memory] [--direct-to-ts] [--wait-for-video WAIT_FOR_VIDEO] [--json-file JSON_FILE] [--remove-ip-from-json] [--clean-urls] [--clean-info-json] [--log-level {DEBUG,VERBOSE,INFO,WARNING,ERROR,CRITICAL}] [--no-console] [--log-file LOG_FILE]
                 [--write-ffmpeg-command] [--stats-as-json] [--ytdlp-options YTDLP_OPTIONS] [--ytdlp-log-level {DEBUG,VERBOSE,INFO,WARNING,ERROR,CRITICAL}] [--dash] [--m3u8] [--force-m3u8] [--proxy PROXY] [--ipv4 | --ipv6] [--stop-chat-when-done STOP_CHAT_WHEN_DONE] [--new-line]  
                 [--monitor-channel] [--members-only] [--upcoming-lookahead UPCOMING_LOOKAHEAD] [--playlist-items PLAYLIST_ITEMS]
                 [ID]

Download YouTube livestreams (https://github.com/CanOfSocks/livestream_dl)

positional arguments:
  ID                    The video URL or ID (type: str)

options:
  -h, --help            show this help message and exit
  --resolution RESOLUTION
                        Desired resolution. Based off yt-dlp's format selection: https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#format-selection. A value of "bv+ba/best" is recommended for most people. (type: str)
  --custom-sort CUSTOM_SORT
                        Custom sorting algorithm for formats based off yt-dlp's format sorting: https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#sorting-formats (type: str)
  --threads THREADS     Number of download threads per format. This will be 2x for an video and audio download. (type: int) (default: 2)
  --batch-size BATCH_SIZE
                        Number of segments before the temporary database is committed to disk. This is useful for reducing disk access instances. (type: int) (default: 5)
  --segment-retries SEGMENT_RETRIES
                        Number of times to retry grabbing a segment. (type: int) (default: 10)
  --no-merge            Don't merge video using ffmpeg (default: True)
  --merge               Merge video using ffmpeg, overrides --no-merge (default: False)
  --cookies COOKIES     Path to cookies file (type: str)
  --output OUTPUT       Path/file name for output files. Supports yt-dlp output formatting (type: str) (default: %(fulltitle)s (%(id)s))
  --ext EXT             Force extension of video file. E.g. '.mp4' (type: str)
  --temp-folder TEMP_FOLDER
                        Path for temporary files. Supports yt-dlp output formatting (type: str)
  --write-thumbnail     Write thumbnail to file (default: False)
  --embed-thumbnail     Embed thumbnail into final file. Ignored if --no-merge is used (default: False)
  --write-info-json     Write info.json to file (default: False)
  --write-description   Write description to file (default: False)
  --keep-temp-files     Keep all temp files i.e. database and/or ts files (default: False)
  --keep-ts-files       Keep all ts files (default: False)
  --live-chat           Get Live chat (default: False)
  --keep-database-file  Keep database file. If using with --direct-to-ts, this keeps the state file (default: False)
  --recovery            Puts downloader into stream recovery mode (default: False)
  --force-recover-merge
                        Forces merging to final file even if all segements could not be recovered (default: False)
  --recovery-failure-tolerance RECOVERY_FAILURE_TOLERANCE
                        Maximum number of fragments that fail to download (exceed the retry limit) and not throw an error. May cause unexpected issues when merging to .ts file and remuxing. (type: int) (default: 0)
  --wait-limit WAIT_LIMIT
                        Set maximum number of wait intervals for new segments. Each wait interval is ~10s (e.g. a value of 20 would be 200s). A mimimum of value of 20 is recommended. Stream URLs are refreshed every 10 intervals. A value of 0 wait until the video moves into        
                        'was_live' or 'post_live' status. (type: int) (default: 0)
  --database-in-memory  Keep stream segments database in memory. Requires a lot of RAM (Not recommended) (default: False)
  --direct-to-ts        Write directly to ts file instead of database. May use more RAM if a segment is slow to download. This overwrites most database options (default: False)
  --wait-for-video WAIT_FOR_VIDEO
                        Wait time (int) or Minimum and maximum (min:max) interval to wait for a video (type: parse_wait)
  --json-file JSON_FILE
                        Path to existing yt-dlp info.json file. Overrides ID and skips retrieving URLs (type: str)
  --remove-ip-from-json
                        Replaces IP entries in info.json with 0.0.0.0 (default: False)
  --clean-urls          Removes stream URLs from info.json that contain potentially identifiable information. These URLs are usually useless once they have expired (default: False)
  --clean-info-json     Enables yt-dlp's 'clean-info-json' option (default: False)
  --log-level {DEBUG,VERBOSE,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level. Default is INFO. Verbose logging is a custom level that includes the INFO logs of yt-dlp. (type: str) (default: INFO)
  --no-console          Do not log messages to the console. (default: True)
  --log-file LOG_FILE   Path to the log file where messages will be saved. (type: str)
  --write-ffmpeg-command
                        Writes FFmpeg command to a txt file (default: False)
  --stats-as-json       Prints stats as a JSON formatted string. Bypasses logging and prints regardless of log level (default: False)
  --ytdlp-options YTDLP_OPTIONS
                        Additional yt-dlp options as a JSON string. Overwrites any options that are already defined by other options. Available options: https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/YoutubeDL.py#L183. E.g. '{"extractor_args": {"youtube": {"player_client":   
                        ["web_creator"]}, "youtubepot-bgutilhttp":{ "base_url": ["http://10.1.1.40:4416"]}}}' if you have installed the potoken plugin (type: str)
  --ytdlp-log-level {DEBUG,VERBOSE,INFO,WARNING,ERROR,CRITICAL}
                        ### NOT IMPLEMENTED ### Optional alternative log level for yt-dlp module tasks (such as video extraction or format selection). Uses main logger if not set (type: str)
  --dash                Gets any available DASH urls as a fallback to adaptive URLs. Dash URLs do not require yt-dlp modification to be used, but can't be used for stream recovery and can cause large info.json files when a stream is in the 'post_live' status (default: False)      
  --m3u8                Gets any available m3u8 urls as a fallback to adaptive URLs. m3u8 URLs do not require yt-dlp modification to be used, but can't be used for stream recovery. m3u8 URLs provide both video and audio in each fragment and could allow for the amount of segment   
                        download requests to be halved (default: False)
  --force-m3u8          Forces use of m3u8 stream URLs (default: False)
  --proxy PROXY         (ALPHA) Specify proxy to use for web requests. Can be a string for a single proxy or a JSON formatted string to specify multiple methods. For multiple, refer to format https://www.python-httpx.org/advanced/proxies. The first proxy specified will be used    
                        for yt-dlp and live chat functions. Not all functions have proxy compatibility enabled at this time. (type: str)
  --ipv4                Force IPv4 only (default: False)
  --ipv6                Force IPv6 only (default: False)
  --stop-chat-when-done STOP_CHAT_WHEN_DONE
                        Wait a maximum of X seconds after a stream is finished to download live chat. This is useful if waiting for chat to end causes hanging. Onl works with chat-downloader live chat downloads. (type: int) (default: 300)
  --new-line            Console messages always print to new line. (Currently only ensured for stats output) (default: False)

Channel Monitor Options:
  --monitor-channel     Use monitor channel feature (Alpha). Specify channel ID in 'ID' argument (e.g. UCxsZ6NCzjU_t4YSxQLBcM5A). Not using the channel ID will attempt to resolve the channel ID. (default: False)
  --members-only        Monitor 'Members Only' playlist for streams instead of 'Streams' playlist. Requires cookies. (default: False)
  --upcoming-lookahead UPCOMING_LOOKAHEAD
                        Maximum time (in hours) to start a downloader instance for a video. (type: int) (default: 24)
  --playlist-items PLAYLIST_ITEMS
                        Maximum number of playlist items to check. (type: int) (default: 50)
```

---

## Downloader methods

There are two download methods available for livestream downloading, using a SQLite database and writing directly to a ts file. For stream recovery, only the SQLite option is available. Merging to a video or audio file (.mp4, .mkv, .ogg etc) will require an extra write of information. The default method is the SQLite method.

### Direct writing to a .ts file

Writing directly to a ts file helps reduce the number of writes to a disk during the recording as it is only necessary to write to the final ts file. This may reduce disk wear over a long period of time of downloading many streams.
As segments are downloaded, the sequence number of the segment is used to decide which segment will be appended to the ts file next. If the "next" segment is not available to be written to the disk, other segments will be stored in RAM until the respective previous segment has been written to the disk. This performs in a similar way to [ytarchive](https://github.com/Kethsar/ytarchive).
To reduce file opening and closing actions on the file system, if multiple sequential segments can be written at once, they will be written into the file at once.
A state file is saved each time segments are written to the disk, saving the latest segment and size of the file, so the downloader doesn't need to re-download segments should the downloading session stop for any reason.
Segment downloads are not guaranteed to finish in a sequential order if more than 1 thread is used. If a segment is particularly slow at downloading, this may increase ram usage significantly while subsequent segments are saved in RAM.

For regular livestream recording that doesn't experience frequent segment download errors, using the direct writing method will work well.

### SQLite

The SQLite downloader method is used to improve handling of non-sequential successful segment downloads. This works by creating and using a basic table of the segment number as an ID and a blob to store the segment data. Once all segments are downloaded, a query is executed to sort all of the downloaded segments into the correct order and saved to a .ts file. This helps significantly to manage downloaded segments when failures occur often, such as unavailable stream recovery. By writing to the database and a final .ts file, this will require 2 full writes of the downloaded data, which may increase wear on flash-based systems. For most users, this increased wear will not be significant in the long-term.
This improves over saving individual segment files like [ytarchive-raw-go](https://www.google.com/search?q=https://github.com/Kethsar/ytarchive%5D(https://github.com/HoloArchivists/ytarchive-raw-go)) as all the downloaded segments are encapsilated into a single file, making it easier for the file system to handle.
In the SQLite method, the existence of a downloaded segment is checked before it is downloaded. This allows failed segments to be "looped back to" later without causing other slowdowns, and ensures some information is saved for a segment (even if it is empty, as is the case sometimes).

---

## Known Issues

* **Concurrent Futures:** The downloader uses `concurrent.futures` for thread management. While robust, stopping the downloader gracefully (e.g., via Keyboard Interrupt) can sometimes be delayed if a thread is stuck on a request.
* **Stream Recovery:** This feature is currently in a "semi-broken" state and may not work reliably for all stream types.
* **Chat Downloader:** The `chat-downloader` dependency occasionally breaks due to YouTube updates; the tool may fall back to `yt-dlp` for chat extraction which has different output formatting.


