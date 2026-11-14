import argparse
import json
import logging
import sqlite3
from pathlib import Path
from typing import Optional

try:
    import download_Live
except ModuleNotFoundError:
    from . import download_Live


def is_sqlite_db(filepath):
    """Robustly checks file headers to determine if an input is an SQLite3 database."""
    path = Path(filepath)
    if not path.is_file():
        return False
    try:
        with open(path, 'rb') as f:
            # SQLite databases always start with this 16-byte header
            return f.read(16) == b'SQLite format 3\000'
    except Exception:
        return False


def extract_segments_to_ts(db_path, ts_path, logger):
    """Extracts BLOBs sequentially from the SQLite database and strips redundant atoms."""
    logger.info(f"Extracting segments from database: {db_path} -> {ts_path}")

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute('SELECT segment_data FROM segments ORDER BY id')
        # Large buffer to reduce syscalls during writing
        with open(ts_path, 'wb', buffering=download_Live.WRITE_BUFFER_SIZE) as f:
            is_first = True
            for (segment_data,) in cursor:
                # Atom cleaning writes zero-copy slices of each segment
                download_Live.write_segment(f, segment_data or b"", clean=True, first=is_first)
                is_first = False
    finally:
        conn.close()
    logger.info(f"Finished extracting: {ts_path}")


def _load_info_json(path: Optional[str], ext: str, logger) -> dict:
    """The video's info.json (if given), with the keys create_mp4 needs filled in."""
    info_dict = {}
    if path:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                info_dict = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load info.json: {e}")
    info_dict.setdefault('id', 'recovered_stream')
    info_dict.setdefault('ext', ext)
    return info_dict


def _describe_input(path: Path, ts_path: Path, info_dict: dict) -> download_Live.FileInfo:
    """
    Work out the format, codecs and stream type of an input file. The format ID comes from the
    file name (e.g. '137' from 'video.137.temp'); codecs come from the info.json if available.
    """
    parts = path.name.split('.')
    format_id = parts[-2] if len(parts) > 1 else 'unknown'

    vcodec, acodec = 'none', 'none'
    file_type = 'video'
    protocol = 'm3u8_native'

    if info_dict.get('formats') or info_dict.get('requested_formats'):
        all_formats = info_dict.get('formats', []) + info_dict.get('requested_formats', [])
        for f in all_formats:
            if str(f.get('format_id')) == format_id:
                vcodec = f.get('vcodec', 'none')
                acodec = f.get('acodec', 'none')
                protocol = f.get('protocol', protocol)
                break
        if vcodec == 'none' and acodec != 'none':
            file_type = 'audio'
    elif 'audio' in path.name.lower():
        # Without format information, guess from the file name
        file_type = 'audio'
        acodec = 'aac'
    else:
        vcodec = 'h264'

    return download_Live.FileInfo(ts_path, file_type=file_type, format=format_id, vcodec=vcodec, acodec=acodec,
                                  protocol=protocol)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Utility to extract and merge partial livestreams into a final container.")
    parser.add_argument('inputs', nargs='+', help="Input SQLite databases (.temp/.db) or .ts files")
    parser.add_argument('-o', '--output', required=True, help="Output destination (e.g., merged.mp4)")
    parser.add_argument('-i', '--info-json', help="Path to info.json for metadata mapping")
    parser.add_argument('-t', '--thumbnail', help="Path to thumbnail to embed")
    parser.add_argument('--ext', default='mp4', choices=['mp4', 'mkv', 'webm'], help="Output container format")
    parser.add_argument('--keep-ts', action='store_true', help="Keep extracted .ts files after a successful merge")
    return parser


def main():
    args = build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    logger = logging.getLogger('recovery_util')
    info_dict = _load_info_json(args.info_json, args.ext, logger)

    # One video and one audio file, keyed by stream type
    stream_dict = {}
    for inp in args.inputs:
        path = Path(inp)
        if not path.exists():
            logger.error(f"File not found: {inp}")
            continue

        # SQLite databases are extracted to .ts first, .ts files are used as they are
        ts_path = path
        if is_sqlite_db(path):
            ts_path = path.with_suffix('.ts')
            extract_segments_to_ts(path, ts_path, logger)

        file_info = _describe_input(path, ts_path, info_dict)
        if file_info.file_type in stream_dict:
            logger.warning(f"Multiple {file_info.file_type} streams detected. The merger specifically maps one video and one audio. Overwriting previous {file_info.file_type}.")
        stream_dict[file_info.file_type] = file_info

    # Same layout as LiveStreamDownloader.file_names (one manifest, numbered 0)
    file_names_dict = {"streams": {0: stream_dict}}
    if args.info_json:
        file_names_dict['info_json'] = download_Live.FileInfo(args.info_json, file_type='info_json')
    if args.thumbnail:
        file_names_dict['thumbnail'] = download_Live.FileInfo(args.thumbnail, file_type='thumbnail')

    options = {
        'filename': args.output,
        'ext': args.ext,
        'embed_thumbnail': bool(args.thumbnail),
        'merge': True,
        'keep_ts_files': args.keep_ts
    }

    # Reuse the downloader's FFmpeg muxing
    logger.info("Initializing FFmpeg metadata processing and merge...")
    downloader = download_Live.LiveStreamDownloader(logger=logger)
    downloader.file_names = file_names_dict
    try:
        downloader.create_mp4(file_names=file_names_dict, info_dict=info_dict, options=options)
        logger.info(f"Successfully recovered to: {args.output}")
    except Exception as e:
        logger.exception(f"Merge failed: {e}")

if __name__ == '__main__':
    main()