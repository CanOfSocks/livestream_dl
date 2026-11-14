"""
Resuming yt-dlp's YouTube live chat download.

yt-dlp downloads a live chat as the 'live_chat' subtitle with YoutubeLiveChatFD, but doesn't
resume one: an existing <name>.live_chat.json is skipped as "already present", and an existing
<name>.live_chat.json.part is appended to while the chat restarts from scratch, so a replay is
written twice and live offsets restart at 0.

ResumableLiveChatFD is yt-dlp's YoutubeLiveChatFD (same requests, same output format) with the
resume handled around it, through the downloader's fragment request and fragment write methods:
- an earlier chat file is gathered into the .part file, which yt-dlp appends to, and a last line
  cut off by a crash is removed first
- chat actions already in the file are not written again (matched by chat item id, or content)
- a replay continues from the last saved video offset instead of from the start
- live offsets continue from the original download start instead of restarting at 0
yt-dlp itself is not modified.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Union

import yt_dlp.YoutubeDL
from yt_dlp.downloader.youtube_live_chat import YoutubeLiveChatFD
from yt_dlp.utils import DownloadCancelled, int_or_none, try_get

LoggerLike = Union[logging.Logger, logging.LoggerAdapter]

# How much of the end of an existing chat file is read to find where to resume. The chat sent
# again on resume (the live chat's recent history, or the replay's 5s look-behind) is far smaller.
RESUME_TAIL_BYTES: int = 8 * 1024 * 1024
# Keys that differ between two copies of the same chat action (excluded from content matching)
_VOLATILE_KEYS = frozenset({'clickTrackingParams', 'trackingParams'})
# Keys that identify a chat item / banner / poll, searched for in this order
_ID_KEYS = ('id', 'targetItemId', 'actionId', 'targetActionId', 'liveChatPollId')
# yt-dlp requests the replay from 5s before the last offset it has
_REPLAY_LOOK_BEHIND_MS: int = 5000
# The "Welcome to live chat!" message YouTube adds to the start of every chat session, with a new
# id each time. The download being resumed already has one.
_SESSION_MESSAGE_RENDERER = 'liveChatViewerEngagementMessageRenderer'


class LiveChatUnavailable(Exception):
    """The video has no live chat to download."""


# --- Reading an existing chat file ---

def action_key(action: dict) -> str:
    """
    Identify one chat action (e.g. an addChatItemAction) so a copy downloaded again can be
    recognised: the action type plus the item's id, or its content (without tracking parameters)
    if it has no id.
    """
    kind = next((k for k in action if k not in _VOLATILE_KEYS), '')
    item_id = _find_id(action.get(kind), depth=4)
    if item_id is not None:
        return f'{kind}:{item_id}'
    content = json.dumps(_strip_volatile(action), sort_keys=True, ensure_ascii=False)
    return f'{kind}#{hashlib.sha1(content.encode()).hexdigest()}'


def _find_id(value: Any, depth: int) -> Optional[str]:
    if depth < 0 or not isinstance(value, dict):
        return None
    for key in _ID_KEYS:
        if isinstance(value.get(key), str):
            return value[key]
    for child in value.values():
        found = _find_id(child, depth - 1)
        if found is not None:
            return found
    return None


def _strip_volatile(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _strip_volatile(v) for k, v in value.items() if k not in _VOLATILE_KEYS}
    if isinstance(value, list):
        return [_strip_volatile(v) for v in value]
    return value


def line_actions(line: dict) -> list:
    """The chat actions in one line of a yt-dlp live chat file (replay action or live pseudo action)."""
    return try_get(line, lambda x: x['replayChatItemAction']['actions'], list) or []


def line_offset(line: dict) -> Optional[int]:
    """videoOffsetTimeMsec of a line: inside replayChatItemAction for replays, at the top level for live."""
    return int_or_none(line.get('videoOffsetTimeMsec')
                       or try_get(line, lambda x: x['replayChatItemAction']['videoOffsetTimeMsec']))


def _is_session_message(action: dict) -> bool:
    return bool(try_get(action, lambda x: x['addChatItemAction']['item'][_SESSION_MESSAGE_RENDERER]))


def _timestamps(line: dict) -> list:
    return [t for t in map(YoutubeLiveChatFD.parse_live_timestamp, line_actions(line)) if t is not None]


@dataclass
class ResumeState:
    """What is already in an existing chat file, read from its end."""
    seen: set = field(default_factory=set)        # action_key of every action in the read window
    oldest_timestamp: Optional[int] = None         # ms; chat older than the window is assumed saved
    last_replay_offset: Optional[int] = None       # ms; where a replay continues
    live_start_time: Optional[int] = None          # ms; base of the live offsets already written
    lines: int = 0

    @classmethod
    def from_lines(cls, lines: list) -> "ResumeState":
        state = cls(lines=len(lines))
        for line in lines:
            for action in line_actions(line):
                state.seen.add(action_key(action))
            timestamps = _timestamps(line)
            if timestamps and (state.oldest_timestamp is None or min(timestamps) < state.oldest_timestamp):
                state.oldest_timestamp = min(timestamps)
            offset = line_offset(line)
            if offset is None:
                continue
            if line.get('isLive'):
                state.last_replay_offset = None
                # yt-dlp's live offset is the item's timestamp minus the time the download started
                if timestamps:
                    state.live_start_time = timestamps[-1] - offset
            else:
                state.last_replay_offset = offset
        return state

    def is_duplicate(self, action: dict) -> bool:
        if not self.lines:
            return False
        timestamp = YoutubeLiveChatFD.parse_live_timestamp(action)
        if timestamp is not None and self.oldest_timestamp is not None and timestamp < self.oldest_timestamp:
            return True
        return action_key(action) in self.seen


def prepare_resume_file(path: str, logger: Optional[LoggerLike] = None) -> ResumeState:
    """
    Make an existing chat file safe to append to and read what it already contains.
    A partial last line (from a crash) is cut off. A file that isn't in yt-dlp's line format (e.g.
    one written by chat-downloader) is moved aside to <path>.old so the two are not mixed.
    """
    logger = logger or logging.getLogger(__name__)
    try:
        size = os.path.getsize(path)
    except FileNotFoundError:
        return ResumeState()
    with open(path, 'rb+') as f:
        head = f.read(64).lstrip()
        if head and not head.startswith(b'{'):
            f.close()
            os.replace(path, path + '.old')
            logger.warning(f"Existing live chat file {path} is not in yt-dlp's format, moved it to {path}.old and starting a new one")
            return ResumeState()
        if size:
            f.seek(size - 1)
            if f.read(1) != b'\n':
                end = _last_newline_end(f, size)
                logger.info(f"Removing an incomplete last line ({size - end} bytes) from {path}")
                f.truncate(end)
                size = end
        # Read the end of the file, starting at a line boundary
        start = max(size - RESUME_TAIL_BYTES, 0)
        f.seek(start)
        tail = f.read(size - start)
    if start:
        tail = tail[tail.find(b'\n') + 1:]
    lines = []
    for raw in tail.splitlines():
        try:
            line = json.loads(raw)
        except ValueError:
            continue
        if isinstance(line, dict):
            lines.append(line)
    return ResumeState.from_lines(lines)


def _last_newline_end(f, size: int) -> int:
    """Position just after the last b'\\n' of the file (0 if there is none)."""
    block = 64 * 1024
    pos = size
    while pos > 0:
        read_from = max(pos - block, 0)
        f.seek(read_from)
        chunk = f.read(pos - read_from)
        index = chunk.rfind(b'\n')
        if index != -1:
            return read_from + index + 1
        pos = read_from
    return 0


def merge_into_part(filename: str, logger: Optional[LoggerLike] = None) -> str:
    """
    Gather an earlier download of this chat into <filename>.part, which yt-dlp appends to: a
    finished <filename> (which yt-dlp would otherwise skip as already downloaded) followed by any
    existing .part.
    """
    logger = logger or logging.getLogger(__name__)
    part = filename + '.part'
    if os.path.exists(filename):
        if os.path.exists(part):
            with open(filename, 'ab') as dest, open(part, 'rb') as src:
                if dest.tell() and not _ends_with_newline(filename):
                    dest.write(b'\n')
                while chunk := src.read(1024 * 1024):
                    dest.write(chunk)
            os.remove(part)
        os.replace(filename, part)
        logger.info(f"Resuming live chat from existing file {filename}")
    elif os.path.exists(part):
        logger.info(f"Resuming live chat from existing file {part}")
    return part


def _ends_with_newline(path: str) -> bool:
    with open(path, 'rb') as f:
        f.seek(-1, os.SEEK_END)
        return f.read(1) == b'\n'


# --- Downloading ---

class ResumableLiveChatFD(YoutubeLiveChatFD):
    """
    yt-dlp's live chat downloader, continuing the chat already in its .part file (see ResumeState).
    should_stop is checked before every chat request; the download then raises DownloadCancelled.
    """

    def __init__(self, ydl: yt_dlp.YoutubeDL, params: dict, resume: Optional[ResumeState] = None,
                 should_stop: Optional[Callable[[], bool]] = None):
        super().__init__(ydl, params)
        self.resume = resume or ResumeState()
        self.should_stop = should_stop or (lambda: False)
        self.written = 0
        self.skipped = 0
        self._is_live = False
        self._seek_pending = False
        self._run_start: int = int(time.time() * 1000)
        self._run_start_exact = False
        self._first_fragment = True

    def real_download(self, filename, info_dict):
        self._is_live = info_dict.get('protocol') == 'youtube_live_chat'
        self._seek_pending = not self._is_live and self.resume.last_replay_offset is not None
        # yt-dlp measures live offsets from its own start time, taken right after this; it is
        # refined from the first chat item with a timestamp (see _rewrite)
        self._run_start = int(time.time() * 1000)
        self._run_start_exact = False
        self._first_fragment = True
        if self.resume.lines:
            where = (f"video offset {self.resume.last_replay_offset / 1000:.1f}s" if self._seek_pending
                     else "the end of the existing file")
            self.to_screen(f'[{self.FD_NAME}] Resuming {"live chat" if self._is_live else "chat replay"} from {where}')
        return super().real_download(filename, info_dict)

    def _download_fragment(self, ctx, frag_url, info_dict, headers=None, request_data=None, *args, **kwargs):
        if self.should_stop():
            raise DownloadCancelled('Live chat download stopped')
        if request_data and self._seek_pending:
            # The first chat replay API request: start where the existing file ends, not at 0
            self._seek_pending = False
            request_data = self._seek(request_data)
        return super()._download_fragment(ctx, frag_url, info_dict, headers, request_data, *args, **kwargs)

    def _seek(self, request_data: bytes) -> bytes:
        try:
            data = json.loads(request_data)
            player_state = data.setdefault('currentPlayerState', {})
        except (ValueError, AttributeError):
            return request_data
        player_state['playerOffsetMs'] = str(max(self.resume.last_replay_offset - _REPLAY_LOOK_BEHIND_MS, 0))
        return json.dumps(data, ensure_ascii=False).encode() + b'\n'

    def _append_fragment(self, ctx, frag_content, *args, **kwargs):
        if frag_content:
            frag_content = self._rewrite(frag_content)
        return super()._append_fragment(ctx, frag_content, *args, **kwargs)

    def _rewrite(self, frag_content: bytes) -> bytes:
        """Drop chat actions that are already saved and continue the existing live offsets."""
        shift_live = self._is_live and self.resume.live_start_time is not None
        if not (self.resume.lines or shift_live):
            self.written += frag_content.count(b'\n')
            return frag_content
        first_fragment, self._first_fragment = self._first_fragment, False
        out = bytearray()
        for raw in frag_content.splitlines():
            if not raw.strip():
                continue
            try:
                line = json.loads(raw)
                actions = line['replayChatItemAction']['actions']
            except (ValueError, KeyError, TypeError):
                out.extend(raw + b'\n')
                continue
            if shift_live and line.get('isLive'):
                self._shift_live_offset(line)
            kept = [a for a in actions if not (self.resume.is_duplicate(a) or (
                first_fragment and self.resume.lines and _is_session_message(a)))]
            self.skipped += len(actions) - len(kept)
            if not kept:
                continue
            line['replayChatItemAction']['actions'] = kept
            out.extend(json.dumps(line, ensure_ascii=False).encode() + b'\n')
            self.written += 1
        return bytes(out)

    def _shift_live_offset(self, line: dict) -> None:
        offset = line_offset(line)
        if offset is None:
            return
        if not self._run_start_exact:
            timestamps = _timestamps(line)
            if timestamps:
                self._run_start = timestamps[-1] - offset
                self._run_start_exact = True
        line['videoOffsetTimeMsec'] = str(offset + self._run_start - self.resume.live_start_time)


def live_chat_subtitle(info_dict: dict) -> dict:
    """The 'live_chat' subtitle entry yt-dlp downloads, built like the YouTube extractor does if missing."""
    sub = try_get(info_dict, lambda x: x['subtitles']['live_chat'][0], dict)
    if sub:
        sub = dict(sub)
    else:
        video_id = info_dict.get('id')
        if not video_id:
            raise LiveChatUnavailable("No video id")
        sub = {
            'url': f'https://www.youtube.com/watch?v={video_id}&bpctr=9999999999&has_verified=1',
            'video_id': video_id,
            'ext': 'json',
            'protocol': ('youtube_live_chat' if info_dict.get('live_status') in ('is_live', 'is_upcoming')
                         else 'youtube_live_chat_replay'),
        }
    sub.setdefault('http_headers', info_dict.get('http_headers'))
    return sub


def download_live_chat(ydl: yt_dlp.YoutubeDL, info_dict: dict, filename: str, *,
                       should_stop: Optional[Callable[[], bool]] = None,
                       logger: Optional[LoggerLike] = None) -> bool:
    """
    Download the live chat of info_dict to filename with yt-dlp, continuing an earlier download of
    it (<filename> or <filename>.part). Returns True once the chat has ended (the file has its final
    name), False if the download failed. Raises DownloadCancelled when should_stop() became true;
    the chat downloaded so far is then in <filename>.part.
    """
    logger = logger or logging.getLogger(__name__)
    sub = live_chat_subtitle(info_dict)
    part = merge_into_part(filename, logger)
    fd = ResumableLiveChatFD(ydl, ydl.params, prepare_resume_file(part, logger), should_stop)
    try:
        success, _ = fd.download(filename, sub, subtitle=True)
        return success
    finally:
        logger.info(f"Live chat: {fd.written} lines written"
                    + (f", {fd.skipped} already downloaded chat actions skipped" if fd.skipped else ""))
