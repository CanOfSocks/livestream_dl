"""
Resuming yt-dlp's YouTube live chat download.

yt-dlp downloads a live chat as the 'live_chat' subtitle with YoutubeLiveChatFD, but doesn't
resume one: an existing <name>.live_chat.json is skipped as "already present", and an existing
<name>.live_chat.json.part is appended to while the chat restarts from scratch, so a replay is
written twice and live offsets restart at 0.

The resume is built around yt-dlp rather than into it, so that yt-dlp updates (needed to keep up
with YouTube) can't break it or lose chat that was already saved:

- Chat saved by earlier runs is gathered into <name>.live_chat.json.resume, a file yt-dlp never
  opens. yt-dlp always downloads into a new file, so how it treats existing files doesn't matter.
- The newly downloaded chat is filtered against the saved chat (ChatFilter): chat actions already
  saved are dropped (matched by item id, or content), live offsets are continued from the original
  download start, and lines in a format it doesn't recognise are kept unchanged.
- ResumableLiveChatFD applies that filter while downloading, through two optional hooks on yt-dlp's
  downloader (the fragment write and the fragment request, which also seeks a replay to where the
  saved chat ends). The hooks never raise; whether they handled everything yt-dlp wrote is
  checked afterwards, and if not, the filter is run over the new file instead (finalize).
- If yt-dlp's live chat downloader can't be subclassed at all, YoutubeDL.dl() downloads the chat
  and the new file is filtered afterwards.

The files of one chat, in order, are chat_files(filename); the full chat is their concatenation.
yt-dlp itself is not modified.
"""
from __future__ import annotations

import collections
import glob
import hashlib
import inspect
import json
import logging
import os
import re
import shutil
import time
import zipfile
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Iterator, Optional, Union

import yt_dlp
from yt_dlp.utils import DownloadCancelled

try:
    from yt_dlp.downloader.youtube_live_chat import YoutubeLiveChatFD
except ImportError:  # moved or removed in a future yt-dlp; YoutubeDL.dl() is used instead
    YoutubeLiveChatFD = None

LoggerLike = Union[logging.Logger, logging.LoggerAdapter]

RESUME_SUFFIX = '.resume'
_TMP_SUFFIX = '.tmp'
_COMPARE_BLOCK = 1024 * 1024
# How much of the end of the saved chat is read to recognise chat downloaded again. The chat sent
# again on resume (the live chat's recent history, or the replay's 5s look-behind) is far smaller.
RESUME_TAIL_BYTES: int = 8 * 1024 * 1024
# The start of the saved chat is also read, for the session messages sent at the start of every
# download (see _SESSION_RENDERERS)
RESUME_HEAD_BYTES: int = 1024 * 1024
# yt-dlp requests the replay from 5s before the last offset it has
REPLAY_LOOK_BEHIND_MS: int = 5000
# Keys that differ between two copies of the same chat action (excluded from content matching)
_VOLATILE_KEYS = frozenset({'clickTrackingParams', 'trackingParams'})
# Keys that identify a chat item / banner / poll, searched for in this order
_ID_KEYS = ('id', 'targetItemId', 'actionId', 'targetActionId', 'liveChatPollId')
# Messages YouTube adds to every chat session (e.g. "Welcome to live chat!") with a new id and
# timestamp each time, so they are matched by their content alone
_SESSION_RENDERERS = frozenset({'liveChatViewerEngagementMessageRenderer'})
# Chat item renderers whose timestampUsec yt-dlp measures live offsets from (the same list as its
# YoutubeLiveChatFD.parse_live_timestamp); other actions get the offset of the line before them
_TIMESTAMP_RENDERERS = (
    'liveChatTextMessageRenderer', 'liveChatPaidMessageRenderer', 'liveChatMembershipItemRenderer',
    'liveChatPaidStickerRenderer', 'liveChatTickerPaidMessageItemRenderer',
    'liveChatTickerSponsorItemRenderer', 'liveChatBannerRenderer',
)
_WRAPPED_RENDERERS = (
    'liveChatTextMessageRenderer', 'liveChatPaidMessageRenderer', 'liveChatMembershipItemRenderer',
    'liveChatPaidStickerRenderer',
)


def chat_files(filename: str) -> list[str]:
    """The files that may hold the chat of filename, oldest content first."""
    return [filename + RESUME_SUFFIX, filename, filename + '.part']


# --- Chat lines ---

def _int(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _get(value, *path):
    for key in path:
        if isinstance(value, dict):
            value = value.get(key)
        elif isinstance(value, list) and isinstance(key, int) and -len(value) <= key < len(value):
            value = value[key]
        else:
            return None
    return value


def line_actions(line: dict) -> Optional[list]:
    """The chat actions of one line of a yt-dlp live chat file, or None if it isn't in that format."""
    actions = _get(line, 'replayChatItemAction', 'actions')
    return actions if isinstance(actions, list) else None


def line_offset(line: dict) -> Optional[int]:
    """videoOffsetTimeMsec of a line: inside replayChatItemAction for replays, at the top level for live."""
    offset = _int(line.get('videoOffsetTimeMsec'))
    return offset if offset is not None else _int(_get(line, 'replayChatItemAction', 'videoOffsetTimeMsec'))


def _item_renderer(action: dict) -> tuple[Optional[str], Optional[dict]]:
    """(name, renderer) of the chat item an action adds, if any."""
    for container in action.values():
        item = _get(container, 'item') if isinstance(container, dict) else None
        if isinstance(item, dict) and len(item) == 1:
            name, renderer = next(iter(item.items()))
            if isinstance(renderer, dict):
                return name, renderer
        banner = _get(container, 'bannerRenderer') if isinstance(container, dict) else None
        if isinstance(banner, dict) and len(banner) == 1:
            name, renderer = next(iter(banner.items()))
            if isinstance(renderer, dict):
                return name, renderer
    return None, None


def action_timestamp(action: dict) -> Optional[int]:
    """When a chat message was sent (ms), from its timestampUsec."""
    name, renderer = _item_renderer(action)
    if name not in _TIMESTAMP_RENDERERS:
        return None
    # Ticker items and banners wrap the message they show
    for inner in (_get(renderer, 'showItemEndpoint', 'showLiveChatItemEndpoint', 'renderer'), _get(renderer, 'contents')):
        if isinstance(inner, dict):
            renderer = next((inner[k] for k in _WRAPPED_RENDERERS if isinstance(inner.get(k), dict)), None)
            break
    usec = _int(_get(renderer, 'timestampUsec'))
    return usec // 1000 if usec is not None else None


def action_key(action: dict) -> str:
    """
    Identify one chat action (e.g. an addChatItemAction) so a copy downloaded again can be
    recognised: the action type plus the item's id, or its content (without tracking parameters)
    if it has no id or is a per-session message.
    """
    kind = next((k for k in action if k not in _VOLATILE_KEYS), '')
    name, renderer = _item_renderer(action)
    if name in _SESSION_RENDERERS:
        content = {k: v for k, v in renderer.items() if k not in ('id', 'timestampUsec')}
        return f'{kind}:{name}#{_content_hash(content)}'
    item_id = _find_id(action.get(kind), depth=4)
    if item_id is not None:
        return f'{kind}:{item_id}'
    return f'{kind}#{_content_hash(action)}'


def _content_hash(value) -> str:
    content = json.dumps(_strip_volatile(value), sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(content.encode()).hexdigest()


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


def _parse_line(raw: bytes) -> Optional[dict]:
    try:
        line = json.loads(raw)
    except ValueError:
        return None
    return line if isinstance(line, dict) else None


def _live_start(lines: Iterable[dict]) -> Optional[int]:
    """
    The time a live download started (ms), which yt-dlp measures live offsets from: the most common
    (timestamp - offset) of live lines. Taking the most common value keeps one wrongly matched line
    from deciding it.
    """
    starts = collections.Counter()
    for line in lines:
        if not line.get('isLive'):
            continue
        offset = line_offset(line)
        timestamps = [t for t in map(action_timestamp, line_actions(line) or []) if t is not None]
        if offset is not None and len(timestamps) == 1:
            starts[timestamps[0] - offset] += 1
    return starts.most_common(1)[0][0] if starts else None


# --- What was saved ---

@dataclass
class ResumeState:
    """What the saved chat contains, read from its start and end."""
    seen: set = field(default_factory=set)        # action_key of the actions read
    oldest_timestamp: Optional[int] = None         # ms; chat older than the end that was read is saved
    last_replay_offset: Optional[int] = None       # ms; where a replay continues
    live_start_time: Optional[int] = None          # ms; base of the live offsets saved
    lines: int = 0

    @classmethod
    def from_lines(cls, tail: list, head: Iterable[dict] = ()) -> "ResumeState":
        state = cls(lines=len(tail))
        for line in head:
            for action in line_actions(line) or []:
                state.seen.add(action_key(action))
        for line in tail:
            for action in line_actions(line) or []:
                state.seen.add(action_key(action))
                timestamp = action_timestamp(action)
                if timestamp is not None and (state.oldest_timestamp is None or timestamp < state.oldest_timestamp):
                    state.oldest_timestamp = timestamp
            offset = line_offset(line)
            if offset is not None:
                state.last_replay_offset = None if line.get('isLive') else offset
        state.live_start_time = _live_start(tail)
        return state

    def is_duplicate(self, action: dict) -> bool:
        if not self.lines:
            return False
        if action_key(action) in self.seen:
            return True
        timestamp = action_timestamp(action)
        return timestamp is not None and self.oldest_timestamp is not None and timestamp < self.oldest_timestamp


def read_resume_state(path: str) -> ResumeState:
    """Read what a (complete-lined) saved chat file contains."""
    try:
        size = os.path.getsize(path)
    except FileNotFoundError:
        return ResumeState()
    with open(path, 'rb') as f:
        head = f.read(min(RESUME_HEAD_BYTES, size))
        start = max(size - RESUME_TAIL_BYTES, 0)
        f.seek(start)
        tail = f.read(size - start)
    if start:
        tail = tail[tail.find(b'\n') + 1:]
    head = head[:head.rfind(b'\n') + 1]
    parse = lambda data: [line for line in map(_parse_line, data.splitlines()) if line is not None]  # noqa: E731
    return ResumeState.from_lines(parse(tail), parse(head) if start else ())


# --- Filtering newly downloaded chat ---

class ChatFilter:
    """
    Drop chat actions that are already saved and continue the saved live offsets. Applying it
    again to its own output changes nothing. Lines it doesn't recognise are passed through.
    """

    def __init__(self, state: ResumeState, run_start: Optional[int] = None):
        self.state = state
        # When the download being filtered started (ms): the base of its own live offsets
        self.run_start = run_start
        self._last_offset: Optional[int] = None
        self.lines = 0
        self.skipped = 0

    @property
    def active(self) -> bool:
        return bool(self.state.lines)

    def filter(self, data: bytes) -> bytes:
        if not self.active:
            self.lines += data.count(b'\n')
            return data
        out = bytearray()
        for raw in data.splitlines(keepends=True):
            out += self.filter_line(raw)
        return bytes(out)

    def filter_line(self, raw: bytes) -> bytes:
        if not raw.strip():
            return b''
        if not raw.endswith(b'\n'):
            raw += b'\n'
        line = _parse_line(raw)
        actions = line_actions(line) if line is not None else None
        if actions is None:
            self.lines += 1
            return raw
        changed = False
        if line.get('isLive') and self.state.live_start_time is not None:
            changed = self._continue_live_offset(line, actions)
        kept = [a for a in actions if not (isinstance(a, dict) and self.state.is_duplicate(a))]
        self.skipped += len(actions) - len(kept)
        if not kept:
            return b''
        if len(kept) != len(actions):
            line['replayChatItemAction']['actions'] = kept
            changed = True
        self.lines += 1
        return json.dumps(line, ensure_ascii=False).encode() + b'\n' if changed else raw

    def _continue_live_offset(self, line: dict, actions: list) -> bool:
        offset = line_offset(line)
        if offset is None or 'videoOffsetTimeMsec' not in line:
            return False
        timestamps = [t for t in (action_timestamp(a) for a in actions if isinstance(a, dict)) if t is not None]
        if timestamps:
            # Measured from the original start instead of this download's
            new_offset = timestamps[-1] - self.state.live_start_time
        elif self._last_offset is not None:
            # yt-dlp gives a line without a timestamp the offset of the line before it
            new_offset = self._last_offset
        elif self.run_start is not None:
            new_offset = offset + self.run_start - self.state.live_start_time
        else:
            return False
        self._last_offset = new_offset
        if new_offset == offset:
            return False
        line['videoOffsetTimeMsec'] = str(new_offset)
        return True


def _lines(path: str) -> Iterator[bytes]:
    """Complete lines of a file (a last line without a newline was cut off and is left out)."""
    with open(path, 'rb') as f:
        for raw in f:
            if raw.endswith(b'\n'):
                yield raw


def _first_live_start(path: str, max_lines: int = 2000) -> Optional[int]:
    lines = []
    for raw in _lines(path):
        line = _parse_line(raw)
        if line is not None and line.get('isLive'):
            lines.append(line)
            if len(lines) >= 50:
                break
        max_lines -= 1
        if max_lines <= 0:
            break
    return _live_start(lines)


# --- Gathering the saved chat ---
#
# Every step below can be interrupted by a crash at any point and simply run again by the next
# download: files are only deleted once their chat is in a file that replaced them atomically or
# that they were filtered into (and filtering again recognises what was already added).

def _is_chat_file(path: str) -> bool:
    with open(path, 'rb') as f:
        head = f.read(64).lstrip()
    return not head or head.startswith(b'{')


def _truncate_partial_line(path: str, logger: LoggerLike) -> None:
    size = os.path.getsize(path)
    if not size:
        return
    with open(path, 'rb+') as f:
        f.seek(size - 1)
        if f.read(1) == b'\n':
            return
        end = size
        while end > 0:
            start = max(end - 64 * 1024, 0)
            f.seek(start)
            index = f.read(end - start).rfind(b'\n')
            if index != -1:
                end = start + index + 1
                break
            end = start
        logger.info(f"Removing an incomplete last line ({size - end} bytes) from {path}")
        f.truncate(end)


def move_aside(path: str, logger: LoggerLike, reason: str) -> str:
    """Keep a file out of the way as <path>.old (or .old.1, .old.2, ... so none is overwritten)."""
    target = path + '.old'
    number = 0
    while os.path.exists(target):
        number += 1
        target = f'{path}.old.{number}'
    os.replace(path, target)
    logger.warning(f"{path} {reason}, moved it to {target}")
    return target


def _remove_if_exists(path: str, logger: LoggerLike) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except OSError as e:
        logger.warning(f"Unable to remove {path}: {e}")


def remove_fragment_files(filename: str, logger: Optional[LoggerLike] = None) -> None:
    """
    Delete the files yt-dlp keeps for each chat request (<name>.part-Frag<N>, plus .part while one is
    being downloaded; <name>-Frag<N> without .part files). Every download numbers them from 0 again,
    so files left by a crash would be taken by the next download as the answer to its own requests
    (an old chat position) or resumed halfway.
    """
    logger = logger or logging.getLogger(__name__)
    for pattern in (glob.escape(filename) + '.part-Frag*', glob.escape(filename) + '-Frag*'):
        for path in glob.glob(pattern):
            _remove_if_exists(path, logger)
            logger.debug(f"Removed leftover live chat fragment {path}")


def _append_filtered(base: str, source: str, logger: LoggerLike, filtered: bool = False) -> None:
    """Append the chat of source that base doesn't have yet (all of it if it was filtered already)."""
    if filtered:
        _truncate_partial_line(source, logger)
        with open(base, 'ab') as out, open(source, 'rb') as src:
            shutil.copyfileobj(src, out, _COMPARE_BLOCK)
        logger.debug(f"Added {source} to the saved chat")
        return
    chat_filter = ChatFilter(read_resume_state(base), run_start=_first_live_start(source))
    with open(base, 'ab') as out:
        for raw in _lines(source):
            out.write(chat_filter.filter_line(raw))
    logger.info(f"Added {chat_filter.lines} live chat lines from {source} to the saved chat"
                + (f" ({chat_filter.skipped} already saved chat actions skipped)" if chat_filter.skipped else ""))


def consolidate(filename: str, logger: Optional[LoggerLike] = None, new_chat_filtered: bool = False) -> ResumeState:
    """
    Gather all chat saved for filename (chat_files) into <filename>.resume, dropping what was
    downloaded twice, and return what it contains. Afterwards neither filename nor its .part (nor
    yt-dlp's fragment files) exist, so yt-dlp starts a new file. A file that isn't in yt-dlp's line
    format (e.g. one written by chat-downloader) is moved aside rather than mixed in.
    new_chat_filtered: filename / its .part were filtered while downloading and are added as they are.
    """
    logger = logger or logging.getLogger(__name__)
    base = filename + RESUME_SUFFIX
    remove_fragment_files(filename, logger)
    _remove_if_exists(base + _TMP_SUFFIX, logger)   # an unfinished restore_from_zip
    for path in chat_files(filename):
        if not os.path.exists(path):
            continue
        if not _is_chat_file(path):
            move_aside(path, logger, "is not in yt-dlp's live chat format")
            continue
        if path == base or not os.path.exists(base) or not os.path.getsize(base):
            if path != base:
                os.replace(path, base)
            _truncate_partial_line(base, logger)
            continue
        _append_filtered(base, path, logger, filtered=new_chat_filtered)
        os.remove(path)
    if os.path.exists(base) and not os.path.getsize(base):
        os.remove(base)
    return read_resume_state(base)


class _ConcatReader:
    """Reads several files as one stream."""

    def __init__(self, paths: Iterable[str]):
        self._paths = list(paths)
        self._file = None

    def read(self, size: int) -> bytes:
        out = bytearray()
        while len(out) < size:
            if self._file is None:
                if not self._paths:
                    break
                self._file = open(self._paths.pop(0), 'rb')
            chunk = self._file.read(size - len(out))
            if not chunk:
                self._file.close()
                self._file = None
                continue
            out += chunk
        return bytes(out)

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _read_exact(stream, size: int) -> bytes:
    out = bytearray()
    while len(out) < size:
        chunk = stream.read(min(size - len(out), _COMPARE_BLOCK))
        if not chunk:
            break
        out += chunk
    return bytes(out)


def _same_start(a, b, size: int) -> bool:
    """Whether the next `size` bytes of two streams are equal."""
    while size > 0:
        block = min(size, _COMPARE_BLOCK)
        chunk = _read_exact(a, block)
        if len(chunk) != block or chunk != _read_exact(b, block):
            return False
        size -= block
    return True


def restore_from_zip(filename: str, zip_path: str, member: str, logger: Optional[LoggerLike] = None) -> None:
    """
    Bring back chat that an earlier run already zipped into zip_path (as `member`), so the download
    continues it instead of a new zip replacing it. A crash while zipping or while restoring can
    leave both the zip and chat files. The chat files are then kept if they continue the zipped
    chat (a restored chat that was downloaded further), replaced by it if they are part of it (the
    zipped files were being deleted), and otherwise added after it.
    Raises KeyError / zipfile.BadZipFile / OSError if the zip can't be read.
    """
    logger = logger or logging.getLogger(__name__)
    base = filename + RESUME_SUFFIX
    tmp = base + _TMP_SUFFIX
    _remove_if_exists(tmp, logger)
    files = [p for p in chat_files(filename) if os.path.exists(p) and _is_chat_file(p)]
    files_size = sum(os.path.getsize(p) for p in files)
    contained = False
    with zipfile.ZipFile(zip_path) as zipf:
        zipped_size = zipf.getinfo(member).file_size
        if files and files_size >= zipped_size:
            with zipf.open(member) as zipped, _ConcatReader(files) as current:
                if _same_start(zipped, current, zipped_size):
                    return
        if files and files_size <= zipped_size:
            with zipf.open(member) as zipped, _ConcatReader(files) as current:
                contained = _same_start(current, zipped, files_size)
            if not contained:
                with zipf.open(member) as zipped, _ConcatReader(files) as current:
                    _skip(zipped, zipped_size - files_size)
                    contained = _same_start(current, zipped, files_size)
        # Restore under a temporary name, so a crash never leaves a partly restored chat
        with zipf.open(member) as zipped, open(tmp, 'wb') as out:
            shutil.copyfileobj(zipped, out, _COMPARE_BLOCK)
    if files and not contained:
        logger.warning(f"The live chat files of {filename} don't match the live chat in {zip_path}, combining them")
        _truncate_partial_line(tmp, logger)
        for path in files:
            _append_filtered(tmp, path, logger)
    os.replace(tmp, base)
    for path in files:
        if path != base:
            _remove_if_exists(path, logger)
    logger.info(f"Continuing the live chat saved in {zip_path}")


def _skip(stream, size: int) -> None:
    while size > 0:
        chunk = stream.read(min(size, _COMPARE_BLOCK))
        if not chunk:
            return
        size -= len(chunk)


# --- Offsets from the stream start ---

def stream_start_ms(info_dict: dict) -> Optional[int]:
    """
    When the stream actually started (ms), from yt-dlp's release_timestamp (the broadcast's start
    time). None for an upcoming stream, whose release_timestamp is only the scheduled start.
    """
    if info_dict.get('live_status') not in ('is_live', 'was_live', 'post_live'):
        return None
    value = info_dict.get('release_timestamp')
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
        return int(value * 1000)
    return None


# The end of a live line as yt-dlp writes it, to change its offset without parsing the whole line
_LIVE_LINE_END = re.compile(rb'"videoOffsetTimeMsec": "(-?\d+)", "isLive": true\}\r?\n$')


class OffsetRebaser:
    """
    Measure live chat offsets from the stream start, like a chat replay, instead of from when the
    chat download started (yt-dlp's live offsets). Every live line moves by the same amount, so the
    offsets between lines stay as yt-dlp wrote them. Replay lines (already measured from the start of
    the video) and lines it doesn't recognise are left alone.
    """

    def __init__(self, shift: int):
        self.shift = shift

    @classmethod
    def for_files(cls, paths: Iterable[str], stream_start: Optional[int]) -> Optional["OffsetRebaser"]:
        """The rebaser for chat files (whose live offsets share one base), or None if nothing to change."""
        if stream_start is None:
            return None
        for path in paths:
            if os.path.exists(path):
                base = _first_live_start(path)
                if base is not None:
                    return cls(base - stream_start) if base != stream_start else None
        return None

    def line(self, raw: bytes) -> bytes:
        if not raw.endswith(b'\n'):
            return raw  # a line still being written
        match = _LIVE_LINE_END.search(raw)
        if match:
            offset = int(match.group(1)) + self.shift
            return raw[:match.start(1)] + str(offset).encode() + raw[match.end(1):]
        line = _parse_line(raw)
        if line is None or not line.get('isLive') or _int(line.get('videoOffsetTimeMsec')) is None:
            return raw
        line['videoOffsetTimeMsec'] = str(_int(line['videoOffsetTimeMsec']) + self.shift)
        return json.dumps(line, ensure_ascii=False).encode() + b'\n'


# --- Downloading ---

def _bind(method, args, kwargs):
    try:
        return inspect.signature(method).bind(*args, **kwargs)
    except (TypeError, ValueError):
        return None


class _ChatHooks:
    """
    The resume hooks of ResumableLiveChatFD. Every hook passes yt-dlp's arguments on unchanged
    when it can't handle them, so a change in yt-dlp only makes the download less efficient.
    """

    def __init__(self, state: ResumeState, should_stop: Callable[[], bool], logger: LoggerLike):
        self.state = state
        self.should_stop = should_stop
        self.logger = logger
        self.filter = ChatFilter(state, run_start=int(time.time() * 1000))
        self.seek_pending = state.lines > 0 and state.last_replay_offset is not None
        self.bytes_written = 0     # bytes given to yt-dlp's fragment write after filtering
        self.failed = False

    def warn_once(self, message: str) -> None:
        if not self.failed:
            self.logger.warning(f"{message} (yt-dlp may have changed); a resumed live chat is checked for "
                                "duplicates after the download instead")
        self.failed = True

    def before_request(self, method, args: tuple, kwargs: dict) -> tuple[tuple, dict]:
        if self.should_stop():
            raise DownloadCancelled('Live chat download stopped')
        if not self.seek_pending:
            return args, kwargs
        try:
            bound = _bind(method, args, kwargs)
            request_data = bound and bound.arguments.get('request_data')
            if not request_data:
                return args, kwargs
            data = json.loads(request_data)
            player_state = _get(data, 'currentPlayerState')
            if not isinstance(player_state, dict) or 'playerOffsetMs' not in player_state:
                return args, kwargs
            # The first chat replay request: start where the saved chat ends rather than at 0
            self.seek_pending = False
            target = max(self.state.last_replay_offset - REPLAY_LOOK_BEHIND_MS, 0)
            if (_int(player_state['playerOffsetMs']) or 0) < target:
                player_state['playerOffsetMs'] = str(target)
                self.logger.info(f"Resuming chat replay from {target / 1000:.1f}s")
                bound.arguments['request_data'] = json.dumps(data, ensure_ascii=False).encode() + b'\n'
                return bound.args, bound.kwargs
        except Exception as e:
            # Without the seek the replay is downloaded from the start and the saved part filtered out
            self.seek_pending = False
            self.logger.debug(f"Unable to seek the chat replay ({e!r})")
        return args, kwargs

    def before_write(self, method, args: tuple, kwargs: dict) -> tuple[tuple, dict]:
        bound = None
        try:
            bound = _bind(method, args, kwargs)
            if bound is None:
                self.warn_once("Unable to read yt-dlp's fragment write arguments")
                return args, kwargs
            # The written data: frag_content, or else the only bytes argument
            data_args = [k for k, v in bound.arguments.items() if isinstance(v, (bytes, bytearray))]
            name = 'frag_content' if 'frag_content' in data_args else (data_args[0] if len(data_args) == 1 else None)
            if name is None:
                self.warn_once("Unable to find the live chat data in yt-dlp's fragment write")
                return args, kwargs
            content = bytes(bound.arguments[name])
            if content:
                content = self.filter.filter(content)
                bound.arguments[name] = content
            self.bytes_written += len(content)
            return bound.args, bound.kwargs
        except Exception as e:
            self.warn_once(f"Unable to filter the live chat while downloading ({e!r})")
            return args, kwargs


def make_resumable(downloader_class: type) -> type:
    """A subclass of a live chat downloader class with the resume hooks (see _ChatHooks)."""

    class ResumableLiveChatFD(downloader_class):
        resume_hooks: Optional[_ChatHooks] = None

        def _download_fragment(self, *args, **kwargs):
            if self.resume_hooks:
                args, kwargs = self.resume_hooks.before_request(super()._download_fragment, args, kwargs)
            return super()._download_fragment(*args, **kwargs)

        def _append_fragment(self, *args, **kwargs):
            if self.resume_hooks:
                args, kwargs = self.resume_hooks.before_write(super()._append_fragment, args, kwargs)
            return super()._append_fragment(*args, **kwargs)

    return ResumableLiveChatFD


ResumableLiveChatFD = make_resumable(YoutubeLiveChatFD) if YoutubeLiveChatFD is not None else None


def live_chat_subtitle(info_dict: dict) -> dict:
    """The 'live_chat' subtitle entry yt-dlp downloads, built like the YouTube extractor does if missing."""
    sub = _get(info_dict, 'subtitles', 'live_chat', 0)
    if isinstance(sub, dict) and sub.get('url'):
        sub = dict(sub)
    else:
        video_id = info_dict.get('id')
        if not video_id:
            raise ValueError("No video id to download the live chat of")
        sub = {
            'url': f'https://www.youtube.com/watch?v={video_id}&bpctr=9999999999&has_verified=1',
            'video_id': video_id,
            'ext': 'json',
            'protocol': ('youtube_live_chat' if info_dict.get('live_status') in ('is_live', 'is_upcoming')
                         else 'youtube_live_chat_replay'),
        }
    if sub.get('http_headers') is None and info_dict.get('http_headers'):
        sub['http_headers'] = info_dict['http_headers']
    return sub


@dataclass
class ChatDownload:
    """The outcome of download_live_chat."""
    filename: str
    status: str                 # 'ended', 'stopped' or 'failed'
    resumed: bool               # saved chat was continued
    filtered: bool              # the new chat was filtered while downloading (see finalize)
    lines: int = 0
    skipped: int = 0
    stream_start: Optional[int] = None  # ms, see stream_start_ms


def download_live_chat(ydl: yt_dlp.YoutubeDL, info_dict: dict, filename: str, *,
                       should_stop: Optional[Callable[[], bool]] = None,
                       logger: Optional[LoggerLike] = None) -> ChatDownload:
    """
    Download the live chat of info_dict with yt-dlp, continuing the chat saved for filename by
    earlier runs. Blocks until the chat ends, the download fails, or should_stop() is true. The chat
    is left in chat_files(filename); call finalize() afterwards.
    """
    logger = logger or logging.getLogger(__name__)
    should_stop = should_stop or (lambda: False)
    sub = live_chat_subtitle(info_dict)
    state = consolidate(filename, logger)
    if state.lines:
        logger.info(f"Continuing the live chat saved in {filename + RESUME_SUFFIX}")
    hooks = _ChatHooks(state, should_stop, logger)

    def stop_hook(_progress):
        # Also called while yt-dlp downloads each chat fragment; works without the request hook
        if should_stop():
            raise DownloadCancelled('Live chat download stopped')

    try:
        fd = ResumableLiveChatFD(ydl, ydl.params) if ResumableLiveChatFD is not None else None
    except Exception as e:
        logger.warning(f"Unable to set up yt-dlp's live chat downloader for resuming ({e!r})")
        fd = None
    try:
        if fd is not None:
            fd.resume_hooks = hooks
            fd.add_progress_hook(stop_hook)
            result = fd.download(filename, sub, subtitle=True)
        else:
            ydl.add_progress_hook(stop_hook)
            result = ydl.dl(filename, sub, subtitle=True)
        success = result[0] if isinstance(result, tuple) else bool(result)
        status = 'ended' if success else 'failed'
    except DownloadCancelled:
        status = 'stopped'
    except Exception as e:
        # Whatever was downloaded is kept; the caller still finalizes and packages it
        logger.exception(f"Live chat download failed: {e!r}")
        status = 'failed'
    filtered = fd is not None and not hooks.failed and _new_chat_size(filename) == hooks.bytes_written
    return ChatDownload(filename, status, resumed=bool(state.lines), filtered=filtered,
                        lines=hooks.filter.lines, skipped=hooks.filter.skipped, stream_start=stream_start_ms(info_dict))


def _new_chat_size(filename: str) -> int:
    return sum(os.path.getsize(p) for p in chat_files(filename)[1:] if os.path.exists(p))


def finalize(download: ChatDownload, logger: Optional[LoggerLike] = None) -> Optional[str]:
    """
    After download_live_chat: merge all of the chat into one file, <filename>, with live offsets
    measured from the stream start when it is known (download.stream_start). Chat that couldn't be
    filtered while downloading (a yt-dlp change) is filtered now. Returns the file, or None if there
    is no chat. Each step replaces files atomically, so a crash here is recovered by the next
    download or finalize.
    """
    logger = logger or logging.getLogger(__name__)
    filename = download.filename
    base = filename + RESUME_SUFFIX
    if not any(os.path.exists(p) for p in chat_files(filename)):
        remove_fragment_files(filename, logger)
        return None
    if os.path.exists(base) or os.path.exists(filename + '.part'):
        consolidate(filename, logger, new_chat_filtered=download.filtered or not download.resumed)
    else:
        remove_fragment_files(filename, logger)
        os.replace(filename, base)
    if not os.path.exists(base):
        return None
    rebaser = OffsetRebaser.for_files([base], download.stream_start)
    if rebaser:
        tmp = base + _TMP_SUFFIX
        with open(base, 'rb') as src, open(tmp, 'wb') as out:
            for raw in src:
                out.write(rebaser.line(raw))
        os.replace(tmp, base)
        logger.info(f"Live chat offsets are now measured from the stream start ({rebaser.shift / 1000:+.1f}s)")
    os.replace(base, filename)
    return filename
