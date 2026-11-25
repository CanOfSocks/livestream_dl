"""
Tests for resuming yt-dlp's live chat download (live_chat.py). Run: python -m unittest discover -s tests -t .

FakeChatFD stands in for yt-dlp's YoutubeLiveChatFD: it writes chat fragments the way yt-dlp does
(through _download_fragment / _append_fragment into <name>.part, renamed when the chat ends), and
can be configured to behave like a changed yt-dlp.
"""
import glob
import json
import logging
import os
import shutil
import tempfile
import threading
import time
import unittest
import zipfile
from unittest import mock

import yt_dlp

import download_Live
import live_chat

LOGGER = logging.getLogger("tests")
LOGGER.addHandler(logging.NullHandler())
LOGGER.propagate = False

START = 1_700_000_000_000  # ms


def text_action(item_id, timestamp_ms, text="hi", tracking="t1"):
    return {
        "clickTrackingParams": tracking,
        "addChatItemAction": {"item": {"liveChatTextMessageRenderer": {
            "id": item_id, "timestampUsec": str(timestamp_ms * 1000),
            "message": {"runs": [{"text": text}]}, "trackingParams": tracking}}, "clientId": "c"},
    }


def welcome_action(item_id, timestamp_ms):
    return {"clickTrackingParams": item_id, "addChatItemAction": {"item": {"liveChatViewerEngagementMessageRenderer": {
        "id": item_id, "timestampUsec": str(timestamp_ms * 1000), "message": {"runs": [{"text": "Welcome"}]}}}}}


def live_line(action, start=START, offset=None):
    """A live chat line as yt-dlp writes it, with offsets measured from `start`."""
    if offset is None:
        offset = live_chat.action_timestamp(action) - start
    return {"replayChatItemAction": {"actions": [action]}, "videoOffsetTimeMsec": str(offset), "isLive": True}


def replay_line(action, offset):
    return {"clickTrackingParams": "r", "replayChatItemAction": {"actions": [action], "videoOffsetTimeMsec": str(offset)}}


def dump(lines):
    return b"".join(json.dumps(line).encode() + b"\n" for line in lines)


def load(paths):
    lines = []
    for path in paths:
        if os.path.exists(path):
            with open(path, "rb") as f:
                lines += [json.loads(raw) for raw in f]
    return lines


def ids(lines):
    return [live_chat.action_key(a).split(":", 1)[1] for line in lines for a in live_chat.line_actions(line)]


class FakeChatFD:
    """Writes the given fragments like yt-dlp's live chat downloader."""
    fragments: list = []
    open_mode = "ab"            # yt-dlp appends to an existing .part file
    bypass_hook = False         # a yt-dlp that writes without _append_fragment
    ended = True

    def __init__(self, ydl, params):
        self.ydl, self.params = ydl, params
        self._progress_hooks = []

    def add_progress_hook(self, hook):
        self._progress_hooks.append(hook)

    def _download_fragment(self, ctx, frag_url, info_dict, headers=None, request_data=None):
        return True

    def _append_fragment(self, ctx, frag_content):
        ctx["dest_stream"].write(frag_content)

    def download(self, filename, info_dict, subtitle=False):
        tmp = filename + ".part"
        with open(tmp, self.open_mode) as dest:
            ctx = {"dest_stream": dest}
            for index, fragment in enumerate(self.fragments):
                for hook in self._progress_hooks:
                    hook({"status": "downloading"})
                request = json.dumps({"currentPlayerState": {"playerOffsetMs": "0"}}).encode() if index else None
                self._download_fragment(ctx, "https://example.invalid", info_dict, None, request)
                if self.bypass_hook:
                    dest.write(fragment)
                else:
                    self._append_fragment(ctx, fragment)
        if not self.ended:
            raise yt_dlp.utils.DownloadCancelled("stopped")   # how yt-dlp ends a stopped download
        os.replace(tmp, filename)
        return True, True


class ChatTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.filename = os.path.join(self.tmp, "out.live_chat.json")
        self.saved, _, self.part = live_chat.chat_files(self.filename)
        self.ydl = yt_dlp.YoutubeDL({"quiet": True})

    def tearDown(self):
        self.ydl.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, path, data: bytes):
        with open(path, "wb") as f:
            f.write(data)

    def read(self, path) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    def download(self, fake: type, info=None, should_stop=None):
        info = info or {"id": "abcdefghijk", "live_status": "is_live"}
        with mock.patch.object(live_chat, "ResumableLiveChatFD", live_chat.make_resumable(fake)):
            result = live_chat.download_live_chat(self.ydl, info, self.filename, should_stop=should_stop, logger=LOGGER)
        live_chat.finalize(result, LOGGER)
        return result

    def chat(self):
        return load(live_chat.chat_files(self.filename))


class ParsingTests(unittest.TestCase):
    def test_timestamps(self):
        self.assertEqual(live_chat.action_timestamp(text_action("a", START)), START)
        ticker = {"addLiveChatTickerItemAction": {"item": {"liveChatTickerPaidMessageItemRenderer": {
            "id": "t", "showItemEndpoint": {"showLiveChatItemEndpoint": {"renderer": {
                "liveChatPaidMessageRenderer": {"id": "p", "timestampUsec": str(START * 1000)}}}}}}}}
        self.assertEqual(live_chat.action_timestamp(ticker), START)
        # yt-dlp doesn't measure offsets from these (the line keeps the previous offset)
        self.assertIsNone(live_chat.action_timestamp(welcome_action("w", START)))
        self.assertIsNone(live_chat.action_timestamp({"markChatItemAsDeletedAction": {"targetItemId": "a"}}))

    def test_keys(self):
        self.assertEqual(live_chat.action_key(text_action("a", START, tracking="x")),
                         live_chat.action_key(text_action("a", START, tracking="y")))
        # Per-session messages differ only in id and timestamp
        self.assertEqual(live_chat.action_key(welcome_action("w1", START)), live_chat.action_key(welcome_action("w2", START + 5)))
        action = {"clickTrackingParams": "x", "liveChatReportModerationStateCommand": {"value": 1}}
        same = {"clickTrackingParams": "y", "liveChatReportModerationStateCommand": {"value": 1}}
        other = {"clickTrackingParams": "x", "liveChatReportModerationStateCommand": {"value": 2}}
        self.assertEqual(live_chat.action_key(action), live_chat.action_key(same))
        self.assertNotEqual(live_chat.action_key(action), live_chat.action_key(other))

    def test_resume_state(self):
        live = live_chat.ResumeState.from_lines([live_line(text_action(f"m{i}", START + 1000 * i)) for i in range(1, 4)])
        self.assertEqual(live.live_start_time, START)
        self.assertIsNone(live.last_replay_offset)
        self.assertTrue(live.is_duplicate(text_action("m2", START + 2000, tracking="other")))
        self.assertFalse(live.is_duplicate(text_action("m9", START + 9000)))
        self.assertTrue(live.is_duplicate(text_action("old", START)))  # older than everything read
        replay = live_chat.ResumeState.from_lines([replay_line(text_action(f"m{i}", START + 1000 * i), 1000 * i) for i in range(1, 4)])
        self.assertEqual(replay.last_replay_offset, 3000)
        self.assertIsNone(replay.live_start_time)


class ChatFilterTests(unittest.TestCase):
    def setUp(self):
        saved = [live_line(text_action(f"m{i}", START + 1000 * i)) for i in range(1, 4)] + [live_line(welcome_action("w1", START), offset=0)]
        self.state = live_chat.ResumeState.from_lines(saved)

    def test_live_resume(self):
        restart = START + 60_000   # the new download measures offsets from here
        new = dump([live_line(text_action("m3", START + 3000), restart),
                    live_line(welcome_action("w2", restart), restart, offset=-57000),
                    live_line(text_action("m70", START + 70_000), restart),
                    live_line({"markChatItemAsDeletedAction": {"targetItemId": "m70"}}, offset=10_000),
                    {"unknown": "format"}]) + b"not json" + b"\n"
        chat_filter = live_chat.ChatFilter(self.state, run_start=restart)
        out = chat_filter.filter(new)
        lines = [json.loads(raw) if raw.startswith(b"{") else raw for raw in out.splitlines()]
        self.assertEqual(lines[0]["videoOffsetTimeMsec"], "70000")
        # A line without a timestamp gets the offset of the line before it, as yt-dlp does
        self.assertEqual(lines[1]["videoOffsetTimeMsec"], "70000")
        self.assertEqual(lines[2:], [{"unknown": "format"}, b"not json"])
        self.assertEqual(chat_filter.skipped, 2)  # m3 and the second welcome message
        # Filtering again changes nothing
        self.assertEqual(live_chat.ChatFilter(self.state, run_start=restart).filter(out), out)

    def test_unchanged_lines_keep_their_bytes(self):
        raw = b'{"replayChatItemAction": {"actions": [' + json.dumps(text_action("m9", START + 9000)).encode() + b']}, "videoOffsetTimeMsec": "9000", "isLive": true}\n'
        self.assertEqual(live_chat.ChatFilter(self.state).filter(raw), raw)

    def test_nothing_saved(self):
        data = dump([replay_line(text_action("m1", START), 0)])
        self.assertEqual(live_chat.ChatFilter(live_chat.ResumeState()).filter(data), data)


class ConsolidateTests(ChatTestBase):
    def test_incomplete_last_line_is_removed(self):
        complete = dump([live_line(text_action("a", START + 1000))])
        self.write(self.part, complete + b'{"replayChatItemAction": {"acti')
        state = live_chat.consolidate(self.filename, LOGGER)
        self.assertEqual(self.read(self.saved), complete)
        self.assertFalse(os.path.exists(self.part))
        self.assertEqual(state.lines, 1)

    def test_single_incomplete_line(self):
        self.write(self.part, b'{"replayChatItemAction"')
        self.assertEqual(live_chat.consolidate(self.filename, LOGGER).lines, 0)
        self.assertFalse(any(os.path.exists(p) for p in live_chat.chat_files(self.filename)))

    def test_other_formats_are_moved_aside(self):
        self.write(self.filename, b'[{"message": "chat-downloader format"}]')
        self.assertEqual(live_chat.consolidate(self.filename, LOGGER).lines, 0)
        self.assertTrue(os.path.exists(self.filename + ".old"))

    def test_all_earlier_files_are_gathered_in_order_without_duplicates(self):
        first = [replay_line(text_action(f"m{i}", START + 1000 * i), 1000 * i) for i in range(0, 3)]
        second = [replay_line(text_action(f"m{i}", START + 1000 * i), 1000 * i) for i in range(1, 5)]
        third = [replay_line(text_action(f"m{i}", START + 1000 * i), 1000 * i) for i in range(4, 7)]
        self.write(self.saved, dump(first))
        self.write(self.filename, dump(second))
        self.write(self.part, dump(third) + b'{"cut off')
        state = live_chat.consolidate(self.filename, LOGGER)
        self.assertEqual(ids(load([self.saved])), [f"m{i}" for i in range(7)])
        self.assertFalse(os.path.exists(self.filename) or os.path.exists(self.part))
        self.assertEqual(state.last_replay_offset, 6000)


class DownloadTests(ChatTestBase):
    def saved_live_chat(self):
        self.write(self.part, dump([live_line(text_action(f"m{i}", START + 1000 * i)) for i in range(1, 4)]
                                   + [live_line(welcome_action("w1", START), offset=3000)]))

    def new_live_fragments(self):
        restart = START + 60_000
        return [dump([live_line(text_action(f"m{i}", START + 1000 * i), restart) for i in (2, 3)]
                     + [live_line(welcome_action("w2", restart), restart, offset=-57000)]),
                b"",
                dump([live_line(text_action(f"m{i}", START + 1000 * i), restart) for i in (70, 71)])]

    def check_resumed_live_chat(self):
        lines = self.chat()
        welcome = next(i for i in ids(lines) if i.startswith("liveChatViewerEngagementMessageRenderer#"))
        self.assertEqual(ids(lines), ["m1", "m2", "m3", welcome, "m70", "m71"])
        self.assertEqual([line["videoOffsetTimeMsec"] for line in lines[-2:]], ["70000", "71000"])

    def test_live_resume_filtered_while_downloading(self):
        self.saved_live_chat()
        fake = type("Fake", (FakeChatFD,), {"fragments": self.new_live_fragments()})
        result = self.download(fake)
        self.assertEqual((result.status, result.resumed, result.filtered), ("ended", True, True))
        self.check_resumed_live_chat()
        # Merged into one file once finished
        self.assertEqual([p for p in live_chat.chat_files(self.filename) if os.path.exists(p)], [self.filename])

    def test_yt_dlp_truncating_existing_files_loses_nothing(self):
        self.saved_live_chat()
        fake = type("Fake", (FakeChatFD,), {"fragments": self.new_live_fragments(), "open_mode": "wb"})
        self.download(fake)
        self.check_resumed_live_chat()

    def test_yt_dlp_writing_without_the_hook_is_filtered_afterwards(self):
        self.saved_live_chat()
        fake = type("Fake", (FakeChatFD,), {"fragments": self.new_live_fragments(), "bypass_hook": True})
        result = self.download(fake)
        self.assertFalse(result.filtered)
        self.check_resumed_live_chat()

    def test_hook_with_changed_signature(self):
        self.saved_live_chat()

        class Renamed(FakeChatFD):
            fragments = self.new_live_fragments()

            def _append_fragment(self, ctx, data, frag_index=None):   # renamed parameter, extra argument
                ctx["dest_stream"].write(data)

        self.assertTrue(self.download(Renamed).filtered)
        self.check_resumed_live_chat()

        for path in live_chat.chat_files(self.filename):
            if os.path.exists(path):
                os.remove(path)
        self.saved_live_chat()
        # Arguments that can't be read at all
        with mock.patch.object(live_chat, "_bind", return_value=None):
            result = self.download(type("Fake", (FakeChatFD,), {"fragments": self.new_live_fragments()}))
        self.assertFalse(result.filtered)
        self.check_resumed_live_chat()

    def test_replay_seeks_to_saved_end(self):
        self.write(self.part, dump([replay_line(text_action(f"m{i}", START + 1000 * i), 1000 * i) for i in range(0, 20)]))
        fragments = [b"", dump([replay_line(text_action(f"m{i}", START + 1000 * i), 1000 * i) for i in range(14, 25)])]
        requests = []

        class Recording(FakeChatFD):
            def _download_fragment(self, ctx, frag_url, info_dict, headers=None, request_data=None):
                requests.append(request_data)
                return True

        Recording.fragments = fragments
        self.download(Recording, info={"id": "abcdefghijk", "live_status": "was_live"})
        # Requested from 5s before the last saved offset instead of 0
        self.assertEqual(json.loads(requests[1])["currentPlayerState"]["playerOffsetMs"], "14000")
        self.assertEqual(ids(self.chat()), [f"m{i}" for i in range(25)])

    def test_stop(self):
        fake = type("Fake", (FakeChatFD,), {"fragments": [b"", b""], "ended": False})
        result = self.download(fake, should_stop=lambda: True)
        self.assertEqual(result.status, "stopped")

    def test_without_the_downloader_class(self):
        self.saved_live_chat()
        fragments = self.new_live_fragments()

        def fake_dl(name, info, subtitle=False, test=False):
            with open(name + ".part", "wb") as f:
                f.write(b"".join(fragments))
            os.replace(name + ".part", name)
            return True, True

        with mock.patch.object(live_chat, "ResumableLiveChatFD", None), mock.patch.object(self.ydl, "dl", fake_dl):
            result = live_chat.download_live_chat(self.ydl, {"id": "abcdefghijk", "live_status": "is_live"}, self.filename, logger=LOGGER)
        live_chat.finalize(result, LOGGER)
        self.assertEqual((result.status, result.filtered), ("ended", False))
        self.check_resumed_live_chat()

    def test_subtitle_entry_built_when_missing(self):
        self.assertEqual(live_chat.live_chat_subtitle({"id": "abcdefghijk", "live_status": "is_live"})["protocol"], "youtube_live_chat")
        self.assertEqual(live_chat.live_chat_subtitle({"id": "abcdefghijk", "live_status": "was_live"})["protocol"],
                         "youtube_live_chat_replay")


class PackagingTests(ChatTestBase):
    def setUp(self):
        super().setUp()
        self.base = os.path.join(self.tmp, "out")
        self.zip = self.base + ".live_chat.zip"

    def zip_chat(self, data: bytes):
        with zipfile.ZipFile(self.zip, "w") as z:
            z.writestr("out.live_chat.json", data)

    def restore(self):
        download_Live.LiveStreamDownloader(logger=LOGGER)._restore_packaged_live_chat(self.base, self.filename)

    def chat_lines(self, ids_):
        return dump([replay_line(text_action(f"m{i}", START + 1000 * i), 1000 * i) for i in ids_])

    def test_zipped_chat_is_restored(self):
        self.zip_chat(b'{"a": 1}\n')
        self.restore()
        self.assertEqual(self.read(self.saved), b'{"a": 1}\n')

    def test_files_continuing_the_zipped_chat_are_kept(self):
        # A restored chat that was downloaded further before the next crash
        self.zip_chat(self.chat_lines(range(3)))
        self.write(self.saved, self.chat_lines(range(3)))
        self.write(self.part, self.chat_lines(range(3, 5)) + b'{"cut')
        self.restore()
        self.assertEqual(self.read(self.saved), self.chat_lines(range(3)))
        self.assertTrue(os.path.exists(self.part))

    def test_crash_while_deleting_zipped_files(self):
        # Zipped, then the .resume file was deleted before the crash, the .part not yet
        self.zip_chat(self.chat_lines(range(3)) + self.chat_lines(range(3, 6)))
        self.write(self.part, self.chat_lines(range(3, 6)))
        self.restore()
        self.assertEqual(ids(self.chat()), [f"m{i}" for i in range(6)])
        self.assertFalse(os.path.exists(self.part))

    def test_crash_while_restoring(self):
        self.zip_chat(self.chat_lines(range(6)))
        self.write(self.saved + ".tmp", self.chat_lines(range(2)))
        self.restore()
        self.assertEqual(self.read(self.saved), self.chat_lines(range(6)))
        self.assertFalse(os.path.exists(self.saved + ".tmp"))

    def test_unrelated_files_are_added_after_the_zipped_chat(self):
        self.zip_chat(self.chat_lines(range(3)))
        self.write(self.part, self.chat_lines(range(2, 5)))
        self.restore()
        self.assertEqual(ids(self.chat()), [f"m{i}" for i in range(5)])

    def test_unreadable_zips_are_kept(self):
        for expected in (".old", ".old.1"):
            self.write(self.zip, b"not a zip")
            self.restore()
            self.assertTrue(os.path.exists(self.zip + expected))
        self.assertFalse(os.path.exists(self.zip))

    def test_zip_holds_all_chat_files_in_order(self):
        self.write(self.saved, b'{"a": 1}\n')
        self.write(self.filename, b'{"b": 2}\n')
        self.write(self.part, b'{"c": 3}\n')
        coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
        self.assertIsNotNone(coordinator._package_live_chat(self.base, livechat_filename=self.filename))
        with zipfile.ZipFile(self.zip) as z:
            self.assertEqual(z.read("out.live_chat.json"), b'{"a": 1}\n{"b": 2}\n{"c": 3}\n')
        self.assertFalse(any(os.path.exists(p) for p in live_chat.chat_files(self.filename)))


class RepeatedCrashTests(ChatTestBase):
    """A live chat download that is interrupted several times, each time leaving different files behind."""

    stream_start = None   # ms; None: unknown, offsets stay measured from the first chat download

    def setUp(self):
        super().setUp()
        self.base = os.path.join(self.tmp, "out")
        self.zip = self.base + ".live_chat.zip"
        self.info = {"id": "abcdefghijk", "live_status": "is_live"}
        if self.stream_start is not None:
            self.info["release_timestamp"] = self.stream_start / 1000

    def coordinator(self):
        coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
        coordinator._live_chat_stream_start = live_chat.stream_start_ms(self.info)
        return coordinator

    def run_download(self, run: int, ended: bool = False, finalize: bool = False):
        """
        Download run `run`: it starts 20s after the previous one, sends 5s of chat history first and
        20 new messages. finalize: the run ended normally (a crash never gets that far).
        """
        restart = START + 20_000 * run
        first = max(0, 20 * run - 5)
        history = [live_line(text_action(f"m{i}", START + 1000 * i), restart) for i in range(first, 20 * run)]
        # The welcome message comes last and gets the offset of the line before it
        last_offset = int(history[-1]["videoOffsetTimeMsec"]) if history else 0
        history.append(live_line(welcome_action(f"w{run}", restart), restart, offset=last_offset))
        new = [live_line(text_action(f"m{i}", START + 1000 * i), restart) for i in range(20 * run, 20 * run + 20)]
        test = self

        class Run(FakeChatFD):
            fragments = [b"", dump(history), dump(new[:10]), dump(new[10:])]

            def download(self, filename, info_dict, subtitle=False):
                # yt-dlp would reuse fragment files left by a crash as its own answers
                test.assertEqual(glob.glob(glob.escape(filename) + "*Frag*"), [])
                return super().download(filename, info_dict, subtitle)

        Run.ended = ended
        if os.path.exists(self.zip):
            self.coordinator()._restore_packaged_live_chat(self.base, self.filename)
        with mock.patch.object(live_chat, "ResumableLiveChatFD", live_chat.make_resumable(Run)):
            result = live_chat.download_live_chat(self.ydl, self.info, self.filename, logger=LOGGER)
        self.assertNotEqual(result.status, "failed")
        if finalize:
            self.assertEqual(live_chat.finalize(result, LOGGER), self.filename)
            # One file
            self.assertEqual([p for p in live_chat.chat_files(self.filename) if os.path.exists(p)], [self.filename])

    def test_interrupted_five_times(self):
        # 1. Killed while waiting between chat requests: a partial line and yt-dlp's fragment files remain
        self.run_download(0)
        with open(self.part, "ab") as f:
            f.write(b'{"replayChatItemAction": {"acti')
        self.write(self.part + "-Frag3", b'{"stale": "response"}')
        self.write(self.part + "-Frag4.part", b'{"stale')

        # 2. Killed while the saved chat was being gathered (part of the .part already added)
        self.run_download(1)
        live_chat.consolidate(self.filename, LOGGER)
        new_lines = self.read(self.saved).splitlines(keepends=True)
        self.write(self.part, b"".join(new_lines[-15:]))
        self.write(self.saved, b"".join(new_lines[:-8]) + new_lines[-8][:10])

        # 3. Chat thread abandoned and zipped by the coordinator, then killed while the zipped
        #    files were being deleted
        self.run_download(2)
        self.coordinator()._package_live_chat(self.base, livechat_filename=self.filename, snapshot=True)
        os.remove(self.saved)

        # 4. Stopped normally (merged into one file and zipped), then killed while restoring the zip
        self.run_download(3, finalize=True)
        self.coordinator()._package_live_chat(self.base, livechat_filename=self.filename)
        self.write(self.saved + ".tmp", b'{"partly": "restored"}\n')

        # 5. Killed while merging the finished chat (offsets already moved, not renamed yet)
        self.run_download(4)
        live_chat.consolidate(self.filename, LOGGER)
        rebaser = live_chat.OffsetRebaser.for_files([self.saved], live_chat.stream_start_ms(self.info))
        if rebaser:
            self.write(self.saved, b"".join(rebaser.line(raw) for raw in self.read(self.saved).splitlines(keepends=True)))
        self.write(self.saved + ".tmp", b'{"half": "rewritten"}\n')

        self.run_download(5, ended=True, finalize=True)
        lines = self.chat()
        chat_ids = [i for i in ids(lines) if not i.startswith("liveChatViewerEngagementMessageRenderer")]
        self.assertEqual(chat_ids, [f"m{i}" for i in range(120)])
        self.assertEqual(len(ids(lines)) - len(chat_ids), 1)  # one welcome message
        # Every offset is measured from the stream start (or the first chat download's start)
        origin = self.stream_start if self.stream_start is not None else START
        for line in lines:
            timestamp = live_chat.action_timestamp(live_chat.line_actions(line)[0])
            if timestamp is not None:
                self.assertEqual(int(line["videoOffsetTimeMsec"]), timestamp - origin)
        self.assertEqual(glob.glob(glob.escape(self.filename) + "*Frag*"), [])


class RepeatedCrashWithStreamStartTests(RepeatedCrashTests):
    stream_start = START - 600_000   # the stream started 10 minutes before the chat download


class MergeTests(ChatTestBase):
    def test_finished_chat_is_one_file_with_stream_start_offsets(self):
        self.write(self.saved, dump([live_line(text_action(f"m{i}", START + 1000 * i)) for i in range(3)]))
        self.write(self.part, dump([live_line(text_action(f"m{i}", START + 1000 * i)) for i in range(3, 5)]
                                   + [live_line(welcome_action("w", START), offset=4000)]))
        download = live_chat.ChatDownload(self.filename, "ended", resumed=True, filtered=True, stream_start=START - 90_000)
        self.assertEqual(live_chat.finalize(download, LOGGER), self.filename)
        self.assertEqual([p for p in live_chat.chat_files(self.filename) if os.path.exists(p)], [self.filename])
        lines = load([self.filename])
        self.assertEqual([line["videoOffsetTimeMsec"] for line in lines], ["90000", "91000", "92000", "93000", "94000", "94000"])
        # Merging again changes nothing
        before = self.read(self.filename)
        live_chat.finalize(download, LOGGER)
        self.assertEqual(self.read(self.filename), before)

    def test_replay_offsets_are_kept(self):
        data = dump([replay_line(text_action("m1", START), 5000)])
        self.write(self.filename, data)
        download = live_chat.ChatDownload(self.filename, "ended", resumed=False, filtered=True, stream_start=START - 90_000)
        live_chat.finalize(download, LOGGER)
        self.assertEqual(self.read(self.filename), data)

    def test_upcoming_release_timestamp_is_not_the_start(self):
        self.assertIsNone(live_chat.stream_start_ms({"live_status": "is_upcoming", "release_timestamp": 1_700_000_000}))
        self.assertEqual(live_chat.stream_start_ms({"live_status": "was_live", "release_timestamp": 1_700_000_000}), START)

    def test_rebaser_keeps_unknown_and_partial_lines(self):
        rebaser = live_chat.OffsetRebaser(1000)
        self.assertEqual(rebaser.line(b'{"x": 1}\n'), b'{"x": 1}\n')
        self.assertEqual(rebaser.line(b'{"replayChatItemAction": {"actions": []}, "videoOffsetTimeMsec": "5", "isLive": true}'),
                         b'{"replayChatItemAction": {"actions": []}, "videoOffsetTimeMsec": "5", "isLive": true}')
        # Not in yt-dlp's key order: parsed instead
        self.assertEqual(json.loads(rebaser.line(b'{"isLive": true, "videoOffsetTimeMsec": "5", "replayChatItemAction": {}}\n'))["videoOffsetTimeMsec"], "1005")

    def test_abandoned_chat_is_zipped_with_stream_start_offsets(self):
        base = os.path.join(self.tmp, "out")
        self.write(self.part, dump([live_line(text_action("m1", START + 1000))]))
        coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
        coordinator._live_chat_stream_start = START - 1000
        coordinator._package_live_chat(base, livechat_filename=self.filename, snapshot=True)
        with zipfile.ZipFile(base + ".live_chat.zip") as z:
            self.assertEqual(json.loads(z.read("out.live_chat.json"))["videoOffsetTimeMsec"], "2000")
        # The source is left for the chat thread, unchanged
        self.assertEqual(load([self.part])[0]["videoOffsetTimeMsec"], "1000")


class ChatRetryTests(ChatTestBase):
    """A live chat that ends while the stream is still being downloaded is downloaded again."""

    def setUp(self):
        super().setUp()
        self.coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
        self.info = {"id": "abcdefghijk", "live_status": "is_live", "release_timestamp": (START - 60_000) / 1000}
        self.calls = []
        poll = mock.patch.object(download_Live, "LIVE_CHAT_RETRY_POLL", 0.01)
        poll.start()
        self.addCleanup(poll.stop)
        # The chat never refreshes the info.json itself
        no_refresh = mock.patch.object(self.coordinator, "refresh_info_json", side_effect=AssertionError("refreshed by the chat"))
        no_refresh.start()
        self.addCleanup(no_refresh.stop)

    def refreshed(self, live_status="is_live", age=-60.0):
        """An info.json refresh made `-age` seconds after now (the chat ends now)."""
        return dict(self.info, live_status=live_status, refresh_time=time.time() - age, title="refreshed")

    def run_chat(self, *after_runs):
        """
        Run the chat thread's download. Run i writes messages m(10i)..m(10i+9) (with the last two of
        the run before it again), then after_runs[i]() is called, as if while it ran.
        """
        test = self

        class Run(FakeChatFD):
            def download(self, filename, info_dict, subtitle=False):
                run = len(test.calls)
                test.calls.append(info_dict)
                restart = START + 10_000 * run
                first = max(0, 10 * run - 2)
                type(self).fragments = [dump([live_line(text_action(f"m{i}", START + 1000 * i), restart)
                                              for i in range(first, 10 * run + 10)])]
                result = super().download(filename, info_dict, subtitle)
                if run < len(after_runs):
                    after_runs[run]()
                return result

        self.infos = []
        original = live_chat.download_live_chat

        def recording(ydl, info_dict, *args, **kwargs):
            self.infos.append(info_dict)
            return original(ydl, info_dict, *args, **kwargs)

        self.coordinator._live_chat_stream_start = live_chat.stream_start_ms(self.info)
        with mock.patch.object(live_chat, "ResumableLiveChatFD", live_chat.make_resumable(Run)), \
                mock.patch.object(live_chat, "download_live_chat", recording):
            self.coordinator._download_chat_with_ytdlp(self.info, {}, self.filename, None)

    def segments_continue(self, seconds=120.0):
        self.coordinator._last_segment_time = time.time() + seconds

    def test_downloaded_again_with_newer_refresh_while_live_and_downloading(self):
        def first():
            self.coordinator.refresh_json = self.refreshed()
            self.segments_continue()

        self.run_chat(first, self.coordinator._media_finished.set)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual([info.get("title") for info in self.infos], [None, "refreshed"])
        # One file, every message once, offsets from the stream start
        self.assertEqual([p for p in live_chat.chat_files(self.filename) if os.path.exists(p)], [self.filename])
        lines = load([self.filename])
        self.assertEqual(ids(lines), [f"m{i}" for i in range(20)])
        self.assertEqual([int(line["videoOffsetTimeMsec"]) for line in lines], [60_000 + 1000 * i for i in range(20)])

    def test_downloaded_again_for_each_newer_refresh(self):
        def again():
            self.coordinator.refresh_json = self.refreshed()
            self.segments_continue()

        self.run_chat(again, again, self.coordinator._media_finished.set)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(ids(load([self.filename])), [f"m{i}" for i in range(30)])

    def test_not_again_when_the_stream_is_no_longer_live(self):
        for status in ("post_live", "was_live", None):
            with self.subTest(status=status):
                self.calls.clear()
                self.coordinator.refresh_json = {}

                def ended(status=status):
                    self.coordinator.refresh_json = self.refreshed(live_status=status)
                    self.segments_continue()

                self.run_chat(ended)
                self.assertEqual(len(self.calls), 1)

    def test_not_again_without_segments_after_the_refresh(self):
        def stalled():
            self.coordinator.refresh_json = self.refreshed()
            self.coordinator._last_segment_time = time.time()   # before the refresh
            threading.Timer(0.2, self.coordinator._media_finished.set).start()

        self.run_chat(stalled)
        self.assertEqual(len(self.calls), 1)

    def test_downloaded_again_with_a_refresh_from_before_the_chat_ended(self):
        # Newer than the info.json the chat used is enough: yt-dlp starts at the live point anyway
        def earlier_refresh():
            self.coordinator.refresh_json = self.refreshed(age=10.0)
            self.segments_continue()

        self.run_chat(earlier_refresh, self.coordinator._media_finished.set)
        self.assertEqual([info.get("title") for info in self.infos], [None, "refreshed"])

    def test_not_again_without_a_newer_refresh(self):
        self.info["refresh_time"] = time.time() + 3600   # the newest refresh already

        def same_refresh():
            self.coordinator.refresh_json = dict(self.info)
            self.segments_continue()
            threading.Timer(0.2, self.coordinator._media_finished.set).start()

        self.run_chat(same_refresh)
        self.assertEqual(len(self.calls), 1)

    def test_not_again_when_segments_stop_soon_after_the_chat_ended(self):
        # The stream's last few segments, still downloaded after the chat ended with the stream
        def last_segments():
            self.coordinator.refresh_json = self.refreshed(age=10.0)
            self.coordinator._last_segment_time = time.time() + download_Live.LIVE_CHAT_RETRY_SEGMENT_DELAY / 2
            threading.Timer(0.2, self.coordinator._media_finished.set).start()

        self.run_chat(last_segments)
        self.assertEqual(len(self.calls), 1)

    def test_asks_for_an_early_refresh_while_segments_continue(self):
        # E.g. the chat failed during a network outage: the stream downloaders' next regular
        # refresh may be hours away, so the chat asks them for one
        delay = mock.patch.object(download_Live, "LIVE_CHAT_REFRESH_REQUEST_DELAY", 0.05)
        delay.start()
        self.addCleanup(delay.stop)
        requests = []

        def stream_downloaders_refresh():
            requests.append(time.time())
            self.coordinator.refresh_json = self.refreshed(age=0.0)
            self.segments_continue()

        def failed():
            self.segments_continue()

        with mock.patch.object(self.coordinator, "request_refresh", side_effect=stream_downloaders_refresh):
            self.run_chat(failed, self.coordinator._media_finished.set)
        self.assertEqual(len(requests), 1)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual([info.get("title") for info in self.infos], [None, "refreshed"])

    def test_no_early_refresh_without_segments(self):
        delay = mock.patch.object(download_Live, "LIVE_CHAT_REFRESH_REQUEST_DELAY", 0.0)
        delay.start()
        self.addCleanup(delay.stop)
        threading.Timer(0.2, self.coordinator._media_finished.set).start()
        with mock.patch.object(self.coordinator, "request_refresh") as request:
            self.run_chat()
        request.assert_not_called()

    def test_waiting_ends_when_the_chat_is_stopped(self):
        threading.Timer(0.2, self.coordinator.chat_stop.set).start()
        self.run_chat()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(ids(load([self.filename])), [f"m{i}" for i in range(10)])

    def test_stopped_chat_is_not_downloaded_again(self):
        self.coordinator.refresh_json = self.refreshed(age=-3600.0)
        self.segments_continue(7200.0)

        class Stopped(FakeChatFD):
            ended = False
            fragments = [dump([live_line(text_action("m0", START))])]

        calls = []
        original = live_chat.download_live_chat

        def counting(*args, **kwargs):
            calls.append(1)
            return original(*args, **kwargs)

        with mock.patch.object(live_chat, "ResumableLiveChatFD", live_chat.make_resumable(Stopped)), \
                mock.patch.object(live_chat, "download_live_chat", counting):
            self.coordinator._download_chat_with_ytdlp(self.info, {}, self.filename, None)
        self.assertEqual(len(calls), 1)

    def test_media_downloads_mark_finished_and_segments(self):
        coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
        before = time.time()
        coordinator.note_segment_downloaded()
        self.assertGreaterEqual(coordinator._last_segment_time, before)
        self.assertFalse(coordinator._media_finished.is_set())


if __name__ == "__main__":
    unittest.main()
