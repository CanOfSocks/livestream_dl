"""
Tests for resuming yt-dlp's live chat download (live_chat.py). Run: python -m unittest discover -s tests -t .

FakeChatFD stands in for yt-dlp's YoutubeLiveChatFD: it writes chat fragments the way yt-dlp does
(through _download_fragment / _append_fragment into <name>.part, renamed when the chat ends), and
can be configured to behave like a changed yt-dlp.
"""
import json
import logging
import os
import shutil
import tempfile
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
        if self.ended:
            os.replace(tmp, filename)
        return self.ended, True


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
        # The saved chat is never handed to yt-dlp
        self.assertTrue(os.path.exists(self.saved))

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
    def test_zipped_chat_is_restored_for_resume(self):
        base = os.path.join(self.tmp, "out")
        with zipfile.ZipFile(base + ".live_chat.zip", "w") as z:
            z.writestr("out.live_chat.json", '{"a": 1}\n')
        download_Live.LiveStreamDownloader(logger=LOGGER)._restore_packaged_live_chat(base, self.filename)
        self.assertEqual(self.read(self.saved), b'{"a": 1}\n')

    def test_existing_files_take_precedence_over_the_zip(self):
        base = os.path.join(self.tmp, "out")
        with zipfile.ZipFile(base + ".live_chat.zip", "w") as z:
            z.writestr("out.live_chat.json", '{"old": 1}\n')
        self.write(self.part, b'{"new": 1}\n')
        download_Live.LiveStreamDownloader(logger=LOGGER)._restore_packaged_live_chat(base, self.filename)
        self.assertFalse(os.path.exists(self.saved))

    def test_zip_holds_all_chat_files_in_order(self):
        base = os.path.join(self.tmp, "out")
        self.write(self.saved, b'{"a": 1}\n')
        self.write(self.filename, b'{"b": 2}\n')
        self.write(self.part, b'{"c": 3}\n')
        coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
        self.assertIsNotNone(coordinator._package_live_chat(base, livechat_filename=self.filename))
        with zipfile.ZipFile(base + ".live_chat.zip") as z:
            self.assertEqual(z.read("out.live_chat.json"), b'{"a": 1}\n{"b": 2}\n{"c": 3}\n')
        self.assertFalse(any(os.path.exists(p) for p in live_chat.chat_files(self.filename)))


if __name__ == "__main__":
    unittest.main()
