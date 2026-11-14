"""Tests for resuming yt-dlp's live chat download (live_chat.py). Run: python -m unittest discover -s tests -t ."""
import json
import logging
import os
import shutil
import tempfile
import unittest
import zipfile

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
    return {"addChatItemAction": {"item": {"liveChatViewerEngagementMessageRenderer": {
        "id": item_id, "timestampUsec": str(timestamp_ms * 1000), "message": {"runs": [{"text": "Welcome"}]}}}}}


def live_line(action, start=START):
    # yt-dlp gives an action without a recognised timestamp (e.g. the welcome message) the previous offset
    timestamp = live_chat.YoutubeLiveChatFD.parse_live_timestamp(action) or start
    return {"replayChatItemAction": {"actions": [action]}, "videoOffsetTimeMsec": str(timestamp - start), "isLive": True}


def replay_line(action, offset):
    return {"clickTrackingParams": "r", "replayChatItemAction": {"actions": [action], "videoOffsetTimeMsec": str(offset)}}


def dump(lines):
    return b"".join(json.dumps(line).encode() + b"\n" for line in lines)


class ChatFileTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.filename = os.path.join(self.tmp, "out.live_chat.json")
        self.part = self.filename + ".part"

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, path, data: bytes):
        with open(path, "wb") as f:
            f.write(data)

    def read(self, path) -> bytes:
        with open(path, "rb") as f:
            return f.read()


class ResumeFileTests(ChatFileTestBase):
    def test_incomplete_last_line_is_removed(self):
        complete = dump([live_line(text_action("a", START + 1000))])
        self.write(self.part, complete + b'{"replayChatItemAction": {"acti')
        state = live_chat.prepare_resume_file(self.part, LOGGER)
        self.assertEqual(self.read(self.part), complete)
        self.assertEqual(state.lines, 1)

    def test_single_incomplete_line_empties_the_file(self):
        self.write(self.part, b'{"replayChatItemAction"')
        state = live_chat.prepare_resume_file(self.part, LOGGER)
        self.assertEqual(self.read(self.part), b"")
        self.assertEqual(state.lines, 0)

    def test_other_formats_are_moved_aside(self):
        self.write(self.part, b'[{"message": "chat-downloader format"}]')
        state = live_chat.prepare_resume_file(self.part, LOGGER)
        self.assertFalse(os.path.exists(self.part))
        self.assertTrue(os.path.exists(self.part + ".old"))
        self.assertEqual(state.lines, 0)

    def test_missing_file(self):
        self.assertEqual(live_chat.prepare_resume_file(self.part, LOGGER).lines, 0)

    def test_finished_file_and_part_are_merged_in_order(self):
        self.write(self.filename, b'{"a": 1}')  # no trailing newline
        self.write(self.part, b'{"b": 2}\n')
        self.assertEqual(live_chat.merge_into_part(self.filename, LOGGER), self.part)
        self.assertFalse(os.path.exists(self.filename))
        self.assertEqual(self.read(self.part), b'{"a": 1}\n{"b": 2}\n')

    def test_finished_file_is_renamed_to_part(self):
        self.write(self.filename, b'{"a": 1}\n')
        live_chat.merge_into_part(self.filename, LOGGER)
        self.assertEqual(self.read(self.part), b'{"a": 1}\n')
        self.assertFalse(os.path.exists(self.filename))

    def test_resume_state_from_live_file(self):
        lines = [live_line(text_action(f"m{i}", START + 1000 * i)) for i in range(1, 4)]
        state = live_chat.ResumeState.from_lines(lines)
        self.assertEqual(state.live_start_time, START)
        self.assertIsNone(state.last_replay_offset)
        self.assertEqual(state.oldest_timestamp, START + 1000)
        self.assertTrue(state.is_duplicate(text_action("m2", START + 2000, tracking="other")))
        self.assertFalse(state.is_duplicate(text_action("m9", START + 9000)))
        # Older than everything that was read: already saved
        self.assertTrue(state.is_duplicate(text_action("old", START)))

    def test_resume_state_from_replay_file(self):
        lines = [replay_line(text_action(f"m{i}", START + 1000 * i), 1000 * i) for i in range(1, 4)]
        state = live_chat.ResumeState.from_lines(lines)
        self.assertEqual(state.last_replay_offset, 3000)
        self.assertIsNone(state.live_start_time)

    def test_actions_without_id_match_by_content(self):
        action = {"clickTrackingParams": "x", "liveChatReportModerationStateCommand": {"value": 1}}
        same = {"clickTrackingParams": "y", "liveChatReportModerationStateCommand": {"value": 1}}
        other = {"clickTrackingParams": "x", "liveChatReportModerationStateCommand": {"value": 2}}
        self.assertEqual(live_chat.action_key(action), live_chat.action_key(same))
        self.assertNotEqual(live_chat.action_key(action), live_chat.action_key(other))


class ResumableFDTests(unittest.TestCase):
    def setUp(self):
        self.ydl = yt_dlp.YoutubeDL({"quiet": True})

    def tearDown(self):
        self.ydl.close()

    def fd(self, existing_lines, protocol):
        fd = live_chat.ResumableLiveChatFD(self.ydl, self.ydl.params, live_chat.ResumeState.from_lines(existing_lines))
        fd._is_live = protocol == "youtube_live_chat"
        fd._seek_pending = not fd._is_live and fd.resume.last_replay_offset is not None
        return fd

    def test_live_resume_skips_saved_chat_and_continues_offsets(self):
        existing = [live_line(text_action(f"m{i}", START + 1000 * i)) for i in range(1, 4)]
        fd = self.fd(existing, "youtube_live_chat")
        # yt-dlp restarted 60s later and measures offsets from its new start time
        restart = START + 60_000
        fragment = dump([live_line(welcome_action("w2", START + 61_000), restart),
                         *(live_line(text_action(f"m{i}", START + 1000 * i), restart) for i in (2, 3, 70, 71))])
        written = [json.loads(line) for line in fd._rewrite(fragment).splitlines()]
        self.assertEqual([live_chat.action_key(live_chat.line_actions(line)[0]) for line in written],
                         ["addChatItemAction:m70", "addChatItemAction:m71"])
        # Offsets are relative to the original start again
        self.assertEqual([line["videoOffsetTimeMsec"] for line in written], ["70000", "71000"])
        self.assertEqual(fd.skipped, 3)
        # The welcome message is only dropped from the first fragment
        self.assertEqual(fd._rewrite(dump([live_line(welcome_action("w3", START + 80_000), restart)])).count(b"\n"), 1)

    def test_replay_resume_filters_overlap_and_seeks(self):
        existing = [replay_line(text_action(f"m{i}", START + 1000 * i), 1000 * i) for i in range(1, 11)]
        fd = self.fd(existing, "youtube_live_chat_replay")
        request = json.dumps({"context": {}, "continuation": "c", "currentPlayerState": {"playerOffsetMs": "0"}}).encode()
        self.assertEqual(json.loads(fd._seek(request))["currentPlayerState"]["playerOffsetMs"], "5000")
        fragment = dump([replay_line(text_action(f"m{i}", START + 1000 * i), 1000 * i) for i in range(5, 14)])
        written = [json.loads(line) for line in fd._rewrite(fragment).splitlines()]
        self.assertEqual([live_chat.line_offset(line) for line in written], [11000, 12000, 13000])

    def test_fresh_download_is_unchanged(self):
        fd = self.fd([], "youtube_live_chat_replay")
        fragment = dump([replay_line(text_action("m1", START), 0)])
        self.assertEqual(fd._rewrite(fragment), fragment)

    def test_stop_raises_download_cancelled(self):
        fd = live_chat.ResumableLiveChatFD(self.ydl, self.ydl.params, should_stop=lambda: True)
        with self.assertRaises(yt_dlp.utils.DownloadCancelled):
            fd._download_fragment({}, "https://example.invalid", {})

    def test_subtitle_entry_built_when_missing(self):
        sub = live_chat.live_chat_subtitle({"id": "abcdefghijk", "live_status": "is_live"})
        self.assertEqual(sub["protocol"], "youtube_live_chat")
        self.assertIn("abcdefghijk", sub["url"])
        sub = live_chat.live_chat_subtitle({"id": "abcdefghijk", "live_status": "was_live"})
        self.assertEqual(sub["protocol"], "youtube_live_chat_replay")


class RestorePackagedChatTests(ChatFileTestBase):
    def test_zipped_chat_is_restored_for_resume(self):
        base = os.path.join(self.tmp, "out")
        with zipfile.ZipFile(base + ".live_chat.zip", "w") as z:
            z.writestr("out.live_chat.json", '{"a": 1}\n')
        coordinator = download_Live.LiveStreamDownloader(logger=LOGGER)
        coordinator._restore_packaged_live_chat(base, self.filename)
        self.assertEqual(self.read(self.part), b'{"a": 1}\n')

    def test_existing_sources_take_precedence(self):
        base = os.path.join(self.tmp, "out")
        with zipfile.ZipFile(base + ".live_chat.zip", "w") as z:
            z.writestr("out.live_chat.json", '{"old": 1}\n')
        self.write(self.part, b'{"new": 1}\n')
        download_Live.LiveStreamDownloader(logger=LOGGER)._restore_packaged_live_chat(base, self.filename)
        self.assertEqual(self.read(self.part), b'{"new": 1}\n')


if __name__ == "__main__":
    unittest.main()
