# D210 lifecycle and HA stream-handoff patch

Version: `3.4.30.post2`

The second patch revision adds an opt-in `direct_fd_handoff` output path for
Home Assistant. It avoids the competing asyncio `StreamReader`, waits for
FFmpeg output rather than camera input bytes, resends MPEG-TS headers, and
closes the handoff pipe during every cleanup path. The behavior is disabled by
default, so ordinary pytapo snapshot, MJPEG, HLS, and two-way-audio paths keep
their original stdout handling.

This repository is a narrowly scoped derivative of
[`JurajNyiri/pytapo`](https://github.com/JurajNyiri/pytapo), based on the
upstream `3.4.30` tag.

The patch adds bounded startup and shutdown handling to the media streamer,
detects stale background tasks, and performs explicit process, pipe, and task
cleanup after errors, cancellation, or timeouts. Its purpose is to prevent a
failed D210 stream request from leaving an unbounded or orphaned streamer.

Package version: `3.4.30.post1`

The original MIT license and attribution are preserved in `LICENSE`. This is
an independently validated test build and is not an official upstream pytapo
release.

The lifecycle change was validated with 24 offline behavioural tests on
Python 3.14. It does not by itself authorize a live camera or Home Assistant
test.
