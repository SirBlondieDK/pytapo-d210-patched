import json
import os
import asyncio
import subprocess
from ._utils import StreamType

HLS_TIME = 1
HLS_LIST_SIZE = 3
HLS_FLAGS = "delete_segments+append_list"


class StreamerCleanupError(RuntimeError):
    """Raised when a streamer cannot prove that all resources were released."""


async def _await_bounded(awaitable, timeout):
    """Await with a timeout without asyncio.wait_for's old cancel race.

    Python 3.9/3.10 asyncio.wait_for() can swallow an external cancellation
    when the inner task completes at the same instant.  This explicit race
    keeps timeout and caller cancellation distinct and always settles the
    inner task before returning control to lifecycle cleanup.
    """
    task = asyncio.create_task(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except BaseException:
        if not task.done():
            task.cancel()
        try:
            await task
        except BaseException:
            pass
        raise
    if task not in done:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        raise TimeoutError(f"operation exceeded {timeout:.3f}s")
    return task.result()


class Streamer:
    """
    In case of multiple streams (different lens) available on device, device sends them all in separate video streams.
    When using this for example in vlc, use
    ff_args={
        "-map-video": "0:v:1",
    } for the other stream, main is on 0:v:1
    """

    def __init__(
        self,
        tapo,
        logFunction=None,
        outputDirectory="./",
        removeFilesInOutputDirectory=False,
        quality="HD",
        window_size=None,
        fileName=None,
        logLevel="debug",
        probeSize=32,
        analyzeDuration=0,
        includeAudio=False,
        mode="pipe",
        ff_args=None,
        preview_params=None,
        device_id=None,
        channels=None,
        streams=None,
        resolutions=None,
        audio=None,
        ignore_limit=None,
        no_data_timeout=10.0,
        process_start_timeout=5.0,
        task_stop_timeout=3.0,
        process_stop_timeout=2.0,
        process_kill_timeout=2.0,
        require_media_packet=False,
        first_packet_timeout=5.0,
        direct_fd_handoff=False,
    ):
        self.no_data_timeout = no_data_timeout
        self.currentAction = "Idle"
        self.logFunction = logFunction
        self.tapo = tapo
        self.fileName = fileName or "stream_output.m3u8"
        self.outputDirectory = outputDirectory
        self.window_size = int(window_size) if window_size else 50
        self.stream_task = None
        self.quality = quality
        self.running = False
        self.logLevel = logLevel
        self.probeSize = probeSize
        self.analyzeDuration = analyzeDuration
        self.includeAudio = includeAudio
        self.mode = mode.lower()
        self.removeFilesInOutputDirectory = removeFilesInOutputDirectory
        self.audio_r = None
        self.audio_w = None
        self.audio_format = None
        self.audio_rate = None
        self._ts_buffer = bytearray()
        self._audio_buffer = bytearray()
        self.ff_args = ff_args or {}
        self.preview_params = preview_params or {}
        self.device_id = device_id
        self.channels = channels
        self.streams = streams
        self.resolutions = resolutions
        self.audio = audio
        self.ignore_limit = ignore_limit
        self.process_start_timeout = process_start_timeout
        self.task_stop_timeout = task_stop_timeout
        self.process_stop_timeout = process_stop_timeout
        self.process_kill_timeout = process_kill_timeout
        self.require_media_packet = require_media_packet
        self.first_packet_timeout = first_packet_timeout
        self.direct_fd_handoff = direct_fd_handoff
        # Kept under the old name for API compatibility.  The event now means
        # FFmpeg has produced video output, not merely that encrypted camera
        # transport bytes reached FFmpeg's stdin.
        self._first_packet = asyncio.Event()
        self._first_input_packet = asyncio.Event()
        self._session_close_lock = asyncio.Lock()
        self.streamProcess = None
        self._ffmpeg_log_task = None
        self._media_session = None
        self._output_r = None
        self._output_w = None
        self._lifecycle_lock = asyncio.Lock()

    @property
    def healthy(self):
        """Return True only while both producer task and FFmpeg are alive."""
        return (
            self.running
            and self.stream_task is not None
            and not self.stream_task.done()
            and self.streamProcess is not None
            and self.streamProcess.returncode is None
        )

    def _log_lifecycle(self, phase, **details):
        if self.logFunction is not None:
            self.logFunction(
                {
                    "currentAction": self.currentAction,
                    "lifecyclePhase": phase,
                    **details,
                }
            )

    @staticmethod
    def _normalize_list(value):
        if isinstance(value, (list, tuple)):
            return list(value)
        return [value]

    def _build_preview_payload(self):
        preview = {
            "audio": ["default"],
            "channels": [0],
            "resolutions": [self.quality],
        }

        if self.audio is not None:
            preview["audio"] = self._normalize_list(self.audio)
        if self.channels is not None:
            preview["channels"] = self._normalize_list(self.channels)
        if self.streams is not None:
            preview["streams"] = self._normalize_list(self.streams)
        if self.resolutions is not None:
            preview["resolutions"] = self._normalize_list(self.resolutions)
        if self.device_id:
            preview["deviceId"] = self.device_id
        if self.ignore_limit is not None:
            preview["ignore_limit"] = self.ignore_limit
        if self.preview_params:
            preview.update(self.preview_params)

        return {
            "type": "request",
            "seq": 1,
            "params": {
                "preview": preview,
                "method": "get",
            },
        }

    async def _init_audio_params(self):
        if not self.includeAudio:
            return
        if self.audio_format is not None and self.audio_rate is not None:
            return

        audio_format = "alaw"
        audio_rate = 8000

        try:
            loop = asyncio.get_event_loop()
            audio_config = await loop.run_in_executor(None, self.tapo.getAudioConfig)
            microphone = audio_config.get("audio_config", {}).get("microphone", {})
            encode_type = str(microphone.get("encode_type", "")).lower()
            if "ulaw" in encode_type:
                audio_format = "mulaw"
            elif "alaw" in encode_type:
                audio_format = "alaw"

            sampling_rate = microphone.get("sampling_rate")
            if sampling_rate is not None:
                audio_rate = int(sampling_rate) * 1000
        except Exception:
            # fall back to defaults if detection fails
            pass

        self.audio_format = audio_format
        self.audio_rate = audio_rate

    async def start(self):
        """Start once, with a bounded subprocess launch and verified rollback."""
        async with self._lifecycle_lock:
            if self.healthy:
                return self._stream_info()

            if self.stream_task is not None or self.streamProcess is not None:
                await self._stop_unlocked()

            self._first_packet.clear()
            self._first_input_packet.clear()
            self._log_lifecycle("start.begin")
            try:
                info = await _await_bounded(
                    self._start_unlocked(), self.process_start_timeout
                )
                # Neither an FFmpeg subprocess nor bytes written to its stdin
                # prove that HA can consume video.  Wait for FFmpeg progress,
                # which confirms that the demux/mux path produced video output.
                if self.require_media_packet:
                    packet_wait = asyncio.create_task(self._first_packet.wait())
                    process_wait = asyncio.create_task(self.streamProcess.wait())
                    try:
                        done, _ = await asyncio.wait(
                            {packet_wait, self.stream_task, process_wait},
                            timeout=self.first_packet_timeout,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if packet_wait not in done:
                            if self.stream_task in done:
                                err = (None if self.stream_task.cancelled()
                                       else self.stream_task.exception())
                                raise RuntimeError(
                                    f"media producer ended before video data: {err!r}"
                                )
                            if process_wait in done:
                                raise RuntimeError(
                                    "FFmpeg exited before video data "
                                    f"(returncode={self.streamProcess.returncode})"
                                )
                            raise TimeoutError("FFmpeg produced no video output")
                    finally:
                        for helper in (packet_wait, process_wait):
                            if not helper.done():
                                helper.cancel()
                        # Event.wait() owns no external resource. Do not await
                        # the cancelled helpers here: on Python 3.9 that can
                        # consume the caller's cancellation. The producer,
                        # socket and FFmpeg cleanup below remain shielded and
                        # explicitly verified before cancellation propagates.
                else:
                    await asyncio.sleep(0)
                if self.stream_task.done():
                    error = (None if self.stream_task.cancelled()
                             else self.stream_task.exception())
                    raise RuntimeError(
                        f"media producer exited during startup: {error!r}"
                    )
                self._log_lifecycle(
                    "start.ready",
                    ffmpeg_pid=self.streamProcess.pid,
                    read_fd=info["read_fd"],
                )
                return info
            except BaseException:
                self._log_lifecycle("start.failed")
                cleanup = asyncio.create_task(self._stop_unlocked())
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    # Do not orphan cleanup if the caller is itself cancelled.
                    await cleanup
                    raise
                raise

    async def _start_unlocked(self):
        self.currentAction = "FFMpeg Starting"

        if self.mode == "hls":
            os.makedirs(self.outputDirectory, exist_ok=True)
            if self.removeFilesInOutputDirectory:
                for f in os.listdir(self.outputDirectory):
                    os.remove(os.path.join(self.outputDirectory, f))

        if self.includeAudio:
            await self._init_audio_params()

        cmd = [
            "ffmpeg",
            "-loglevel",
            self.ff_args.get("-loglevel", f"{self.logLevel}"),
        ]
        if self.direct_fd_handoff:
            cmd += [
                "-nostats",
                "-progress",
                "pipe:2",
            ]
        cmd += [
            "-probesize",
            self.ff_args.get("-probesize", f"{self.probeSize}"),
            "-analyzeduration",
            self.ff_args.get("-analyzeduration", f"{self.analyzeDuration}"),
            "-f",
            "mpegts",
            "-i",
            "pipe:0",
        ]

        if "-frames:v" in self.ff_args:
            cmd += [
                "-frames:v",
                self.ff_args["-frames:v"],
            ]

        pass_fds = ()
        video_map = self.ff_args.get("-map-video", "0:v:0")
        audio_map = self.ff_args.get("-map-audio", "1:a:0")

        if self.includeAudio:
            self.audio_r, self.audio_w = os.pipe()
            pass_fds = (self.audio_r,)
            cmd += [
                "-f",
                self.audio_format or "alaw",
                "-ar",
                f"{self.audio_rate or 8000}",
                "-i",
                f"/dev/fd/{self.audio_r}",
                "-map",
                video_map,
                "-map",
                audio_map,
                "-c:v",
                self.ff_args.get("-c:v", "copy"),
            ]
            if "-vsync" in self.ff_args:
                cmd += [
                    "-vsync",
                    self.ff_args["-vsync"],
                ]
            cmd += [
                "-c:a",
                self.ff_args.get("-c:a", "aac"),
                "-b:a",
                self.ff_args.get("-b:a", "128k"),
            ]
        else:
            cmd += ["-map", video_map]
            if "-vsync" in self.ff_args:
                cmd += [
                    "-vsync",
                    self.ff_args["-vsync"],
                ]
            cmd += ["-c:v", self.ff_args.get("-c:v", "copy")]

        if self.mode == "hls":
            cmd += [
                "-f",
                self.ff_args.get("-f", "hls"),
                "-hls_time",
                f"{HLS_TIME}",
                "-hls_list_size",
                f"{HLS_LIST_SIZE}",
                "-hls_flags",
                HLS_FLAGS,
                os.path.join(self.outputDirectory, self.fileName),
            ]
        else:
            output_format = self.ff_args.get("-f", "mpegts")
            cmd += [
                "-f",
                output_format,
            ]
            if output_format == "mpegts" and self.direct_fd_handoff:
                cmd += [
                    "-mpegts_flags",
                    "+resend_headers",
                    "-muxdelay",
                    "0",
                ]
            cmd += ["pipe:1"]

        stdout_target = subprocess.PIPE
        if self.mode == "pipe" and self.direct_fd_handoff:
            # Do not use asyncio's stdout=PIPE for HA direct streams.  Its
            # StreamReader transport eagerly drains the kernel pipe into a
            # private Python buffer, racing PyAV which opens pipe:<read_fd> in a
            # worker thread.  Give FFmpeg a dedicated OS pipe instead so there
            # is exactly one reader: Home Assistant's stream worker.
            self._output_r, self._output_w = os.pipe()
            stdout_target = self._output_w

        try:
            self.streamProcess = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=subprocess.PIPE,
                stdout=stdout_target,
                stderr=subprocess.PIPE,
                pass_fds=pass_fds,
                # ffmpeg can emit very long stderr lines (a single warning/error with
                # no newline). The default StreamReader limit is 64 KiB, which makes
                # readline() raise ValueError on such a line; give it more headroom.
                limit=2**20,  # 1 MiB
            )
        except BaseException:
            self._close_output_pipe()
            raise
        finally:
            # create_subprocess_exec duplicated the write end for the child.
            # Keeping the parent copy open would prevent EOF reaching PyAV.
            if self._output_w is not None:
                os.close(self._output_w)
                self._output_w = None

        self._ffmpeg_log_task = asyncio.create_task(
            self._print_ffmpeg_logs(self.streamProcess.stderr)
        )

        self.running = True
        if self.stream_task is None or self.stream_task.done():
            self.stream_task = asyncio.create_task(self._stream_to_ffmpeg())

        return self._stream_info()

    def _stream_info(self):
        read_fd = None
        if self.mode == "pipe" and self.streamProcess is not None:
            if self.direct_fd_handoff:
                read_fd = self._output_r
            elif self.streamProcess.stdout is not None:
                read_fd = self.streamProcess.stdout._transport.get_extra_info(
                    "pipe"
                ).fileno()

        return {
            "ffmpegProcess": self.streamProcess,
            "streamProcess": self.stream_task,
            "read_fd": read_fd,
        }

    async def _print_ffmpeg_logs(self, stderr):
        while True:
            try:
                line = await stderr.readline()
            except ValueError:
                # An ffmpeg stderr line exceeded the StreamReader buffer limit.
                # asyncio's readline() discards the oversized data and raises
                # ValueError; skip that line and keep pumping logs instead of
                # letting this fire-and-forget task die with an unretrieved
                # exception (which crashes the log task and spams a traceback).
                continue
            if not line:
                break
            decoded = line.decode(errors="replace").strip()
            if decoded.startswith("frame="):
                try:
                    frame_count = int(decoded.split("=", 1)[1].strip())
                except ValueError:
                    frame_count = 0
                if frame_count > 0 and not self._first_packet.is_set():
                    self._first_packet.set()
                    self._log_lifecycle("producer.first_ffmpeg_output")
            if self.logFunction is not None:
                self.logFunction(
                    {
                        "currentAction": self.currentAction,
                        "ffmpegLog": decoded,
                    }
                )

    def _close_output_pipe(self):
        """Close both parent-side output descriptors idempotently."""
        for name in ("_output_r", "_output_w"):
            fd = getattr(self, name)
            if fd is None:
                continue
            try:
                os.close(fd)
            except OSError:
                pass
            setattr(self, name, None)

    async def _close_media_session(self, session):
        """Close the real media socket, including cancelled __aenter__ attempts.

        pytapo 3.4.30 HttpMediaSession.close() skips a writer opened before
        _started=True, so close and verify that writer ourselves as fallback.
        Keep the session reference on any uncertainty (fail closed).
        """
        if session is None:
            return
        async with self._session_close_lock:
            if self._media_session is not session:
                return
            errors = []
            try:
                await asyncio.wait_for(session.close(), timeout=2.0)
            except Exception as err:
                errors.append(f"session.close: {err!r}")

            writer = getattr(session, "_writer", None)
            if writer is not None:
                try:
                    if not writer.is_closing():
                        writer.close()
                    await asyncio.wait_for(writer.wait_closed(), timeout=2.0)
                except Exception as err:
                    errors.append(f"socket writer: {err!r}")

            reader_task = getattr(session, "_response_handler_task", None)
            if reader_task is not None and not reader_task.done():
                reader_task.cancel()
                try:
                    await asyncio.wait_for(reader_task, timeout=1.0)
                except asyncio.CancelledError:
                    pass
                except Exception as err:
                    errors.append(f"response handler: {err!r}")

            if (getattr(session, "started", False)
                    or (writer is not None and not writer.is_closing())
                    or (reader_task is not None and not reader_task.done())):
                errors.append("media session not confirmed closed")
            if errors:
                raise StreamerCleanupError("; ".join(errors))
            self._media_session = None
            self._log_lifecycle("producer.socket_closed")

    async def _stream_to_ffmpeg(self):
        mediaSession = None
        try:
            mediaSession = self.tapo.getMediaSession(StreamType.Stream)
            self._media_session = mediaSession
            mediaSession.set_window_size(self.window_size)
            self.currentAction = "Streaming"

            async with mediaSession:
                self._log_lifecycle("producer.media_session_connected")
                payload = json.dumps(self._build_preview_payload())

                async for resp in mediaSession.transceive(
                    payload, no_data_timeout=self.no_data_timeout
                ):
                    if (
                        not self.running
                        or self.streamProcess.returncode is not None
                        or self.streamProcess.stdin.is_closing()
                    ):
                        break

                    if resp.mimetype != "video/mp2t":
                        continue

                    # Audio - Flush complete 160‑byte A‑law frames (20 ms @ 8 kHz)
                    if self.includeAudio and resp.audioPayload:
                        self._audio_buffer += resp.audioPayload

                        while len(self._audio_buffer) >= 160:
                            frame = self._audio_buffer[:160]
                            self._audio_buffer = self._audio_buffer[160:]
                            try:
                                os.write(self.audio_w, frame)
                            except OSError:
                                break

                    # Video – re‑assemble & byte‑align to 188‑byte TS cells
                    self._ts_buffer += resp.plaintext

                    # drop leading garbage until the first real sync byte (0x47)
                    while len(self._ts_buffer) >= 188 and self._ts_buffer[0] != 0x47:
                        pos = self._ts_buffer.find(0x47, 1)
                        if pos == -1:
                            # no sync byte in current buffer – wait for more data
                            self._ts_buffer.clear()
                            break
                        self._ts_buffer = self._ts_buffer[pos:]

                    # forward only full, correctly aligned 188‑byte packets
                    while len(self._ts_buffer) >= 188:
                        if self.streamProcess.stdin.is_closing():
                            return
                        packet = self._ts_buffer[:188]
                        self._ts_buffer = self._ts_buffer[188:]
                        self.streamProcess.stdin.write(packet)
                        if not self._first_input_packet.is_set():
                            self._first_input_packet.set()
                            self._log_lifecycle("producer.first_mpegts_packet")

                    if not self.includeAudio:
                        try:
                            # If the consumer stops reading the pipe, ffmpeg output hangs.
                            # This causes ffmpeg to stop reading stdin, which makes drain()
                            # block forever. A timeout cleanly breaks the deadlock.
                            await asyncio.wait_for(
                                self.streamProcess.stdin.drain(), timeout=15.0
                            )
                        except (
                            ConnectionResetError,
                            BrokenPipeError,
                            ConnectionAbortedError,
                            asyncio.TimeoutError,
                        ):
                            self.running = False
                            break
        except (OSError, asyncio.TimeoutError) as err:
            if self.logFunction is not None:
                self.logFunction(
                    {"currentAction": self.currentAction, "streamError": repr(err)}
                )
        finally:
            self.running = False
            try:
                if mediaSession is not None:
                    cleanup = asyncio.create_task(
                        self._close_media_session(mediaSession)
                    )
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        # Wait for actual socket cleanup even under cancellation.
                        await cleanup
                        raise
            finally:
                if (
                    self.streamProcess is not None
                    and self.streamProcess.stdin is not None
                    and not self.streamProcess.stdin.is_closing()
                ):
                    self.streamProcess.stdin.close()

    async def stop(self):
        """Stop with bounded waits; never hide a still-running producer."""
        async with self._lifecycle_lock:
            cleanup = asyncio.create_task(self._stop_unlocked())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise

    async def _stop_unlocked(self):
        self.currentAction = "Stopping stream"
        self.running = False

        self._log_lifecycle("stop.begin")
        cleanup_errors = []
        task = self.stream_task
        process = self.streamProcess

        if task is not None and not task.done():
            task.cancel()
            done, _ = await asyncio.wait({task}, timeout=self.task_stop_timeout)
            if not done and self._media_session is not None:
                # Closing the media session is the operation that releases TCP/8800.
                try:
                    await asyncio.wait_for(
                        self._media_session.close(), timeout=self.task_stop_timeout
                    )
                except Exception as err:
                    cleanup_errors.append(f"media session close failed: {err!r}")
                done, _ = await asyncio.wait(
                    {task}, timeout=self.task_stop_timeout
                )
            if not done:
                cleanup_errors.append("media producer task is still running")
        # Even an already-finished producer may have left an open socket.
        if self._media_session is not None:
            try:
                await self._close_media_session(self._media_session)
            except Exception as err:
                cleanup_errors.append(f"media connection not closed: {err!r}")

        if task is not None and task.done() and not task.cancelled():
            error = task.exception()
            if error is not None:
                self._log_lifecycle(
                    "stop.producer_error", error=repr(error)
                )

        if process is not None:
            if process.stdin is not None and not process.stdin.is_closing():
                process.stdin.close()
            # Closing the read end makes FFmpeg observe a broken consumer and
            # also unblocks HA/PyAV before we wait for the worker to stop.
            self._close_output_pipe()
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(
                        process.wait(), timeout=self.process_stop_timeout
                    )
                except asyncio.TimeoutError:
                    self._log_lifecycle("stop.kill", ffmpeg_pid=process.pid)
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    try:
                        await asyncio.wait_for(
                            process.wait(), timeout=self.process_kill_timeout
                        )
                    except asyncio.TimeoutError:
                        cleanup_errors.append(
                            f"FFmpeg pid {process.pid} did not exit after kill"
                        )
                except OSError as err:
                    cleanup_errors.append(f"FFmpeg terminate failed: {err!r}")

        log_task = self._ffmpeg_log_task
        if log_task is not None and not log_task.done():
            log_task.cancel()
            done, _ = await asyncio.wait({log_task}, timeout=1.0)
            if not done:
                cleanup_errors.append("FFmpeg log task is still running")

        for fd_name in ("audio_r", "audio_w"):
            fd = getattr(self, fd_name)
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
                setattr(self, fd_name, None)

        task_stopped = task is None or task.done()
        process_stopped = process is None or process.returncode is not None
        # Only a successful, verified close clears _media_session. Do not infer
        # socket closure from a cancelled producer or started=False alone.
        session_stopped = self._media_session is None

        if not (task_stopped and process_stopped and session_stopped):
            cleanup_errors.append(
                "cleanup verification failed "
                f"(task_stopped={task_stopped}, "
                f"process_stopped={process_stopped}, "
                f"session_stopped={session_stopped})"
            )

        if cleanup_errors:
            self._log_lifecycle("stop.failed", errors=cleanup_errors)
            raise StreamerCleanupError("; ".join(cleanup_errors))

        self.stream_task = None
        self.streamProcess = None
        if process is not None:
            for pipe in (process.stdout, process.stderr):
                transport = getattr(pipe, "_transport", None)
                if transport is not None:
                    transport.close()
        self._ffmpeg_log_task = None
        self._close_output_pipe()
        self._ts_buffer.clear()
        self._audio_buffer.clear()
        self._log_lifecycle("stop.complete")
