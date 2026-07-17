# listener

`listener` is the supervised speech-to-text component. `listener` is a thin
client for `listener-daemon`; it never reads or mutates captures directly.

## Commands

```sh
listener toggle
listener status
listener start
listener stop <session>
listener cancel <session>
listener list
listener retry <session>
```

`toggle` atomically starts an idle daemon or stops its active capture; it is the
hotkey-facing command because it never races a separate `status` read. `start`
returns a session number. Pass that number to `stop` or `cancel`. `status`
reports the in-memory active session without exposing transcript text or running
recovery, migration, or retention work.

`list` returns one typed record per known capture. Its state is:

- `Recovering`: a crash-recovery `.listenerlog` is still present;
- `Retryable`: a compact WebM/Opus artifact is ready to transcribe;
- `Failed`: the most recent conversion or provider attempt failed; retry is
  still safe;
- `Completed`: a canonical WebM/Opus artifact whose transcript exists in
  Listener history; it remains available until its three-day terminal retention
  horizon expires.

Terminal captures remain in `list` while their canonical audio is retained.
Retry a failed capture after inspecting it:

```sh
listener list
listener retry 87
listener list
```

`retry` uses the retained compact artifact (or first recovers and compacts a
legacy `.listenerlog`), calls the configured OpenAI transcription backend, then
sends the transcript to configured outputs and appends it to history. A failed
retry leaves its compact artifact in place and marks it `Failed`; it is not
lost and can be retried again.

## Capture and artifact lifecycle

While recording, Listener commits PCM records to one owner-only (`0700`
directory, `0600` file) crash-recoverable `capture-<session>.listenerlog` under
`$XDG_STATE_HOME/listener/captures` (normally
`~/.local/state/listener/captures`). In a separate encoder worker it immediately
feeds each already-committed record to FFmpeg, which incrementally writes
`capture-<session>.webm.part`: mono 16 kHz Opus-in-WebM, 24 kbit/s with the
Opus `voip` application. This worker never blocks the capture writer; a failed
encoder leaves the durable log intact for recovery.

The `.part` file is an unfinished container, not a retry artifact and is not
shown by `listener list`. On normal `stop`, Listener closes the encoder input,
waits only for the active container to flush and finalizes it atomically as
`capture-<session>.webm`. It validates that the container decodes as Opus before
removing the active `.listenerlog` and any raw PCM export. This WebM extension
is the sole canonical retained audio format: it provides a broadly interoperable
container around Opus and is also the transcription input.

A private `capture-<session>.terminal` record is capture-store metadata, not
audio. It records terminal outcome and the terminal completion clock. A single
snapshot-bounded background maintenance pass at daemon startup, never an active
capture or an interactive request, recovers a crash-survived `.listenerlog`,
re-encodes every decodable legacy `capture-<session>.*` audio container through
a temporary WebM/Opus file, verifies it before deleting the source, and removes
any duplicate legacy source once a canonical artifact exists. A corrupt or
non-convertible source is removed and remains observable as `Failed` through
its terminal record. Exactly one canonical retained audio artifact remains for
each terminal capture.

The default terminal audio horizon is three days from terminal capture
completion (`LISTENER_CAPTURE_RETENTION_DAYS` may override it); an optional
`LISTENER_CAPTURE_RETENTION_MAXIMUM_BYTES` can reclaim older terminal captures
earlier. Reaping removes audio and terminal metadata together. Failed,
cancelled, corrupt, and non-convertible terminal artifacts follow the same
three-day bound. The separate transcript history policy remains independent:
completed transcripts are history, not old audio.

Completed transcript history is an owner-only append-only projection at
`$XDG_DATA_HOME/listener/history.jsonl` (normally
`~/.local/share/listener/history.jsonl`). `listener-recall` reads this history
newest first and copies a selected transcript to the clipboard.

For long recordings, Listener slices the compact artifact into 600-second Opus
WebM requests before uploading, joins the returned transcript parts in order,
and therefore does not rely on a single upload fitting the provider's duration
or token limit. Each request is also checked against OpenAI's 25 MiB upload
limit.

## Service and configuration

Run the daemon through the installed service; the ordinary CLI talks to its
working Unix socket. The normal user-visible workflow is:

```sh
listener toggle
# dictate
listener toggle
listener-recall
```

`cancel <session>` stops active recording and retains its recoverable log; it
does not upload or deliver text. It can later be recovered with
`listener retry <session>`.

Environment configuration:

- `LISTENER_SOCKET`: ordinary daemon socket.
- `LISTENER_STATUS_SOCKET`: UI-safe status stream socket.
- `LISTENER_CAPTURE_STORE`: capture directory.
- `LISTENER_CAPTURE_PROGRAM`: `parecord`-compatible capture command.
- `LISTENER_FFMPEG_PROGRAM`: FFmpeg-compatible encoder (default `ffmpeg`).
- `LISTENER_HISTORY_STORE`: transcript history file.
- `LISTENER_HISTORY_RETENTION_DAYS`: hard-delete transcript records older than this age (default: provisional 90 days).
- `LISTENER_HISTORY_MAXIMUM_BYTES`: hard cap for the retained JSONL projection (default: provisional 16 MiB).
- `LISTENER_CAPTURE_RETENTION_DAYS`: optional hard age cap for failed or cancelled capture media. Unset by default; owner policy is required before media deletion.
- `LISTENER_CAPTURE_RETENTION_MAXIMUM_BYTES`: optional hard byte cap for failed or cancelled capture media; oldest sessions are removed first. Unset by default.
- `LISTENER_LATENCY_TRACE`: optional owner-private, transition-only trace path. When set, it records request receipt, capture/encoder startup, and state publication timestamps; unset production operation writes no trace.
- `LISTENER_CLIPBOARD_PROGRAM`: clipboard command (default `wl-copy`).
- `LISTENER_TRANSCRIPTION_CUSTOMIZATION_ARCHIVE`: optional vocabulary archive.

The production backend reads the existing OpenAI credential at request time
and uses `gpt-4o-transcribe`. The development-only
`LISTENER_DEVELOPMENT_TRANSCRIPTION_PROGRAM` seam is not the production path.
