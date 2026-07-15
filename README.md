# listener

`listener` is the supervised speech-to-text component. `listener` is a thin
client for `listener-daemon`; it never reads or mutates captures directly.

## Commands

```sh
listener start
listener status
listener stop <session>
listener cancel <session>
listener list
listener retry <session>
```

`start` returns a session number. Pass that number to `stop` or `cancel`.
`status` reports the active session without exposing transcript text.

`list` returns one typed record per known capture. Its state is:

- `Recovering`: a crash-recovery `.listenerlog` is still present;
- `Retryable`: a compact WebM/Opus artifact is ready to transcribe;
- `Failed`: the most recent conversion or provider attempt failed; retry is
  still safe;
- `Completed`: a transcript exists in Listener history.

For example, retry a failed capture after inspecting it:

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
`capture-<session>.webm`; it does not re-encode the recording. Listener then
validates the completed WebM before removing the `.listenerlog` and any legacy
`capture-<session>.raw.s16le` export. The compact WebM is the single retained
audio source for retry; there is no cron job or background retention sweep.

If Listener or the host stops unexpectedly, the `.listenerlog` remains the
recoverable source and the unfinished `.webm.part` is ignored. `listener retry
<session>` discards that partial container, recovers the validated log records,
and creates a fresh compact WebM before transcription. No raw full-duration
working export is used for normal live captures; it remains only in the legacy
recovery path.

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
listener start
# dictate
listener status
listener stop <session>
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
- `LISTENER_CLIPBOARD_PROGRAM: clipboard command (default `wl-copy`).
- `LISTENER_TRANSCRIPTION_CUSTOMIZATION_ARCHIVE`: optional vocabulary archive.

The production backend reads the existing OpenAI credential at request time
and uses `gpt-4o-transcribe`. The development-only
`LISTENER_DEVELOPMENT_TRANSCRIPTION_PROGRAM` seam is not the production path.
