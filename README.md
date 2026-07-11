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

While recording, Listener writes exactly one owner-only (`0700` directory,
`0600` file) crash-recoverable `capture-<session>.listenerlog` under
`$XDG_STATE_HOME/listener/captures` (normally
`~/.local/state/listener/captures`). It is not a second recording: it is the
durable write-ahead recording format. Its per-record headers, checksums, and
commit trailers account for its small overhead.

On stop or retry, Listener validates the log, exports a short-lived raw `s16le`
working file, and encodes `capture-<session>.webm`: mono 16 kHz Opus-in-WebM,
24 kbit/s with the Opus `voip` application. WebM is OpenAI-supported and is
appropriate for ordinary microphone speech. The temporary raw file is removed.
After the compact file has been validated, Listener removes the `.listenerlog`
and any legacy `capture-<session>.raw.s16le` export. The compact WebM is the
single retained audio source for retry; there is no cron job or background
retention sweep.

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
- `LISTENER_CLIPBOARD_PROGRAM`: clipboard command (default `wl-copy`).
- `LISTENER_TRANSCRIPTION_CUSTOMIZATION_ARCHIVE`: optional vocabulary archive.

The production backend reads the existing OpenAI credential at request time
and uses `gpt-4o-transcribe`. The development-only
`LISTENER_DEVELOPMENT_TRANSCRIPTION_PROGRAM` seam is not the production path.
