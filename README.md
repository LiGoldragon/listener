# listener

`listener` is the speech-to-text component runtime. It owns the `listener` CLI,
`meta-listener` CLI, and `listener-daemon` process.

The first implementation slice is scoped to default input capture, continuous
durable disk write, internal OpenAI batch transcription on stop, system
clipboard delivery, typed cancellation, and a UI-safe status stream. The daemon
listens on the configured working socket, starts capture from the system default
input through `parecord --device=@DEFAULT_SOURCE@`, writes a single growing
Listener recording log while recording, recovers that log on stop, exports a raw
`s16le` PCM view for batch transcription, and dispatches the transcript to
configured output targets. Cancel stops the active capture and retains the
`.listenerlog` artifact without exporting audio for transcription, calling
OpenAI, or delivering text.

The active capture artifact is a custom `.listenerlog` file, not a standard
audio container. It starts with a self-describing header for version, `s16le`
format, sample rate, channel count, frame size, source, session, and start time.
Each appended record carries sequence, cumulative frame and byte offsets,
payload length, CRC32 checksum, payload bytes, and a commit trailer. Listener
flushes and `fdatasync`s the file after the header and after each payload
record, then fsyncs the parent directory after creating the file. Recovery scans
only the valid prefix and truncates the first incomplete or corrupt record tail.
The writer creates `.listenerlog` files exclusively. On daemon restart, Listener
scans existing capture logs, recovers idle orphan logs, and allocates the next
active artifact after the existing `capture-<session>.listenerlog` names.

Start/stop/cancel state conflicts are returned as typed public replies from
`signal-listener`: already-active capture, no active capture, and active versus
requested session mismatch.

When a capture stops with a successful transcript, Listener appends the
transcript to a private, append-only history file at
`$XDG_DATA_HOME/listener/history.jsonl` (typically
`~/.local/share/listener/history.jsonl`), created with owner-only directory and
file permissions. Each JSON line records the Unix-millisecond timestamp, the
capture session, and the transcript text. A cancelled capture writes no history
entry. The `listener-recall` client reads that history newest first, shows a
`fuzzel --dmenu` picker over one-line previews, and copies the full chosen
transcript back to the system clipboard. Recall reads the history file directly
and does not require the daemon.

Production transcription is Listener-owned. The daemon runs a bounded internal
OpenAI transcription actor that converts the recovered raw `s16le` PCM export
to a WAV upload, reads the provider credential at request time with
`gopass show -o openai/api-key`, calls OpenAI REST transcription with
`gpt-4o-transcribe`, and returns only the transcript to the existing stop reply
and delivery path. The development-only `LISTENER_DEVELOPMENT_TRANSCRIPTION_PROGRAM`
seam may be used for local backend experiments; it is not the normal production
path.

The UI-safe status stream is a newline-delimited JSON Unix socket at
`$XDG_RUNTIME_DIR/listener/status.sock` by default. Frames are shaped as
`{"state":"idle|recording|transcribing|cancelled|copied|error","level":0.0}`
and never include transcript text. The default `parecord` capture command
requests low-latency capture, and the writer samples live recording levels in
roughly 50 ms PCM windows while keeping `.listenerlog` record durability intact.

Environment knobs:

- `LISTENER_SOCKET`: ordinary daemon socket path.
- `LISTENER_META_SOCKET`: owner/meta socket path.
- `LISTENER_STATUS_SOCKET`: UI-safe status stream socket path.
- `LISTENER_CAPTURE_STORE`: durable capture directory.
- `LISTENER_CAPTURE_PROGRAM`: parecord-compatible capture command.
- `LISTENER_DEVELOPMENT_TRANSCRIPTION_PROGRAM`: development-only batch
  transcription command.
- `LISTENER_CLIPBOARD_PROGRAM`: clipboard command, default `wl-copy`.
- `LISTENER_HISTORY_STORE`: transcript history file, default
  `$XDG_DATA_HOME/listener/history.jsonl`.
- `LISTENER_RECALL_SELECTOR`: recall picker command, default `fuzzel`.
