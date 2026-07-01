# listener

`listener` is the speech-to-text component runtime. It owns the `listener` CLI,
`meta-listener` CLI, and `listener-daemon` process.

The first implementation slice is scoped to default input capture, continuous
durable disk write, batch transcription on stop, and system clipboard delivery.
The daemon listens on the configured working socket, starts capture from the
system default input through `parecord --device=@DEFAULT_SOURCE@`, writes a
single growing Listener recording log while recording, recovers that log on
stop, exports a raw `s16le` PCM view for batch transcription, and dispatches the
transcript to configured output targets.

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

Transcription is a narrow backend seam. Set `LISTENER_TRANSCRIPTION_PROGRAM` to
a batch command that accepts the recovered raw `s16le` PCM export path and
writes transcript text to stdout. Without that variable, Listener returns an
explicit not-configured stub transcript instead of claiming speech recognition
happened.

Environment knobs:

- `LISTENER_SOCKET`: ordinary daemon socket path.
- `LISTENER_META_SOCKET`: owner/meta socket path.
- `LISTENER_CAPTURE_STORE`: durable capture directory.
- `LISTENER_CAPTURE_PROGRAM`: parecord-compatible capture command.
- `LISTENER_TRANSCRIPTION_PROGRAM`: batch transcription command.
- `LISTENER_CLIPBOARD_PROGRAM`: clipboard command, default `wl-copy`.
