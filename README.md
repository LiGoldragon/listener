# listener

`listener` is the speech-to-text component runtime. It owns the `listener` CLI,
`meta-listener` CLI, and `listener-daemon` process.

The first implementation slice is scoped to default input capture, continuous
durable disk write, batch transcription on stop, and system clipboard delivery.
The daemon listens on the configured working socket, starts capture from the
system default input through `parecord --device=@DEFAULT_SOURCE@`, streams raw
audio bytes to a durable artifact while recording, transcribes the artifact on
stop, and dispatches the transcript to configured output targets.

Transcription is a narrow backend seam. Set `LISTENER_TRANSCRIPTION_PROGRAM` to
a batch command that accepts the artifact path and writes transcript text to
stdout. Without that variable, Listener returns an explicit not-configured stub
transcript instead of claiming speech recognition happened.

Environment knobs:

- `LISTENER_SOCKET`: ordinary daemon socket path.
- `LISTENER_META_SOCKET`: owner/meta socket path.
- `LISTENER_CAPTURE_STORE`: durable capture directory.
- `LISTENER_CAPTURE_PROGRAM`: parecord-compatible capture command.
- `LISTENER_TRANSCRIPTION_PROGRAM`: batch transcription command.
- `LISTENER_CLIPBOARD_PROGRAM`: clipboard command, default `wl-copy`.
