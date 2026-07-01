# listener

`listener` is the speech-to-text component runtime scaffold. It will own the
`listener` CLI, `meta-listener` CLI, and `listener-daemon` process.

The first implementation slice is scoped to default input capture, continuous
durable disk write, batch transcription on stop, and system clipboard delivery.
The scaffold keeps those as typed contracts and architecture direction; it does
not implement capture, transcription, or clipboard effects yet.
