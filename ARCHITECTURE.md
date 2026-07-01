# listener - architecture

`listener` is the speech-to-text component runtime. It is a fresh component
family, separate from the forked Whisrs repository.

## Direction

The first vertical slice is:

- capture from the default system input;
- write captured audio continuously to durable disk while capture is active;
- transcribe the batch when capture stops;
- deliver the resulting text to the system clipboard as the first configured
  output target.

Listener runs in the NixOS desktop audio context: PipeWire, WirePlumber,
pipewire-pulse, BlueZ, and CriomOS-home carry forward as operating context for
later implementation.

## Packaging

The component family uses the local three-repo mold:

```text
listener                  runtime repo: CLI, meta CLI, daemon, state/effects
signal-listener           ordinary peer-callable wire contract
meta-signal-listener      owner/meta configuration wire contract
```

The CLI binaries are clients. They do not own durable state, open the capture
store directly, or bypass the daemon path.

## Owned

- `listener` thin CLI entry point.
- `meta-listener` thin owner/meta CLI entry point.
- `listener-daemon` runtime entry point.
- Runtime implementation of audio capture, durable capture writes,
  transcription execution, and output delivery.
- Typed configuration archive helpers over
  `signal_listener::ListenerDaemonConfiguration`.

## Not Owned

- Ordinary wire vocabulary lives in `signal-listener`.
- Owner/meta wire vocabulary lives in `meta-signal-listener`.
- The forked Whisrs repository remains separate; reuse happens later through
  explicit library seams.
- Later safeguards are outside this scaffold: redundant multi-track capture,
  Bluetooth disconnect guard, RMS/silence alarms, heartbeat/watchdog, alerts,
  and typing into windows.

## Code Map

- `src/main.rs` is the ordinary CLI entry point.
- `src/bin/meta_listener.rs` is the owner/meta CLI entry point.
- `src/bin/listener_daemon.rs` is the daemon entry point.
- `src/configuration.rs` wraps the shared daemon configuration contract and
  proves binary archive round trips.
- `src/command.rs` is the thin ordinary CLI client.
- `src/daemon.rs` owns the blocking Unix-socket daemon loop.
- `src/runtime.rs` lowers `signal-listener` operations into runtime state and
  effects.
- `src/capture.rs`, `src/transcription.rs`, and `src/delivery.rs` hold the
  explicit effect seams.
- `src/meta.rs` is still an owner/meta CLI scaffold.
- `tests/configuration.rs` proves the shared typed configuration archive.
- `tests/runtime.rs` proves active durable artifact writes, stop reply shape,
  and output-target dispatch.

## Status

The first vertical slice is implemented with a blocking local Unix socket and
one active capture. Capture uses a parecord-compatible process against the
system default source and streams raw `s16le` bytes to disk. Transcription uses
`LISTENER_TRANSCRIPTION_PROGRAM` when configured; otherwise it returns an
explicit not-configured stub transcript. Clipboard delivery uses `wl-copy` by
default through the typed output-target dispatcher.
