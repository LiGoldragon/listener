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
- Runtime implementation of audio capture, durable capture-log writes,
  transcription-input export, transcription execution, and output delivery.
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
- `src/recording_log.rs` owns the one-file append-only Listener recording log,
  recovery scanner, idempotent truncation, and raw PCM export.
- `src/meta.rs` is still an owner/meta CLI scaffold.
- `tests/configuration.rs` proves the shared typed configuration archive.
- `tests/recording_log.rs` proves header recovery, torn-tail recovery, and
  idempotent truncation.
- `tests/capture.rs` proves the production capture writer commits a payload
  record before capture stop through the writer sync boundary.
- `tests/runtime.rs` proves active durable artifact writes, stop-time recovery
  export, stop reply shape, and output-target dispatch.

## Status

The first vertical slice is implemented with a blocking local Unix socket and
one active capture. Capture uses a parecord-compatible process against the
system default source and writes one growing `.listenerlog` artifact. The log
header records version, `s16le` sample format, sample rate, channel count,
frame size, input source, session, and start time. Each PCM record carries
sequence, cumulative frame and byte offsets, payload length, CRC32 checksum,
payload bytes, and a commit trailer. The writer flushes and `fdatasync`s after
the header and each record, and fsyncs the parent directory after creating the
file so the path is discoverable after a crash.

On stop, Listener scans the log from the header, accepts only complete records
with matching sequence, offsets, checksums, and commit trailers, and truncates
the first incomplete or corrupt tail to the last valid record boundary.
Recovery is idempotent. The configured transcription program receives a
recovered raw `s16le` PCM export path, not the custom `.listenerlog` path.
Without `LISTENER_TRANSCRIPTION_PROGRAM`, Listener returns an explicit
not-configured stub transcript. Clipboard delivery uses `wl-copy` by default
through the typed output-target dispatcher.
