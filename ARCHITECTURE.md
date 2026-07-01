# listener - architecture

`listener` is the speech-to-text component runtime. It is born as a fresh
component family, separate from the forked Whisrs repository.

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
  transcription execution, and output delivery once later slices implement them.
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
- `src/command.rs`, `src/meta.rs`, and `src/daemon.rs` are skeleton-honest
  runtime surfaces.
- `tests/configuration.rs` proves the shared typed configuration archive.

## Status

The repo is a scaffold. The binaries compile and report that transport/runtime
behavior is not implemented. Later implementation workers should add the daemon
transport spine before adding audio capture behavior.
