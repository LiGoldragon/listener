# listener - architecture

`listener` is the speech-to-text component runtime. It is a fresh component
family, separate from the forked Whisrs repository.

## Direction

The first vertical slice is:

- capture from the default system input;
- write captured audio continuously to durable disk while capture is active;
- transcribe the batch through Listener's internal OpenAI actor when capture
  stops;
- cancel an active capture while retaining the durable artifact and skipping
  transcription and delivery;
- deliver the resulting text to the system clipboard as the first configured
  output target.
- append each successful transcript to a private local history store and recall
  a past transcript back to the clipboard through a fuzzel picker.
- publish UI-safe capture/transcription/delivery state without transcript text.

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
- `listener-recall` thin recall client entry point.
- Runtime implementation of audio capture, durable capture-log writes,
  transcription-input export, internal OpenAI transcription execution, output
  delivery, local UI-safe status streaming, and the private transcript history
  store.
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
  effects. Status is a direct projection of that state; `Toggle` atomically
  selects start or stop from the daemon-owned active slot.
- `src/capture.rs`, `src/transcription.rs`, and `src/delivery.rs` hold the
  explicit effect seams.
- `src/status.rs` owns the local newline-JSON status stream and microphone
  level projection through blocking accept and event waits, not a polling loop.
- `src/maintenance.rs` owns one snapshot-bounded background startup pass for
  recovery, migration, intermediate cleanup, and retention.
- `src/latency.rs` owns opt-in, transition-only latency timestamps; it is
  entirely dormant without `LISTENER_LATENCY_TRACE`.
- `src/history.rs` owns the typed, bounded transcript history store and its
  JSONL projection under the XDG data directory.
- `src/recall.rs` owns the transcript recall flow: read history newest first,
  drive a fuzzel dmenu picker, and copy the chosen transcript to the clipboard.
- `src/bin/listener_recall.rs` is the thin `listener-recall` client entry point.
- `src/recording_log.rs` owns the one-file append-only Listener recording log,
  exclusive creation, recovery scanner, idempotent truncation, and raw PCM
  export.
- `src/meta.rs` is still an owner/meta CLI scaffold.
- `tests/configuration.rs` proves the shared typed configuration archive.
- `tests/recording_log.rs` proves header recovery, torn-tail recovery, and
  idempotent truncation.
- `tests/capture.rs` proves the production capture writer commits a payload
  record before capture stop through the writer sync boundary.
- `tests/runtime.rs` proves active durable artifact writes, stop-time recovery
  export, stop reply shape, cancel retention, no-transcription/no-delivery
  cancel behavior, output-target dispatch, transcript-history append on stop,
  and history untouched on cancel.
- `tests/history.rs` proves append/read-back ordering, limit truncation,
  multiline round trip, owner-only permissions, and empty-store reads.
- `tests/recall.rs` proves the read-select-copy recall flow end to end, empty
  history, and cancelled selection through stub selector and clipboard programs.

## Status

The first vertical slice is implemented with a blocking local Unix socket and
one active capture. Capture uses a parecord-compatible process against the
system default source and writes one growing `.listenerlog` artifact. The log
header records version, `s16le` sample format, sample rate, channel count,
frame size, input source, session, and start time. Each PCM record carries
sequence, cumulative frame and byte offsets, payload length, CRC32 checksum,
payload bytes, and a commit trailer. The writer flushes and `fdatasync`s after
the header and each record, creates the log path exclusively with owner-only
permissions, and fsyncs the parent directory after creating the file so the path
is discoverable after a crash. Capture-store directories and raw PCM exports are
also owner-only.

On stop, Listener scans the log from the header, accepts only complete records
with matching sequence, offsets, checksums, and commit trailers, and truncates
the first incomplete or corrupt tail to the last valid record boundary.
Recovery is idempotent. At daemon startup, Listener snapshots existing capture
sessions and performs one background maintenance pass over only that snapshot:
it recovers every crash-survived log, but migrates, cleans intermediates, and
applies retention only to sessions with terminal metadata. An unterminalized
`.listenerlog` remains durable recovery evidence and is never encoded or
reaped by that pass. `status` never scans storage; capture allocation skips any
existing `capture-<session>` artifact family before creating a recovery log,
including compact, terminal, partial, and unfinalized artifacts. Cancel stops the active capture using the same capture shutdown path
and returns the retained `.listenerlog` artifact without recovering/exporting
audio for transcription, sending OpenAI actor mail, or invoking output delivery.
Idle recovery removes abandoned `.webm.part`, `.webm.encoding`, and raw-export
intermediates. It also migrates terminal legacy capture media through a
verified temporary WebM/Opus artifact before deleting the source. The canonical
retained audio is therefore one `capture-<session>.webm` containing Opus; an
active `.listenerlog` is recovery state and is never reaped. The capture store's
private `capture-<session>.terminal` record is lifecycle metadata, not audio:
it stores terminal outcome and completion time. Failed, cancelled, corrupt, and
non-convertible terminal captures remain observable through this record and
are bounded by the default three-day horizon from terminal capture completion
(`LISTENER_CAPTURE_RETENTION_DAYS`, with an optional
`LISTENER_CAPTURE_RETENTION_MAXIMUM_BYTES` cap). Listener transcribes the
compact WebM through its internal OpenAI actor, reads `gopass openai/api-key`
at request time, and calls OpenAI REST transcription with `gpt-4o-transcribe`.
Clipboard delivery uses `wl-copy` by default through the typed output-target
dispatcher.

On a successful stop, before delivery, the runtime appends the transcript to a
private bounded history store at `$XDG_DATA_HOME/listener/history.jsonl`
(overridable with `LISTENER_HISTORY_STORE`), created with owner-only directory
and file permissions. A successful history append marks the capture completed,
but does not make the audio history: the canonical WebM remains until terminal
retention reaps it. If transcript persistence fails, the compact audio remains
retryable rather than being silently discarded. The typed `TranscriptHistoryEntry` carries the record and
its JSON line is the human/interchange projection: Unix-millisecond timestamp,
capture session, and transcript text. The store atomically compacts before
appends and reads, hard-deleting expired records and the oldest records beyond
its byte budget. The provisional defaults are 90 days and 16 MiB, configurable
through `LISTENER_HISTORY_RETENTION_DAYS` and
`LISTENER_HISTORY_MAXIMUM_BYTES`. History is a best-effort convenience
projection, so a history-write failure never aborts the stop or drops the
already-produced transcript. A cancelled capture skips this step and writes no
history entry. `listener-recall` reads the history newest first, presents a
`fuzzel --dmenu` picker over one-line previews (selector overridable with
`LISTENER_RECALL_SELECTOR`), and copies the full chosen transcript to the
clipboard through the same clipboard command; it reads the history file directly
and does not open the daemon path.

The status stream is local to the runtime repo rather than a transcript-bearing
public Signal reply. `listener-daemon` starts a state-bearing newline-JSON Unix
socket server at `$XDG_RUNTIME_DIR/listener/status.sock` by default. New clients
receive the current event immediately, then pushed events. Events contain only
`state` and normalized microphone `level`; transcript text stays only in the
existing typed stop reply and delivery path. `starting` is published before
capture setup, while `recording` is published only when the capture writer has
committed PCM; the widget therefore reacts immediately without treating process
startup as recorded audio. Recording levels are RMS over `s16le` PCM with
`1.0 - exp(-rms * 18.0)`, clamped to `0.0..=1.0`. The default `parecord`
command requests low-latency delivery, starts before FFmpeg setup, and the
capture writer samples live levels in roughly 50 ms PCM windows instead of
waiting for the `.listenerlog` maximum record payload. The socket acceptor
blocks for clients and the broadcaster blocks for events or the terminal-idle
deadline; the former 25 ms polling wakeup is absent. Copied, cancelled, and
error events are terminal UI events and the stream returns to idle after a
short delay. Status clients are written nonblocking so a slow reader is dropped
instead of blocking publication to other clients. When
`LISTENER_LATENCY_TRACE` is set, only request receipt, capture/encoder startup,
and state-transition publications append owner-private timestamps; default
operation performs no instrumentation I/O.

Ordinary lifecycle conflicts stay on the public reply surface as typed
`signal-listener` outcomes: start while recording reports the active session,
stop or cancel while idle reports no active capture, and stop or cancel with a
different session reports both active and requested sessions. `Toggle` selects
start or stop atomically in this same daemon state transition, so hotkeys never
read status to choose a follow-up operation. These are not lowered to
`RequestUnimplemented`.
