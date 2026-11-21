# Listener 1

Listener records a microphone to its own Ogg Opus job file, tries the configured speech services in order, then hands the verbatim transcript to the desktop. The Nexus owns capture, durable job state and scheduling. The Translator owns text, credentials, HTTP and desktop calls. Their connections carry typed Signal; the audio stays on disk.

## Run an isolated place

Build `packages.x86_64-linux.default`. Its `bin` directory contains `listener-nexus`, `listener-translator`, `listener` and `listener-meta`. The Nexus launcher replaces itself with the typed engine; the launch PID is the engine PID.

The two services each take one startup datom. `Place` is `{ name absoluteRuntime absoluteState }`. For the clients to reach it, put `{ name }` in `${XDG_CONFIG_HOME:-$HOME/.config}/nexus/listener-nexus.datom` and use `$XDG_RUNTIME_DIR/nexus/name` as its runtime directory. The clients read only those three environment variables for routing. They take one datom, or no arguments to display their Signal schema.

- `listener-nexus 'Create.{ <place> <publication-capacity> <configuration> }'` creates a fresh store and starts serving. It refuses to replace an existing store.
- `listener-nexus 'Open.{ <place> <publication-capacity> }'` serves an existing store with its persisted configuration. The publication capacity is a positive in-memory backlog setting, independent of the store.
- `listener-translator '<place>'` serves the ordinary, meta and emission sockets. Start it before opening existing work that should resume external effects immediately.
- `listener 'Toggle'` starts a recording, or finalizes the current recording and queues its hearing. `listener 'Subscribe'` opens on the retained non-Delivered jobs. `Subscribed.Changed` updates that view; `Subscribed.Lagged` replaces it with a full snapshot. The meta subscription follows the same framing for settings.

Memory is `<State>/listener.memory`, with the table `listener` and record key `state` in schema 1. Kept audio is `<AudioDirectory>/<JobId>.opus`. Audio and text age out independently. Existing stores from the legacy Listener are outside this format.

## Positional configuration

The initial configuration is the `Configuration` from `listener-library` at the runtime's Cargo pin. The immutable ordinary and meta Signal declarations, also printed by the clients, are in `crates/listener-entry/ethos`. This example uses a private Pulse fixture, one loopback HTTP fixture and dummy credential and desktop programs; set paths and the source to your fixture's values.

```datom
{
  [ { fixture OpenAiTranscription http://127.0.0.1:8123 fixture-model fixture-key en «Transcribe all words exactly as heard.» 20 } ]
  [ fixture ]
  Pasted AsRecorded Forever Forever [ Listener ] 32 Named.fixture.monitor 50
  /example/paste /example/copy /example/read-key /example/state/audio
  { 65536 32 }
  { Named.«unix:/example/runtime/pulse.sock» Listener 48000 Mono
    3840 15360 8 TwentyMilliseconds Voice Variable 5 Voice Automatic
    Disabled Disabled 0 2 SyncEachPage Interrupt }
}
```

The recording settings above, in order, are Pulse server and application name, sample rate and channels, capture fragment and buffer bytes, pending PCM frames, Opus frame duration, application, bitrate control, complexity, signal, bandwidth, discontinuous transmission, forward error correction, expected loss percentage, packets per page, durability, and gap handling.

Set backends and their order separately using `listener-meta 'Configure.Backend.{ ... }'` and `listener-meta 'Configure.Order.[ ... ]'`. Terms use `Configure.AddTerm.Term` and `Configure.RemoveTerm.Term` with the term in the payload. `Configure.Delivery.Pasted`, `Copied` and `Kept` select delivery. `Configure.Source.Named.fixture.monitor` selects a named Pulse source. Configure the companion `RecordingSettings.PulseServer` for the fixture's own daemon.

## External boundaries

The secret program is called with the configured secret-entry name as its sole argument and supplies the key on standard output. Paste and copy programs take no arguments; they read the exact UTF-8 transcript from standard input. Their exit status is the observed outcome. Typed notifications use the desktop session's `org.freedesktop.Notifications.Notify`; a successful method reply carries the notification ID.

OpenAI transcription posts multipart audio, model, language, prompt and `response_format=json` to `/v1/audio/transcriptions`, with bearer authentication. Gemini posts one user content with prompt and base64 inline `audio/ogg` to `/v1beta/models/<model>:generateContent`, with `x-goog-api-key`. The configured service address is the base, and language, instruction and terms shape the request. HTTPS is required outside literal loopback. Redirects are not followed.

## Recovery

Open repairs the tail of an interrupted Ogg recording through the last complete sound page and re-hears retained Recording and Transcribing jobs. Unfinished attempts may recur; a completed hearing commits its transcript and preceding failures together. A persisted outstanding delivery claim is atomically settled as Kept on open: the desktop effect may already have happened. It is never replayed automatically; Recall is explicit. When several jobs are already Waiting behind an occupied claim, they drain in recording order even under AsReady; AsReady can claim an idle handoff without waiting for an earlier transcription.
