# Agent Instructions - Listener

## Repo Role

Listener is the speech-to-text component runtime. It owns the `listener` CLI,
owner-side `meta-listener` CLI, and supervised `listener-daemon`. The ordinary
wire vocabulary lives in `signal-listener`; owner/meta configuration lives in
`meta-signal-listener`.

## Current Phase

This repo is a scaffold for the first Listener vertical slice:

- default input capture;
- continuous durable disk write while capture is active;
- batch transcription when capture stops;
- text delivery to the system clipboard as the first configured output.

The scaffold does not implement audio capture, transcription, or clipboard
mutation. Keep later safeguards out until an implementation slice accepts them:
redundant multi-track capture, Bluetooth disconnect guards, RMS/silence alarms,
heartbeat/watchdog, alerts, and typing into windows.

## Local Rules

- Use Jujutsu for version control.
- Use Nix for build and test entry points.
- Keep the CLI thin: it talks to the daemon through `signal-listener`.
- Keep meta traffic on `meta-signal-listener`.
- Do not extend the forked Whisrs inside this repo; harvest from it later only
  through explicit library seams.
