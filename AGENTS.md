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

## Protos estate status

Stack: correct-new destination
Status: active component, current checkout legacy-wired
This checkout is not proof of correct-new adoption.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
