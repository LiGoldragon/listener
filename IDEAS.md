# listener ideas

## Possible future epic: target-locked transcript insertion

Listener's safe default remains clipboard delivery plus transcript history. A future component may add automatic insertion only if it can target the destination without relying on whichever Wayland surface happens to hold focus when injected input lands.

Accepted constraints for that future work:

- Do not revive `uinput`, `wtype`, virtual-keyboard, libei, or automated paste as a default safe insertion path; those mechanisms feed the current seat/focus and cannot name a destination window.
- Capture the intended target at recording start only for cooperative targets that expose a stable address, such as a terminal pane, editor RPC socket, application socket, or purpose-built agent UI endpoint.
- Deliver transcripts through a structured target protocol rather than synthesized key events.
- Keep every successful transcript in history and copy it to the clipboard as recovery before any automatic insertion attempt.
- If focus-following injection is ever explored, keep it explicitly experimental and fail closed on focus change, physical input, monitor failure, or uncertain target identity.
- Treat spoken submit phrases, such as a trailing “over and out,” as structured delivery metadata: strip the phrase from delivered text and request a separate submit action only on backends that can perform it safely.

The first useful experiment is a cooperative target backend for one addressable surface, proving that Listener can record, transcribe, strip a submit phrase, and deliver to the original target while focus has already moved elsewhere.
