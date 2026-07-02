# skills - listener

Before editing this repo, read the component-architecture, contract-repo,
micro-components, Rust, Nix, and testing discipline named by the primary
workspace.

Keep Listener as a daemon-first component:

- `listener` is the thin ordinary CLI client.
- `meta-listener` is the thin owner/meta CLI client.
- `listener-recall` is the thin recall client over the local transcript history.
- `listener-daemon` owns runtime state and effects.
- ordinary wire types come from `signal-listener`.
- meta wire types come from `meta-signal-listener`.

Do not vendor or extend Whisrs in this repo. Later implementation slices may
harvest from Whisrs only behind explicit library seams.
