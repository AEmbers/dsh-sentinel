# Core invariants

Every change to the runtime must keep these. They exist because each one was
either broken once or is one edit away from breaking.

1. **Watch ids are session-scoped.** Allocation restarts at `watch-1` per
   session. Any consumer that crosses sessions — the webhook hook URL, any
   global lookup — must carry the session qualifier (`&s=<sessionId>`). A bare
   id only ever resolves inside one session's fold.
2. **Snapshots come in two domains: placeholder and content.** A placeholder
   (`<absent>`, `<unreachable>`, `<none>`, `<push-only>`) means "the target
   itself is missing", not data. Patterns are tested against content only; the
   edge a pattern waits for is placeholder → matching content. New sensor
   kinds must register their "target missing" snapshot in the placeholder set
   in `src/sensors.ts`.
3. **Firing is edge-triggered, never level-triggered.** A pattern fires on
   no-match → match; a pattern-less watch fires on any snapshot change after
   the baseline. Cooldown rate-limits fires; it is not the re-arm mechanism.
4. **Delivery is at-least-once.** A fire is logged to the sidecar before
   delivery; the `delivered` watermark lets a restart requeue fires that never
   reached the agent. Consumers must tolerate a duplicate wakeup.
5. **One duty owner per DSH_HOME.** Probing and delivery happen only under the
   lease; passive instances defer and take over when the lease expires. The TTL
   must stay at least twice `heartbeatMs`, and the runtime refuses to construct
   otherwise: the owner renews once per heartbeat, so the lease age sawtooths up
   to one heartbeat, and with a shorter TTL every cycle contains a window where
   a second instance sees a stale lease held by a live pid, claims duty beside
   the owner, and delivers the same wakeup twice. A duplicated wakeup leaves no
   corruption to recover from — it just looks like a flake forever, which is
   why this is a load-time refusal rather than a runtime warning.
6. **The sidecar log is the only carrier of truth.** Memory state is a fold of
   `sentinel.jsonl`; every mutation is an appended change, never an edit. A
   corrupt row fails the fold loudly.
7. **Watches outlive their session's agent.** The host has no session-deleted
   event, so watches of a deleted session keep probing until cancelled by
   hand. Manual cancel (`POST /plugins/dsh-sentinel/cancel`, the ✕ in every
   UI row) is the kill switch of last resort; never assume the agent is
   reachable. The state route exposes `pendingWakeups` per watch so queued
   wakeups are visible, not silent.
8. **The restart handoff never kills from inside the host.** `sentinel_restart`
   only *arranges*: it persists the marker watch's baseline, then hands off to
   a detached helper. The kill happens on the calling agent's idle edge, so a
   restart can never truncate the turn that requested it. Two consequences to
   preserve: the baseline must be probed and committed before the spawn (the
   fold records a baseline only on a subscription's first observation, so a
   restart landing first would leave the replacement host nothing to compare
   against — see `SentinelRuntime.primeBaseline`), and the tool must refuse
   outright when no relaunch command can be derived, because a host that does
   not come back is strictly worse than no restart.
9. **Path-provably-safe before libuv.** Anything handed to `fs.watch` on
   Windows must first be canonicalized through `realpath` (which expands 8.3
   short-name components) and must be refused when that leaves a directory arm
   with a short-name component still in it. `uv` *aborts* the process — not an
   exception any `try` can catch — on such a path, so a mistake here takes down
   the whole harness, not just the watch. Falling back to heartbeat polling is
   always the correct failure mode.
