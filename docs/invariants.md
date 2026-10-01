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
10. **Never declare a DSH host core package as an ordinary dependency.**
    `@deepseek-ai/dsh-tools`, `dsh-llm`, `dsh-scope`, `cordis`, `dsh-attachment`
    and `dsh-system-prompt` are host contracts and belong in
    `peerDependencies` (plus `devDependencies` so this repo still builds).
    `dsh-app-boot`'s `createRuntimeResolution` reserves every profile-installed
    direct dependency, which **removes that name from the host's half of the
    resolution table**; Node resolves the profile copy first regardless. The
    plugin does not just carry a second copy — it takes the host's identity
    away from every other consumer in the profile. This is the dsh-excel-chat
    failure mode dsh-market reports as "tool calls die, minimal preset fails to
    mount". Cold-load proof: a headless profile with this plugin installed and
    no local `@deepseek-ai/dsh-tools`/`dsh-llm` boots and serves its tools.
11. **A registered watch must never outlive the thing that would feed it.**
    `armRestart` registers its wakeup watch first and then does work that can
    throw, so *everything* after the registration lives inside one `try` that
    cancels on failure. Getting this wrong does not fail loudly: the watch is
    live, it points at a marker no helper will ever write, and it probes for the
    life of the host. It happened, and had to be cancelled by hand.
12. **Read host services through `ctx.get`, never as a bare property.**
    `webServer` is deliberately not in the inject list, so `ctx.webServer`
    throws `cannot get property "webServer" without inject` on a real cordis
    context, while `ctx.get('webServer')` and the injected scope both serve it.
    A ctx doubled as a plain object allows the bare read, so no test written
    against that double can catch it — `strictServiceAccess` exists to close
    that hole. Keep the guarded direct read as a fallback: `ctx.get` is itself
    optional.
13. **A supervised host is never relaunched underneath its supervisor — the
    supervisor is taken down first and relaunched instead.** If the host was
    spawned with an IPC channel (`process.channel !== undefined` — how the
    Desktop app starts it), something else owns its lifecycle. Relaunching the
    host first takes the web port, so the supervisor's own host then dies of
    `listen EADDRINUSE` on every retry until the orphan is killed by hand —
    three identical startup crashes and a Task Manager visit, in the run that
    established this. But leaving the supervisor alone is not enough either: it
    answers any host exit with a modal dialog, so the operator still has to
    click, which defeats the feature. Kill the supervisor's whole tree
    **supervisor-first** — the app must be gone before its child's `close`
    handler can run, because that handler calls `fail()` for *every* exit code
    — then write the marker and relaunch the app with `ELECTRON_RUN_AS_NODE`
    stripped so the GUI comes back and starts its own host. Only when the
    supervisor cannot be relaunched that way does the guard fall back to
    writing the marker and stopping.
