# dsh-sentinel

English | [中文](README.zh.md)

Condition-driven wakeup for [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness): the agent registers a watch, goes to sleep — even closes the session — and the sentinel wakes it when the condition happens. Every subscription and every fire is a user-visible session event, and the browser dock shows what is on duty.

![Sentinel dock panel, expanded](docs/preview/sentinel-panel.png)

## How it works

The node half owns one server-lifetime runtime that folds a plugin-owned sidecar log (`$DSH_HOME/sentinel.jsonl`) into live subscriptions, probes every sensor on a shared 5s heartbeat, and delivers wakeups through the official followup channel — resuming a dormant session's agent first when needed. Subscriptions therefore survive process restarts, and conditions that become true while the server is down late-fire on the next probe.

Watching is a resident-process concern: probing and fire delivery only run while a long-running dsh process (typically `dsh web`) is up. Headless one-shot runs load the plugin and can create, list and cancel watches, but nothing probes after the process exits — those watches become active once a resident process starts.

One duty owner per `$DSH_HOME`: a lease file (`sentinel.lease`) makes the first process own probing and delivery; a second dsh process on the same home stays passive (tools work, writes persist to the shared sidecar) and takes over within one lease TTL of the owner dying. The owner re-reads the sidecar every heartbeat, so watches created on a passive instance are adopted automatically. Delivery is at-least-once: a fire logged but not delivered before a crash is requeued on the next boot from its `delivered` watermark.

The browser half is a dock card above the composer (the `conversation.input.dock` family) listing the session's active watches — sensor, target, live probe state, fire budget, next-probe countdown — plus recent fire history when expanded. It polls the read-only state route and renders nothing when the session has no watches.

Two surfaces make the server-global watch set visible. A **Sentinel entry in the sidebar's global panel list** (`sidebar.panellist`, id `sentinel`) opens the watch table in the central column (`main`, the same id) — its glyph carries a state dot while any watch is active. The dashboard is a standalone table of every watch across every session: session (active/dormant), sensor, target, pattern, fire budget, last probe state, next probe.

![Global dashboard](docs/preview/sentinel-dashboard.png)

## Sensors

| Kind | Engine | Fires on |
| --- | --- | --- |
| `file` | path snapshot + inotify push | snapshot change (sub-second); accelerated by fs events |
| `command` | read-only shell line, probed on an interval | output/exit-code change |
| `http` | URL probed on an interval | status/body change |
| `process` | `pgrep -f` pattern, probed on an interval | match-set change |
| `port` | TCP connect to `[host:]port`, probed on an interval | reachability change (open/closed/timeout) |
| `webhook` | pure push | any POST to the returned hook URL |

With `pattern`, probe kinds fire on the no-match→match edge of that regex and webhooks accept only matching payloads; without it, probe kinds fire on any change after the baseline.

## Configuration

All deployment-tunable knobs live in the plugin's config schema (defaults in parentheses); override them on the bundle row in your profile's `cordis.patch.yml`:

```yaml
- id: dsh-sentinel
  name: dsh-sentinel
  config:
    heartbeatMs: 5000            # probe round interval
    probeConcurrency: 8          # in-flight probes per round
    maxSubscriptionsPerSession: 16
    maxPendingWakeups: 8         # queued wakeups per session before dropping oldest
    defaultIntervalSeconds: 30   # when a watch does not specify one (5–86400)
    defaultCooldownSeconds: 60
    dutyLeaseTtlMs: 30000        # passive-instance takeover window after the owner dies
    notifyWebhookUrl: ''         # optional: POST every fire here as JSON
    restartCommand: ''           # how the restart helper relaunches the host (default: this process's own command line)
    restartIdleTimeoutMs: 120000 # how long it waits for the calling turn to end before killing the host anyway
    restartRespawnGraceMs: 8000  # after the kill, how long to wait for a supervisor to bring the host back
```

Invalid values fail plugin load with a schema error rather than misbehaving at runtime. `dutyLeaseTtlMs` must be at least twice `heartbeatMs` and the runtime refuses to start otherwise — the owner renews once per heartbeat, so a shorter lease is stale for part of every cycle and a second instance can claim duty beside it and deliver the same wakeup twice.

`notifyWebhookUrl` fans every fire out of the harness as a JSON POST (`{plugin, event, sessionId, id, kind, target, note, fireNumber, maxFires, summary, after}`) — point it at a Lark/WeCom/Slack bot or any receiver. Delivery is at-most-once: a failed POST warns in the log and never blocks the in-harness wakeup.

## Tools

- `sentinel_watch` — register a watch: `kind`, `target`, optional `pattern`, `interval` (1–3600s, default 30), `note` (delivered verbatim with every wakeup), `maxFires` (default 1: one-shot), `cooldown` (default 60s), optional `ttl`.
- `sentinel_list` — active watches with live probe state.
- `sentinel_cancel` — cancel one watch by id.
- `sentinel_restart` — restart the dsh host and be woken back into the same session (see below).

### Restarting the host and coming back

`sentinel_restart({ note, confirm: true })` exists for the one thing a plugin normally cannot do: a plugin
lives *inside* the process it would have to restart. Install a plugin, change host config, rebuild a native
module — the change only lands after a bounce, and a bounce normally means asking the human to do it by hand.

The handoff is built so that the destructive half happens outside the host, and the surviving half needs no
running process at all:

1. The tool registers a one-shot `file` watch on `$DSH_HOME/sentinel-restart/ready-<id>.flag` and — crucially —
   **probes and persists that watch's baseline before anything is killed**. The fold only records a baseline on
   a subscription's first observation, so without this step a restart landing before the first heartbeat would
   leave the replacement host nothing to compare against, and it would silently absorb the marker as its own
   baseline instead of firing on it.
2. A detached helper (staged twice, so it no longer hangs off the host's parent-PID chain) waits for the calling
   agent to reach an **idle edge**. That is what stops a restart from truncating the very turn that asked for it.
3. The helper kills the host tree, writes the marker while the host is down, then waits
   `restartRespawnGraceMs` before relaunching — a supervisor (the desktop app watching its host child) may
   bring the host back on its own, and doubling it would collide on the port.
4. The replacement host folds the sidecar, re-seeds the probe baseline, sees the marker as a real snapshot
   change against `<absent>`, and wakes the dormant session with your note.

The marker file is the *entire* interface between the two processes: no port, no URL, no readiness handshake
to get wrong. When the tool cannot determine a relaunch command (no entry script in `process.argv` and no
`restartCommand` configured) it refuses instead of killing a host nothing would bring back.

Because the kill happens on the idle edge, call `sentinel_restart` last in a turn and then **end the turn**;
anything after it would be cut off. It takes down the whole dsh host — every session it serves, and any plugin
holding the port. That is what "restart" means here.

### Verified end to end, on a live desktop host

Run on Windows against DSH 0.2.0-rc.2, desktop profile. The helper log for the handoff
(`$DSH_HOME/sentinel-restart/helper-<id>.log`) reads:

```
07:32:55.587Z helper started (pid 39392), target host pid 28252
07:32:55.606Z handed off to stage 2 (pid 3556)
07:33:00.217Z agent reported idle
07:33:02.869Z killing host descendants: [26 pids] then pid 28252
07:33:02.872Z host pid 28252 is gone
07:33:02.873Z marker written: …\ready-28252-mup7vsr2.flag
07:33:10.877Z relaunching: "<Electron exe>" --expose-internals "<…dsh-desktop-host\lib\index.js>" …
07:33:10.889Z helper done
```

What that pins down:

- The helper spawned with the tool call, then waited **4.6 s** for the idle edge before touching anything —
  it did not kill the host out from under the running turn.
- `killing host descendants` is 26 pids, killed in reverse-depth order and excluding the helper's own tree.
- The **8.008 s** between `marker written` and `relaunching` is `restartRespawnGraceMs` doing its job: the
  desktop app supervises its host and calls `this.fail()` on an unexpected exit rather than respawning it, so
  after the grace window with nothing listening the helper relaunched the host itself.
- The new host came up as a **different pid** (35684, parented to the since-exited stage-2 helper — detached,
  as designed), claimed the duty lease (`state` reports `duty.pid === 35684`), folded the sidecar, stored the
  marker's baseline, and delivered the wakeup into the original session. No port, URL, or readiness handshake
  was involved at any point — the marker file was the whole interface.
- The guard cleaned up after itself: `request-<id>.json` is gone, `ready-<id>.flag` is kept as the record, and
  `helper-v1.mjs` is retained for reuse.

**That self-relaunch was wrong for a supervised host, and this run is how we found out.** The desktop app is
the host's *parent*: it survives the kill, shows its `dsh desktop host stopped` dialog, and — crucially —
starts its *own* host when the operator clicks 重启应用. Our detached replacement had already taken
`0.0.0.0:19387`, so the app's host died of `listen EADDRINUSE` and the dialog came back. Three consecutive
attempts produced three identical crash reports (`07-33-36`, `07-33-50`, `07-34-05`), and the port only freed
up when the orphan was killed by hand from Task Manager.

So the helper now refuses to relaunch under a supervisor, and says so in its log and in the tool's receipt.
`supervised` is set when the host was spawned with an IPC channel (`process.channel !== undefined`) — exactly
how the desktop app starts its host (`stdio: [..., 'ipc']`) and never how a shell or headless launch does.
Under a supervisor the correct move is to write the marker and stop: the supervisor owns the host lifecycle,
and the marker plus the durable watch mean the wakeup fires whenever it brings the host back, however long
the operator takes.

The desktop app window is still the casualty of the kill itself — it is the host's parent, so it survives and
lands on that dialog. In supervised mode that dialog is now the *intended* recovery path: click 重启应用 (or
close and reopen the app), the app starts a fresh host, that host binds the port normally, finds the marker,
and wakes your session.

## Routes

- `GET /plugins/dsh-sentinel/state?sessionId=…` — read-only state for the dock and the sidebar panel (omit `sessionId` for every session).
- `GET /plugins/dsh-sentinel/dashboard` — the server-global watch table.
- `POST /plugins/dsh-sentinel/hook?id=watch-N&s=<sessionId>` — webhook entry; put a `curl` into a CI job, git hook, or another machine's script to wake the agent. Watch ids are per session, so the `s` qualifier is what keeps two sessions' `watch-1` hooks from colliding (the tool hands out the full URL). URLs without `s` still work and resolve to the first matching webhook watch.
- `POST /plugins/dsh-sentinel/cancel?sessionId=…&id=watch-N` — manual cancel. The dashboard table and every UI row carry a ✕ that calls this, so a watch can always be stopped by hand — including orphaned ones whose session (and agent) is long gone; the host has no session-deleted event, so this is the kill switch of last resort.
- All four routes enforce a browser-trust fence: browser-marked cross-site requests (a malicious page can form-POST to localhost) and DNS-rebinding attempts (Host/Origin naming a DNS host) get 403. Headerless clients such as `curl` and CI jobs are unaffected. The state route also reports `duty` (lease heartbeat age) and `droppedWakeups` per session (queued wakeups dropped by the `maxPendingWakeups` cap).

First-probe semantics: a pattern-less watch absorbs its first observation as the baseline (no fire), while a pattern watch whose target already matches fires on the first probe — the condition already holds.

## Compatibility

Verified against these harness versions (plugin loads, duty lease is held, web routes answer):

- `0.2.0-rc.2` — 2026-10-01, Windows desktop deployment: harness ranges re-pinned to the 0.2.0 line and
  `@deepseek-ai/schemastery` aligned to the host's `^3.18.4`, which is what the stricter 0.2.0 schema typing
  needs (`Schema<Config>` no longer accepted a schema whose `meta.default` carries schemastery's newer
  `Volatile` markers). Two Windows defects were found and fixed on the way: `fs.watch` on a **directory**
  whose path still holds an 8.3 short component (e.g. `C:\Users\ADMINI~1\...`) makes libuv abort the whole
  process — `Assertion failed: !_wcsnicmp(filename, dir, dirlen), file src\win\fs-event.c, line 72`, exit
  0xC0000409, uncatchable — so the file-watch arm now canonicalizes with `realpath` (which expands short
  names on Windows) and refuses a directory arm whose path it cannot prove safe, falling back to heartbeat
  polling; and the command-probe fixture used POSIX-only `printf`/`exit`. Verified: `pnpm typecheck` clean and
  all 70 tests pass on Windows, including the e2e file-watch push test that previously aborted the runner.
  Two latent flakes in that suite were also found and closed rather than papered over: the duty-owner test ran
  `dutyLeaseTtlMs: 400` under `heartbeatMs: 500`, a configuration in which the single-owner guarantee is false
  by construction for ~20% of every cycle (the runtime now refuses it), and several tests slept fixed durations
  where a file watch's first probe had to land before the watched file appeared — the assertion was really
  "the scheduler was kind", so they now poll the durable sidecar row instead. Measured across 29 consecutive
  full-suite runs after the change: one failure that never reproduced (in a run overlapping a concurrent
  build), against roughly one failing run in three before. The harness imports also moved from `dependencies`
  to `peerDependencies` — see "Why the harness imports are peers" below, which is what clears the dsh-market
  host-dependency warning and stops the plugin reserving `@deepseek-ai/dsh-tools` and `dsh-llm` away from the
  host
- `0.1.7-rc.2` — 2026-09-29, clean-profile upgrade rehearsal against a copy of the live profile: 0.1.7 removed the shared catch-all `plugin` message-source kind (every producer now declares its own), so a wakeup carries `{ kind: 'sentinel' }` — same `context` placement in the transcript, and it renders as "Sentinel" on both lines. The harness dependency range was also re-pinned to the 0.1.7 line, because under strict semver `>=0.1.5-rc.2 <0.2.0` does **not** admit `0.1.7-rc.2` (the prerelease rule); left alone, a 0.1.7 host would have resolved this plugin's harness imports to 0.1.5 copies — the exact drift the 0.1.5 alignment removed. Verified: `pnpm typecheck` and all 63 tests pass, the plugin activates and holds the duty lease, the web routes answer, and the served client bundle carries `sidebar.panellist` (65 boot rows)
- `0.1.5-rc.2` — 2026-09-15, live web deployment after the 0.1.5 alignment: the plugin's whole runtime import closure resolves to the deployed line (its harness imports are declared dependencies, so a profile's older hoisted copies can no longer shadow them), the client half builds against the real 0.1.5 types with no shims, `pnpm typecheck` and all 63 tests pass, and a live file watch fired through inotify 1s after the change and the wakeup was delivered into the session as a plugin-sourced message; after the restart the deployment serves the new client half (bundle rev changed, `sidebar.panellist` present, 54 boot rows)
- `0.1.5-alpha.2` — 2026-09-09, temporary web-profile smoke: Node plugin load, duty lease, state/dashboard routes, and the browser plugin bundle all worked with no browser-console errors; `conversation.input.dock` remains a supported session-scoped list slot, and the plugin sidecar is unaffected by the Session V3 migration
- `0.1.1-rc.2` — 2026-08-26, source-build smoke: git install into a web profile, duty lease held, state and dashboard routes answer
- `0.1.0-rc.8` — 2026-08-20, scratch-profile smoke
- `0.1.0-rc.7` — 2026-08-20, live web deployment

Compatibility means the cordis loader entries, the `ctx.agents` followup channel, the declared slot seats, and the web routes keep working; file an issue if a harness version breaks any of them.

### Why the harness imports are peers, not dependencies

`@deepseek-ai/dsh-tools` and `@deepseek-ai/dsh-llm` are the two host packages this plugin imports at runtime (`defineTool`, `createUserMessage`), and they are declared as **peerDependencies**. That is not cosmetic.

`dsh-app-boot`'s `createRuntimeResolution` builds the resolution table plugins load through from two scopes: the installation anchor (the host's own `@deepseek-ai/*` copies) and the profile. `installedProfilePackageNames` collects the profile's direct dependencies that exist on disk — described in its own comment as "installed direct dependencies that Node resolves before profile fallback" — and passes them as `reserved`, which **drops those names from the host's side of the table**. Node resolves them from the profile first regardless. So a plugin that declares a host core package as an ordinary dependency does not merely carry a second copy: it takes the name away from the host for the rest of the profile.

dsh-market documents the observed result and puts exactly these names in `KNOWN_SHARED_HOST_PACKAGES`: "the dsh-excel-chat failure mode where the plugin's copy gets hoisted to the profile root and shadows the host's version (tool calls die, minimal preset fails to mount)". Installing this plugin the old way put `@deepseek-ai/dsh-llm`, `dsh-tools` and `dsh-scope` plus six transitive packages at the profile root, in precisely that configuration.

Declaring them as peers binds the plugin to the host's single instance and lets DSH's own compatibility checker compare the peer range against the running host, so a version gap becomes a visible warning instead of silent drift. They stay in `devDependencies` so this repo still typechecks and builds standalone. `@deepseek-ai/dsh-scope` was dropped outright — nothing imported it.

> This reverses the reasoning the 0.1.5/0.1.7 notes above record, which made the imports *dependencies* so a profile's hoisted copies could not shadow them. That concern was real, but it was aimed at undeclared resolution; declaring the peer is the correct form of the same intent, and the dependency form carries a cost those notes did not account for.

Verified by cold-loading: a throwaway `headless` profile with this plugin installed, **no** `@deepseek-ai/dsh-tools` or `dsh-llm` anywhere in its `node_modules`, boots and answers `sentinel_list` — the host supplies both packages through the resolution table.

## Install

This is the fork [`AEmbers/dsh-sentinel`](https://github.com/AEmbers/dsh-sentinel), tracking upstream
[`fuhefei/dsh-sentinel`](https://github.com/fuhefei/dsh-sentinel) with the 0.2.0-line compatibility work and the
`sentinel_restart` tool. Build artifacts are committed, so a git-source install runs no build:

```sh
dsh plugin --profile <profile> add "github:AEmbers/dsh-sentinel#v0.13.0"
```

Upstream, through the official bundle channel or from git:

```sh
dsh plugin --profile web add dsh-sentinel
dsh plugin --profile web add "github:fuhefei/dsh-sentinel#v0.12.1"
```

Alternatively, add the node half manually through a patch-list configuration over the shipped base:

```yaml
# cordis.patch.yml
- insert:
    - id: dsh-sentinel
      name: dsh-sentinel
```

The browser half ships in the same package (`./client`) and is injected by the Web UI's plugin loader.

### Sidebar surface (0.1.5 and later)

The dock, the sidebar entry and the dashboard all work on a stock host: the entry registers into the official `sidebar.panellist` seat and its panel into the layout's `main` slot, with no host patch.

Through 0.1.2 the global view instead grew a branch under each watched session row, which needed the session-row holes the official tree never declared; that path is retired with `patches/session-row-holes.patch` kept only for those older trees. 0.1.5 dropped the hole, so a plugin built for it must use the panel seat above.

### better-sidebar integration (optional)

When [dsh-better-sidebar](https://github.com/omdsh-dev/DSH-better-sidebar) is installed in the same profile, sentinel registers its global watch table as a sidebar tab (`dsh-sentinel:watches`, in the **+** menu) through better-sidebar's documented `ctx.betterSidebar.registerTab` extension surface: every watch server-wide with live probe state, fire budgets and recent fire history, fed by one shared poller. No configuration needed; without better-sidebar the registration is silently skipped and the dock / panel / dashboard keep working as before.

![Sentinel tab inside the better-sidebar workbench](docs/preview/sentinel-better-sidebar-tab.png)

### Plays well with

Install [dsh-notification](https://github.com/omdsh-dev/dsh-notification) alongside sentinel and the whole wakeup loop reaches your desktop: sentinel wakes the agent, the agent works the turn, and the turn's completion fires a desktop notification — no integration needed, the two plugins compose on their own.

## Develop

```sh
npm install
npm run build     # tsc -b + tsdown (lib/index.js, lib/client.js)
npm test          # vitest: domain fold/normalize, sensors, dashboard escaping, e2e wakeup flow
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).
