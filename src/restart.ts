/**
 * Restart handoff: replace the running host process and come back to the
 * session that asked for it.
 *
 * The shape of the problem. A plugin lives *inside* the host it would have to
 * restart, so it cannot both die and revive itself. The handoff therefore uses
 * three pieces that each survive what the previous one cannot:
 *
 *   1. The calling session registers a one-shot `file` watch on a marker path
 *      under `$DSH_HOME/sentinel-restart/`, and the baseline for that watch is
 *      probed and persisted BEFORE anything is killed. That persisted baseline
 *      is the whole trick: the replacement host re-seeds its probe state from
 *      it, so a marker written during the downtime is a real snapshot change
 *      and fires the moment the new host starts probing.
 *
 *   2. A detached helper process (this file's embedded script) owns the
 *      destructive half. It waits for the agent to go idle, kills the host
 *      tree, writes the marker, and — only if nothing else already brought the
 *      host back — relaunches it.
 *
 *   3. The sentinel runtime, rebuilt from its sidecar log inside the new host,
 *      folds the watch, sees the marker, and resumes the dormant session with
 *      the note. No port, no URL, no readiness handshake: the marker file is
 *      the entire interface between the two processes.
 *
 * Killing is deliberately done by the OUTSIDE process, never by the tool. The
 * tool returns a receipt and the turn finishes normally; only once the agent
 * is idle does the helper pull the plug, so a restart cannot truncate the very
 * turn that requested it.
 */
import { spawn } from 'node:child_process'
import { mkdir, readdir, rm, stat, writeFile } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join } from 'node:path'

/** `$DSH_HOME/sentinel-restart` — every artifact of the handoff lives here. */
export function restartDir(): string {
  const home = process.env['DSH_HOME'] ?? join(homedir(), '.dsh')
  return join(home, 'sentinel-restart')
}

export function requestPath(id: string): string {
  return join(restartDir(), `request-${id}.json`)
}

/** The helper polls for this file; the runtime writes it on the agent's idle edge. */
export function goPath(id: string): string {
  return join(restartDir(), `go-${id}`)
}

/** The watched marker: absent before the restart, written during the downtime. */
export function markerPath(id: string): string {
  return join(restartDir(), `ready-${id}.flag`)
}

export function helperPath(): string {
  return join(restartDir(), 'helper-v1.mjs')
}

/** A handoff request, serialized to disk for the detached helper. */
export interface RestartRequest {
  readonly version: 1
  readonly id: string
  /** The host process to replace (the one running this plugin). */
  readonly hostPid: number
  readonly sessionId: string
  readonly watchId: string
  readonly goPath: string
  readonly markerPath: string
  readonly logPath: string
  /** Relaunch as argv (verified-safe default) … */
  readonly argv?: readonly string[]
  /** … or as a shell command line (operator-supplied override). */
  readonly command?: string
  readonly cwd: string
  /** Web port the host served on, if known; used only to detect a supervised respawn. */
  readonly port?: number
  /**
   * True when the host was spawned with an IPC channel, i.e. something parented
   * it and expects to supervise it — the Desktop app does exactly this
   * (`stdio: [..., 'ipc']`). Such a supervisor owns the host's lifecycle and
   * will not respawn it after an unexpected exit, but it *stays alive* and
   * brings the host back when the operator restarts the app. Relaunching
   * ourselves in that window is worse than doing nothing: the replacement binds
   * the web port first, so the supervisor's own host then dies of EADDRINUSE
   * and every retry fails until someone kills the orphan by hand. Observed for
   * real — three consecutive `listen EADDRINUSE: address already in use
   * 0.0.0.0:19387` startup crashes.
   */
  readonly supervised?: boolean
  /**
   * The supervising process — the desktop app that parented this host. Set
   * together with `supervisorRelaunch`, and only when the supervisor can
   * actually be brought back (see below). The helper takes this whole tree down
   * *including the supervisor itself*, and it does so supervisor-first: the app
   * reacts to its host's exit by calling `this.fail()` in the child's `close`
   * handler, and no exit code avoids that — the only way to keep the modal
   * dialog off the screen is for the app to be gone before it can run.
   */
  readonly supervisorPid?: number
  /**
   * How to bring the supervisor back, as argv. The helper strips
   * `ELECTRON_RUN_AS_NODE` (inherited from the host, which *is* Electron running
   * as Node) and launches this detached, so the desktop app starts normally and
   * spawns a host of its own — which binds the port and picks up the marker.
   *
   * `app.relaunch()` is the app's own restart, but nothing outside the app can
   * invoke it, and a second launch is only routed to `second-instance`, which
   * focuses the running window instead of restarting anything. Killing and
   * relaunching is therefore the whole of the automatic path.
   */
  readonly supervisorRelaunch?: readonly string[]
  readonly idleTimeoutMs: number
  readonly respawnGraceMs: number
}

/**
 * Rebuild the command line that started this process.
 *
 * `process.argv[1]` is the host entry script and `process.execArgv` carries the
 * node-level flags the launcher passed (the desktop app uses `--expose-internals`),
 * so reassembling both is what makes the relaunch equivalent rather than
 * approximately equivalent. Returns undefined when there is no entry script to
 * point at, which is the one case where the tool must refuse instead of killing
 * a host it cannot bring back.
 */
export function derivedRelaunch(
  execPath: string = process.execPath,
  argv: readonly string[] = process.argv,
  execArgv: readonly string[] = process.execArgv,
): { argv: string[]; cwd: string } | undefined {
  const entry = argv[1]
  if (entry === undefined || entry === '') return undefined
  return { argv: [execPath, ...execArgv, entry, ...argv.slice(2)], cwd: process.cwd() }
}

/** Human-readable form of a relaunch plan, for receipts and logs. */
export function describeRelaunch(argv: readonly string[]): string {
  return argv.map(part => (part.includes(' ') ? `"${part}"` : part)).join(' ')
}

/** Delete handoff residue from previous runs (called on each new request). */
export async function pruneRestartArtifacts(): Promise<void> {
  let entries: string[]
  try {
    entries = await readdir(restartDir())
  } catch {
    return
  }
  const cutoff = Date.now() - 24 * 60 * 60 * 1000
  await Promise.all(entries.map(async entry => {
    if (entry === 'helper-v1.mjs') return
    const path = join(restartDir(), entry)
    try {
      const info = await stat(path)
      if (info.mtimeMs < cutoff) await rm(path, { recursive: true, force: true })
    } catch {
      // A racing cleanup already took it; nothing to do.
    }
  }))
}

/**
 * Materialize the helper script. Split out from {@link spawnRestartHelper} so
 * the script can be syntax-checked (and its format guaranteed) without ever
 * starting a process that kills things.
 */
export async function writeRestartHelper(): Promise<string> {
  await mkdir(restartDir(), { recursive: true })
  await writeFile(helperPath(), HELPER_SOURCE, 'utf8')
  return helperPath()
}

/**
 * Write the helper script and start it detached.
 *
 * `detached` + `unref` is what makes the helper outlive its parent, and the
 * helper re-spawns itself once more before doing anything destructive: on
 * Windows the host tree is killed through the parent-PID chain, so a helper
 * that still hung off the host would be killed by its own hand.
 */
export async function spawnRestartHelper(request: RestartRequest): Promise<void> {
  await writeRestartHelper()
  await writeFile(requestPath(request.id), `${JSON.stringify(request, null, 2)}\n`, 'utf8')
  const child = spawn(process.execPath, [helperPath(), requestPath(request.id)], {
    // The desktop host runs the Electron binary as Node; the relaunched helper
    // must do the same. Plain Node ignores the variable.
    env: { ...process.env, ELECTRON_RUN_AS_NODE: '1' },
    cwd: request.cwd,
    detached: true,
    windowsHide: true,
    stdio: 'ignore',
  })
  child.unref()
}

/**
 * The detached helper, embedded so the package stays a single artifact and the
 * script can never drift from the request format it has to read.
 *
 * Written without template literals or backticks: this source is itself a
 * template-literal body, so `$` + `{` would be interpolated here rather than
 * shipped.
 */
const HELPER_SOURCE = `// dsh-sentinel restart helper (generated; do not edit — see src/restart.ts).
import { readFile, writeFile, appendFile, mkdir, unlink } from 'node:fs/promises'
import { spawn, execFileSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname } from 'node:path'

const requestFile = process.argv[2]
const request = JSON.parse(await readFile(requestFile, 'utf8'))
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const log = async (message) => {
  try {
    await appendFile(request.logPath, new Date().toISOString() + ' ' + message + '\\n')
  } catch {
    // Logging is best-effort; never let it change the outcome.
  }
}

const isAlive = (pid) => {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    return error && error.code === 'EPERM'
  }
}

const settleMs = 2000

try {
  await mkdir(dirname(request.logPath), { recursive: true })
  await log('helper started (pid ' + String(process.pid) + '), target host pid ' + String(request.hostPid))

  // Re-spawn one stage further out and let the parent exit: the host tree is
  // killed by walking parent PIDs, and an orphan no longer hangs off it.
  if (process.env.DSH_SENTINEL_RESTART_STAGE !== '2') {
    const stage2 = spawn(process.execPath, [process.argv[1], requestFile], {
      env: { ...process.env, DSH_SENTINEL_RESTART_STAGE: '2' },
      cwd: request.cwd,
      detached: true,
      windowsHide: true,
      stdio: 'ignore',
    })
    stage2.unref()
    await log('handed off to stage 2 (pid ' + String(stage2.pid) + ')')
    process.exit(0)
  }

  // 1. Wait for the agent to go idle. The runtime writes the go file on the
  //    idle edge, so the turn that asked for the restart finishes cleanly.
  const deadline = Date.now() + request.idleTimeoutMs
  while (!existsSync(request.goPath) && Date.now() < deadline) await sleep(500)
  if (existsSync(request.goPath)) await log('agent reported idle')
  else await log('idle timeout elapsed (' + String(request.idleTimeoutMs) + 'ms); proceeding anyway')

  await sleep(settleMs)

  // 2. Kill the host and everything it spawned, but never our own subtree.
  //    When a supervisor is recorded we take its whole tree instead — and take
  //    the supervisor FIRST. The Desktop app answers its host's exit from the
  //    child close handler by calling this.fail(), and it does that for EVERY
  //    exit code, so no graceful shutdown keeps the modal dialog off the
  //    screen. Killing the app before the host is the only thing that does.
  const rootPid = request.supervisorPid === undefined ? request.hostPid : request.supervisorPid
  const victims = collectDescendants(rootPid).filter((pid) => pid !== process.pid)
  await log((request.supervisorPid === undefined ? 'killing host descendants: [' : 'killing supervisor tree: [') + victims.join(',') + '] then pid ' + String(rootPid))
  if (request.supervisorPid !== undefined) {
    try { process.kill(rootPid) } catch (error) { await log('supervisor kill failed: ' + String(error)) }
  }
  for (const pid of victims.reverse()) {
    try { process.kill(pid) } catch { /* already gone */ }
  }
  if (request.supervisorPid === undefined) {
    try { process.kill(request.hostPid) } catch (error) { await log('host kill failed: ' + String(error)) }
  }

  // 3. Confirm the host, and any supervisor, actually died. Marker-writes are
  //    only meaningful once the old prober is gone; otherwise the stale host
  //    would fire the wakeup against a restart that never happened.
  const supervisorAlive = () => request.supervisorPid !== undefined && isAlive(request.supervisorPid)
  for (let waited = 0; waited < 10000 && (isAlive(request.hostPid) || supervisorAlive()); waited += 250) await sleep(250)
  if (isAlive(request.hostPid) || supervisorAlive()) {
    await log('host or supervisor still alive after 10s; aborting without writing the marker')
    await unlink(requestFile).catch(() => {})
    process.exit(1)
  }
  await log('host pid ' + String(request.hostPid) + ' is gone')

  // 4. The marker IS the wakeup. Written while the host is down, it is a real
  //    snapshot change against the baseline persisted before the restart.
  await writeFile(request.markerPath, new Date().toISOString() + '\\n', 'utf8')
  await log('marker written: ' + request.markerPath)

  // 5. Give whatever launched us a chance to bring the host back on its own.
  //    A supervisor (the Desktop app) does NOT respawn an unexpected exit: it
  //    parks on a fatal dialog and waits for the operator. But it IS still
  //    alive and owns the host, so relaunching here would take the web port
  //    from the host that supervisor starts on restart — and every retry then
  //    dies of EADDRINUSE until someone kills the orphan by hand. So never
  //    relaunch under a supervisor. The marker is already written and the
  //    watch is durable, so the wakeup fires whenever the host comes back,
  //    however long the operator takes.
  await sleep(request.respawnGraceMs)
  if (await hostAnswers(request)) {
    await log('host came back on its own; not relaunching')
  } else if (request.supervisorRelaunch !== undefined) {
    // The supervisor went down with the host, so nothing brings it back but us.
    // Its own app.relaunch() is unreachable from outside, and a second launch
    // is only routed to second-instance, which focuses the running window. So
    // the app is relaunched here, and it starts a host of its own.
    const argv = request.supervisorRelaunch
    await log('relaunching the supervising app: ' + argv.join(' '))
    const appEnv = { ...process.env }
    delete appEnv.ELECTRON_RUN_AS_NODE
    const app = spawn(argv[0], argv.slice(1), {
      cwd: dirname(argv[0]),
      env: appEnv,
      detached: true,
      windowsHide: true,
      stdio: 'ignore',
    })
    app.unref()
    // The port probe is only meaningful when the request carried one.
    if (request.port !== undefined) {
      let up = false
      for (let waited = 0; waited < 30000 && !up; waited += 1000) {
        await sleep(1000)
        up = await hostAnswers(request)
      }
      await log(up ? 'supervising app is back and serving' : 'supervising app did not serve within 30s')
    }
  } else if (request.supervised === true) {
    await log('host is supervised but the supervisor cannot be relaunched from here; not relaunching. Restart the app that owns this host — the wakeup marker is already written and the watch survives the wait.')
  } else if (request.command !== undefined) {
    await log('relaunching via shell command: ' + request.command)
    const child = spawn(request.command, {
      shell: true,
      cwd: request.cwd,
      env: { ...process.env, ELECTRON_RUN_AS_NODE: '1' },
      detached: true,
      windowsHide: true,
      stdio: 'ignore',
    })
    child.unref()
  } else {
    const argv = request.argv || []
    await log('relaunching: ' + argv.map((p) => (p.indexOf(' ') >= 0 ? '"' + p + '"' : p)).join(' '))
    const child = spawn(argv[0], argv.slice(1), {
      cwd: request.cwd,
      env: { ...process.env, ELECTRON_RUN_AS_NODE: '1' },
      detached: true,
      windowsHide: true,
      stdio: 'ignore',
    })
    child.unref()
  }

  await unlink(requestFile).catch(() => {})
  await unlink(request.goPath).catch(() => {})
  await log('helper done')
  process.exit(0)
} catch (error) {
  await log('helper failed: ' + String(error && error.stack ? error.stack : error))
  process.exit(1)
}

/**
 * Is the recorded host port answering again? Probe kinds are not available
 * here, so this is a plain TCP connect against the web port the request
 * carried; a false negative only costs one redundant relaunch attempt.
 */
async function hostAnswers(req) {
  if (req.port === undefined || req.port === null) return false
  const net = await import('node:net')
  return await new Promise((resolve) => {
    const socket = net.connect({ host: '127.0.0.1', port: req.port })
    let settled = false
    const finish = (value) => {
      if (settled) return
      settled = true
      socket.destroy()
      resolve(value)
    }
    socket.setTimeout(2000, () => finish(false))
    socket.on('connect', () => finish(true))
    socket.on('error', () => finish(false))
  })
}

/** Every descendant of a pid, deepest last, so callers can kill in reverse. */
function collectDescendants(rootPid) {
  if (process.platform !== 'win32') return []
  let rows
  try {
    const raw = execFileSync('powershell', [
      '-NoProfile',
      '-NonInteractive',
      '-Command',
      'Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId | ConvertTo-Json -Compress',
    ], { encoding: 'utf8', windowsHide: true, timeout: 20000 })
    const parsed = JSON.parse(raw)
    rows = Array.isArray(parsed) ? parsed : [parsed]
  } catch {
    return []
  }
  const byParent = new Map()
  for (const row of rows) {
    if (row && typeof row.ParentProcessId === 'number') {
      const list = byParent.get(row.ParentProcessId) || []
      list.push(row.ProcessId)
      byParent.set(row.ParentProcessId, list)
    }
  }
  const acc = []
  const walk = (pid) => {
    for (const child of byParent.get(pid) || []) {
      acc.push(child)
      walk(child)
    }
  }
  walk(rootPid)
  return acc
}
`
