import { execFileSync } from 'node:child_process'
import { mkdir, mkdtemp, readFile, readdir, rm, utimes, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, describe, expect, it } from 'vitest'
import {
  derivedRelaunch,
  describeRelaunch,
  goPath,
  helperPath,
  markerPath,
  pruneRestartArtifacts,
  requestPath,
  restartDir,
  writeRestartHelper,
} from '../src/restart.ts'

const originalHome = process.env['DSH_HOME']
const dirs: string[] = []

afterAll(async () => {
  await Promise.all(dirs.map(dir => rm(dir, { recursive: true, force: true })))
  if (originalHome === undefined) delete process.env['DSH_HOME']
  else process.env['DSH_HOME'] = originalHome
})

async function freshHome(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-restart-'))
  dirs.push(dir)
  process.env['DSH_HOME'] = dir
  return dir
}

describe('derivedRelaunch', () => {
  it('rebuilds the exact command line that started this process', () => {
    const plan = derivedRelaunch(
      'C:\\host.exe',
      ['C:\\host.exe', 'C:\\cli.js', 'web', '--port', '19387'],
      ['--expose-internals'],
    )
    expect(plan).toEqual({
      // execArgv is what keeps launcher-level node flags (the desktop app's
      // --expose-internals) in the relaunch instead of silently dropping them.
      argv: ['C:\\host.exe', '--expose-internals', 'C:\\cli.js', 'web', '--port', '19387'],
      cwd: process.cwd(),
    })
  })

  it('refuses to invent a command line when there is no entry script', () => {
    // The one case that must abort: a relaunch we cannot point at anything.
    expect(derivedRelaunch('C:\\host.exe', ['C:\\host.exe'], [])).toBeUndefined()
    expect(derivedRelaunch('C:\\host.exe', ['C:\\host.exe', ''], [])).toBeUndefined()
  })
})

describe('describeRelaunch', () => {
  it('quotes only the parts that need it', () => {
    expect(describeRelaunch(['C:\\a b\\host.exe', '--flag', 'C:\\cli.js']))
      .toBe('"C:\\a b\\host.exe" --flag C:\\cli.js')
  })
})

describe('restart artifacts', () => {
  it('keeps every path under $DSH_HOME/sentinel-restart', async () => {
    const home = await freshHome()
    const base = join(home, 'sentinel-restart')
    expect(restartDir()).toBe(base)
    expect(requestPath('x')).toBe(join(base, 'request-x.json'))
    expect(goPath('x')).toBe(join(base, 'go-x'))
    expect(markerPath('x')).toBe(join(base, 'ready-x.flag'))
    expect(helperPath()).toBe(join(base, 'helper-v1.mjs'))
  })

  it('prunes day-old residue but never the helper itself', async () => {
    await freshHome()
    await mkdir(restartDir(), { recursive: true })
    const helper = await writeRestartHelper()
    const stale = requestPath('old')
    const fresh = requestPath('new')
    await writeFile(stale, '{}', 'utf8')
    await writeFile(fresh, '{}', 'utf8')
    const past = new Date(Date.now() - 48 * 60 * 60 * 1000)
    await utimes(stale, past, past)

    await pruneRestartArtifacts()

    expect(await readdir(restartDir())).toContain('helper-v1.mjs')
    expect(await readdir(restartDir())).toContain('request-new.json')
    expect(await readdir(restartDir())).not.toContain('request-old.json')
    // The helper is rewritten on every handoff, so pruning must leave it alone.
    expect(await readFile(helper, 'utf8')).toContain('dsh-sentinel restart helper')
  })
})

describe('the embedded helper script', () => {
  it('is valid ES module source that node can parse', async () => {
    await freshHome()
    const helper = await writeRestartHelper()
    // A syntax error here would only surface on a real restart, i.e. at the
    // worst possible moment, on a machine that is being killed by it.
    expect(() => execFileSync(process.execPath, ['--check', helper], { stdio: 'pipe' })).not.toThrow()
  })

  it('reads the request the runtime actually writes', async () => {
    await freshHome()
    const helper = await writeRestartHelper()
    const source = await readFile(helper, 'utf8')
    // The script and the writer are two halves of one format; these are the
    // field names it dereferences, so a rename on one side fails here.
    for (const field of [
      'hostPid', 'goPath', 'markerPath', 'logPath', 'idleTimeoutMs',
      'respawnGraceMs', 'cwd', 'argv', 'command',
    ]) {
      expect(source).toContain(`request.${field}`)
    }
  })
})
