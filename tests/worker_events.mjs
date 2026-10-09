// Fully local compiled-Worker routes, D1 triggers and live SSE. No deploy.
import { readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import assert from 'node:assert/strict'

const { Miniflare } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')
const fixture = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const mf = new Miniflare({
  modules: true,
  scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [
    {type: 'ESModule', include: ['**/*.js', '**/*.mjs']},
    {type: 'CompiledWasm', include: ['**/*.wasm']},
  ],
  compatibilityDate: '2024-01-01',
  bindings: {CHAIN: 'main'},
  d1Databases: ['DB'],
  outboundService: () => { throw new Error('external requests disabled in the lane harness') },
})
try {
  const db = await mf.getD1Database('DB')
  for (const name of readdirSync('migrations').filter(n => n.endsWith('.sql')).sort()) {
    // D1.exec splits on newlines. Let SQLite identify complete statements,
    // including whole trigger bodies, then apply the migration as one batch.
    const split = spawnSync('python3', ['-c', `
import json, sqlite3, sys
statements, pending = [], ''
for char in sys.stdin.read():
    pending += char
    if char == ';' and sqlite3.complete_statement(pending):
        statements.append(pending)
        pending = ''
print(json.dumps(statements))
`], {input: readFileSync('migrations/' + name, 'utf8'), encoding: 'utf8'})
    assert.equal(split.status, 0, split.stderr)
    await db.batch(JSON.parse(split.stdout).map(sql => db.prepare(sql)))
  }
  const [root, a1, b1, b2] = fixture.headers
  const works = new Map()
  for (const [h, active, tip] of [[root, 1, 1], [a1, 1, 0], [b1, 0, 0], [b2, 0, 0]]) {
    const size = h.bits >>> 24
    const target = BigInt(h.bits & 0x7fffff) << BigInt(8 * (size - 3))
    const work = (1n << 256n) / (target + 1n) + (works.get(h.previousHash) ?? 0n)
    works.set(h.hash, work)
    const statement = db.prepare('INSERT INTO headers (previous_hash,height,is_active,is_chain_tip,hash,chain_work,version,merkle_root,time,bits,nonce) VALUES (?,?,?,?,?,?,?,?,?,?,?)')
      .bind(h.previousHash, h.height, active, tip, h.hash, work.toString(16).padStart(64, '0'), h.version, h.merkleRoot, h.time, h.bits, h.nonce)
    if (h.hash === a1.hash) {
      await db.batch([statement, db.prepare('UPDATE headers SET is_chain_tip = 0 WHERE is_chain_tip = 1'), db.prepare('UPDATE headers SET is_chain_tip = 1 WHERE hash = ?').bind(a1.hash)])
    } else await statement.run()
  }
  await db.batch([
    db.prepare('UPDATE headers SET is_active = 0, is_chain_tip = 0 WHERE height > ?').bind(root.height),
    db.prepare('UPDATE headers SET is_active = 1 WHERE hash IN (?,?)').bind(b1.hash, b2.hash),
    db.prepare('UPDATE headers SET is_chain_tip = 1 WHERE hash = ?').bind(b2.hash),
  ])
  const get = (path, headers) => mf.dispatchFetch('https://worker.test' + path, {headers})
  const polled = await get('/events?since=0')
  assert.equal(polled.status, 200)
  assert.equal(polled.headers.get('cache-control'), 'no-store')
  const pageText = await polled.text()
  const page = JSON.parse(pageText)
  assert.equal(page.v, 1)
  const reorg = page.events.find(e => e.event.kind === 'reorg')
  assert.deepEqual(reorg.event, fixture.envelope)
  const replay = await get('/events/stream?since=0')
  assert.equal(replay.headers.get('content-type'), 'text/event-stream')
  const reader = replay.body.getReader()
  const chunk = new TextDecoder().decode((await reader.read()).value)
  assert(chunk.includes(`id: ${reorg.cursor}\ndata: ${JSON.stringify(reorg.event)}\n\n`))
  await reader.cancel()
  for (const prefix of ['/v2', '/chaintracks/v2']) {
    const compatibility = await get(prefix + '/reorg/stream?since=0')
    const stream = compatibility.body.getReader()
    const bytes = new TextDecoder().decode((await stream.read()).value)
    const rows = bytes.split('\n').filter(line => line.startsWith('data: ')).map(line => JSON.parse(line.slice(6)))
    assert.deepEqual(rows, [fixture.view])
    await stream.cancel()
    const tip = await get(prefix + '/tip/stream')
    const tips = tip.body.getReader()
    const tipRows = new TextDecoder().decode((await tips.read()).value).split('\n').filter(line => line.startsWith('data: ')).map(line => JSON.parse(line.slice(6)))
    assert.equal(tipRows[0].hash, b2.hash)
    assert.equal(Object.keys(tipRows[0]).length, 8)
    await tips.cancel()
  }
  assert.equal((await get('/events?since=-1')).status, 400)
  assert.equal((await get('/events?since=9007199254740991')).status, 409)
  const live = await get('/events/stream?since=0', {'Last-Event-ID': page.cursor})
  const liveReader = live.body.getReader()
  assert.equal(new TextDecoder().decode((await liveReader.read()).value), ': heartbeat\n\n')
  const next = {v: 1, kind: 'frozen', outpoint: {txid: a1.merkleRoot, vout: 0}}
  await db.prepare('INSERT INTO chain_events(payload) VALUES (?)').bind(JSON.stringify(next)).run()
  const pushed = new TextDecoder().decode((await liveReader.read()).value)
  assert(pushed.includes('data: ' + JSON.stringify(next)))
  await liveReader.cancel()
  const since = JSON.parse(await (await get('/events?since=' + page.cursor)).text())
  assert.deepEqual(since.events.map(e => e.event), [next])
  const fresh = await get('/events/stream', {'Last-Event-ID': since.cursor})
  const heartbeat = fresh.body.getReader()
  await heartbeat.read()
  assert.equal(new TextDecoder().decode((await heartbeat.read()).value), ': heartbeat\n\n')
  await heartbeat.cancel()
  console.log('Local compiled Worker GREEN: D1 migrations/triggers, poll/SSE byte equality, both TS route aliases, Last-Event-ID, live delivery, 15 s heartbeat, bad/future cursor statuses')
} finally {
  await mf.dispose()
}
