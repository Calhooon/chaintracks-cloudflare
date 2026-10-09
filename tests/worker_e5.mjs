// E5 on the compiled Worker, fully local: the push source. A scripted peer
// plays Arcade's chaintracks v2 tip stream (an SSE response this script
// writes into); every other outbound request is caught and answered here;
// none reaches the network. No deploy. Real mainnet headers (src/testdata)
// through the Worker's own routes and the object's door.
//
// MINIFLARE_MODULE=<miniflare>/dist/src/index.js node tests/worker_e5.mjs
import { readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import assert from 'node:assert/strict'

const { Miniflare, Response } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')

// The real run 886001..=888000 (src/testdata/README.md).
const FIRST = 886001
const run = readFileSync('src/testdata/main_886001_888000.bin')
const raw = height => run.subarray((height - FIRST) * 80, (height - FIRST + 1) * 80)
const sha = b => createHash('sha256').update(b).digest()
const flip = b => Buffer.from(b).reverse().toString('hex')
const fieldsOf = (b, height) => ({
  version: b.readUInt32LE(0), previousHash: flip(b.subarray(4, 36)), merkleRoot: flip(b.subarray(36, 68)),
  time: b.readUInt32LE(68), bits: b.readUInt32LE(72), nonce: b.readUInt32LE(76), height, hash: flip(sha(sha(b))),
})
const fields = height => fieldsOf(raw(height), height)

const ANCHOR = 886147
const TIP = 886200
const ARCADE = 'arcade.test'
const WOC = 'api.whatsonchain.com'
const BITAILS = 'api.bitails.io'
const HEARTBEAT_S = 3

// ─── The scripted peer ───────────────────────────────────────────────────
const out = []
const connections = []
const byHash = new Map()
let streamRefuses = false
// A courier request: what the minute poll asks (the stream and the walk's by-hash reads are not).
const courierRequests = () => out.filter(u => u.host === WOC || u.host === BITAILS || (u.host === ARCADE && !u.pathname.endsWith('/tip/stream') && !u.pathname.includes('/header/hash/'))).length
const enc = new TextEncoder()
const refuse = () => new Response('unavailable', {status: 503})
const json = value => new Response(JSON.stringify(value), {headers: {'content-type': 'application/json'}})
const outboundService = request => {
  const url = new URL(request.url)
  out.push(url)
  if (url.host === ARCADE && url.pathname === '/chaintracks/v2/tip/stream') {
    if (streamRefuses) return refuse()
    let controller
    const body = new ReadableStream({start(c) { controller = c }})
    connections.push({
      lastEventId: request.headers.get('last-event-id'),
      accept: request.headers.get('accept'),
      send: text => controller.enqueue(enc.encode(text)),
      close: () => controller.close(),
    })
    return new Response(body, {headers: {'content-type': 'text/event-stream', 'cache-control': 'no-cache'}})
  }
  if (url.host === ARCADE) {
    const m = url.pathname.match(/^\/chaintracks\/v2\/header\/hash\/([0-9a-f]{64})$/)
    if (m && byHash.has(m[1])) return json(byHash.get(m[1]))
  }
  // Every courier refuses the poll: the tip moves by the push alone.
  return refuse()
}
const tipEvent = (f, id) => (id ? `id: ${id}\n` : '') + `data: ${JSON.stringify(f)}\n\n`

const mf = new Miniflare({
  modules: true,
  scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [
    {type: 'ESModule', include: ['**/*.js', '**/*.mjs']},
    {type: 'CompiledWasm', include: ['**/*.wasm']},
  ],
  compatibilityDate: '2024-01-01',
  bindings: {
    CHAIN: 'main',
    CHECKPOINTS: `${ANCHOR}:${fields(ANCHOR).hash}`,
    ADMIN_TOKEN: 'local',
    ARCADE_URL: `https://${ARCADE}`,
    UPSTREAM_CHAINTRACKS_URL: '',
    TIP_STREAM_HEARTBEAT_S: String(HEARTBEAT_S),
  },
  d1Databases: ['DB'],
  durableObjects: {TIP_STREAM: {className: 'TipStream', useSQLite: true}},
  outboundService,
})
const until = async (what, test, ms = 15000) => {
  const end = Date.now() + ms
  for (;;) {
    const got = await test()
    if (got) return got
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`)
    await new Promise(r => setTimeout(r, 100))
  }
}
try {
  const db = await mf.getD1Database('DB')
  for (const name of readdirSync('migrations').filter(n => n.endsWith('.sql')).sort()) {
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
  const admin = {Authorization: 'Bearer local'}
  const call = (path, init) => mf.dispatchFetch('https://worker.test' + path, init)
  const body = async r => JSON.parse(await r.text())
  const height = async () => (await body(await call('/currentHeight'))).value
  const ns = await mf.getDurableObjectNamespace('TIP_STREAM')
  const stub = ns.get(ns.idFromName('tip'))
  const status = async () => JSON.parse(await (await stub.fetch('https://tip-stream/status')).text())
  const active = async h => (await db.prepare('SELECT hash FROM headers WHERE height = ?1 AND is_active = 1').bind(h).all()).results.map(r => r.hash)
  const worker = await mf.getWorker()
  const cron = () => worker.scheduled({cron: '* * * * *'})

  // The store: the real run through the Worker's own ingest.
  const seeded = await call(`/admin/ingest?start=${FIRST}`, {method: 'POST', headers: admin, body: run.subarray(0, (TIP - FIRST + 1) * 80).toString('hex')})
  assert.equal(seeded.status, 200, await seeded.clone().text())
  assert.equal(await height(), TIP)

  // The cron wakes the object; it connects to the peer's tip stream.
  out.length = 0
  await cron()
  const c1 = await until('the first connection', () => connections[0])
  assert.equal(c1.accept, 'text/event-stream')
  assert.equal(c1.lastEventId, null, 'nothing to resume from on the first connect')
  const firstTick = courierRequests()
  assert.equal(firstTick, 3, 'the waking tick found the stream down and polled: three couriers asked')

  // Three headers pushed, with ids, a keepalive between: stored, activated, the tip moved.
  c1.send(': keepalive\n\n')
  for (let i = 1; i <= 3; i++) c1.send(tipEvent(fields(TIP + i), String(i)))
  await until('the tip at TIP+3', async () => (await height()) === TIP + 3)
  for (let i = 1; i <= 3; i++) assert.deepEqual(await active(TIP + i), [fields(TIP + i).hash], `${TIP + i} stored, the active row`)
  let st = await status()
  assert.equal(st.live, true)
  assert.deepEqual([st.state.events, st.state.stored, st.state.refused, st.state.lastEventId], [3, 3, 0, '3'])
  assert(!out.some(u => u.host === ARCADE && u.pathname.includes('/header/')), 'no header was fetched: the push carried them')
  assert.deepEqual((await body(await call('/getPresentHeight'))).value, TIP + 3, 'the present height is the push\'s, to the second')

  // Item 3: ten cron ticks behind the live push ask no courier.
  out.length = 0
  const LIVE_TICKS = 10
  for (let i = 0; i < LIVE_TICKS; i++) await cron()
  assert.equal(courierRequests(), 0, 'the push covers every tick: no courier asked')
  let polls = (await status()).polls
  assert.deepEqual([polls.pollsRun, polls.pollsSkipped, polls.pollRequests], [1, LIVE_TICKS, 3])

  // The peer drops the stream: the alarm reconnects, from the last id.
  c1.close()
  const c2 = await until('the reconnect', () => connections[1])
  assert.equal(c2.lastEventId, '3', 'the reconnect resumes from the last event id')
  st = await status()
  assert.equal(st.state.drops, 1)
  assert.match(st.state.lastDrop, /closed/)

  // A wrong header from the peer: its fields hash honestly, its work fails. Refused, nothing stored.
  const wrong = Buffer.from(raw(TIP + 4))
  wrong.writeUInt32LE(wrong.readUInt32LE(76) + 1, 76)
  c2.send(tipEvent(fieldsOf(wrong, TIP + 4), '4'))
  await until('the refusal', async () => (await status()).state.refused === 1)
  assert.equal(await height(), TIP + 3, 'the tip did not move')
  assert.deepEqual(await active(TIP + 4), [])
  assert.equal((await db.prepare('SELECT COUNT(*) AS n FROM headers WHERE height = ?1').bind(TIP + 4).first()).n, 0, 'no row of the wrong header')
  const fault = (await db.prepare('SELECT last_error FROM sync_state WHERE id = 1').first()).last_error
  assert.match(fault, /^push: /, 'the refusal is a logged fault')
  // The right one after it.
  c2.send(tipEvent(fields(TIP + 4), '5'))
  await until('the tip at TIP+4', async () => (await height()) === TIP + 4)

  // The peer goes silent: past the heartbeat the object drops the stream and reconnects.
  const c3 = await until('the heartbeat reconnect', () => connections[2], (HEARTBEAT_S + 10) * 1000)
  st = await status()
  assert.match(st.state.lastDrop, /heartbeat/)
  assert.equal(c3.lastEventId, '5')

  // As Arcade does on every connect: the current tip, no id (known), then a tip above a gap.
  byHash.set(fields(TIP + 5).hash, fields(TIP + 5))
  byHash.set(fields(TIP + 6).hash, fields(TIP + 6))
  c3.send(tipEvent(fields(TIP + 4)))
  c3.send(tipEvent(fields(TIP + 7)))
  await until('the tip at TIP+7', async () => (await height()) === TIP + 7)
  st = await status()
  assert.equal(st.state.known, 1, 'the repeat is known')
  assert.equal(out.filter(u => u.host === ARCADE && u.pathname.includes('/header/hash/')).length, 2, 'the gap took two reads by hash from the rung that holds them')
  // the ladder's start rotates by the minute: the refusing rungs asked ahead of it count too
  assert(st.state.walkRequests >= 2, `the walk's requests are counted: ${st.state.walkRequests}`)
  assert.equal(st.state.lastEventId, '5', 'an event with no id keeps the last one')

  // The cron leaves a live object alone.
  const wake = await body(await stub.fetch('https://tip-stream/wake', {method: 'POST'}))
  assert.equal(wake.woke, false)

  // The stream down (the peer refuses every connect): each tick polls again, every courier asked.
  streamRefuses = true
  c3.close()
  await until('the stream down', async () => !(await status()).live)
  out.length = 0
  const DOWN_TICKS = 5
  for (let i = 0; i < DOWN_TICKS; i++) await cron()
  const downRequests = courierRequests()
  assert.equal(downRequests, 3 * DOWN_TICKS, 'the fallback: three couriers a tick while the stream is down')
  polls = (await status()).polls
  // (the skips include this check's own /wake above, a tick the push covered)
  assert.deepEqual([polls.pollsRun, polls.pollsSkipped, polls.pollRequests], [1 + DOWN_TICKS, LIVE_TICKS + 1, 3 + downRequests])
  assert.equal(polls.lastReason, 'the stream is down')
  const ticks = polls.pollsRun + polls.pollsSkipped
  console.log(`E5 poll count: ${ticks} wakes (${ticks - 1} cron ticks and this check's own /wake); ${polls.pollsRun} polled (${polls.pollRequests} courier requests), ${polls.pollsSkipped} covered by the push; without the push every cron tick polls (${3 * (ticks - 1)} requests at three a tick): ${3 * (ticks - 1) - polls.pollRequests} removed`)
  console.log(`Local compiled Worker GREEN (E5): the cron woke the object; the peer's stream pushed three headers, stored and activated; the drop reconnected from the last id; a wrong header was refused with nothing stored; the silent peer was dropped at the ${HEARTBEAT_S} s heartbeat and reconnected; a repeat was known and a gap walked by hash; the tip ${TIP} to ${TIP + 7} by the push alone; ${LIVE_TICKS} ticks behind the live push asked no courier, ${DOWN_TICKS} with the stream down polled every courier`)
} finally {
  await mf.dispose()
}
