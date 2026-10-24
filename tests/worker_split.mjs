// Local D1, the compiled cron, and all feed carriers the Worker implements.
// The native witness covers validated ingest; this seed isolates the D1 wiring.
import assert from 'node:assert/strict'
import { readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
const { Miniflare, Response } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')

const digest = bytes => createHash('sha256').update(bytes).digest()
const child = (parent, label) => {
  const bytes = Buffer.alloc(80)
  bytes.writeUInt32LE(1, 0)
  Buffer.from(parent.hash, 'hex').reverse().copy(bytes, 4)
  const merkle = digest(Buffer.from(label))
  merkle.copy(bytes, 36)
  bytes.writeUInt32LE(parent.time + 600, 68)
  bytes.writeUInt32LE(0x207fffff, 72)
  let hash
  let nonce = 0
  do {
    bytes.writeUInt32LE(nonce++, 76)
    hash = Buffer.from(digest(digest(bytes))).reverse().toString('hex')
  } while (BigInt('0x' + hash) > (0x7fffffn << 232n))
  return {version: 1, previousHash: parent.hash, merkleRoot: Buffer.from(merkle).reverse().toString('hex'),
    time: parent.time + 600, bits: 0x207fffff, nonce: nonce - 1, height: parent.height + 1, hash}
}
const root = child({hash: '0'.repeat(64), height: 99, time: 1700000000}, 'root')
const a = [root], b = [root]
for (let i = 1; i <= 4; i++) {
  a.push(child(a[i - 1], 'a' + i))
  b.push(child(b[i - 1], 'b' + i))
}
let left = a[2], right = b[2], rightDown = false
const delivered = []
const json = value => new Response(JSON.stringify(value), {headers: {'content-type': 'application/json'}})
const mf = new Miniflare({
  modules: true,
  scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [{type: 'ESModule', include: ['**/*.js', '**/*.mjs']}, {type: 'CompiledWasm', include: ['**/*.wasm']}],
  compatibilityDate: '2024-01-01',
  // CHAIN_EVENT_FEED: the gate runs this script under "1" and "2" (ci.yml)
  bindings: {CHAIN_EVENT_FEED: process.env.CHAIN_EVENT_FEED ?? '1', CHAIN: 'main', ARCADE_URL: 'https://arcade.test', BITAILS_URL: 'https://bitails.test',
    ARCADE_IMPLEMENTATION: 'peer', SPLIT_DEPTH: '3',
    CHAIN_EVENT_WEBHOOK_URLS: 'https://consumer.example/events', CHAIN_EVENT_WEBHOOK_TOKEN: 'local'},
  d1Databases: ['DB'],
  outboundService: async request => {
    const url = new URL(request.url)
    if (url.hostname === 'consumer.example') {
      delivered.push(await request.text())
      return new Response('ok')
    }
    if (url.pathname.endsWith('/chain/info')) return json({blocks: left.height, bestblockhash: left.hash})
    if (url.hostname === 'arcade.test' && url.pathname === '/chaintracks/v2/tip') {
      return rightDown ? new Response('down', {status: 503}) : json(right)
    }
    if (url.hostname === 'bitails.test' && url.pathname === '/network/info') return json({blocks: left.height, bestBlockhash: left.hash})
    throw new Error('unexpected outbound request intercepted')
  },
})
try {
  const db = await mf.getD1Database('DB')
  for (const name of readdirSync('migrations').filter(n => n.endsWith('.sql')).sort()) {
    const parsed = spawnSync('python3', ['-c', `
import json, sqlite3, sys
statements, pending = [], ''
for char in sys.stdin.read():
    pending += char
    if char == ';' and sqlite3.complete_statement(pending):
        statements.append(pending)
        pending = ''
print(json.dumps(statements))
`], {input: readFileSync('migrations/' + name, 'utf8'), encoding: 'utf8'})
    assert.equal(parsed.status, 0, parsed.stderr)
    await db.batch(JSON.parse(parsed.stdout).map(sql => db.prepare(sql)))
  }
  for (const h of [...a, ...b.slice(1)]) {
    await db.prepare(`INSERT INTO headers(previous_header_id,previous_hash,height,is_active,is_chain_tip,hash,chain_work,version,merkle_root,time,bits,nonce)
      VALUES ((SELECT header_id FROM headers WHERE hash = ?),?,?,?,?,?,?,?,?,?,?,?)`)
      .bind(h.previousHash, h.previousHash, h.height, a.includes(h) ? 1 : 0, h === a[4] ? 1 : 0,
        h.hash, (BigInt(h.height - root.height + 1) * 2n).toString(16).padStart(64, '0'), h.version, h.merkleRoot, h.time, h.bits, h.nonce).run()
  }
  await db.prepare('UPDATE headers SET previous_header_id = NULL WHERE is_active = 1').run()
  await db.prepare('UPDATE sync_state SET validation_complete = 1 WHERE id = 1').run()
  const worker = await mf.getWorker()
  const cron = () => worker.scheduled({cron: '* * * * *'})
  const get = path => mf.dispatchFetch('https://worker.test' + path)
  const splitRows = async () => (await db.prepare("SELECT payload FROM chain_events WHERE json_extract(payload, '$.kind') IN ('split', 'splitResolved') ORDER BY cursor").all()).results
  await cron()
  assert.equal((await splitRows()).length, 0)
  const info = (await (await get('/getInfo')).json()).value
  assert.equal(info.lastSeenHeight, left.height)
  assert.equal(info.rungTips.length, 3)
  assert.equal(info.rungTips.find(r => r.name === 'arcade').implementation, 'peer')
  assert.equal(info.rungTips.find(r => r.name === 'arcade').isActive, false)
  assert.equal(info.rungTips.find(r => r.name === 'woc').chainWork, '6'.padStart(64, '0'))
  left = a[3]; right = b[3]
  await cron()
  const opened = await splitRows()
  assert.equal(opened.length, 1)
  const event = JSON.parse(opened[0].payload)
  assert.equal(event.kind, 'split')
  assert.equal(event.v, 2)
  assert.equal(event.key, 'split:' + root.hash)
  assert.equal(event.forkHeight, root.height + 1)
  assert.ok(delivered.includes(opened[0].payload), 'webhook preserves stored bytes')
  const polled = await get('/events?since=0')
  assert.equal(polled.headers.get('x-chain-event-version'), '2')
  const page = await polled.json()
  assert.equal(page.v, 1, 'carrier stays version 1; envelope selects its schema')
  const item = page.events.find(e => e.event.kind === 'split')
  assert.deepEqual(item.event, event)
  const response = await get('/events/stream?since=0')
  const reader = response.body.getReader()
  const frame = new TextDecoder().decode((await reader.read()).value)
  assert.ok(frame.includes(`id: ${item.cursor}\ndata: ${opened[0].payload}\n\n`))
  await reader.cancel()
  rightDown = true; left = a[4]
  await cron()
  assert.equal((await splitRows()).length, 1, 'an unavailable rung retains its branch')
  rightDown = false; right = a[3]
  await cron()
  await cron()
  const closed = await splitRows()
  assert.equal(closed.length, 2)
  assert.deepEqual(JSON.parse(closed[1].payload), {v: 2, kind: 'splitResolved', key: event.key, forkHeight: event.forkHeight})
  assert.ok(delivered.includes(closed[1].payload))
  assert.equal((await (await get('/currentHeight')).json()).value, a[4].height)
  console.log('Local compiled Worker GREEN (split): depth 2/3, bulk parent hashes, per-rung tips, config labels, failed rung, shared key, poll/SSE/webhook bytes, resolution; outbound intercepted')
} finally {
  await mf.dispose()
}
