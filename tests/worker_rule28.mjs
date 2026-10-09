// Rule 28 on the compiled Worker, fully local: what leaves the service, and
// to whom. Every outbound request is caught and answered here; none reaches
// the network. No deploy. Real mainnet headers (src/testdata) through the
// Worker's own routes, so every header meets the node's rules on the way in.
//
// MINIFLARE_MODULE=<miniflare>/dist/src/index.js node tests/worker_rule28.mjs
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
const hashOf = height => flip(sha(sha(raw(height))))
const fields = height => {
  const b = raw(height)
  return {
    version: b.readUInt32LE(0), previousHash: flip(b.subarray(4, 36)), merkleRoot: flip(b.subarray(36, 68)),
    time: b.readUInt32LE(68), bits: b.readUInt32LE(72), nonce: b.readUInt32LE(76), height, hash: hashOf(height),
  }
}

// The store holds 886001..=TIP; the couriers hold the blocks above it.
const ANCHOR = 886147
const TIP = 886200
const WOC = 'api.whatsonchain.com'
const ARCADE = 'arcade-v2-us-1.bsvblockchain.tech'
const BITAILS = 'api.bitails.io'
const PEER = 'peer.test'
const FILE_HOST = 'cdn.projectbabbage.com'
const FOREIGN = 'evil.example'

// Every request that leaves the Worker, in order.
let bitailsServes = false
const out = []
const hosts = () => out.map(u => u.host)
const json = (value, status = 200) => new Response(JSON.stringify(value), {status, headers: {'content-type': 'application/json'}})
const refuse = () => new Response('unavailable', {status: 503})
const outboundService = request => {
  const url = new URL(request.url)
  out.push(url)
  // WhatsOnChain refuses everything: the courier the base asked alone.
  if (url.host === WOC) return refuse()
  if (url.host === ARCADE) {
    // a peer header service: it holds the two blocks above our tip, by height
    const m = url.pathname.match(/^\/chaintracks\/v2\/header\/height\/(\d+)$/)
    if (m && +m[1] > TIP && +m[1] <= TIP + 2) return json(fields(+m[1]))
    return refuse()
  }
  if (url.host === BITAILS) {
    const m = url.pathname.match(/^\/block\/height\/(\d+)$/)
    if (bitailsServes && m && +m[1] > TIP + 2 && +m[1] <= TIP + 6) return json({hash: hashOf(+m[1]), height: +m[1], header: raw(+m[1]).toString('hex')})
    return refuse()
  }
  if (url.host === PEER) return refuse()
  if (url.host === FILE_HOST && url.pathname.endsWith('NetBlockHeaders.json')) {
    // a listing that names another host for every file
    return json({files: Array.from({length: 10}, (_, i) => ({fileName: `mainNet_${i}.headers`, firstHeight: i * 100000, sourceUrl: `https://${FOREIGN}/blockheaders`}))})
  }
  return new Response('not found', {status: 404})
}

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
    // the owner's checkpoint that anchors this local store (as CHECKPOINTS anchors production's)
    CHECKPOINTS: `${ANCHOR}:${hashOf(ANCHOR)}`,
    ADMIN_TOKEN: 'local',
    UPSTREAM_CHAINTRACKS_URL: `https://${PEER}`,
  },
  d1Databases: ['DB'],
  outboundService,
})
try {
  const db = await mf.getD1Database('DB')
  for (const name of readdirSync('migrations').filter(n => n.endsWith('.sql')).sort()) {
    // D1.exec splits on newlines; let SQLite find the complete statements (tests/worker_events.mjs).
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

  // The store: the real run through the Worker's own ingest (the node's rules on every header).
  const seeded = await call(`/admin/ingest?start=${FIRST}`, {method: 'POST', headers: admin, body: run.subarray(0, (TIP - FIRST + 1) * 80).toString('hex')})
  assert.equal(seeded.status, 200, await seeded.clone().text())
  assert.equal((await body(seeded)).value.inserted, TIP - FIRST + 1)
  assert.deepEqual((await body(await call('/currentHeight'))), {status: 'success', value: TIP})
  assert.deepEqual(hosts(), [], 'the ingest and the tip ask no one')

  // H11: /getPresentHeight answers from the store; no request leaves the Worker.
  assert.deepEqual(await body(await call('/getPresentHeight')), {status: 'success', value: TIP})
  await db.prepare('UPDATE sync_state SET last_seen_height = ?1 WHERE id = 1').bind(TIP + 2).run()
  assert.deepEqual(await body(await call('/getPresentHeight')), {status: 'success', value: TIP + 2}, 'the larger of the served tip and the last seen height')
  await db.prepare('UPDATE sync_state SET last_seen_height = ?1 WHERE id = 1').bind(TIP - 50).run()
  assert.deepEqual(await body(await call('/getPresentHeight')), {status: 'success', value: TIP})
  assert.deepEqual(hosts(), [], 'H11: /getPresentHeight is one request in and none out')

  // H12: the read-through. WhatsOnChain refuses; the block lands from the next courier, checked.
  const fresh = await call(`/findHeaderHexForHeight?height=${TIP + 1}`)
  assert.equal(fresh.status, 200, 'a fresh block does not depend on one courier')
  assert.equal((await body(fresh)).value.hash, hashOf(TIP + 1))
  assert(hosts().includes(ARCADE), 'the read-through asked the ladder')
  assert(hosts().every(h => [WOC, ARCADE, BITAILS].includes(h)), `only the couriers: ${hosts()}`)
  const root = await call(`/isValidRootForHeight?height=${TIP + 2}&root=${fields(TIP + 2).merkleRoot}`)
  assert.deepEqual(await body(root), {status: 'success', value: true}, 'the root of a fresh block, read through the ladder')
  assert.deepEqual(await body(await call('/currentHeight')), {status: 'success', value: TIP + 2})
  // no courier holds the next block yet: unable to verify, never "invalid", and every courier was asked
  out.length = 0
  const beyond = await call(`/isValidRootForHeight?height=${TIP + 3}&root=${fields(TIP + 3).merkleRoot}`)
  assert.equal(beyond.status, 404)
  assert.match((await body(beyond)).description, /unable to verify/)
  assert.deepEqual([...new Set(hosts())].sort(), [ARCADE, BITAILS, WOC].sort(), 'could not look: every courier was asked before the answer')

  // H13: the backfill. Two couriers refuse; the third serves the span; a rung is asked at most three times.
  out.length = 0
  bitailsServes = true
  const stored = async () => (await db.prepare('SELECT COUNT(*) AS n FROM headers WHERE height BETWEEN ?1 AND ?2').bind(TIP + 3, TIP + 6).first()).n
  assert.equal(await stored(), 0)
  const filled = await body(await call(`/admin/backfill?from=${TIP + 3}&to=${TIP + 6}`, {headers: admin}))
  assert.deepEqual(filled.value, {from: TIP + 3, to: TIP + 6, fetched: 4, inserted: 4, nextFrom: TIP + 7})
  assert.equal(await stored(), 4, 'the span landed, each header through the node\'s rules')
  for (const refusing of [WOC, ARCADE]) assert(hosts().filter(h => h === refusing).length <= 3, `${refusing} is skipped after three faults`)
  assert.equal(hosts().filter(h => h === BITAILS).length, 4)

  // H14, H15: the bootstrap asks the upstream peer first; the file host is pinned.
  out.length = 0
  const boot = await call('/admin/bulk-sync?file=9', {headers: admin})
  assert.notEqual(boot.status, 200, 'neither source served the span in this harness')
  assert.equal(out[0].host, PEER, 'the upstream peer is asked first')
  assert.equal(out[0].pathname + out[0].search, '/getHeaders?height=900000&count=1000')
  assert.deepEqual(out.slice(1).map(u => u.host + u.pathname), [
    FILE_HOST + '/blockheaders/mainNetBlockHeaders.json',
    FILE_HOST + '/blockheaders/mainNet_9.headers',
  ], 'then the file host, and the file from the pinned host')
  assert(!hosts().includes(FOREIGN), 'a sourceUrl naming another host is ignored')
  // &source=file asks the file host alone
  out.length = 0
  await call('/admin/bulk-sync?file=9&source=file', {headers: admin})
  assert.deepEqual([...new Set(hosts())], [FILE_HOST])

  console.log('Local compiled Worker GREEN (Rule 28): /getPresentHeight asks no one; the read-through and the backfill read the ladder (WhatsOnChain refusing throughout); unable to verify after every courier; the bootstrap asks the peer first and the pinned file host next, never the host a listing names')
} finally {
  await mf.dispose()
}
