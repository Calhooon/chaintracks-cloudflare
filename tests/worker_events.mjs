// Fully local compiled-Worker routes, D1 triggers and live SSE. No deploy.
import { readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import assert from 'node:assert/strict'

const { Miniflare } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')
const fixture = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const worker = (bindings, outboundService) => new Miniflare({
  modules: true,
  scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [
    {type: 'ESModule', include: ['**/*.js', '**/*.mjs']},
    {type: 'CompiledWasm', include: ['**/*.wasm']},
  ],
  compatibilityDate: '2024-01-01',
  bindings: {CHAIN: 'main', ...bindings},
  d1Databases: ['DB'],
  outboundService: outboundService ?? (() => { throw new Error('external requests disabled in the lane harness') }),
})
async function migrate(db) {
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
}
// The heartbeat comment: 0.6.0's, or feed 2's with the served tip's age.
const isHeartbeat = (feed, text) => feed === '2' ? /^: heartbeat tipAge=\d+\n\n$/.test(text) : text === ': heartbeat\n\n'

// Every 0.6.0 check, under CHAIN_EVENT_FEED "1" (the 0.6.0 feed) and "2".
for (const feed of ['1', '2']) {
const mf = worker({CHAIN_EVENT_FEED: feed})
try {
  const db = await mf.getD1Database('DB')
  await migrate(db)
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
  assert(isHeartbeat(feed, new TextDecoder().decode((await liveReader.read()).value)))
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
  assert(isHeartbeat(feed, new TextDecoder().decode((await heartbeat.read()).value)))
  await heartbeat.cancel()
  // chaintracks-cloudflare #8: a stored row the Worker cannot decode (the
  // beta's fork shape, depth -1) is delivered unreadable at its cursor.
  const beta = {v: 1, kind: 'fork', height: reorg.event.newTip.height + 1, depth: -1, competingTips: [reorg.event.deactivatedHeaders[0], reorg.event.newTip]}
  await db.prepare('INSERT INTO chain_events(payload) VALUES (?)').bind(JSON.stringify(beta)).run()
  const held = await get('/events?since=' + since.cursor)
  assert.equal(held.status, 200)
  const heldPage = JSON.parse(await held.text())
  assert.equal(heldPage.events.length, 1)
  const bad = heldPage.events[0]
  assert.equal(heldPage.cursor, bad.cursor)
  assert.equal(heldPage.hasMore, false)
  assert.deepEqual(Object.keys(bad.event), ['v', 'kind', 'cursor', 'fault'])
  assert.equal(bad.event.v, 1)
  assert.equal(bad.event.kind, 'unreadable')
  assert.equal(String(bad.event.cursor), bad.cursor)
  assert(bad.event.fault.startsWith('unknownShape: invalid value: integer `-1`'), bad.event.fault)
  const framed = (await get('/events/stream?since=' + since.cursor)).body.getReader()
  assert(new TextDecoder().decode((await framed.read()).value).includes(`id: ${bad.cursor}\ndata: ${JSON.stringify(bad.event)}\n\n`))
  await framed.cancel()
  const viewed = (await get('/v2/reorg/stream?since=' + since.cursor)).body.getReader()
  assert.equal(new TextDecoder().decode((await viewed.read()).value), `id: ${bad.cursor}\n\n`)
  await viewed.cancel()
  console.log(`Local compiled Worker GREEN (feed ${feed}): D1 migrations/triggers, poll/SSE byte equality, both TS route aliases, Last-Event-ID, live delivery, 15 s heartbeat, bad/future cursor statuses, an undecodable stored row unreadable at its cursor (#8)`)
} finally {
  await mf.dispose()
}
}

// The witness of the 0.7.0 feed: bsv-stack-lean
// corpus/scenarios/2026-10-23-chaintracks-reorg-event-with-a-cursor.md, its
// steps 1 to 8 on a chain tall enough for the settled line (the witness's
// heights 100 to 104 here 500 to 504, so a height more than 400 below the
// tip exists). Headers are written by SQL (the routes read rows; the
// ingest's proof of work is the host tests'). Feed "1" with the edge unset
// is the 0.6.0 column, feed "2" with HEADER_EDGE_DEPTH "400" the design's.
const sha = label => createHash('sha256').update(label).digest('hex')
const now = Math.floor(Date.now() / 1000)
const synthetic = (label, height, previousHash, work) => ({
  label, height, previousHash, hash: sha(label), merkleRoot: sha('root ' + label),
  version: 536870912, time: now - 600 * (510 - height), bits: 0x1d00ffff, nonce: height,
  chainWork: work.toString(16).padStart(64, '0'),
})
const insert = (db, h, active, tip) => db.prepare('INSERT INTO headers (previous_hash,height,is_active,is_chain_tip,hash,chain_work,version,merkle_root,time,bits,nonce) VALUES (?,?,?,?,?,?,?,?,?,?,?)')
  .bind(h.previousHash, h.height, active, tip, h.hash, h.chainWork, h.version, h.merkleRoot, h.time, h.bits, h.nonce)
const decoder = new TextDecoder()
const text = async reader => decoder.decode((await reader.read()).value)
const ids = chunk => chunk.split('\n').filter(l => l.startsWith('id: ')).map(l => l.slice(4))

for (const [feed, edgeDepth] of [['1', undefined], ['2', '400']]) {
  const posts = []
  const purges = []
  const bindings = {
    CHAIN_EVENT_FEED: feed,
    ADMIN_TOKEN: 'local',
    CHAIN_EVENT_WEBHOOK_URLS: 'https://a.example/events,https://b.example/events',
    CHAIN_EVENT_WEBHOOK_TOKEN: 'local',
    ARCADE_URL: 'https://arcade.test', BITAILS_URL: 'https://bitails.test',
  }
  if (edgeDepth) Object.assign(bindings, {HEADER_EDGE_DEPTH: edgeDepth, HEADER_EDGE_ZONE_ID: 'zone-test', HEADER_EDGE_PURGE_TOKEN: 'synthetic-purge'})
  const mf = worker(bindings, async request => {
    const url = new URL(request.url)
    if (url.hostname.endsWith('.example')) {
      posts.push({host: url.hostname, cursor: request.headers.get('x-chain-event-cursor')})
      return new Response('ok')
    }
    if (url.hostname === 'api.cloudflare.com') {
      purges.push({url: request.url, method: request.method, auth: request.headers.get('authorization'), body: await request.text()})
      return new Response('{"success":true}')
    }
    // Every courier is down: the tick sees no new header.
    return new Response('down', {status: 503})
  })
  try {
    const db = await mf.getD1Database('DB')
    await migrate(db)
    const cron = async () => (await mf.getWorker()).scheduled({cron: '* * * * *'})
    const get = (path, headers) => mf.dispatchFetch('https://worker.test' + path, {headers})
    const rowsAfter = async c => (await db.prepare('SELECT cursor, payload FROM chain_events WHERE cursor > ? ORDER BY cursor').bind(c).all()).results
    const headCursor = async () => (await db.prepare('SELECT COALESCE(MAX(cursor), 0) AS c FROM chain_events').first()).c
    // A minute passes for the 0.6.0 timer's dedup key without a clock.
    const nextMinute = () => db.prepare("UPDATE chain_events SET event_key = 'aged:' || cursor WHERE event_key LIKE 'tipAge:%'").run()
    // The initial chain: a0 to a503 active, the tip a503; b502 to b504 from a501, not yet seen.
    const a = []
    for (let h = 0; h <= 503; h++) a.push(synthetic('a' + h, h, h ? a[h - 1].hash : '0'.repeat(64), BigInt(h + 1) * 2n))
    const b = {501: a[501]}
    for (let h = 502; h <= 504; h++) b[h] = synthetic('b' + h, h, b[h - 1].hash, BigInt(h + 1) * 2n + 1n)
    for (let i = 0; i < a.length; i += 100) await db.batch(a.slice(i, i + 100).map(h => insert(db, h, 1, 0)))
    await db.prepare('UPDATE headers SET is_chain_tip = 1 WHERE hash = ?').bind(a[503].hash).run()
    const c0 = await headCursor()
    assert(c0 > 0)
    // Step 7's outbox: target a is caught up at c0, target b has no cursor.
    await db.prepare('INSERT INTO chain_event_deliveries(target, cursor) VALUES (?, ?)').bind('=https://a.example/events', c0).run()

    // Step 1: a minute's tick, no new header. Feed 2: no row (CF3); feed 1: one tipAge.
    await nextMinute()
    await cron()
    const step1 = await rowsAfter(c0)
    assert.deepEqual(step1.map(r => JSON.parse(r.payload).kind), feed === '2' ? [] : ['tipAge'])
    // Target a was at the head before the tick: it hears only the tick's own rows (none under feed 2).
    assert.deepEqual(posts.filter(p => p.host === 'a.example').map(p => p.cursor), step1.map(r => String(r.cursor)))
    assert(posts.filter(p => p.host === 'b.example').length > 0, 'the behind target gets its page')
    posts.length = 0
    const c1 = await headCursor()

    // Client A holds the stream from c1; caught up, it hears the heartbeat.
    const clientA = (await get('/events/stream', {'Last-Event-ID': String(c1)})).body.getReader()
    assert(isHeartbeat(feed, await text(clientA)))

    // Step 2: b502 to b504 reach the store in one batch; b504 has more work.
    await db.batch([
      ...[502, 503, 504].map(h => insert(db, b[h], 0, 0)),
      db.prepare('UPDATE headers SET is_active = 0, is_chain_tip = 0 WHERE height > 501'),
      db.prepare('UPDATE headers SET is_active = 1 WHERE hash IN (?,?,?)').bind(b[502].hash, b[503].hash, b[504].hash),
      db.prepare('UPDATE headers SET is_chain_tip = 1 WHERE hash = ?').bind(b[504].hash),
    ])
    const step2 = await rowsAfter(c1)
    const reorgs = step2.filter(r => JSON.parse(r.payload).kind === 'reorg')
    assert.equal(reorgs.length, 1, 'one reorg, journaled once')
    const reorg = reorgs[0]
    const envelope = JSON.parse(reorg.payload)
    assert.equal(envelope.forkHeight, 502)
    assert.equal(envelope.depth, 2)
    assert.deepEqual(envelope.deactivatedHeaders.map(h => h.hash), [a[503].hash, a[502].hash])
    assert.equal(envelope.newTip.hash, b[504].hash)
    const after = step2.filter(r => r.cursor > reorg.cursor).map(r => JSON.parse(r.payload).kind)
    assert.deepEqual(after, ['tip', 'tipAge'])
    const cHead = await headCursor()

    // Step 3: client A's stream wakes (within 15 s) and receives the rows above c1.
    const woke = await text(clientA)
    assert(woke.includes(`id: ${reorg.cursor}\ndata: ${reorg.payload}\n\n`), 'the reorg byte for byte')
    assert.deepEqual(ids(woke), step2.map(r => String(r.cursor)))
    await clientA.cancel()

    // Step 4: client B reconnects from c1 and receives each row once; a second
    // reconnect from the last id it received receives none of them again.
    const clientB = (await get('/events/stream', {'Last-Event-ID': String(c1)})).body.getReader()
    const replayed = await text(clientB)
    assert.deepEqual(ids(replayed), step2.map(r => String(r.cursor)))
    await clientB.cancel()
    const again = (await get('/events/stream', {'Last-Event-ID': ids(replayed).at(-1)})).body.getReader()
    assert(isHeartbeat(feed, await text(again)))
    await again.cancel()

    // Step 6: a second tick, no new header. Feed 2: no row; feed 1: one tipAge.
    await nextMinute()
    await cron()
    assert.deepEqual((await rowsAfter(cHead)).map(r => JSON.parse(r.payload).kind), feed === '2' ? [] : ['tipAge'])

    // Step 5: the retention passes client C's cursor. Every row through the
    // reorg is made 100 days old; the tick deletes the chain rows and raises
    // the floor in their transaction (feed 2); feed 1 deletes nothing.
    await db.prepare('UPDATE chain_events SET created_at = ? WHERE cursor <= ?').bind(now - 100 * 86400, reorg.cursor).run()
    await cron()
    const cOld = c0
    const expired = await get('/events?since=' + cOld)
    if (feed === '2') {
      const floor = (await db.prepare('SELECT floor FROM chain_event_state WHERE id = 1').first()).floor
      assert.equal(floor, reorg.cursor, 'the floor is the greatest deleted chain cursor')
      assert.equal(expired.status, 410)
      assert.equal(expired.headers.get('cache-control'), 'no-store')
      assert.deepEqual(await expired.json(), {v: 1, error: 'cursorExpired', floor: String(floor), tip: {height: 504, hash: b[504].hash}})
      assert.equal((await get('/events/stream', {'Last-Event-ID': String(cOld)})).status, 410)
      // The re-read rule: C reads its own tip's height, finds another hash,
      // walks its remembered tips down to the match; the height above it is
      // the fork height, the reorg's own; then it reads from the floor.
      const held = [a[503], a[502], a[501], a[500]]
      let match
      for (const tip of held) {
        const answer = await (await get(`/v2/header/height/${tip.height}`)).json()
        if (answer.value.hash === tip.hash) { match = tip; break }
      }
      assert.equal(match.height + 1, envelope.forkHeight)
      const resumed = await get('/events?since=' + floor)
      assert.equal(resumed.status, 200)
      assert.deepEqual((await resumed.json()).events.map(e => e.event.kind), (await rowsAfter(floor)).map(r => JSON.parse(r.payload).kind))
    } else {
      assert.equal(expired.status, 200, '0.6.0: no retention, the page from any cursor')
      assert((await expired.json()).events.some(e => e.event.kind === 'reorg'))
    }

    // Step 8: the edge. 502 is within the line (no-cache in both modes); 2 is
    // more than 400 below the tip.
    for (const path of ['/v2/header/height/502', '/chaintracks/v2/header/height/502.bin', '/v2/header/hash/' + b[502].hash, '/findHeaderHexForHeight?height=502']) {
      const r = await get(path)
      assert.equal(r.status, 200, path)
      assert.equal(r.headers.get('cache-control'), 'no-cache', path)
      assert.equal(r.headers.get('cache-tag'), null, path)
    }
    const nonce = async path => (await (await get(path)).json()).value.nonce
    const settledPaths = ['/v2/header/height/2', '/findHeaderHexForHeight?height=2', '/v2/header/hash/' + a[2].hash]
    for (const path of settledPaths) {
      const r = await get(path)
      assert.equal(r.status, 200, path)
      const v1 = !path.startsWith('/v2')
      if (edgeDepth) {
        assert.equal(r.headers.get('cache-control'), 'public, max-age=86400', path)
        assert.equal(r.headers.get('cache-tag'), 'h-2,hb-0', path)
      } else {
        assert.equal(r.headers.get('cache-control'), v1 ? null : 'public, max-age=3600', path)
        assert.equal(r.headers.get('cache-tag'), null, path)
      }
    }
    const root = await get(`/isValidRootForHeight?height=2&root=${a[2].merkleRoot}`)
    assert.equal((await root.json()).value, true)
    const range = await get('/v2/headers?height=0&count=10')
    assert.equal(range.headers.get('x-header-count'), '10')
    assert.equal(range.headers.get('cache-control'), edgeDepth ? 'public, max-age=86400' : 'public, max-age=3600')
    assert.equal(range.headers.get('cache-tag'), edgeDepth ? 'hb-0' : null)
    // The D1 row changes under the edge: a kept answer is the colo's copy.
    await db.prepare('UPDATE headers SET nonce = 77 WHERE height = 2').run()
    assert.equal(await nonce('/v2/header/height/2'), edgeDepth ? 2 : 77)
    assert.equal(await nonce('/findHeaderHexForHeight?height=2'), edgeDepth ? 2 : 77)
    assert.equal(await nonce('/v2/header/height/502'), 502, 'within the line, always D1')
    if (edgeDepth) {
      // The operator's purge: one zone request of the tags, the colo's copy deleted.
      const purged = await get('/admin/edge-purge?from=2&to=2', {Authorization: 'Bearer local'})
      assert.equal(purged.status, 404, 'the purge is a POST')
      const answer = await mf.dispatchFetch('https://worker.test/admin/edge-purge?from=2&to=2', {method: 'POST', headers: {Authorization: 'Bearer local'}})
      assert.equal(answer.status, 200)
      const value = (await answer.json()).value
      assert.equal(value.tags, 2)
      // the v2 and v1 height answers and the by-hash answer read above
      assert.equal(value.localDeleted, 3)
      assert.deepEqual(value.zoneStatuses, [200])
      assert.deepEqual(purges, [{
        url: 'https://api.cloudflare.com/client/v4/zones/zone-test/purge_cache',
        method: 'POST', auth: 'Bearer synthetic-purge', body: JSON.stringify({tags: ['h-2', 'hb-0']}),
      }])
      assert.equal(await nonce('/v2/header/height/2'), 77, 'the colo reads D1 again')
      assert.equal(await nonce('/v2/header/hash/' + a[2].hash), 77)
      assert.equal((await mf.dispatchFetch('https://worker.test/admin/edge-purge?from=2&to=2', {method: 'POST'})).status, 401)
    }
    console.log(`Local compiled Worker GREEN (the witness, feed ${feed}, edge ${edgeDepth ?? 'unset'}): the depth-2 reorg journaled once with its cursor; the stream heard it; the replay once; ${feed === '2' ? 'the floor and 410 cursorExpired with the re-read' : 'no retention'}; a same-tip tick journals ${feed === '2' ? 'nothing' : 'one tipAge'}; the outbox skipped the caught-up target; no-cache within the line, ${edgeDepth ? 'the edge below it and the tag purge' : 'the 0.6.0 lifetimes below it'}`)
  } finally {
    await mf.dispose()
  }
}

// Step 3's bound (CF4): the test hook lowers the bound to the open's one
// query, so the stream ends with retry: at once, never feedUnavailable.
{
  const mf = worker({CHAIN_EVENT_FEED: '2', CHAIN_EVENT_STREAM_BOUND: '1'})
  try {
    await migrate(await mf.getD1Database('DB'))
    // The whole body to its EOF (the two frames may share a chunk).
    const body = await (await mf.dispatchFetch('https://worker.test/events/stream?since=0')).text()
    assert.equal(body, ': heartbeat\n\nretry: 1000\n\n')
    console.log('Local compiled Worker GREEN (the stream bound): retry: 1000 and EOF at the bound, no event: error')
  } finally {
    await mf.dispose()
  }
}
