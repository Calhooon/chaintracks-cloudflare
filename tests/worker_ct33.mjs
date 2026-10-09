// #33 on the compiled Worker, fully local: the 956433 class through the
// routes an operator calls. Real mainnet headers (src/testdata) through the
// Worker's own ingest; the orphan row is written past the rules, as the code
// before P0-4 wrote it. No request leaves the Worker. No deploy.
//
// MINIFLARE_MODULE=<miniflare>/dist/src/index.js node tests/worker_ct33.mjs
import { readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import assert from 'node:assert/strict'

const { Miniflare } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')

// The real run 886001..=888000 (src/testdata/README.md).
const FIRST = 886001
const run = readFileSync('src/testdata/main_886001_888000.bin')
const raw = height => run.subarray((height - FIRST) * 80, (height - FIRST + 1) * 80)
const sha = b => createHash('sha256').update(b).digest()
const flip = b => Buffer.from(b).reverse().toString('hex')
const hashOf = height => flip(sha(sha(raw(height))))
const rootOf = height => flip(raw(height).subarray(36, 68))

const ANCHOR = 886147
const TIP = 886600
// The fork height: above the anchor, and more than 400 below the tip (the
// reorg walk's limit, the reason production answered 500).
const H = 886180
const ORPHAN = sha('#33 the orphan').toString('hex')
const ORPHAN_ROOT = sha('#33 the root of the orphan').toString('hex')

const out = []
const mf = new Miniflare({
  modules: true,
  scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [
    {type: 'ESModule', include: ['**/*.js', '**/*.mjs']},
    {type: 'CompiledWasm', include: ['**/*.wasm']},
  ],
  compatibilityDate: '2024-01-01',
  bindings: {CHAIN: 'main', CHECKPOINTS: `${ANCHOR}:${hashOf(ANCHOR)}`, ADMIN_TOKEN: 'local'},
  d1Databases: ['DB'],
  outboundService: request => { out.push(request.url); throw new Error('external requests disabled in the lane harness') },
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
  const value = async r => { const t = await r.text(); assert.equal(r.status, 200, t); return JSON.parse(t).value }
  const push = (start, bytes) => call(`/admin/ingest?start=${start}`, {method: 'POST', headers: admin, body: Buffer.from(bytes).toString('hex')})
  const flagsOf = async hash => db.prepare('SELECT is_active, is_chain_tip, previous_header_id, header_id FROM headers WHERE hash = ?').bind(hash).first()

  // The store: the real run through the Worker's own ingest, a tip extension.
  const seeded = await value(await push(FIRST, run.subarray(0, (TIP - FIRST + 1) * 80)))
  assert.deepEqual([seeded.inserted, seeded.canonicalized, seeded.outcome], [TIP - FIRST + 1, TIP - FIRST + 1, 'active'])
  assert.deepEqual(await value(await call('/currentHeight')), TIP)

  // The link check is an admin route, and a linked chain answers linked.
  assert.equal((await call('/admin/linkcheck')).status, 401)
  assert.equal((await call('/admin/linkcheck?from=x', {headers: admin})).status, 400)
  assert.equal((await call(`/admin/linkcheck?from=${H}&to=${H - 1}`, {headers: admin})).status, 400)
  assert.deepEqual(await value(await call(`/admin/linkcheck?from=${FIRST}`, {headers: admin})), {
    from: FIRST, to: TIP, rows: TIP - FIRST + 1, linked: true, broken: [], truncated: false, checkedThrough: TIP,
  })

  // The 956433 shape: the orphan the active row at H, the chain's row absent, H+1 naming the chain's block.
  await db.batch([
    db.prepare('DELETE FROM headers WHERE hash = ?').bind(hashOf(H)),
    db.prepare('INSERT INTO headers (previous_hash,height,is_active,is_chain_tip,hash,chain_work,version,merkle_root,time,bits,nonce) VALUES (?,?,1,0,?,?,?,?,?,?,?)')
      .bind(hashOf(H - 1), H, ORPHAN, '0'.repeat(64), 0x20000000, ORPHAN_ROOT, raw(H).readUInt32LE(68), raw(H).readUInt32LE(72), 0),
  ])
  // What production answered: the chain's root false, the orphan's true, the chain's header not served.
  assert.deepEqual(await value(await call(`/isValidRootForHeight?height=${H}&root=${rootOf(H)}`)), false)
  assert.deepEqual(await value(await call(`/isValidRootForHeight?height=${H}&root=${ORPHAN_ROOT}`)), true)
  assert.equal((await call(`/findHeaderHexForBlockHash?hash=${hashOf(H)}`)).status, 404)

  // The link check names the height, from the store's floor and from the genesis.
  const broken = {height: H, active: ORPHAN, nextNames: hashOf(H), next: hashOf(H + 1)}
  assert.deepEqual(await value(await call(`/admin/linkcheck?from=${FIRST}`, {headers: admin})), {
    from: FIRST, to: TIP, rows: TIP - FIRST + 1, linked: false, broken: [broken], truncated: false, checkedThrough: TIP,
  })
  const fromGenesis = await value(await call('/admin/linkcheck', {headers: admin}))
  assert.deepEqual([fromGenesis.from, fromGenesis.to, fromGenesis.rows], [0, TIP, TIP - FIRST + 1])
  assert.deepEqual(fromGenesis.broken, [
    {height: FIRST - 1, active: null, nextNames: flip(raw(FIRST).subarray(4, 36)), next: hashOf(FIRST)},
    broken,
  ], 'from the genesis: where this store starts, and the orphan')

  // A push the rules refuse writes nothing (422): the header of another height.
  const refused = await push(H, raw(H + 5))
  assert.equal(refused.status, 422, await refused.clone().text())

  // The repair: the operator pushes the chain's 80 bytes. Before #33 this answered 500 after the insert.
  const repaired = await push(H, raw(H))
  assert.equal(repaired.status, 200, await repaired.clone().text())
  assert.deepEqual(JSON.parse(await repaired.text()).value, {
    start: H, parsed: 1, inserted: 1, canonicalized: 1, outcome: 'activated',
    deactivated: [ORPHAN], childRelinked: hashOf(H + 1), reason: null,
  })
  const row = await flagsOf(hashOf(H))
  assert.deepEqual([row.is_active, row.is_chain_tip], [1, 0])
  assert.equal((await flagsOf(ORPHAN)).is_active, 0)
  assert.equal((await flagsOf(hashOf(H + 1))).previous_header_id, row.header_id)
  assert.deepEqual(await value(await call(`/isValidRootForHeight?height=${H}&root=${rootOf(H)}`)), true)
  assert.deepEqual(await value(await call(`/isValidRootForHeight?height=${H}&root=${ORPHAN_ROOT}`)), false)
  assert.equal((await call(`/findHeaderHexForBlockHash?hash=${hashOf(H)}`)).status, 200)
  assert.deepEqual(await value(await call('/currentHeight')), TIP, 'the tip did not move')
  assert.equal((await db.prepare('SELECT pending_reorg_from AS h FROM sync_state WHERE id = 1').first()).h, H)
  assert.equal((await value(await call(`/admin/linkcheck?from=${FIRST}`, {headers: admin}))).linked, true)

  // The same push again is what the store already serves.
  const again = await value(await push(H, raw(H)))
  // (`inserted` is the batch writer's count, which counts a header already stored; not asserted.)
  assert.deepEqual([again.canonicalized, again.outcome, again.deactivated], [1, 'active', []])

  // The whole stored chain passes the node's rules after the repair.
  const pass = await value(await call('/admin/revalidate?restart=1&steps=50', {headers: admin}))
  assert.deepEqual([pass.validationComplete, pass.validationFault, pass.validatedHeight], [true, null, TIP])

  assert.deepEqual(out, [], 'nothing left the Worker')
  console.log('Local compiled Worker GREEN (#33): the link check is admin-only and names the one unlinked height (and the store\'s floor from the genesis); the ingest of the chain\'s header answers activated with the orphan deactivated and the child relinked, the tip unmoved; the roots answer right; the link check and the re-validation are clean after')
} finally {
  await mf.dispose()
}
