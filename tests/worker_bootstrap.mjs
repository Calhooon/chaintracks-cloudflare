// Two real 100,000-header spans through the compiled Worker on an empty D1.
// Only the fixture download reaches the pinned public file host; Worker
// requests are intercepted and served from the cache, with no peer or explorer.
// CARGO_TARGET_DIR=<lane-target> node tests/worker_bootstrap.mjs
import assert from 'node:assert/strict'
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'

const { Miniflare, Response } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')
assert.ok(process.env.CARGO_TARGET_DIR, 'set CARGO_TARGET_DIR for the fixture cache')
const cache = resolve(process.env.CARGO_TARGET_DIR, 'bootstrap-fixtures')
mkdirSync(cache, {recursive: true})
const base = 'https://cdn.projectbabbage.com/blockheaders/'
const files = []
for (let index = 0; index < 2; index++) {
  const name = `mainNet_${index}.headers`
  const file = resolve(cache, name)
  if (!existsSync(file)) {
    const response = await fetch(base + name, {redirect: 'error'})
    assert.equal(response.status, 200, `fixture ${name}`)
    const bytes = Buffer.from(await response.arrayBuffer())
    assert.equal(bytes.length, 100000 * 80, `fixture ${name} length`)
    writeFileSync(file, bytes)
  }
  const bytes = readFileSync(file)
  assert.equal(bytes.length, 100000 * 80, `cached ${name} length`)
  console.log(`Fixture ${name}: ${bytes.length} bytes sha256=${createHash('sha256').update(bytes).digest('hex')}`)
  files.push(bytes)
}

const outbound = []
const mf = new Miniflare({
  modules: true,
  scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [
    {type: 'ESModule', include: ['**/*.js', '**/*.mjs']},
    {type: 'CompiledWasm', include: ['**/*.wasm']},
  ],
  compatibilityDate: '2024-01-01',
  bindings: {CHAIN: 'main', ADMIN_TOKEN: 'local', UPSTREAM_CHAINTRACKS_URL: 'https://peer.test'},
  d1Databases: ['DB'],
  outboundService: request => {
    outbound.push(request.url)
    if (request.url === base + 'mainNetBlockHeaders.json') {
      return new Response(JSON.stringify({files: files.map((_, index) => ({
        fileName: `mainNet_${index}.headers`, firstHeight: index * 100000,
      }))}), {headers: {'content-type': 'application/json'}})
    }
    const index = files.findIndex((_, i) => request.url === base + `mainNet_${i}.headers`)
    assert.notEqual(index, -1, `unexpected outbound request: ${request.url}`)
    return new Response(files[index], {headers: {'content-type': 'application/octet-stream'}})
  },
})
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
  const call = path => mf.dispatchFetch('https://worker.test' + path)
  const info = async () => {
    const response = await call('/getInfo')
    assert.equal(response.status, 200)
    return (await response.json()).value
  }
  assert.equal((await info()).headerCount, 0)
  for (let index = 0; index < 2; index++) {
    const response = await mf.dispatchFetch(`https://worker.test/admin/bulk-sync?file=${index}&source=file`, {
      headers: {Authorization: 'Bearer local'},
    })
    const answer = await response.text()
    console.log(`bulk-sync file=${index}: HTTP ${response.status} ${answer}`)
    const state = await info()
    console.log(`getInfo: headerCount=${state.headerCount} heightLive=${state.heightLive}`)
    const ancestry = await db.prepare('SELECT COUNT(*) AS n, SUM(is_active) AS active, COUNT(previous_header_id) AS linked FROM headers WHERE height BETWEEN 98784 AND 99999').first()
    console.log(`stored 98784..99999: ${JSON.stringify(ancestry)}`)
    assert.equal(response.status, 200, answer)
    const value = JSON.parse(answer).value
    assert.equal(value.source, 'fileHost')
    assert.equal(value.headersInFile, 100000)
    assert.equal(value.inserted, 100000)
    assert.equal(state.headerCount, (index + 1) * 100000)
    assert.equal(state.heightLive, (index + 1) * 100000 - 1)
    assert.equal(ancestry.n, 1216)
    assert.equal(ancestry.active, 1216)
  }
  assert.deepEqual(outbound, [0, 1].flatMap(index => [
    base + 'mainNetBlockHeaders.json', base + `mainNet_${index}.headers`,
  ]))
  console.log('Local compiled Worker GREEN (bootstrap): both spans HTTP 200; 200000 headers; heightLive 199999; stored ancestry across the retarget; no peer or explorer')
} finally {
  await mf.dispose()
}
