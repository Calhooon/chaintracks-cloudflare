import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { execFileSync } from 'node:child_process'
const { Miniflare } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')
const mf = new Miniflare({
  modules: true,
  scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [
    {type: 'ESModule', include: ['**/*.js', '**/*.mjs']},
    {type: 'CompiledWasm', include: ['**/*.wasm']},
  ],
  compatibilityDate: '2024-01-01',
  outboundService: () => { throw new Error('external requests disabled') },
})
try {
  const response = await mf.dispatchFetch('https://worker.test/health')
  assert.equal(response.status, 200)
  assert.match(response.headers.get('content-type'), /application\/json/)
  const body = await response.json()
  assert.deepEqual(Object.keys(body).sort(), ['build', 'service', 'status'])
  assert.equal(body.service, 'chaintracks')
  assert.equal(body.status, 'ok')
  assert.deepEqual(Object.keys(body.build).sort(), ['built_at', 'crates', 'dirty', 'lib_rev'])
  assert.equal(body.build.lib_rev, execFileSync('git', ['rev-parse', 'HEAD'], {encoding: 'utf8'}).trim())
  assert.equal(body.build.dirty, execFileSync('git', ['status', '--porcelain'], {encoding: 'utf8'}).trim() !== '')
  assert.match(body.build.built_at, /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/)
  assert.ok(Number.isFinite(Date.parse(body.build.built_at)))
  const own = readFileSync('Cargo.toml', 'utf8').match(/^version = "([^"]+)"$/m)[1]
  const lock = readFileSync('Cargo.lock', 'utf8')
  const bsvRs = lock.split('[[package]]').find(p => /^name = "bsv-rs"$/m.test(p))?.match(/^version = "([^"]+)"$/m)?.[1]
  assert.deepEqual(body.build.crates, bsvRs ? {'rust-chaintracks': own, 'bsv-rs': bsvRs} : {'rust-chaintracks': own})
  console.log(JSON.stringify(body))
  console.log('Local compiled Worker GREEN (health, no database or outbound requests)')
} finally {
  await mf.dispose()
}
