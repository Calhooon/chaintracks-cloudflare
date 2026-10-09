// #32: run the actual parser and SSE reconnect loop at ts-stack@fb1b2da.
// Bun transpiles modules read with git show. No npm install, network or wallet.
import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { posix } from 'node:path'
import assert from 'node:assert/strict'

const pin = 'fb1b2dad56d207b21aa5f763f0c56113cacedd5d'
const reference = process.env.TS_STACK_REPO ?? '/Users/johncalhoun/bsv/upstream/bsv-blockchain/ts-stack'
const prefix = 'packages/wallet/wallet-toolbox/src/services/chaintracker/chaintracks/'
const clientPath = prefix + 'GoChaintracksServiceClient.ts'
const blobs = new Map<string, string>()
const regtest = process.argv.includes('--regtest')
function source(path: string): string {
  if (path.split('/').some(p => /^(secrets|SECRETS|\.env)/.test(p) || /\.(pem|key)$/.test(p))) {
    throw new Error('forbidden reference path')
  }
  if (!blobs.has(path)) blobs.set(path, execFileSync('git', ['-C', reference, 'show', `${pin}:${path}`], { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 }))
  return blobs.get(path)!
}
const provider = { name: 'pinned-ts-stack', setup(build) {
  const resolve = args => {
    if (args.path.startsWith('node:')) return undefined
    let path: string
    if (args.path === 'pinned-client') path = clientPath
    else if (args.path === '@bsv/sdk') path = 'packages/sdk/mod.ts'
    else if (args.path.startsWith('@bsv/sdk/')) path = 'packages/sdk/src/' + args.path.slice('@bsv/sdk/'.length)
    else if (args.path.startsWith('.') && args.importer.startsWith('packages/')) path = posix.normalize(posix.join(posix.dirname(args.importer), args.path))
    else return undefined
    path = path.replace(/\.js$/, '.ts')
    if (!/\.(ts|json)$/.test(path)) path += '.ts'
    return { namespace: 'pin', path }
  }
  build.onResolve({ filter: /.*/, namespace: 'file' }, resolve)
  build.onResolve({ filter: /.*/, namespace: 'pin' }, resolve)
  build.onLoad({ filter: /.*/, namespace: 'pin' }, args => {
    let contents = source(args.path)
    if (regtest && args.path === prefix + 'util/blockHeaderUtilities.ts') {
      // Explicit harness parameter: the reference's production PoW limit
      // at lines 601-605 refuses regtest. All parser/SSE code is unchanged.
      const needle = 'const proofOfWorkLimit = convertBitsToTarget(0x1d00ffff)'
      assert.equal(contents.split(needle).length, 2)
      contents = contents.replace(needle, 'const proofOfWorkLimit = convertBitsToTarget(0x207fffff)')
    }
    return { contents, loader: args.path.endsWith('.json') ? 'json' : 'ts' }
  })
}}
const compiled = await Bun.build({entrypoints: [import.meta.dir + '/pinned_client_entry.ts'], plugins: [provider], target: 'bun'})
if (!compiled.success) throw new AggregateError(compiled.logs, 'pinned reference build failed')
const bundle = import.meta.dir + `/../build/reference-client-${regtest ? 'regtest' : 'mainnet'}.mjs`
await Bun.write(bundle, compiled.outputs[0])
const { GoChaintracksServiceClient } = await import(bundle)
const input = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const fixture = regtest ? input : input.mainnet
const go = { orphanedHashes: [fixture.oldTip.hash], commonAncestor: fixture.ancestor, newTip: fixture.view.newTip, depth: fixture.view.depth }

async function replay(payload: unknown) {
  const observations: unknown[][] = []
  let reconnects = 0
  const client = new GoChaintracksServiceClient('main', 'https://reference.invalid', {
    fetch: async () => new Response(`data: ${JSON.stringify(payload)}\n\n`, { headers: { 'Content-Type': 'text/event-stream' } }),
    reconnectWaitMsecs: 1,
    reconnectWaitMaxMsecs: 1
  })
  const abort = () => { for (const sub of client.subscriptions.values()) sub.abort.abort() }
  client.waitForReconnect = async () => { reconnects++; abort() }
  const id = await client.subscribeReorgs((...args: unknown[]) => { observations.push(args); abort() })
  await client.subscriptions.get(id)?.done
  return { observations, reconnects }
}
const rejected = await replay(go)
assert.equal(rejected.observations.length, process.argv.includes('--expect-go-delivery') ? 1 : 0, 'Go shape must reach the listener')
assert.equal(rejected.reconnects, 1)
console.log('Go shape RED: listener=0, silent reconnect=1 (actual pinned SSE client)')
const control = await replay(fixture.control)
assert.equal(control.observations.length, 1)
assert.equal(control.reconnects, 0)
assert.equal(control.observations[0][0], 1)
assert.equal(control.observations[0][1].hash, fixture.oldTip.hash)
assert.equal(control.observations[0][2].hash, fixture.envelope.newTip.hash)
assert.equal(control.observations[0][3][0].hash, fixture.oldTip.hash)
const green = await replay(fixture.view)
assert.deepEqual(green, control)
console.log('TS control GREEN; Worker envelope -> compatibility view GREEN: listener=1, reconnect=0')
console.log(`pin=${pin}; ${blobs.size} pinned modules; ${regtest ? 'explicit regtest PoW-limit adapter' : 'unmodified mainnet codec control'}; format, hash and PoW checked`)
