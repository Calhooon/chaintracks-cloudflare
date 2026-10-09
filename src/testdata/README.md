# Real header runs for the P0-4 tests

Raw 80-byte block headers, concatenated, heights ascending. Every file is a
copy, or a cut, of a file Teranode ships at `v0.16.0` (commit
`4edb60a40c6628ff0ef60231c2628efd7103fde5`, the sibling bsv-script-lean's pin
of Teranode, its `docs/PROVENANCE.md` section 1), read with
`git show v0.16.0:<path>` on 2026-10-08. Teranode's
`test/testnet_headers_README.md` names WhatsOnChain as the origin of its
testnet files. Each run is hash-linked end to end, and the node's checkpoints
inside them (mainnet 478558 and 504031, testnet 0 and 546) carry the hashes of
bitcoin-sv v1.2.3 `src/chainparams.cpp` (checked when copied, and by the tests).

| file | heights | from | sha256 |
|---|---|---|---|
| `main_886001_888000.bin` | 886001..=888000 | `services/blockchain/886001_888000_headers.bin`, whole | `106deacffa0eba0db49f814f1c3672222a81f792e514bf343478dcf055385bd9` |
| `main_477792_479824.bin` | 477792..=479824 | `services/blockchain/testdata/mainnet_headers_477792_504031.bin`, bytes 0..162640 | `fca160682c7c7179dbb354cce4ef001dee922b0c15d19754b32595d98a831834` |
| `main_501984_504031.bin` | 501984..=504031 | the same file, its last 163840 bytes | `48999616d05872e958709c1ac964022d0979394e348a0a1013d87b6a3b40ca35` |
| `test_0_547.bin` | 0..=547 | `services/blockchain/testdata/testnet_headers_0_547.bin`, whole | `8a6ba287b6a64cff223c4ffadccce85f8af292ad2a13b1427f20534083f9100e` |
| `test_1602530_1602710.bin` | 1602530..=1602710 | `test/testnet_headers_1602530_1602710.bin`, whole | `bb0f4db69dd334354cc4a59912e94549dffe1437ad16f373c525f85959f776c1` |

The whole of `mainnet_headers_477792_504031.bin` (26,240 headers, sha256
`5e5536bdc70c5fa860b0737984c8cc51c9f8fbb552694253bdcc46e594ed877f`) is not
copied; the ignored test `whole_eda_era_from_checkpoint_478558_to_checkpoint_504031`
replays it from a path.

`WITNESS_1602683` in `retarget_tests.rs` is not chain data: a testnet header
mined on 2026-10-08 on top of the real 1602682 (nine minutes of hashing on
the Studio, never broadcast), with real proof of work at the minimum
difficulty and a time 600 s after its parent, so testnet's rule refuses its
bits.
