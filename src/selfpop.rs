//! Self-populating R2 bulk store.
//!
//! On idle, `/health`-ok cron ticks (caught up so it never competes with
//! catch-up CPU), the worker fills the R2 bulk header files from the public CDN
//! itself — no manual `wrangler r2 object put`, and no permanent per-read CDN
//! dependency once populated. Two phases, at most ONE unit of work per tick:
//!
//!   Phase 1 (populate): stream a missing file CDN→R2 (no 8MB buffer — see
//!     `r2::stream_cdn_file_to_r2`), then a cheap size head. Marks it
//!     present-but-unverified.
//!   Phase 2 (verify + self-heal): read the file back from R2 in bounded chunks,
//!     check full header linkage + the CDN index's last hash. On success mark
//!     verified; on ANY mismatch delete the R2 object (reads revert to the CDN
//!     read-through) and let the next tick re-populate.
//!
//! Fail-safe: a corrupt/incomplete bulk file can only make the wallet REJECT a
//! proof (it compares the merkleRoot client-side), never false-confirm — so
//! serving present-but-unverified data while Phase 2 catches up is safe.
//!
//! Presence is authoritative in R2 (via `head`); the `bulk_pop_state` table
//! (migration 0004) only holds verification progress + a soft lease. Kept
//! entirely separate from the admin strict-scan import path.

use worker::*;

use crate::types::{BlockHeader, Chain};

/// Soft lease TTL: while a tick works a file it holds the lease; a later tick
/// only reclaims the file once this expires (crash recovery). Well above a
/// tick's wall time, well below anything a human would wait.
const LEASE_SECS: i64 = 120;

/// Headers verified per tick. Each is a double-SHA256; kept small so a verify
/// tick stays well under the ~10ms Free-plan CPU budget.
const VERIFY_CHUNK: u32 = 1_000;

#[derive(serde::Deserialize, Default)]
struct PopRow {
    verified: Option<f64>,
    verify_offset: Option<f64>,
    verify_prev_hash: Option<String>,
    lease_until: Option<f64>,
}

/// One self-pop unit of work. Best-effort: the caller ignores errors so a
/// self-pop hiccup never disturbs the cron's SPV-critical path.
pub async fn tick(env: &Env, chain: &Chain) -> Result<()> {
    let db = env.d1("DB")?;
    let bucket = match env.bucket("BULK_HEADERS") {
        Ok(b) => b,
        Err(_) => return Ok(()), // no bucket bound → nothing to do
    };

    // Snapshot layout (file count + per-file count/last_hash) from the CDN index.
    let listing = crate::woc::WocClient::get_bulk_file_listing(chain).await?;
    if listing.files.is_empty() {
        return Ok(());
    }

    let now = (Date::now().as_millis() / 1000) as i64;
    // Collision-resistant per-invocation token so the compare-and-claim has a
    // single winner even if a cron tick and an /admin/self-pop call race within
    // the same millisecond (a millis timestamp would collide and both would then
    // read their own token back and believe they won).
    let mut tok = [0u8; 16];
    getrandom::getrandom(&mut tok)
        .map_err(|e| worker::Error::RustError(format!("getrandom: {e}")))?;
    let token = hex::encode(tok);

    // Populate takes priority over verify: fill every missing file first (~one
    // per tick) so the per-read CDN dependency is removed quickly, then verify
    // the present files lazily. At most ONE unit of work per tick.
    let mut verify_candidate: Option<(u32, PopRow)> = None;
    for idx in 0..listing.files.len() as u32 {
        let file = &listing.files[idx as usize];
        // Fail-closed: only act on files the CDN index fully describes (a header
        // count + a last hash). Without them we cannot size-check the stream or
        // finalise verification, so leave the file to the CDN read-through.
        if file.count.unwrap_or(0) == 0 || file.last_hash.as_deref().unwrap_or("").is_empty() {
            continue;
        }

        let row: PopRow = crate::d1::Query::new(
            "SELECT verified, verify_offset, verify_prev_hash, lease_until \
             FROM bulk_pop_state WHERE file_idx = ?",
        )
        .bind(idx)
        .first(&db)
        .await?
        .unwrap_or_default();

        if row.verified.unwrap_or(0.0) as i64 == 1 {
            continue; // already fully verified
        }
        if row.lease_until.unwrap_or(0.0) as i64 > now {
            continue; // a recent/crashed tick still holds the lease
        }

        if crate::r2::bulk_object_size(&bucket, chain, idx).await?.is_none() {
            // Missing → populate now (priority), if we win the compare-and-claim.
            if claim_lease(&db, idx, now, &token).await? {
                populate(&db, &bucket, chain, idx, file).await?;
                return Ok(());
            }
            continue; // lost the claim; another worker has this file
        } else if verify_candidate.is_none() {
            // idx>0 needs the PREVIOUS file's index last hash for the cross-file
            // boundary check; without it we cannot fully verify, so don't select
            // it (fail-closed — it stays present and served via read-through).
            let boundary_ok = idx == 0
                || listing.files[(idx - 1) as usize]
                    .last_hash
                    .as_deref()
                    .is_some_and(|h| !h.is_empty());
            if boundary_ok {
                verify_candidate = Some((idx, row));
            }
        }
    }

    // Nothing missing → verify one chunk of the lowest present-but-unverified file.
    if let Some((idx, row)) = verify_candidate {
        if claim_lease(&db, idx, now, &token).await? {
            verify_chunk(
                &db,
                &bucket,
                chain,
                idx,
                &listing.files[idx as usize],
                &listing.files,
                &row,
            )
            .await?;
        }
    }

    Ok(())
}

/// Compare-and-claim the soft lease for `idx`: take it only if free (expired),
/// then read the token back to confirm THIS caller won — a single winner even if
/// a cron tick and an `/admin/self-pop` call race (R2 caps same-key writes).
/// Creates the row on first touch. Returns true iff we now hold the lease.
async fn claim_lease(db: &D1Database, idx: u32, now: i64, token: &str) -> Result<bool> {
    crate::d1::Query::new(
        "INSERT INTO bulk_pop_state (file_idx, lease_until, lease_token, updated_at) \
         VALUES (?, ?, ?, datetime('now')) \
         ON CONFLICT(file_idx) DO UPDATE SET lease_until = ?, lease_token = ?, \
         updated_at = datetime('now') WHERE bulk_pop_state.lease_until < ?",
    )
    .bind(idx)
    .bind(now + LEASE_SECS)
    .bind(token)
    .bind(now + LEASE_SECS)
    .bind(token)
    .bind(now)
    .run(db)
    .await?;

    #[derive(serde::Deserialize)]
    struct TokRow {
        lease_token: Option<String>,
    }
    let row: Option<TokRow> =
        crate::d1::Query::new("SELECT lease_token FROM bulk_pop_state WHERE file_idx = ?")
            .bind(idx)
            .first(db)
            .await?;
    Ok(row.and_then(|r| r.lease_token).as_deref() == Some(token))
}

/// Phase 1: stream the file CDN→R2, then sanity-check its size against the CDN
/// index count. On a size mismatch, delete it (leave absent → retry next tick).
async fn populate(
    db: &D1Database,
    bucket: &Bucket,
    chain: &Chain,
    idx: u32,
    file: &crate::woc::BulkHeaderFileInfo,
) -> Result<()> {
    crate::r2::stream_cdn_file_to_r2(bucket, chain, idx).await?;

    let expected = file.count.unwrap_or(0) as u64 * 80;
    match crate::r2::bulk_object_size(bucket, chain, idx).await? {
        Some(size) if expected > 0 && size == expected => {
            // Present + right size → ready for Phase-2 verification from offset 0.
            // Release the lease so the next tick can start verifying immediately.
            crate::d1::Query::new(
                "UPDATE bulk_pop_state SET verified = 0, verify_offset = 0, \
                 verify_prev_hash = '', lease_until = 0, updated_at = datetime('now') \
                 WHERE file_idx = ?",
            )
            .bind(idx)
            .run(db)
            .await?;
            console_log!("Self-pop: populated bulk file {idx} ({size} bytes) — pending verify");
        }
        other => {
            console_log!(
                "Self-pop: file {idx} bad size after put ({other:?} != {expected}) — deleting",
            );
            let _ = crate::r2::delete_bulk_file(bucket, chain, idx).await;
            // Release the lease so a later tick retries the populate.
            crate::d1::Query::new(
                "UPDATE bulk_pop_state SET lease_until = 0, updated_at = datetime('now') \
                 WHERE file_idx = ?",
            )
            .bind(idx)
            .run(db)
            .await?;
        }
    }
    Ok(())
}

/// Phase 2: verify one bounded chunk read back from R2. Advances the resume
/// point on success, marks the file verified when the last header matches the
/// CDN index, and self-heals (delete + reset) on any linkage/parse/last-hash
/// mismatch.
async fn verify_chunk(
    db: &D1Database,
    bucket: &Bucket,
    chain: &Chain,
    idx: u32,
    file: &crate::woc::BulkHeaderFileInfo,
    files: &[crate::woc::BulkHeaderFileInfo],
    row: &PopRow,
) -> Result<()> {
    let total = file.count.unwrap_or(0);
    let first_height = file
        .first_height
        .unwrap_or(idx * crate::r2::HEADERS_PER_FILE);
    let offset = row.verify_offset.unwrap_or(0.0) as u32;
    if total == 0 || offset >= total {
        // Nothing to verify (the metadata gate should prevent total==0) —
        // release the lease so the file is never wedged holding it.
        crate::d1::Query::new(
            "UPDATE bulk_pop_state SET lease_until = 0, updated_at = datetime('now') \
             WHERE file_idx = ?",
        )
        .bind(idx)
        .run(db)
        .await?;
        return Ok(());
    }

    // Effective predecessor hash: within a file, the previous chunk's last hash
    // (stored). At a file's first chunk, the PRIOR file's index-declared last
    // hash, so cross-file linkage is checked too (genesis file has none).
    let effective_prev = if offset == 0 {
        if idx > 0 {
            match files[(idx - 1) as usize].last_hash.as_deref() {
                Some(h) if !h.is_empty() => h.to_string(),
                _ => {
                    // No previous-file last hash → the cross-file boundary can't
                    // be checked; fail-closed (release the lease, leave the file
                    // present-but-unverified rather than trusting it).
                    crate::d1::Query::new(
                        "UPDATE bulk_pop_state SET lease_until = 0, \
                         updated_at = datetime('now') WHERE file_idx = ?",
                    )
                    .bind(idx)
                    .run(db)
                    .await?;
                    return Ok(());
                }
            }
        } else {
            String::new() // genesis file has no predecessor to link to
        }
    } else {
        row.verify_prev_hash.clone().unwrap_or_default()
    };

    let want = VERIFY_CHUNK.min(total - offset);
    let bytes = match crate::r2::read_bulk_range(bucket, chain, idx, offset, want).await? {
        Some(b) if b.len() >= 80 => b,
        _ => {
            // Object vanished or short read → self-heal.
            self_heal(db, bucket, chain, idx, "verify read failed").await?;
            return Ok(());
        }
    };

    match verify_chunk_linkage(&bytes, first_height + offset, &effective_prev) {
        Ok((last_hash, n)) => {
            let new_offset = offset + n;
            if new_offset >= total {
                // Final chunk: the file's last header must match the CDN index.
                let idx_last = file.last_hash.clone().unwrap_or_default();
                if !idx_last.is_empty() && !last_hash.eq_ignore_ascii_case(&idx_last) {
                    self_heal(db, bucket, chain, idx, "last hash != CDN index").await?;
                    return Ok(());
                }
                crate::d1::Query::new(
                    "UPDATE bulk_pop_state SET verified = 1, verify_offset = ?, \
                     verify_prev_hash = ?, lease_until = 0, updated_at = datetime('now') \
                     WHERE file_idx = ?",
                )
                .bind(new_offset)
                .bind(last_hash)
                .bind(idx)
                .run(db)
                .await?;
                console_log!("Self-pop: VERIFIED bulk file {idx} ({total} headers)");
            } else {
                // Release the lease so the next tick continues this file's next chunk.
                crate::d1::Query::new(
                    "UPDATE bulk_pop_state SET verify_offset = ?, verify_prev_hash = ?, \
                     lease_until = 0, updated_at = datetime('now') WHERE file_idx = ?",
                )
                .bind(new_offset)
                .bind(last_hash)
                .bind(idx)
                .run(db)
                .await?;
            }
        }
        Err(broken_at) => {
            self_heal(
                db,
                bucket,
                chain,
                idx,
                &format!("linkage break at header {}", first_height + offset + broken_at),
            )
            .await?;
        }
    }
    Ok(())
}

/// Delete a bad R2 object and reset its state so the next idle tick re-populates
/// it. Reads meanwhile fall back to the CDN read-through (fail-safe).
async fn self_heal(
    db: &D1Database,
    bucket: &Bucket,
    chain: &Chain,
    idx: u32,
    why: &str,
) -> Result<()> {
    console_log!("Self-pop: self-heal bulk file {idx} ({why}) — deleting + re-queueing");
    let _ = crate::r2::delete_bulk_file(bucket, chain, idx).await;
    crate::d1::Query::new(
        "UPDATE bulk_pop_state SET verified = 0, verify_offset = 0, verify_prev_hash = '', \
         lease_until = 0, updated_at = datetime('now') WHERE file_idx = ?",
    )
    .bind(idx)
    .run(db)
    .await
}

/// Verify that a chunk of concatenated 80-byte headers links internally and to
/// `prev_hash` (empty = don't check the first header's predecessor). Returns
/// `(last_hash, headers_checked)` on success, or `Err(index_within_chunk)` at
/// the first parse/linkage break. Pure — the verifier's core, unit-tested.
fn verify_chunk_linkage(
    bytes: &[u8],
    first_height: u32,
    prev_hash: &str,
) -> core::result::Result<(String, u32), u32> {
    let mut prev = prev_hash.to_string();
    let mut last = String::new();
    let mut n: u32 = 0;
    for (k, chunk) in bytes.chunks(80).enumerate() {
        if chunk.len() < 80 {
            break; // trailing partial (ranged EOF) — stop cleanly
        }
        let h = match BlockHeader::from_bytes(chunk, first_height + k as u32) {
            Some(h) => h,
            None => return Err(k as u32),
        };
        if !prev.is_empty() && !h.previous_hash.eq_ignore_ascii_case(&prev) {
            return Err(k as u32);
        }
        prev = h.hash.clone();
        last = h.hash;
        n += 1;
    }
    Ok((last, n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlockHeader;

    // Build a valid-looking 80-byte header whose previous_hash we control, then
    // reparse it so the test uses the SAME hashing the verifier uses.
    fn header_bytes(prev_display: &str, nonce: u32) -> ([u8; 80], BlockHeader) {
        let mut b = [0u8; 80];
        // version
        b[0..4].copy_from_slice(&1u32.to_le_bytes());
        // previousHash: display hex → wire (reverse 32 bytes)
        let raw = hex::decode(prev_display).unwrap();
        let mut wire = raw.clone();
        wire.reverse();
        b[4..36].copy_from_slice(&wire);
        // merkleRoot left zero; time/bits/nonce
        b[68..72].copy_from_slice(&1_700_000_000u32.to_le_bytes());
        b[72..76].copy_from_slice(&0x1d00ffffu32.to_le_bytes());
        b[76..80].copy_from_slice(&nonce.to_le_bytes());
        let parsed = BlockHeader::from_bytes(&b, 0).unwrap();
        (b, parsed)
    }

    const ZERO: &str = "0000000000000000000000000000000000000000000000000000000000000000";

    #[test]
    fn empty_prev_skips_first_check_then_links() {
        // header A (prev = zero), header B (prev = A.hash) → a valid 2-chunk run
        let (a_bytes, a) = header_bytes(ZERO, 1);
        let (b_bytes, b) = header_bytes(&a.hash, 2);
        let mut buf = Vec::new();
        buf.extend_from_slice(&a_bytes);
        buf.extend_from_slice(&b_bytes);
        let (last, n) = verify_chunk_linkage(&buf, 0, "").unwrap();
        assert_eq!(n, 2);
        assert_eq!(last, b.hash);
    }

    #[test]
    fn detects_linkage_break() {
        // A, then a header whose prev is wrong (zero, not A.hash) → break at 1
        let (a_bytes, _a) = header_bytes(ZERO, 1);
        let (bad_bytes, _bad) = header_bytes(ZERO, 2);
        let mut buf = Vec::new();
        buf.extend_from_slice(&a_bytes);
        buf.extend_from_slice(&bad_bytes);
        assert_eq!(verify_chunk_linkage(&buf, 0, "").unwrap_err(), 1);
    }

    #[test]
    fn checks_first_header_against_supplied_prev() {
        // Single header with prev = zero, but we assert it should follow X.
        let (a_bytes, _a) = header_bytes(ZERO, 1);
        let x = "1111111111111111111111111111111111111111111111111111111111111111";
        assert_eq!(verify_chunk_linkage(&a_bytes, 0, x).unwrap_err(), 0);
    }

    #[test]
    fn trailing_partial_is_ignored() {
        let (a_bytes, a) = header_bytes(ZERO, 1);
        let mut buf = Vec::new();
        buf.extend_from_slice(&a_bytes);
        buf.extend_from_slice(&[0u8; 40]); // half a header at EOF
        let (last, n) = verify_chunk_linkage(&buf, 0, "").unwrap();
        assert_eq!(n, 1);
        assert_eq!(last, a.hash);
    }
}
