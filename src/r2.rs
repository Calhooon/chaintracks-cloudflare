//! R2 storage for bulk header binary files.
//!
//! Exports block headers from D1 to R2 as concatenated 80-byte binary files
//! (same format as Babbage CDN). Each file contains up to 100,000 headers.
//! Also generates an index JSON (mainNetBlockHeaders.json) for client bootstrap.
//!
//! Based on ~/bsv/rust-wallet-infra/src/r2.rs for the R2 API pattern.

use worker::{Bucket, D1Database};

use crate::storage;
use crate::types::Chain;

pub const HEADERS_PER_FILE: u32 = 100_000;

/// Index JSON that lists all available bulk header files.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkHeaderIndex {
    pub root_folder: String,
    pub json_filename: String,
    pub headers_per_file: u32,
    pub files: Vec<BulkFileEntry>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkFileEntry {
    pub chain: String,
    pub count: u32,
    pub file_name: String,
    pub first_height: u32,
    pub source_url: String,
}

/// Export a range of headers from D1 to R2 as a single binary file.
///
/// Returns the number of headers written.
pub async fn export_bulk_file(
    db: &D1Database,
    bucket: &Bucket,
    chain: &Chain,
    file_index: u32,
    _cdn_base_url: &str,
) -> worker::Result<u32> {
    let start_height = file_index * HEADERS_PER_FILE;

    // Read headers from D1
    let hex_str = storage::get_headers_hex(db, start_height, HEADERS_PER_FILE).await?;
    if hex_str.is_empty() {
        return Ok(0);
    }

    // Decode hex to binary
    let bytes =
        hex::decode(&hex_str).map_err(|e| worker::Error::RustError(format!("hex decode: {e}")))?;
    let header_count = (bytes.len() / 80) as u32;

    // ANCHOR GUARD (review M-1): a hole at the FILE START passes the
    // linkage check below (rows begin at start+k, internally contiguous,
    // every header shifted k slots). The first returned header must be the
    // active header AT start_height.
    if let Some(first_chunk) = bytes.chunks(80).next() {
        if first_chunk.len() == 80 {
            let first_hash = crate::types::compute_block_hash(first_chunk);
            match storage::find_header_for_height(db, start_height).await? {
                Some(expected) if expected.hash.eq_ignore_ascii_case(&first_hash) => {}
                Some(expected) => {
                    return Err(worker::Error::RustError(format!(
                        "export {file_index}: first header {} is not the active header at height {} ({}) — refusing misaligned file",
                        first_hash, start_height, expected.hash
                    )));
                }
                None => {
                    return Err(worker::Error::RustError(format!(
                        "export {file_index}: no active header at start height {} — refusing",
                        start_height
                    )));
                }
            }
        }
    }

    // HOLE GUARD (audit M4): get_headers_hex silently SKIPS missing heights,
    // and bulk-file consumers index by offset = (height-first)*80 — one hole
    // and every later header in the file sits at the wrong height. A partial
    // trailing file is only legal for the LAST (tip) file; enforce
    // contiguity by verifying linkage across the whole span.
    let mut prev_hash: Option<String> = None;
    for (i, chunk) in bytes.chunks(80).enumerate() {
        if chunk.len() < 80 {
            break;
        }
        if let Some(h) = crate::types::BlockHeader::from_bytes(chunk, start_height + i as u32) {
            if let Some(ref expected_prev) = prev_hash {
                if !h.previous_hash.eq_ignore_ascii_case(expected_prev) {
                    return Err(worker::Error::RustError(format!(
                        "export {file_index}: linkage break at offset {i} (height {}): header links {} but prior header is {} — refusing to publish a misaligned bulk file",
                        start_height + i as u32, h.previous_hash, expected_prev
                    )));
                }
            }
            prev_hash = Some(h.hash);
        }
    }

    // Write binary file to R2
    let file_name = format!("{chain}Net_{file_index}.headers");
    bucket
        .put(&file_name, bytes)
        .execute()
        .await
        .map_err(|e| worker::Error::RustError(format!("R2 put {file_name}: {e}")))?;

    worker::console_log!(
        "Exported {header_count} headers to R2: {file_name} (height {start_height}-{})",
        start_height + header_count - 1
    );

    Ok(header_count)
}

/// Export all available headers from D1 to R2 and update the index JSON.
///
/// Returns total headers exported.
pub async fn export_all(
    db: &D1Database,
    bucket: &Bucket,
    chain: &Chain,
    cdn_base_url: &str,
) -> worker::Result<ExportResult> {
    let tip_height = storage::get_chain_tip_height(db).await?;
    let num_files = tip_height / HEADERS_PER_FILE + 1;

    let mut total_exported = 0u32;
    let mut files = Vec::new();

    for i in 0..num_files {
        let start_height = i * HEADERS_PER_FILE;
        let count = export_bulk_file(db, bucket, chain, i, cdn_base_url).await?;

        if count > 0 {
            files.push(BulkFileEntry {
                chain: format!("{chain}"),
                count,
                file_name: format!("{chain}Net_{i}.headers"),
                first_height: start_height,
                source_url: cdn_base_url.to_string(),
            });
            total_exported += count;
        }
    }

    // Write index JSON
    let index = BulkHeaderIndex {
        root_folder: cdn_base_url.to_string(),
        json_filename: format!("{chain}NetBlockHeaders.json"),
        headers_per_file: HEADERS_PER_FILE,
        files,
    };

    let index_json = serde_json::to_string_pretty(&index)
        .map_err(|e| worker::Error::RustError(format!("json: {e}")))?;

    let index_filename = format!("{chain}NetBlockHeaders.json");
    bucket
        .put(&index_filename, index_json.into_bytes())
        .execute()
        .await
        .map_err(|e| worker::Error::RustError(format!("R2 put index: {e}")))?;

    worker::console_log!("R2 export complete: {total_exported} headers in {num_files} files");

    Ok(ExportResult {
        total_exported,
        file_count: num_files,
    })
}

pub struct ExportResult {
    pub total_exported: u32,
    pub file_count: u32,
}

/// Serve a file from R2 (for the /headers/ route).
pub async fn serve_file(bucket: &Bucket, key: &str) -> worker::Result<Option<Vec<u8>>> {
    let obj = bucket
        .get(key)
        .execute()
        .await
        .map_err(|e| worker::Error::RustError(format!("R2 get {key}: {e}")))?;

    match obj {
        Some(obj) => {
            let body = obj
                .body()
                .ok_or_else(|| worker::Error::RustError("R2 object has no body".into()))?;
            let bytes = body
                .bytes()
                .await
                .map_err(|e| worker::Error::RustError(format!("R2 read {key}: {e}")))?;
            Ok(Some(bytes))
        }
        None => Ok(None),
    }
}

// ─── Bulk read (bulk/live split) ────────────────────────────────────────────

/// Read one header from the R2 bulk store by height via a ranged read (80
/// bytes at the height's byte offset). Falls back to a projectbabbage CDN
/// read-through if the file isn't in R2 yet, so header reads never fail while
/// R2 is being populated or if it has a gap.
pub async fn read_bulk_header(
    bucket: &Bucket,
    chain: &Chain,
    height: u32,
) -> worker::Result<Option<crate::types::BlockHeader>> {
    let file_idx = height / HEADERS_PER_FILE;
    let offset = (height % HEADERS_PER_FILE) as u64 * 80;
    let key = format!("{}Net_{}.headers", chain.as_str(), file_idx);

    // Fast path: our own R2 bucket, ranged 80-byte read (server-enforced, so
    // it returns exactly the requested slice or fewer bytes past EOF).
    match bucket
        .get(&key)
        .range(worker::Range::OffsetWithLength { offset, length: 80 })
        .execute()
        .await
    {
        Ok(Some(obj)) => {
            if let Some(body) = obj.body() {
                if let Ok(bytes) = body.bytes().await {
                    if bytes.len() == 80 {
                        if let Some(h) = crate::types::BlockHeader::from_bytes(&bytes, height) {
                            return Ok(Some(h));
                        }
                    }
                }
            }
        }
        Ok(None) => {} // not in R2 yet → CDN fallback
        Err(e) => worker::console_log!("R2 bulk read {key} failed: {e:?}"),
    }

    // Fallback: read-through from the CDN (R2 not populated yet / gap).
    crate::woc::cdn_header_by_height(chain, height).await
}

/// Stream a whole bulk header file from the CDN straight into R2, without ever
/// buffering the ~8MB body in the worker (Free-plan CPU is ~10ms/tick, and
/// materialising 8MB into a Vec would blow it). The CDN response body is a
/// ReadableStream; R2 `put` takes one directly, so bytes flow CDN→R2 as I/O,
/// not CPU. Content is verified separately (size head + Phase-2 readback).
pub async fn stream_cdn_file_to_r2(
    bucket: &Bucket,
    chain: &Chain,
    file_idx: u32,
) -> worker::Result<()> {
    let url = format!(
        "https://cdn.projectbabbage.com/blockheaders/{}Net_{}.headers",
        chain.as_str(),
        file_idx
    );
    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Get);
    let request = worker::Request::new_with_init(&url, &init)?;
    let response = worker::Fetch::Request(request).send().await?;
    let status = response.status_code();
    if !(200..300).contains(&status) {
        return Err(worker::Error::RustError(format!("CDN {url} HTTP {status}")));
    }

    let key = format!("{}Net_{}.headers", chain.as_str(), file_idx);
    let (_, body) = response.into_parts();
    match body {
        worker::ResponseBody::Stream(stream) => {
            bucket
                .put(&key, stream)
                .execute()
                .await
                .map_err(|e| worker::Error::RustError(format!("R2 stream put {key}: {e}")))?;
            Ok(())
        }
        _ => Err(worker::Error::RustError(format!(
            "CDN {url}: response body was not a stream"
        ))),
    }
}

/// Byte size of a bulk file already in R2, or None if absent (write-once guard
/// + post-populate size sanity check).
pub async fn bulk_object_size(
    bucket: &Bucket,
    chain: &Chain,
    file_idx: u32,
) -> worker::Result<Option<u64>> {
    let key = format!("{}Net_{}.headers", chain.as_str(), file_idx);
    Ok(bucket.head(&key).await?.map(|obj| obj.size() as u64))
}

/// Ranged read of `count` consecutive 80-byte headers from a bulk R2 file,
/// starting at header index `header_offset` within the file. Returns the raw
/// bytes (may be short at EOF) or None if the object is absent. Used by the
/// Phase-2 lazy verifier.
pub async fn read_bulk_range(
    bucket: &Bucket,
    chain: &Chain,
    file_idx: u32,
    header_offset: u32,
    count: u32,
) -> worker::Result<Option<Vec<u8>>> {
    let key = format!("{}Net_{}.headers", chain.as_str(), file_idx);
    let offset = header_offset as u64 * 80;
    let length = count as u64 * 80;
    match bucket
        .get(&key)
        .range(worker::Range::OffsetWithLength { offset, length })
        .execute()
        .await
    {
        Ok(Some(obj)) => match obj.body() {
            Some(body) => Ok(Some(body.bytes().await?)),
            None => Ok(None),
        },
        Ok(None) => Ok(None),
        Err(e) => Err(worker::Error::RustError(format!(
            "R2 range read {key}: {e}"
        ))),
    }
}

/// Delete a bulk file from R2 (self-heal: a file that fails Phase-2 verification
/// is removed so reads fall back to the CDN and the next tick re-populates it).
pub async fn delete_bulk_file(bucket: &Bucket, chain: &Chain, file_idx: u32) -> worker::Result<()> {
    let key = format!("{}Net_{}.headers", chain.as_str(), file_idx);
    bucket
        .delete(&key)
        .await
        .map_err(|e| worker::Error::RustError(format!("R2 delete {key}: {e}")))
}

/// One-time populate: download a whole bulk CDN file and store it in R2 under
/// the same key (`mainNet_{idx}.headers`), so subsequent old-height reads hit
/// R2 instead of the CDN. Returns bytes written.
pub async fn import_file_from_cdn(
    bucket: &Bucket,
    chain: &Chain,
    file_idx: u32,
) -> worker::Result<usize> {
    // Integrity metadata from the CDN index.
    let listing = crate::woc::WocClient::get_bulk_file_listing(chain).await?;
    let info = listing
        .files
        .get(file_idx as usize)
        .ok_or_else(|| worker::Error::RustError(format!("no CDN file at index {file_idx}")))?;
    let expected_count = info
        .count
        .ok_or_else(|| worker::Error::RustError("CDN index missing file count".into()))?;
    let first_height = info.first_height.unwrap_or(file_idx * HEADERS_PER_FILE);

    let client = crate::woc::WocClient::new(chain, None);
    let bytes = client.download_bulk_file_raw(chain, file_idx).await?;
    if bytes.is_empty() {
        return Ok(0);
    }

    // Verify byte-alignment and that the file ends at the canonical last block
    // BEFORE trusting it — read_bulk_header trusts offset math blindly, so a
    // mid-file hole or truncation would silently mislabel every header (which
    // could reject a valid SPV proof as invalid). A hole changes both the
    // length and the last-header hash.
    if bytes.len() as u64 != expected_count as u64 * 80 {
        return Err(worker::Error::RustError(format!(
            "bulk file {file_idx}: {} bytes, expected {} ({}×80)",
            bytes.len(),
            expected_count as u64 * 80,
            expected_count
        )));
    }
    // Full linkage scan: every header must chain to its predecessor, so a
    // mid-file splice (correct length + last hash but a wrong middle header)
    // cannot slip through and later yield a definitive-but-wrong SPV answer.
    let mut prev: Option<String> = None;
    for (i, chunk) in bytes.chunks(80).enumerate() {
        let h = crate::types::BlockHeader::from_bytes(chunk, first_height + i as u32)
            .ok_or_else(|| {
                worker::Error::RustError(format!("bulk file {file_idx}: parse fail at index {i}"))
            })?;
        if let Some(pp) = &prev {
            if !h.previous_hash.eq_ignore_ascii_case(pp) {
                return Err(worker::Error::RustError(format!(
                    "bulk file {file_idx}: linkage break at height {}",
                    first_height + i as u32
                )));
            }
        }
        prev = Some(h.hash);
    }
    // The chain's final hash must match the CDN index's lastHash. Required —
    // refuse to import a file the index can't vouch for (hardening: don't
    // let the in-worker path be an unverified integrity gate).
    let expected_last = info.last_hash.as_deref().ok_or_else(|| {
        worker::Error::RustError(format!(
            "bulk file {file_idx}: CDN index has no lastHash — refusing to import unverifiable file"
        ))
    })?;
    match prev.as_deref() {
        Some(last) if last.eq_ignore_ascii_case(expected_last) => {}
        other => {
            return Err(worker::Error::RustError(format!(
                "bulk file {file_idx}: last hash {other:?} != index {expected_last}"
            )))
        }
    }

    let key = format!("{}Net_{}.headers", chain.as_str(), file_idx);
    let n = bytes.len();
    bucket
        .put(&key, bytes)
        .execute()
        .await
        .map_err(|e| worker::Error::RustError(format!("R2 put {key}: {e}")))?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers_per_file() {
        assert_eq!(HEADERS_PER_FILE, 100_000);
    }

    #[test]
    fn test_bulk_read_offset_math() {
        // height -> (file_idx, byte offset)
        let h = 452_253u32;
        assert_eq!(h / HEADERS_PER_FILE, 4);
        assert_eq!((h % HEADERS_PER_FILE) as u64 * 80, 52_253 * 80);
        // file-9 last covered height 942_760 -> file 9, offset 42_760*80
        let h = 942_760u32;
        assert_eq!(h / HEADERS_PER_FILE, 9);
        assert_eq!((h % HEADERS_PER_FILE) as u64 * 80, 42_760 * 80);
    }

    #[test]
    fn test_file_name_format() {
        let chain = Chain::Main;
        let name = format!("{chain}Net_0.headers");
        assert_eq!(name, "mainNet_0.headers");

        let name = format!("{chain}Net_9.headers");
        assert_eq!(name, "mainNet_9.headers");
    }

    #[test]
    fn test_index_json_serde() {
        let index = BulkHeaderIndex {
            root_folder: "https://example.com/headers".to_string(),
            json_filename: "mainNetBlockHeaders.json".to_string(),
            headers_per_file: 100_000,
            files: vec![BulkFileEntry {
                chain: "main".to_string(),
                count: 100_000,
                file_name: "mainNet_0.headers".to_string(),
                first_height: 0,
                source_url: "https://example.com/headers".to_string(),
            }],
        };

        let json = serde_json::to_string(&index).unwrap();
        let parsed: BulkHeaderIndex = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.files.len(), 1);
        assert_eq!(parsed.files[0].file_name, "mainNet_0.headers");
        assert_eq!(parsed.files[0].first_height, 0);
        assert_eq!(parsed.headers_per_file, 100_000);
    }

    #[test]
    fn test_index_json_matches_babbage_format() {
        // Verify our index JSON format matches the Babbage CDN format
        // that clients expect (from cdn.projectbabbage.com/blockheaders/)
        let babbage_json = serde_json::json!({
            "rootFolder": "https://cdn.projectbabbage.com/blockheaders",
            "jsonFilename": "mainNetBlockHeaders.json",
            "headersPerFile": 100000,
            "files": [{
                "chain": "main",
                "count": 100000,
                "fileName": "mainNet_0.headers",
                "firstHeight": 0,
                "sourceUrl": "https://cdn.projectbabbage.com/blockheaders"
            }]
        });

        // Parse with our types
        let index: BulkHeaderIndex = serde_json::from_value(babbage_json).unwrap();
        assert_eq!(
            index.root_folder,
            "https://cdn.projectbabbage.com/blockheaders"
        );
        assert_eq!(index.headers_per_file, 100000);
        assert_eq!(index.files[0].file_name, "mainNet_0.headers");
    }

    #[test]
    fn test_file_index_to_height_range() {
        // File 0: heights 0-99999
        assert_eq!(0 * HEADERS_PER_FILE, 0);
        assert_eq!(0 * HEADERS_PER_FILE + HEADERS_PER_FILE - 1, 99_999);

        // File 5: heights 500000-599999
        assert_eq!(5 * HEADERS_PER_FILE, 500_000);
        assert_eq!(5 * HEADERS_PER_FILE + HEADERS_PER_FILE - 1, 599_999);

        // File 9: heights 900000+
        assert_eq!(9 * HEADERS_PER_FILE, 900_000);
    }

    #[test]
    fn test_num_files_calculation() {
        // 931,772 headers → 10 files (0-9)
        let tip = 931_771u32;
        let num_files = tip / HEADERS_PER_FILE + 1;
        assert_eq!(num_files, 10);

        // 100,000 headers → 2 files (0 full, 1 empty but generated)
        let tip = 99_999u32;
        let num_files = tip / HEADERS_PER_FILE + 1;
        assert_eq!(num_files, 1);
    }
}
