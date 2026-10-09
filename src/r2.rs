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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers_per_file() {
        assert_eq!(HEADERS_PER_FILE, 100_000);
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
        let file = 0u32;
        assert_eq!(file * HEADERS_PER_FILE, 0);
        assert_eq!(file * HEADERS_PER_FILE + HEADERS_PER_FILE - 1, 99_999);

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
