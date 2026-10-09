//! E5: the reader's half of a Server-Sent Events stream, and the tip event's
//! header. Pure, so the host suite runs it; the tip stream's object
//! (`tip_stream.rs`) feeds it the bytes the peer sends.
//!
//! The format is the WHATWG event stream: lines end in LF or CRLF (a lone CR
//! is not accepted as an end of line here; neither peer below sends one), a
//! blank line dispatches the event, a line that starts with `:` is a comment,
//! `field: value` with one optional space after the colon, `data` lines
//! joined with LF, `id` the last event id (one carrying NUL is ignored), any
//! other field ignored.
//!
//! The peer of record is Arcade's chaintracks v2 `/chaintracks/v2/tip/stream`
//! ([SRC] arcade@1ae1208 services/chaintracks_server/routes.go:453-490, the
//! route mounted at service.go:150): `data: <the tip as JSON>` and a blank
//! line, the current tip at once on every connect, each new tip after, a
//! `: keepalive` comment every 15 s (routes.go:519-536), no `id` field, so
//! no replay from a `Last-Event-ID`. The JSON is go-chaintracks' BlockHeader
//! (`version`, `previousHash`, `merkleRoot`, `time`, `bits`, `nonce`,
//! `height`, `hash`; [SRC] go-chaintracks@c7eeda1 chaintracks/types.go:11-17
//! over go-sdk@v1.7.1 block/header.go:19-26, hashes in display order through
//! chainhash's MarshalJSON), the shape the courier ladder already reads from
//! Arcade's header routes (`couriers::ArcadeHeader`). This service's own
//! `/events/stream` (#32, `docs/CHAIN-EVENTS.md`) sends ids and a version-1
//! envelope; a `tip` envelope is read too, so a peer of ours can be the
//! source by a change of URL.

use crate::types::BlockHeader;

/// One dispatched event, or a comment (the peer's heartbeat).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Item {
    Comment,
    Event {
        /// The `id` field of this event, when it carried one.
        id: Option<String>,
        /// The `event` field (`message` when absent).
        event: String,
        data: String,
    },
}

/// The bytes of a stream, fed in the chunks they arrive in.
#[derive(Debug, Default)]
pub(crate) struct Parser {
    pending: Vec<u8>,
    data: Vec<String>,
    event: Option<String>,
    id: Option<String>,
    has_fields: bool,
}

/// A line longer than this with no end is a peer fault, not a header: the
/// reader drops the stream rather than grow without bound. A tip event is
/// about 400 bytes.
pub(crate) const MAX_LINE: usize = 64 * 1024;

impl Parser {
    /// The items completed by `chunk`, in order. A line split across chunks
    /// is held until its end arrives.
    pub fn feed(&mut self, chunk: &[u8]) -> Result<Vec<Item>, String> {
        self.pending.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some(end) = self.pending.iter().position(|b| *b == b'\n') {
            let mut line: Vec<u8> = self.pending.drain(..=end).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8_lossy(&line).into_owned();
            if let Some(item) = self.line(&line) {
                out.push(item);
            }
        }
        if self.pending.len() > MAX_LINE {
            return Err(format!("a line of more than {MAX_LINE} bytes with no end"));
        }
        Ok(out)
    }

    fn line(&mut self, line: &str) -> Option<Item> {
        if line.is_empty() {
            if !self.has_fields {
                return None;
            }
            self.has_fields = false;
            let id = self.id.take();
            let event = self.event.take().unwrap_or_else(|| "message".into());
            let data = std::mem::take(&mut self.data).join("\n");
            return Some(Item::Event { id, event, data });
        }
        if line.starts_with(':') {
            return Some(Item::Comment);
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "data" => {
                self.data.push(value.to_string());
                self.has_fields = true;
            }
            "event" => {
                self.event = Some(value.to_string());
                self.has_fields = true;
            }
            "id" if !value.contains('\0') => {
                self.id = Some(value.to_string());
                self.has_fields = true;
            }
            _ => {}
        }
        None
    }
}

/// The header a tip event carries: Arcade's JSON header, or this service's
/// own version-1 `tip` envelope. `Ok(None)` for an envelope of another kind
/// (a fork, a reorg, a tip's age: the header of the new tip arrives as its
/// own `tip`). The claimed hash must be the hash of the fields; the door
/// checks everything else.
pub(crate) fn tip_header(data: &str) -> Result<Option<BlockHeader>, String> {
    let value: serde_json::Value =
        serde_json::from_str(data).map_err(|e| format!("tip event: not JSON: {e}"))?;
    let header = match value.get("kind") {
        None => value,
        Some(kind) => {
            if value.get("v") != Some(&serde_json::json!(1)) {
                return Err(format!(
                    "tip event: an envelope of version {} is not read",
                    value.get("v").cloned().unwrap_or_default()
                ));
            }
            if kind != "tip" {
                return Ok(None);
            }
            value
                .get("header")
                .cloned()
                .ok_or("tip event: a tip envelope with no header")?
        }
    };
    let header: crate::couriers::ArcadeHeader =
        serde_json::from_value(header).map_err(|e| format!("tip event: not a header: {e}"))?;
    header
        .into_block_header()
        .map(Some)
        .map_err(|e| format!("tip event: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real header at 886001 (`src/testdata/README.md`).
    fn real() -> BlockHeader {
        let run = include_bytes!("testdata/main_886001_888000.bin");
        BlockHeader::from_bytes(&run[..80], 886_001).unwrap()
    }

    /// The header as Arcade's stream sends it (go-chaintracks' BlockHeader).
    fn arcade_json(h: &BlockHeader) -> String {
        serde_json::json!({
            "version": h.version, "previousHash": h.previous_hash, "merkleRoot": h.merkle_root,
            "time": h.time, "bits": h.bits, "nonce": h.nonce, "height": h.height, "hash": h.hash,
        })
        .to_string()
    }

    fn event(id: Option<&str>, data: &str) -> Item {
        Item::Event {
            id: id.map(str::to_string),
            event: "message".into(),
            data: data.into(),
        }
    }

    #[test]
    fn arcade_s_stream_reads_as_a_keepalive_and_one_tip_event() {
        let h = real();
        let bytes = format!(": keepalive\n\ndata: {}\n\n", arcade_json(&h));
        let mut p = Parser::default();
        let items = p.feed(bytes.as_bytes()).unwrap();
        assert_eq!(items, vec![Item::Comment, event(None, &arcade_json(&h))]);
        let Item::Event { data, .. } = &items[1] else {
            unreachable!()
        };
        let got = tip_header(data).unwrap().unwrap();
        assert_eq!((got.height, got.hash.as_str()), (h.height, h.hash.as_str()));
        assert_eq!(got.to_bytes(), h.to_bytes());
    }

    #[test]
    fn an_event_split_across_chunks_waits_for_its_blank_line() {
        let text = "id: 41\r\nevent: tip\r\ndata: one\r\ndata: two\r\n\r\n";
        let mut p = Parser::default();
        let mut items = Vec::new();
        for b in text.as_bytes().chunks(3) {
            items.extend(p.feed(b).unwrap());
        }
        assert_eq!(
            items,
            vec![Item::Event {
                id: Some("41".into()),
                event: "tip".into(),
                data: "one\ntwo".into()
            }]
        );
    }

    #[test]
    fn a_field_without_a_value_and_unknown_fields_follow_the_format() {
        let mut p = Parser::default();
        let items = p
            .feed(b"retry: 5\nid\ndata\n\nid: a\0b\ndata:x\n\n\n")
            .unwrap();
        assert_eq!(
            items,
            vec![event(Some(""), ""), event(None, "x")],
            "an empty id resets it; an id with NUL is ignored; a blank line alone dispatches nothing"
        );
    }

    #[test]
    fn a_line_with_no_end_past_the_bound_is_a_peer_fault() {
        let mut p = Parser::default();
        assert!(p.feed(&vec![b'a'; MAX_LINE + 1]).is_err());
    }

    #[test]
    fn our_own_tip_envelope_is_read_and_other_kinds_are_passed_over() {
        let h = real();
        let header = crate::events::EventHeader::from(&h);
        let tip = serde_json::json!({"v": 1, "kind": "tip", "height": h.height, "hash": h.hash, "time": h.time, "header": header});
        let got = tip_header(&tip.to_string()).unwrap().unwrap();
        assert_eq!(got.hash, h.hash);
        let age = serde_json::json!({"v": 1, "kind": "tipAge", "seconds": 5, "tip": header});
        assert!(tip_header(&age.to_string()).unwrap().is_none());
        let v2 = serde_json::json!({"v": 2, "kind": "tip", "header": header});
        assert!(
            tip_header(&v2.to_string()).is_err(),
            "a version not read is a fault, never skipped"
        );
    }

    #[test]
    fn a_tip_whose_claimed_hash_is_not_its_fields_hash_is_refused_before_the_door() {
        let mut h = real();
        h.nonce = h.nonce.wrapping_add(1);
        let err = tip_header(&arcade_json(&h)).unwrap_err();
        assert!(err.contains("integrity"), "{err}");
        assert!(tip_header("not json").is_err());
        assert!(tip_header(r#"{"height": 1}"#).is_err());
    }
}
