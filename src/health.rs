//! Build identity, independent of chain storage and readiness.
pub(crate) fn health(rungs: &[crate::rungs::Definition]) -> serde_json::Value {
    // The crates the Worker carries: its own, and bsv-rs only when the lock
    // names it (a key for an absent dependency would read as drift).
    let mut crates = serde_json::Map::new();
    crates.insert(
        "rust-chaintracks".into(),
        serde_json::Value::from(env!("CARGO_PKG_VERSION")),
    );
    if let Some(version) = option_env!("CHAINTRACKS_BSV_RS") {
        crates.insert("bsv-rs".into(), serde_json::Value::from(version));
    }
    serde_json::json!({
        "service": "chaintracks",
        "status": "ok",
        "checks": { "rungSpan": rung_span(rungs) },
        "build": {
            "lib_rev": option_env!("CHAINTRACKS_LIB_REV").unwrap_or("unknown"),
            "dirty": option_env!("CHAINTRACKS_DIRTY").unwrap_or("false") == "true",
            "built_at": option_env!("CHAINTRACKS_BUILT_AT").unwrap_or("1970-01-01T00:00:00Z"),
            "crates": crates
        }
    })
}

pub(crate) fn rung_span(rungs: &[crate::rungs::Definition]) -> serde_json::Value {
    let mut labels: Vec<_> = rungs.iter().map(|r| r.implementation.as_str()).collect();
    labels.sort_unstable();
    labels.dedup();
    let spans =
        labels.contains(&"reference") && (labels.contains(&"teranode") || labels.contains(&"peer"));
    let known = labels
        .iter()
        .all(|l| matches!(*l, "reference" | "teranode" | "peer"));
    serde_json::json!({
        "critical": true,
        "status": if spans && known { "ok" } else { "warn" },
        "labels": labels,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn rung_span_has_both_allowed_spans_and_warns_on_missing_labels() {
        let check = |labels: &[&str]| {
            super::rung_span(
                &labels
                    .iter()
                    .enumerate()
                    .map(|(i, label)| crate::rungs::Definition {
                        name: i.to_string(),
                        implementation: (*label).into(),
                    })
                    .collect::<Vec<_>>(),
            )
        };
        assert_eq!(check(&["reference", "teranode"])["status"], "ok");
        assert_eq!(check(&["reference", "peer"])["status"], "ok");
        for labels in [
            &["reference"][..],
            &["peer", "teranode"][..],
            &[][..],
            &["reference", "unknown"][..],
        ] {
            let value = check(labels);
            assert_eq!(value["status"], "warn");
            assert_eq!(value["critical"], true);
            assert!(value["labels"].is_array());
        }
    }

    #[test]
    fn health_shape() {
        let value = super::health(&crate::rungs::definitions(
            &crate::types::Chain::Main,
            |_| None,
        ));
        assert_eq!(value["service"], "chaintracks");
        assert_eq!(value["status"], "ok");
        assert_eq!(value.as_object().unwrap().len(), 4);
        let build = &value["build"];
        assert_eq!(build.as_object().unwrap().len(), 4);
        let rev = build["lib_rev"].as_str().unwrap();
        assert!(
            rev == "unknown" || (rev.len() == 40 && rev.bytes().all(|b| b.is_ascii_hexdigit()))
        );
        assert!(build["dirty"].is_boolean());
        let at = build["built_at"].as_str().unwrap();
        assert!(at.ends_with('Z'));
        assert!(chrono::DateTime::parse_from_rfc3339(at).is_ok());
        let crates = build["crates"].as_object().unwrap();
        assert_eq!(crates["rust-chaintracks"], env!("CARGO_PKG_VERSION"));
        assert!(crates.len() == 1 || crates["bsv-rs"].is_string());
    }
}
