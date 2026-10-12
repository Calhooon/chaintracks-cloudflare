//! Build identity, independent of chain storage and readiness.
pub(crate) fn health() -> serde_json::Value {
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
        "build": {
            "lib_rev": option_env!("CHAINTRACKS_LIB_REV").unwrap_or("unknown"),
            "dirty": option_env!("CHAINTRACKS_DIRTY").unwrap_or("false") == "true",
            "built_at": option_env!("CHAINTRACKS_BUILT_AT").unwrap_or("1970-01-01T00:00:00Z"),
            "crates": crates
        }
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn health_shape() {
        let value = super::health();
        assert_eq!(value["service"], "chaintracks");
        assert_eq!(value["status"], "ok");
        assert_eq!(value.as_object().unwrap().len(), 3);
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
