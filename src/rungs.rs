//! Latest courier observations, separate from the store's choice of tip.
use crate::d1::{HeaderDb, Query};
use crate::types::Chain;
use serde::{Deserialize, Serialize};
use worker::{Error, Result};

pub const DEFAULT_SPLIT_DEPTH: u32 = 3;
pub const ANCESTRY_LIMIT: u32 = 400;

// The label describes the chain followed by the rung, not its HTTP protocol.
const DEFINITIONS: [(&str, &str, &str); 3] = [
    ("woc", "reference", "WOC_IMPLEMENTATION"),
    ("arcade", "teranode", "ARCADE_IMPLEMENTATION"),
    ("bitails", "reference", "BITAILS_IMPLEMENTATION"),
];

#[derive(Debug, Clone)]
pub struct Definition {
    pub name: String,
    pub implementation: String,
}

pub fn default_implementation(name: &str) -> &str {
    DEFINITIONS
        .iter()
        .find(|d| d.0 == name)
        .map_or("peer", |d| d.1)
}

pub fn definitions(chain: &Chain, var: impl Fn(&str) -> Option<String>) -> Vec<Definition> {
    DEFINITIONS
        .iter()
        .take(if matches!(chain, Chain::Main) { 3 } else { 1 })
        .map(|&(name, label, key)| Definition {
            name: name.into(),
            implementation: var(key)
                .filter(|s| !s.trim().is_empty())
                .map_or_else(|| label.into(), |s| s.trim().into()),
        })
        .collect()
}

pub fn configured(env: &worker::Env, chain: &Chain) -> Vec<Definition> {
    definitions(chain, |key| env.var(key).ok().map(|v| v.to_string()))
}

pub fn split_depth(value: Option<&str>) -> Result<u32> {
    match value {
        None => Ok(DEFAULT_SPLIT_DEPTH),
        Some(s) => s
            .parse::<u32>()
            .ok()
            .filter(|n| (1..=ANCESTRY_LIMIT).contains(n))
            .ok_or_else(|| Error::RustError("SPLIT_DEPTH must be an integer in 1..=400".into())),
    }
}

#[derive(Debug, Clone)]
pub struct Observation {
    pub hash: String,
    pub height: u32,
    pub observed_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RungTip {
    pub name: String,
    pub implementation: String,
    pub hash: Option<String>,
    pub height: Option<u32>,
    pub chain_work: Option<String>,
    pub time: Option<u32>,
    pub observed_at: Option<u64>,
    pub is_active: bool,
}

pub async fn read(db: &impl HeaderDb) -> Result<Vec<RungTip>> {
    #[derive(Deserialize)]
    struct Row {
        payload: String,
    }
    let rows: Vec<Row> = Query::new(
        "SELECT json_object('name', name, 'implementation', implementation, 'hash', hash, 'height', height, 'chainWork', chain_work, 'time', time, 'observedAt', observed_at, 'isActive', json(CASE WHEN is_active = 1 THEN 'true' ELSE 'false' END)) AS payload FROM sync_rung_headers ORDER BY name",
    ).all(db).await?;
    rows.into_iter()
        .map(|r| serde_json::from_str(&r.payload).map_err(|e| Error::RustError(e.to_string())))
        .collect()
}

/// Include unobserved rungs and apply the current deploy's labels on reads.
pub fn for_config(definitions: &[Definition], tips: Vec<RungTip>) -> Vec<RungTip> {
    definitions
        .iter()
        .map(|d| {
            let mut tip = tips
                .iter()
                .find(|t| t.name == d.name)
                .cloned()
                .unwrap_or_else(|| RungTip {
                    name: d.name.clone(),
                    implementation: d.implementation.clone(),
                    hash: None,
                    height: None,
                    chain_work: None,
                    time: None,
                    observed_at: None,
                    is_active: false,
                });
            tip.implementation.clone_from(&d.implementation);
            tip
        })
        .collect()
}

/// One transaction replaces the bounded snapshot and journals its transitions.
/// Failed rungs retain their prior observation; late ticks cannot replace a
/// newer observation. Removed rungs cease to keep a split open.
pub async fn record(
    db: &impl HeaderDb,
    rungs: &[(Definition, Option<Observation>)],
    depth: u32,
) -> Result<()> {
    let names: Vec<_> = rungs.iter().map(|(d, _)| &d.name).collect();
    let names = serde_json::to_string(&names).map_err(|e| Error::RustError(e.to_string()))?;
    let mut batch = vec![Query::new(
        "DELETE FROM sync_rung_tips WHERE name NOT IN (SELECT value FROM json_each(?))",
    )
    .bind(names)];
    for (d, observation) in rungs {
        batch.push(Query::new(
            "INSERT INTO sync_rung_tips(name, implementation) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET implementation = excluded.implementation",
        ).bind(d.name.as_str()).bind(d.implementation.as_str()));
        if let Some(o) = observation {
            batch.push(Query::new(
                "UPDATE sync_rung_tips SET hash = ?1, height = ?2, observed_at = ?3 WHERE name = ?4 AND (observed_at IS NULL OR observed_at <= ?3)",
            ).bind(o.hash.as_str()).bind(o.height).bind(o.observed_at).bind(d.name.as_str()));
        }
    }
    batch.push(
        Query::new("UPDATE chain_split_signal SET depth = ?, tick = tick + 1 WHERE id = 1")
            .bind(depth),
    );
    db.batch(batch).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definitions_use_deploy_overrides_and_depth_is_bounded() {
        let main = definitions(&Chain::Main, |key| {
            (key == "ARCADE_IMPLEMENTATION").then(|| "peer".into())
        });
        assert_eq!(main.len(), 3);
        assert_eq!(main[1].implementation, "peer");
        assert_eq!(definitions(&Chain::Test, |_| None).len(), 1);
        assert_eq!(for_config(&main, vec![]).len(), 3);
        assert_eq!(split_depth(None).unwrap(), 3);
        assert_eq!(split_depth(Some("2")).unwrap(), 2);
        for value in ["0", "401", "-1", "invalid"] {
            assert!(split_depth(Some(value)).is_err());
        }
    }
}
