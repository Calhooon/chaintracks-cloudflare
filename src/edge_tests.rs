//! CF5, the edge half's bands, tags, keys and purge shape (the route's own
//! cache reads and writes run in the compiled-Worker check,
//! `tests/worker_events.mjs`).

use crate::edge::{self, Edge, Key};

#[test]
fn the_depth_var_is_off_unset_and_never_nearer_than_the_reorg_bound() {
    assert_eq!(Edge::parse(None), Edge::Off);
    assert_eq!(Edge::parse(Some("")), Edge::Off);
    assert_eq!(Edge::parse(Some("deep")), Edge::Off);
    assert_eq!(Edge::parse(Some("400")), Edge::Depth(400));
    assert_eq!(Edge::parse(Some(" 1000 ")), Edge::Depth(1000));
    assert_eq!(
        Edge::parse(Some("10")),
        Edge::Depth(400),
        "never inside a journaled reorg"
    );
    assert_eq!(Edge::Off.line(), 400);
}

/// Witness step 8: with the tip at 104, height 102 is within the line
/// (`no-cache` in both modes); a height more than 400 below is settled.
#[test]
fn the_bands_answer_no_cache_within_the_line_and_a_day_below_it() {
    let tip = Some(1_000);
    assert!(!edge::settled(tip, 1_000, 400));
    assert!(!edge::settled(tip, 600, 400), "exactly 400 below is within");
    assert!(edge::settled(tip, 599, 400));
    assert!(!edge::settled(tip, 1_001, 400), "above the tip");
    assert!(
        !edge::settled(None, 0, 400),
        "no served tip, nothing settled"
    );
    let v060 = Some("public, max-age=3600");
    assert_eq!(edge::control(Edge::Off, false, v060), Some("no-cache"));
    assert_eq!(edge::control(Edge::Off, true, v060), v060);
    assert_eq!(
        edge::control(Edge::Off, true, None),
        None,
        "v1 had no lifetime"
    );
    assert_eq!(
        edge::control(Edge::Depth(400), false, v060),
        Some("no-cache")
    );
    assert_eq!(
        edge::control(Edge::Depth(400), true, v060),
        Some("public, max-age=86400")
    );
}

#[test]
fn a_height_carries_its_tag_and_its_thousand_and_a_range_its_thousands() {
    assert_eq!(edge::height_tags(956_433), "h-956433,hb-956");
    assert_eq!(edge::height_tags(2), "h-2,hb-0");
    assert_eq!(edge::range_tags(1_999, 2_000), "hb-1,hb-2");
    assert_eq!(edge::range_tags(5, 900), "hb-0");
}

/// The purge's request shape: `{"tags":[...]}`, at most 100 tags a request,
/// each height's tag and each thousand's once.
#[test]
fn the_purge_names_each_tag_once_and_at_most_100_a_request() {
    let tags = edge::purge_tags(&[956_433, 956_432, 956_433, 957_001]);
    assert_eq!(
        tags,
        vec!["h-956432", "h-956433", "h-957001", "hb-956", "hb-957"]
    );
    let bodies = edge::purge_bodies(&tags);
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&bodies[0]).unwrap(),
        serde_json::json!({"tags": ["h-956432", "h-956433", "h-957001", "hb-956", "hb-957"]})
    );
    let span: Vec<u32> = (0..1_000).collect();
    let many = edge::purge_tags(&span);
    assert_eq!(many.len(), 1_001);
    let bodies = edge::purge_bodies(&many);
    assert_eq!(bodies.len(), 11);
    assert!(bodies.iter().all(|b| {
        serde_json::from_str::<serde_json::Value>(b).unwrap()["tags"]
            .as_array()
            .unwrap()
            .len()
            <= 100
    }));
    assert_eq!(
        edge::purge_url("zone"),
        "https://api.cloudflare.com/client/v4/zones/zone/purge_cache"
    );
}

#[test]
fn every_answer_has_one_canonical_key_and_a_replacement_deletes_its_own() {
    let o = "https://worker.test";
    assert_eq!(
        Key::HeightV1(2).url(o),
        "https://worker.test/__edge/v1/height/2"
    );
    assert_eq!(
        Key::HeightV2 {
            height: 2,
            bin: true
        }
        .url(o),
        "https://worker.test/__edge/v2/height/2.bin"
    );
    assert_eq!(
        Key::Root {
            height: 2,
            root: "ab".into()
        }
        .url(o),
        "https://worker.test/__edge/root/2/ab"
    );
    assert_eq!(
        Key::RangeV2 {
            start: 1,
            count: 3,
            bin: false
        }
        .url(o),
        "https://worker.test/__edge/v2/range/1/3"
    );
    let keys = edge::local_keys(o, &[2], &["AB".into()]);
    assert_eq!(
        keys,
        vec![
            "https://worker.test/__edge/v1/height/2",
            "https://worker.test/__edge/v2/height/2",
            "https://worker.test/__edge/v2/height/2.bin",
            "https://worker.test/__edge/v1/hash/AB",
            "https://worker.test/__edge/v2/hash/ab",
            "https://worker.test/__edge/v2/hash/ab.bin",
        ]
    );
}
