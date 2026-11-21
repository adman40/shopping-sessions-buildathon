use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use shopping_sessions_domain::ParseOutcome;
use shopping_sessions_parser::{
    WindowClassification, discover_input, load_campaigns, load_manifest, load_products,
    parse_events,
};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "shopping-sessions-parser-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn create_layout(root: &Path) {
    for directory in ["events", "clicks", "links"] {
        fs::create_dir(root.join(directory)).unwrap();
    }
    fs::write(root.join("products.csv"), b"product_id,category\n").unwrap();
    fs::write(root.join("campaigns.csv"), b"campaign_id,channel\n").unwrap();
    fs::write(
        root.join("manifest.json"),
        br#"{"window_start":"2026-10-01T00:00:00Z","window_end":"2026-10-15T00:00:00Z"}"#,
    )
    .unwrap();
}

#[test]
fn discovery_is_sorted_direct_and_case_sensitive() {
    let directory = TestDirectory::new("discovery");
    create_layout(directory.path());
    for name in ["z.csv", "a.csv", "middle.CSV", "notes.csv.bak"] {
        fs::write(directory.path().join("events").join(name), b"header\n").unwrap();
    }
    fs::create_dir(directory.path().join("events/nested.csv")).unwrap();
    fs::write(
        directory.path().join("events/nested.csv/part.csv"),
        b"header\n",
    )
    .unwrap();

    let discovered = discover_input(directory.path()).unwrap();
    let names: Vec<_> = discovered
        .event_files
        .iter()
        .map(|path| path.file_name().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["a.csv", "z.csv"]);
}

#[test]
fn each_file_loses_exactly_its_first_physical_line() {
    let directory = TestDirectory::new("headers");
    create_layout(directory.path());
    fs::write(
        directory.path().join("events/b.csv"),
        b"not,a,validated,header,at,all,!\ne2,u2,,bad,type,,\n",
    )
    .unwrap();
    fs::write(
        directory.path().join("events/a.csv"),
        b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\ne1,u1,2026-10-01T00:00:00Z,2026-10-01T00:00:00Z,view,p1,\r\n",
    )
    .unwrap();

    let discovered = discover_input(directory.path()).unwrap();
    let parsed = parse_events(&discovered.event_files).unwrap();
    assert_eq!(parsed.rows.len(), 2);
    let ParseOutcome::Accepted(first) = parsed.rows[0] else {
        panic!("first row should be grammar-valid");
    };
    assert_eq!(parsed.resolve(first.event_id), Some(&b"e1"[..]));
    assert_eq!(parsed.resolve(first.price_cents), Some(&b"\r"[..]));

    let ParseOutcome::Accepted(second) = parsed.rows[1] else {
        panic!("semantic dirt must survive grammar parsing");
    };
    assert_eq!(parsed.resolve(second.timestamp), Some(&b""[..]));
    assert_eq!(parsed.resolve(second.ingest_timestamp), Some(&b"bad"[..]));
}

#[test]
fn catalogs_and_manifest_preserve_bytes_and_validate_contracts() {
    let directory = TestDirectory::new("metadata");
    create_layout(directory.path());
    fs::write(
        directory.path().join("products.csv"),
        b"product_id,category\n\"p1\",\" shoes \"\np2,books\r\n",
    )
    .unwrap();
    fs::write(
        directory.path().join("campaigns.csv"),
        b"campaign_id,channel\nc_1,search\n",
    )
    .unwrap();

    let products = load_products(&directory.path().join("products.csv")).unwrap();
    assert_eq!(products.entries.len(), 2);
    assert_eq!(products.resolve(products.entries[0].0), Some(&b"p1"[..]));
    assert_eq!(
        products.resolve(products.entries[0].1),
        Some(&b" shoes "[..])
    );
    assert_eq!(
        products.resolve(products.entries[1].1),
        Some(&b"books\r"[..])
    );

    let campaigns = load_campaigns(&directory.path().join("campaigns.csv")).unwrap();
    assert_eq!(campaigns.entries.len(), 1);

    let manifest = load_manifest(&directory.path().join("manifest.json")).unwrap();
    assert_eq!(
        manifest.window.classify_event(b"2026-10-01T00:00:00Z"),
        WindowClassification::InWindow(manifest.window_start_ms)
    );
    assert_eq!(
        manifest.window.classify_event(b"2026-10-15T00:00:00Z"),
        WindowClassification::OutsideWindow
    );
    assert_eq!(
        manifest.window.classify_click(b"2026-09-24T00:00:00Z"),
        WindowClassification::InWindow(manifest.window.click_start_ms())
    );
}

#[test]
fn malformed_catalog_or_manifest_fails_closed() {
    let directory = TestDirectory::new("bad-metadata");
    create_layout(directory.path());
    fs::write(
        directory.path().join("products.csv"),
        b"product_id,category\np1,books,extra\n",
    )
    .unwrap();
    assert!(load_products(&directory.path().join("products.csv")).is_err());

    fs::write(
        directory.path().join("manifest.json"),
        br#"{"window_start":"2026-10-01T00:00:00+00:00","window_end":"2026-10-15T00:00:00Z"}"#,
    )
    .unwrap();
    assert!(load_manifest(&directory.path().join("manifest.json")).is_err());
}
