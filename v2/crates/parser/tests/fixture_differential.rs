use std::fs;
use std::path::{Path, PathBuf};

use shopping_sessions_domain::{ByteSpan, ParseOutcome, ParseStatus};
use shopping_sessions_parser::{
    DiscoveredInput, ParsedBatch, discover_input, parse_clicks, parse_events, parse_fields,
    parse_links,
};

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../fixtures")
}

fn reference_rows(paths: &[PathBuf], width: usize) -> Vec<ParseOutcome<Vec<Vec<u8>>>> {
    let mut rows = Vec::new();
    for path in paths {
        let bytes = fs::read(path).unwrap();
        let mut physical = bytes.split(|byte| *byte == b'\n');
        physical.next();
        let trailing_sentinel = bytes.ends_with(b"\n");
        let count = physical.clone().count();
        for (index, line) in physical.enumerate() {
            if trailing_sentinel && index + 1 == count {
                continue;
            }
            rows.push(match parse_fields(line, width) {
                ParseOutcome::Accepted(fields) => {
                    ParseOutcome::Accepted(fields.into_iter().map(<[u8]>::to_vec).collect())
                }
                ParseOutcome::Rejected(status) => ParseOutcome::Rejected(status),
            });
        }
    }
    rows
}

fn resolved<T>(
    batch: &ParsedBatch<T>,
    spans: impl Fn(&T) -> Vec<ByteSpan>,
) -> Vec<ParseOutcome<Vec<Vec<u8>>>> {
    batch
        .rows
        .iter()
        .map(|outcome| match outcome {
            ParseOutcome::Accepted(row) => ParseOutcome::Accepted(
                spans(row)
                    .into_iter()
                    .map(|span| batch.resolve(span).unwrap().to_vec())
                    .collect(),
            ),
            ParseOutcome::Rejected(status) => ParseOutcome::Rejected(*status),
        })
        .collect()
}

#[test]
fn every_fixture_field_and_status_matches_the_reference() {
    let mut cases: Vec<_> = fs::read_dir(fixture_root())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().unwrap().is_dir())
        .collect();
    cases.sort_by_key(std::fs::DirEntry::file_name);
    assert_eq!(cases.len(), 36);

    for case in cases {
        let input = case.path().join("input");
        let discovered = discover_input(&input).unwrap();
        compare_links(&discovered, &case.file_name().to_string_lossy());
        compare_events(&discovered, &case.file_name().to_string_lossy());
        compare_clicks(&discovered, &case.file_name().to_string_lossy());
    }
}

fn compare_links(input: &DiscoveredInput, case: &str) {
    let batch = parse_links(&input.link_files).unwrap();
    assert_eq!(
        resolved(&batch, |row| vec![row.user_id_a, row.user_id_b]),
        reference_rows(&input.link_files, 2),
        "links in {case}"
    );
}

fn compare_events(input: &DiscoveredInput, case: &str) {
    let batch = parse_events(&input.event_files).unwrap();
    assert_eq!(
        resolved(&batch, |row| vec![
            row.event_id,
            row.user_id,
            row.timestamp,
            row.ingest_timestamp,
            row.event_type,
            row.product_id,
            row.price_cents,
        ]),
        reference_rows(&input.event_files, 7),
        "events in {case}"
    );
}

fn compare_clicks(input: &DiscoveredInput, case: &str) {
    let batch = parse_clicks(&input.click_files).unwrap();
    assert_eq!(
        resolved(&batch, |row| vec![
            row.click_id,
            row.user_id,
            row.campaign_id,
            row.timestamp,
        ]),
        reference_rows(&input.click_files, 4),
        "clicks in {case}"
    );
}

#[test]
fn malformed_fixture_lines_retain_specific_statuses() {
    let input = fixture_root().join("L3/input");
    let discovered = discover_input(&input).unwrap();
    let batch = parse_links(&discovered.link_files).unwrap();
    assert!(
        batch
            .rows
            .contains(&ParseOutcome::Rejected(ParseStatus::EmptyLine))
    );
    assert!(
        batch
            .rows
            .contains(&ParseOutcome::Rejected(ParseStatus::WrongFieldCount))
    );
    assert!(
        batch
            .rows
            .contains(&ParseOutcome::Rejected(ParseStatus::MisplacedQuote))
    );
}
