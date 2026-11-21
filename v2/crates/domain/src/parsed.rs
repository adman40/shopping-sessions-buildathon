use serde::{Deserialize, Serialize};

use crate::ByteSpan;

/// Exact parser rejection status retained for counters and differential tests.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParseStatus {
    EmptyLine,
    WrongFieldCount,
    MisplacedQuote,
    InvalidUtf8,
    ArenaOverflow,
}

/// A grammar-level parser result.
///
/// Accepted records may still contain empty or semantically invalid fields.
/// Deduplication and validation order belongs to reducer slices, not the parser.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParseOutcome<T> {
    Accepted(T),
    Rejected(ParseStatus),
}

/// Parsed link fields referencing the parser's immutable arena.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawLink {
    pub user_id_a: ByteSpan,
    pub user_id_b: ByteSpan,
}

/// Parsed event fields referencing the parser's immutable arena.
///
/// Every span is byte-exact after removing only legal whole-field CSV quotes.
/// Timestamps and prices remain raw so reducers can preserve dedup-before-validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawEvent {
    pub event_id: ByteSpan,
    pub user_id: ByteSpan,
    pub timestamp: ByteSpan,
    pub ingest_timestamp: ByteSpan,
    pub event_type: ByteSpan,
    pub product_id: ByteSpan,
    pub price_cents: ByteSpan,
}

/// Parsed click fields referencing the parser's immutable arena.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawClick {
    pub click_id: ByteSpan,
    pub user_id: ByteSpan,
    pub campaign_id: ByteSpan,
    pub timestamp: ByteSpan,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_rows_do_not_imply_semantic_validity() {
        let empty = ByteSpan::new(0, 0).unwrap();
        let row = RawLink {
            user_id_a: empty,
            user_id_b: empty,
        };
        assert_eq!(ParseOutcome::Accepted(row), ParseOutcome::Accepted(row));
    }

    #[test]
    fn parse_status_has_stable_json_names() {
        assert_eq!(
            serde_json::to_string(&ParseStatus::MisplacedQuote).unwrap(),
            "\"misplaced_quote\""
        );
    }
}
