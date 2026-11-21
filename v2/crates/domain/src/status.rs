use serde::{Deserialize, Serialize};

use crate::ParseStatus;

/// Canonical reason a grammar-valid or malformed input row does not survive.
///
/// Reducers retain these distinctions for telemetry and differential tests.
/// Validation remains ordered according to `rules.md`; this enum does not permit
/// a consumer to validate before the required deduplication step.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "reason", content = "parse_status")]
pub enum DropReason {
    Parse(ParseStatus),
    EmptyRequiredId,
    Duplicate,
    EmptyUserId,
    InvalidTimestamp,
    OutsideWindow,
    InvalidEventType,
    EmptyProductId,
    InvalidPrice,
    LateEvent,
    UnknownCampaign,
}

/// Final per-row disposition used by parser and reducer accounting.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RecordStatus {
    Accepted,
    Dropped(DropReason),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_preserves_parse_detail() {
        let status = RecordStatus::Dropped(DropReason::Parse(ParseStatus::MisplacedQuote));
        assert_eq!(
            status,
            RecordStatus::Dropped(DropReason::Parse(ParseStatus::MisplacedQuote))
        );
        assert_eq!(
            serde_json::to_string(&DropReason::LateEvent).unwrap(),
            "{\"reason\":\"late_event\"}"
        );
    }
}
