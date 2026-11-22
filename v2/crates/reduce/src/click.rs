//! Click deduplication, validation, and compact-row emission.
//!
//! Parsing and identity construction stay behind their slice boundaries. This
//! module deliberately asks the timestamp parser only after exact click-ID
//! deduplication, so an invalid duplicate cannot change validation order.

use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;

use shopping_sessions_domain::time::ATTRIBUTION_WINDOW_MS;
use shopping_sessions_domain::{
    ByteSpan, CampaignCode, Canonicalize, CompactClick, CompactClickBatch, DropReason,
    ParseOutcome, Phase, PipelineError, PipelineErrorKind, RawClick, RecordStatus,
};

/// Read-only exact campaign lookup used by click reduction.
pub trait ResolveCampaign {
    /// Returns the dense code for an exact, case-sensitive campaign ID.
    ///
    /// # Errors
    ///
    /// Returns an input or invariant error when the campaign state cannot be queried.
    fn resolve_campaign(&self, campaign_id: &[u8]) -> Result<Option<CampaignCode>, PipelineError>;
}

/// Exact campaign-ID index for clean campaign catalogs.
#[derive(Clone, Debug, Default)]
pub struct CampaignCatalog {
    by_id: HashMap<Box<[u8]>, CampaignCode>,
}

impl CampaignCatalog {
    /// Builds an exact campaign index.
    ///
    /// # Errors
    ///
    /// Returns an input-contract error for an empty or duplicate campaign ID.
    pub fn new(
        entries: impl IntoIterator<Item = (Vec<u8>, CampaignCode)>,
    ) -> Result<Self, PipelineError> {
        let mut by_id = HashMap::new();
        for (campaign_id, code) in entries {
            if campaign_id.is_empty() {
                return Err(click_error(
                    PipelineErrorKind::InputContract,
                    "campaign catalog contains an empty ID",
                ));
            }
            if by_id.insert(campaign_id.into_boxed_slice(), code).is_some() {
                return Err(click_error(
                    PipelineErrorKind::InputContract,
                    "campaign catalog contains a duplicate ID",
                ));
            }
        }
        Ok(Self { by_id })
    }
}

impl ResolveCampaign for CampaignCatalog {
    fn resolve_campaign(&self, campaign_id: &[u8]) -> Result<Option<CampaignCode>, PipelineError> {
        Ok(self.by_id.get(campaign_id).copied())
    }
}

/// Click reduction output with one disposition for every parser outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClickReduction {
    pub batch: CompactClickBatch,
    pub statuses: Vec<RecordStatus>,
}

/// Deduplicates and validates clicks, then emits compact rows.
///
/// `parse_timestamp` must implement the parser slice's strict F1/F2/F3 grammar.
/// It is invoked only for unique, nonempty click IDs. `arena` must remain
/// immutable for this call; accepted click IDs are copied into the returned
/// batch, which is then independent of the parser arena.
///
/// # Errors
///
/// Returns an invariant error for an invalid parser span, propagates identity
/// and campaign lookup failures, and returns a resource error if one compact
/// batch cannot address all retained click-ID bytes with `ByteSpan`.
pub fn reduce_clicks(
    rows: &[ParseOutcome<RawClick>],
    arena: &[u8],
    window_start_ms: i64,
    window_end_ms: i64,
    parse_timestamp: impl Fn(&[u8]) -> Option<i64>,
    identities: &impl Canonicalize,
    campaigns: &impl ResolveCampaign,
) -> Result<ClickReduction, PipelineError> {
    reduce_clicks_with_hasher::<RandomState>(
        rows,
        arena,
        window_start_ms,
        window_end_ms,
        parse_timestamp,
        identities,
        campaigns,
    )
}

/// Reduces parser segments while retaining one global click-ID deduplication state.
///
/// Every row's spans are resolved only against the arena paired with its segment.
///
/// # Errors
///
/// Returns the same failures as [`reduce_clicks`].
pub fn reduce_click_segments<'a>(
    segments: impl IntoIterator<Item = (&'a [u8], &'a [ParseOutcome<RawClick>])>,
    window_start_ms: i64,
    window_end_ms: i64,
    parse_timestamp: impl Fn(&[u8]) -> Option<i64>,
    identities: &impl Canonicalize,
    campaigns: &impl ResolveCampaign,
) -> Result<ClickReduction, PipelineError> {
    let rows = segments.into_iter().flat_map(|(arena, rows)| {
        rows.iter().cloned().map(move |outcome| (outcome, arena))
    });
    reduce_located_clicks_with_hasher::<RandomState>(
        rows,
        window_start_ms,
        window_end_ms,
        parse_timestamp,
        identities,
        campaigns,
    )
}

#[derive(Clone, Copy)]
struct Candidate<'a> {
    row_index: usize,
    row: RawClick,
    arena: &'a [u8],
}

#[allow(clippy::too_many_arguments)]
fn reduce_clicks_with_hasher<S: BuildHasher + Default>(
    rows: &[ParseOutcome<RawClick>],
    arena: &[u8],
    window_start_ms: i64,
    window_end_ms: i64,
    parse_timestamp: impl Fn(&[u8]) -> Option<i64>,
    identities: &impl Canonicalize,
    campaigns: &impl ResolveCampaign,
) -> Result<ClickReduction, PipelineError> {
    reduce_located_clicks_with_hasher::<S>(
        rows.iter().cloned().map(|outcome| (outcome, arena)),
        window_start_ms,
        window_end_ms,
        parse_timestamp,
        identities,
        campaigns,
    )
}

#[allow(clippy::too_many_arguments)]
fn reduce_located_clicks_with_hasher<'a, S: BuildHasher + Default>(
    rows: impl IntoIterator<Item = (ParseOutcome<RawClick>, &'a [u8])>,
    window_start_ms: i64,
    window_end_ms: i64,
    parse_timestamp: impl Fn(&[u8]) -> Option<i64>,
    identities: &impl Canonicalize,
    campaigns: &impl ResolveCampaign,
) -> Result<ClickReduction, PipelineError> {
    let click_window_start = window_start_ms
        .checked_sub(ATTRIBUTION_WINDOW_MS)
        .ok_or_else(|| {
            click_error(
                PipelineErrorKind::InputContract,
                "click window start underflows milliseconds",
            )
        })?;
    if window_end_ms < window_start_ms {
        return Err(click_error(
            PipelineErrorKind::InputContract,
            "window end precedes window start",
        ));
    }

    let mut statuses = Vec::new();
    let mut candidates = Vec::new();
    let mut seen = HashSet::<Box<[u8]>, S>::with_hasher(S::default());

    for (outcome, arena) in rows {
        let row_index = statuses.len();
        statuses.push(RecordStatus::Accepted);
        let row = match outcome {
            ParseOutcome::Accepted(row) => row,
            ParseOutcome::Rejected(parse_status) => {
                statuses[row_index] = RecordStatus::Dropped(DropReason::Parse(parse_status));
                continue;
            }
        };

        let click_id = resolve_span(row.click_id, arena, "click_id")?;
        if click_id.is_empty() {
            statuses[row_index] = RecordStatus::Dropped(DropReason::EmptyRequiredId);
            continue;
        }
        if !seen.insert(click_id.into()) {
            statuses[row_index] = RecordStatus::Dropped(DropReason::Duplicate);
            continue;
        }
        candidates.push(Candidate {
            row_index,
            row,
            arena,
        });
    }

    // Exact-ID winner state is phase-local. Validation and the returned batch
    // resolve IDs from their owning arenas, so this map can be released now.
    drop(seen);

    let mut records = Vec::with_capacity(candidates.len());
    let mut click_ids = Vec::with_capacity(candidates.len());
    let mut click_id_bytes = Vec::new();

    for candidate in candidates {
        let row = candidate.row;
        let arena = candidate.arena;
        let user_id = resolve_span(row.user_id, arena, "user_id")?;
        if user_id.is_empty() {
            statuses[candidate.row_index] = RecordStatus::Dropped(DropReason::EmptyUserId);
            continue;
        }

        let timestamp_bytes = resolve_span(row.timestamp, arena, "timestamp")?;
        let Some(timestamp_ms) = parse_timestamp(timestamp_bytes) else {
            statuses[candidate.row_index] = RecordStatus::Dropped(DropReason::InvalidTimestamp);
            continue;
        };
        if timestamp_ms < click_window_start || timestamp_ms >= window_end_ms {
            statuses[candidate.row_index] = RecordStatus::Dropped(DropReason::OutsideWindow);
            continue;
        }

        let campaign_id = resolve_span(row.campaign_id, arena, "campaign_id")?;
        let Some(campaign) = campaigns.resolve_campaign(campaign_id)? else {
            statuses[candidate.row_index] = RecordStatus::Dropped(DropReason::UnknownCampaign);
            continue;
        };
        let person = identities.canonicalize(user_id)?.code;
        let click_id = resolve_span(row.click_id, arena, "click_id")?;
        let click_id_span = append_click_id(&mut click_id_bytes, click_id)?;

        records.push(CompactClick {
            timestamp_ms,
            person,
            campaign,
        });
        click_ids.push(click_id_span);
    }

    let batch = CompactClickBatch::new(records, click_ids, click_id_bytes)?;
    Ok(ClickReduction { batch, statuses })
}

fn resolve_span<'a>(
    span: ByteSpan,
    arena: &'a [u8],
    field_name: &str,
) -> Result<&'a [u8], PipelineError> {
    span.resolve(arena).ok_or_else(|| {
        click_error(
            PipelineErrorKind::Invariant,
            format!("click {field_name} span is out of bounds"),
        )
    })
}

fn append_click_id(arena: &mut Vec<u8>, click_id: &[u8]) -> Result<ByteSpan, PipelineError> {
    let offset = u32::try_from(arena.len()).map_err(|_| {
        click_error(
            PipelineErrorKind::ResourceLimit,
            "compact click-ID arena exceeds 32-bit offsets",
        )
    })?;
    let length = u32::try_from(click_id.len()).map_err(|_| {
        click_error(
            PipelineErrorKind::ResourceLimit,
            "one click ID exceeds the 32-bit span limit",
        )
    })?;
    let span = ByteSpan::new(offset, length).ok_or_else(|| {
        click_error(
            PipelineErrorKind::ResourceLimit,
            "compact click-ID arena exceeds the 32-bit span limit",
        )
    })?;
    arena.extend_from_slice(click_id);
    Ok(span)
}

fn click_error(kind: PipelineErrorKind, message: impl Into<Box<str>>) -> PipelineError {
    PipelineError::new(kind, message).in_phase(Phase::Clicks)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::hash::{BuildHasherDefault, Hasher};

    use shopping_sessions_domain::{ByteHandle, CanonicalPerson, ParseStatus, PersonCode};

    use super::*;

    #[derive(Default)]
    struct ArenaBuilder {
        bytes: Vec<u8>,
    }

    impl ArenaBuilder {
        fn field(&mut self, value: &[u8]) -> ByteSpan {
            let offset = u32::try_from(self.bytes.len()).unwrap();
            let length = u32::try_from(value.len()).unwrap();
            self.bytes.extend_from_slice(value);
            ByteSpan::new(offset, length).unwrap()
        }

        fn click(&mut self, click_id: &[u8], user: &[u8], campaign: &[u8], ts: &[u8]) -> RawClick {
            RawClick {
                click_id: self.field(click_id),
                user_id: self.field(user),
                campaign_id: self.field(campaign),
                timestamp: self.field(ts),
            }
        }
    }

    struct FakeIdentity {
        people: HashMap<Vec<u8>, PersonCode>,
    }

    impl Canonicalize for FakeIdentity {
        fn canonicalize(&self, user_bytes: &[u8]) -> Result<CanonicalPerson, PipelineError> {
            let code = self
                .people
                .get(user_bytes)
                .copied()
                .unwrap_or(PersonCode(99));
            Ok(CanonicalPerson {
                code,
                canonical_name: ByteHandle(code.0),
            })
        }
    }

    fn identities() -> FakeIdentity {
        FakeIdentity {
            people: HashMap::from([
                (b"u1".to_vec(), PersonCode(1)),
                (b"linked-a".to_vec(), PersonCode(7)),
                (b"linked-b".to_vec(), PersonCode(7)),
            ]),
        }
    }

    fn campaigns() -> CampaignCatalog {
        CampaignCatalog::new([
            (b"c1".to_vec(), CampaignCode(11)),
            (b"c2".to_vec(), CampaignCode(12)),
        ])
        .unwrap()
    }

    fn decimal_timestamp(bytes: &[u8]) -> Option<i64> {
        let text = std::str::from_utf8(bytes).ok()?;
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        text.parse().ok()
    }

    #[derive(Default)]
    struct ZeroHasher;

    impl Hasher for ZeroHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    type CollidingState = BuildHasherDefault<ZeroHasher>;

    #[test]
    fn exact_dedup_survives_hash_collisions_and_quoted_equivalents() {
        let mut arena = ArenaBuilder::default();
        // A parser removes legal whole-field quotes, so these two source forms
        // arrive as distinct spans containing the same exact ID bytes.
        let first = arena.click(b"duplicate", b"u1", b"c1", b"100000000");
        let quoted_copy = arena.click(b"duplicate", b"u1", b"c1", b"100000000");
        let colliding_distinct = arena.click(b"distinct", b"u1", b"c1", b"100000000");
        let rows = [
            ParseOutcome::Accepted(first),
            ParseOutcome::Accepted(quoted_copy),
            ParseOutcome::Accepted(colliding_distinct),
        ];

        let parse_calls = Cell::new(0);
        let result = reduce_clicks_with_hasher::<CollidingState>(
            &rows,
            &arena.bytes,
            700_000_000,
            800_000_000,
            |bytes| {
                parse_calls.set(parse_calls.get() + 1);
                decimal_timestamp(bytes)
            },
            &identities(),
            &campaigns(),
        )
        .unwrap();

        assert_eq!(parse_calls.get(), 2, "duplicates must not be validated");
        assert_eq!(result.batch.records().len(), 2);
        assert_eq!(result.batch.click_id(0), Some(&b"duplicate"[..]));
        assert_eq!(result.batch.click_id(1), Some(&b"distinct"[..]));
        assert_eq!(
            result.statuses,
            [
                RecordStatus::Accepted,
                RecordStatus::Dropped(DropReason::Duplicate),
                RecordStatus::Accepted,
            ]
        );
    }

    #[test]
    fn segmented_arenas_share_one_global_click_deduplication_state() {
        let mut first_arena = ArenaBuilder::default();
        let first = first_arena.click(b"duplicate", b"u1", b"c1", b"100000000");
        let first_rows = [ParseOutcome::Accepted(first)];

        let mut second_arena = ArenaBuilder::default();
        let duplicate = second_arena.click(b"duplicate", b"u1", b"c1", b"100000000");
        let distinct = second_arena.click(b"distinct", b"u1", b"c2", b"100000001");
        let second_rows = [
            ParseOutcome::Accepted(duplicate),
            ParseOutcome::Accepted(distinct),
        ];

        const TINY_ARENA_LIMIT: usize = 48;
        assert!(first_arena.bytes.len() <= TINY_ARENA_LIMIT);
        assert!(second_arena.bytes.len() <= TINY_ARENA_LIMIT);
        assert!(first_arena.bytes.len() + second_arena.bytes.len() > TINY_ARENA_LIMIT);

        let parse_calls = Cell::new(0);
        let result = reduce_click_segments(
            [
                (first_arena.bytes.as_slice(), first_rows.as_slice()),
                (second_arena.bytes.as_slice(), second_rows.as_slice()),
            ],
            700_000_000,
            800_000_000,
            |bytes| {
                parse_calls.set(parse_calls.get() + 1);
                decimal_timestamp(bytes)
            },
            &identities(),
            &campaigns(),
        )
        .unwrap();

        assert_eq!(parse_calls.get(), 2);
        assert_eq!(result.batch.records().len(), 2);
        assert_eq!(result.batch.click_id(0), Some(&b"duplicate"[..]));
        assert_eq!(result.batch.click_id(1), Some(&b"distinct"[..]));
        assert_eq!(
            result.statuses,
            [
                RecordStatus::Accepted,
                RecordStatus::Dropped(DropReason::Duplicate),
                RecordStatus::Accepted,
            ]
        );
    }

    #[test]
    fn malformed_empty_and_invalid_rows_are_classified_after_dedup() {
        let mut arena = ArenaBuilder::default();
        let empty_id = arena.click(b"", b"u1", b"c1", b"1000");
        let empty_user = arena.click(b"empty-user", b"", b"c1", b"1000");
        let bad_time = arena.click(b"bad-time", b"u1", b"c1", b"not-a-time");
        let too_early = arena.click(b"too-early", b"u1", b"c1", b"95199999");
        let unknown = arena.click(b"unknown", b"u1", b"missing", b"100000000");
        let valid = arena.click(b"valid", b"u1", b"c1", b"100000000");
        let rows = [
            ParseOutcome::Rejected(ParseStatus::MisplacedQuote),
            ParseOutcome::Accepted(empty_id),
            ParseOutcome::Accepted(empty_user),
            ParseOutcome::Accepted(bad_time),
            ParseOutcome::Accepted(too_early),
            ParseOutcome::Accepted(unknown),
            ParseOutcome::Accepted(valid),
        ];

        let result = reduce_clicks(
            &rows,
            &arena.bytes,
            700_000_000,
            800_000_000,
            decimal_timestamp,
            &identities(),
            &campaigns(),
        )
        .unwrap();

        assert_eq!(result.batch.records().len(), 1);
        assert_eq!(
            result.statuses,
            [
                RecordStatus::Dropped(DropReason::Parse(ParseStatus::MisplacedQuote)),
                RecordStatus::Dropped(DropReason::EmptyRequiredId),
                RecordStatus::Dropped(DropReason::EmptyUserId),
                RecordStatus::Dropped(DropReason::InvalidTimestamp),
                RecordStatus::Dropped(DropReason::OutsideWindow),
                RecordStatus::Dropped(DropReason::UnknownCampaign),
                RecordStatus::Accepted,
            ]
        );
    }

    #[test]
    fn click_window_is_inclusive_at_start_and_exclusive_at_end() {
        let mut arena = ArenaBuilder::default();
        let at_start = arena.click(b"start", b"u1", b"c1", b"100000000");
        let before_start = arena.click(b"before", b"u1", b"c1", b"99999999");
        let before_end = arena.click(b"before-end", b"u1", b"c1", b"799999999");
        let at_end = arena.click(b"end", b"u1", b"c1", b"800000000");
        let rows = [
            ParseOutcome::Accepted(at_start),
            ParseOutcome::Accepted(before_start),
            ParseOutcome::Accepted(before_end),
            ParseOutcome::Accepted(at_end),
        ];

        let result = reduce_clicks(
            &rows,
            &arena.bytes,
            704_800_000,
            800_000_000,
            decimal_timestamp,
            &identities(),
            &campaigns(),
        )
        .unwrap();

        assert_eq!(result.batch.records().len(), 2);
        assert_eq!(result.batch.records()[0].timestamp_ms, 100_000_000);
        assert_eq!(result.batch.records()[1].timestamp_ms, 799_999_999);
        assert_eq!(
            result.statuses,
            [
                RecordStatus::Accepted,
                RecordStatus::Dropped(DropReason::OutsideWindow),
                RecordStatus::Accepted,
                RecordStatus::Dropped(DropReason::OutsideWindow),
            ]
        );
    }

    #[test]
    fn same_timestamp_ids_are_owned_and_remain_exactly_resolvable() {
        let mut arena = ArenaBuilder::default();
        let larger_id = arena.click(b"click-z", b"u1", b"c1", b"100000000");
        let smaller_id = arena.click(b"click-a", b"u1", b"c2", b"100000000");
        let rows = [
            ParseOutcome::Accepted(larger_id),
            ParseOutcome::Accepted(smaller_id),
        ];

        let result = reduce_clicks(
            &rows,
            &arena.bytes,
            700_000_000,
            800_000_000,
            decimal_timestamp,
            &identities(),
            &campaigns(),
        )
        .unwrap();
        arena.bytes.fill(b'x');

        assert_eq!(
            result.batch.records()[0].timestamp_ms,
            result.batch.records()[1].timestamp_ms
        );
        assert_eq!(result.batch.click_id(0), Some(&b"click-z"[..]));
        assert_eq!(result.batch.click_id(1), Some(&b"click-a"[..]));
        assert!(result.batch.click_id(1).unwrap() < result.batch.click_id(0).unwrap());
    }

    #[test]
    fn linked_ids_resolve_to_one_canonical_person() {
        let mut arena = ArenaBuilder::default();
        let first = arena.click(b"a", b"linked-a", b"c1", b"100000000");
        let second = arena.click(b"b", b"linked-b", b"c2", b"100000001");
        let rows = [
            ParseOutcome::Accepted(first),
            ParseOutcome::Accepted(second),
        ];

        let result = reduce_clicks(
            &rows,
            &arena.bytes,
            700_000_000,
            800_000_000,
            decimal_timestamp,
            &identities(),
            &campaigns(),
        )
        .unwrap();

        assert_eq!(result.batch.records()[0].person, PersonCode(7));
        assert_eq!(result.batch.records()[1].person, PersonCode(7));
    }

    #[test]
    fn campaign_catalog_is_exact_and_rejects_bad_catalogs() {
        let catalog = CampaignCatalog::new([(b"Case".to_vec(), CampaignCode(3))]).unwrap();
        assert_eq!(
            catalog.resolve_campaign(b"Case").unwrap(),
            Some(CampaignCode(3))
        );
        assert_eq!(catalog.resolve_campaign(b"case").unwrap(), None);
        assert!(CampaignCatalog::new([(Vec::new(), CampaignCode(1))]).is_err());
        assert!(
            CampaignCatalog::new([
                (b"same".to_vec(), CampaignCode(1)),
                (b"same".to_vec(), CampaignCode(2)),
            ])
            .is_err()
        );
    }
}
