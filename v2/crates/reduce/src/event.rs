//! Event deduplication, validation, and compact emission.
//!
//! This module deliberately depends only on frozen domain contracts. Parser,
//! identity, and catalog implementations satisfy the narrow read-only traits
//! below when the V2 slices are integrated.

use std::collections::HashMap;
use std::collections::hash_map::{Entry, RandomState};
use std::hash::BuildHasher;

use shopping_sessions_domain::time::LATE_EVENT_MS;
use shopping_sessions_domain::{
    ByteSpan, Canonicalize, CategoryCode, CompactEvent, CompactEventBatch, DropReason, EventKind,
    EventPayload, EventPayloadCode, ParseOutcome, PipelineError, PipelineErrorKind, ProductCode,
    RawEvent,
};

/// Event-window bounds in epoch milliseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventWindow {
    start_ms: i64,
    end_ms: i64,
}

impl EventWindow {
    /// Creates a nonempty half-open event window.
    ///
    /// # Errors
    ///
    /// Returns an input-contract error when `start_ms >= end_ms`.
    pub fn new(start_ms: i64, end_ms: i64) -> Result<Self, PipelineError> {
        if start_ms >= end_ms {
            return Err(PipelineError::new(
                PipelineErrorKind::InputContract,
                "event window start must precede its end",
            ));
        }
        Ok(Self { start_ms, end_ms })
    }

    #[must_use]
    pub const fn contains(self, timestamp_ms: i64) -> bool {
        timestamp_ms >= self.start_ms && timestamp_ms < self.end_ms
    }
}

/// Timestamp parsing boundary supplied by the parser slice.
pub trait TimestampParser {
    /// Parses an exact byte field into UTC epoch milliseconds.
    fn parse_timestamp(&self, bytes: &[u8]) -> Option<i64>;
}

/// Product and category resolution returned by the catalog implementation.
///
/// An unknown nonempty product has `product: None` and the catalog's dense
/// `unknown` category code. Known products carry both dense codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedProduct {
    pub product: Option<ProductCode>,
    pub category: CategoryCode,
}

/// Read-only product catalog boundary used by event reduction.
pub trait ProductCatalog {
    /// Resolves a nonempty, byte-exact product ID.
    fn resolve_product(&self, product_id: &[u8]) -> ResolvedProduct;
}

/// Reconciled event-reduction counts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventCounters {
    pub input_rows: u64,
    pub malformed_rows: u64,
    pub empty_event_id_rows: u64,
    pub duplicate_rows: u64,
    pub winning_rows: u64,
    pub dropped_winners: u64,
    pub output_rows: u64,
}

impl EventCounters {
    /// Checks that every input row has exactly one terminal disposition.
    #[must_use]
    pub const fn reconciles(self) -> bool {
        self.input_rows
            == self.malformed_rows
                + self.empty_event_id_rows
                + self.duplicate_rows
                + self.dropped_winners
                + self.output_rows
            && self.winning_rows == self.dropped_winners + self.output_rows
    }
}

/// Compact events plus exact post-parse drop accounting.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EventReduction {
    pub batch: CompactEventBatch,
    pub counters: EventCounters,
    drop_reasons: HashMap<DropReason, u64>,
}

impl EventReduction {
    /// Number of rows dropped for one exact reason.
    #[must_use]
    pub fn dropped_for(&self, reason: DropReason) -> u64 {
        self.drop_reasons.get(&reason).copied().unwrap_or(0)
    }

    /// Exact reason counts, including parser rejections and duplicates.
    #[must_use]
    pub fn drop_reasons(&self) -> &HashMap<DropReason, u64> {
        &self.drop_reasons
    }
}

#[derive(Clone, Copy, Debug)]
struct Winner {
    row: RawEvent,
    ingest_ms: i64,
}

#[derive(Clone, Copy, Debug)]
struct PendingEvent {
    timestamp_ms: i64,
    person: shopping_sessions_domain::PersonCode,
    payload: EventPayload,
    event_id: ByteSpan,
}

/// Selects exact event-ID winners, validates them, resolves dimensions, and
/// returns compact events sorted by `(person_code, timestamp_ms)`.
///
/// Grammar-rejected rows and rows with empty event IDs are discarded before
/// winner selection. All other semantic validation occurs only after the
/// minimum-ingest winner for each exact event ID is known.
///
/// # Errors
///
/// Returns an input-contract error for invalid parser-owned spans or an invalid
/// `ingest_ts`, an identity error from `Canonicalize`, or a resource error if a
/// compact payload index would exceed `u32`.
pub fn reduce_events<T, I, P>(
    rows: impl IntoIterator<Item = ParseOutcome<RawEvent>>,
    arena: &[u8],
    timestamps: &T,
    identity: &I,
    products: &P,
    window: EventWindow,
) -> Result<EventReduction, PipelineError>
where
    T: TimestampParser,
    I: Canonicalize,
    P: ProductCatalog,
{
    reduce_events_with_hasher(
        rows,
        arena,
        timestamps,
        identity,
        products,
        window,
        RandomState::new(),
    )
}

fn reduce_events_with_hasher<T, I, P, S>(
    rows: impl IntoIterator<Item = ParseOutcome<RawEvent>>,
    arena: &[u8],
    timestamps: &T,
    identity: &I,
    products: &P,
    window: EventWindow,
    hasher: S,
) -> Result<EventReduction, PipelineError>
where
    T: TimestampParser,
    I: Canonicalize,
    P: ProductCatalog,
    S: BuildHasher,
{
    let mut counters = EventCounters::default();
    let mut drop_reasons = HashMap::new();
    let mut winners: HashMap<Box<[u8]>, Winner, S> = HashMap::with_hasher(hasher);

    for outcome in rows {
        counters.input_rows += 1;
        let row = match outcome {
            ParseOutcome::Rejected(status) => {
                counters.malformed_rows += 1;
                increment_reason(&mut drop_reasons, DropReason::Parse(status));
                continue;
            }
            ParseOutcome::Accepted(row) => row,
        };

        let event_id = resolve(row.event_id, arena, "event_id")?;
        if event_id.is_empty() {
            counters.empty_event_id_rows += 1;
            increment_reason(&mut drop_reasons, DropReason::EmptyRequiredId);
            continue;
        }
        let ingest_bytes = resolve(row.ingest_timestamp, arena, "ingest_ts")?;
        let ingest_ms = timestamps.parse_timestamp(ingest_bytes).ok_or_else(|| {
            PipelineError::new(
                PipelineErrorKind::InputContract,
                "parser supplied an invalid event ingest_ts",
            )
        })?;
        let winner = Winner { row, ingest_ms };

        match winners.entry(event_id.into()) {
            Entry::Vacant(entry) => {
                entry.insert(winner);
            }
            Entry::Occupied(mut entry) => {
                counters.duplicate_rows += 1;
                increment_reason(&mut drop_reasons, DropReason::Duplicate);
                if ingest_ms < entry.get().ingest_ms {
                    entry.insert(winner);
                }
            }
        }
    }

    counters.winning_rows = u64::try_from(winners.len()).unwrap_or(u64::MAX);

    // Consuming the map drops its owned event-ID keys and hash buckets before
    // compact records are sorted. Winner rows retain only parser-arena spans.
    let winner_rows: Vec<Winner> = winners.into_values().collect();
    let mut pending = Vec::with_capacity(winner_rows.len());

    for winner in winner_rows {
        match validate_winner(winner, arena, timestamps, identity, products, window)? {
            Ok(event) => pending.push(event),
            Err(reason) => {
                counters.dropped_winners += 1;
                increment_reason(&mut drop_reasons, reason);
            }
        }
    }

    pending.sort_by(|left, right| {
        (left.person, left.timestamp_ms)
            .cmp(&(right.person, right.timestamp_ms))
            .then_with(|| {
                let left_id = left.event_id.resolve(arena).unwrap_or_default();
                let right_id = right.event_id.resolve(arena).unwrap_or_default();
                left_id.cmp(right_id)
            })
    });

    let mut batch = CompactEventBatch {
        records: Vec::with_capacity(pending.len()),
        payloads: Vec::with_capacity(pending.len()),
    };
    for event in pending {
        let payload_index = u32::try_from(batch.payloads.len()).map_err(|_| {
            PipelineError::new(
                PipelineErrorKind::ResourceLimit,
                "compact event payload table exceeds u32 indexes",
            )
        })?;
        batch.payloads.push(event.payload);
        batch.records.push(CompactEvent {
            timestamp_ms: event.timestamp_ms,
            person: event.person,
            payload: EventPayloadCode(payload_index),
        });
    }
    batch.validate()?;
    counters.output_rows = u64::try_from(batch.records.len()).unwrap_or(u64::MAX);
    debug_assert!(counters.reconciles());

    Ok(EventReduction {
        batch,
        counters,
        drop_reasons,
    })
}

fn validate_winner<T, I, P>(
    winner: Winner,
    arena: &[u8],
    timestamps: &T,
    identity: &I,
    products: &P,
    window: EventWindow,
) -> Result<Result<PendingEvent, DropReason>, PipelineError>
where
    T: TimestampParser,
    I: Canonicalize,
    P: ProductCatalog,
{
    let row = winner.row;
    let user_id = resolve(row.user_id, arena, "user_id")?;
    if user_id.is_empty() {
        return Ok(Err(DropReason::EmptyUserId));
    }

    let timestamp_bytes = resolve(row.timestamp, arena, "ts")?;
    let Some(timestamp_ms) = timestamps.parse_timestamp(timestamp_bytes) else {
        return Ok(Err(DropReason::InvalidTimestamp));
    };
    if !window.contains(timestamp_ms) {
        return Ok(Err(DropReason::OutsideWindow));
    }

    let event_type = resolve(row.event_type, arena, "event_type")?;
    let kind = match event_type {
        b"view" => EventKind::View,
        b"cart" => EventKind::Cart,
        b"purchase" => EventKind::Purchase,
        _ => return Ok(Err(DropReason::InvalidEventType)),
    };

    let product_id = resolve(row.product_id, arena, "product_id")?;
    if product_id.is_empty() {
        return Ok(Err(DropReason::EmptyProductId));
    }
    let resolved_product = products.resolve_product(product_id);

    let payload = if kind == EventKind::Purchase {
        let price_bytes = resolve(row.price_cents, arena, "price_cents")?;
        let Some(price_cents) = parse_price(price_bytes) else {
            return Ok(Err(DropReason::InvalidPrice));
        };
        EventPayload::purchase(resolved_product.category, price_cents).ok_or_else(|| {
            PipelineError::new(PipelineErrorKind::Invariant, "validated price rejected")
        })?
    } else {
        EventPayload::non_purchase(kind).ok_or_else(|| {
            PipelineError::new(
                PipelineErrorKind::Invariant,
                "non-purchase payload rejected",
            )
        })?
    };

    if winner.ingest_ms.saturating_sub(timestamp_ms) > LATE_EVENT_MS {
        return Ok(Err(DropReason::LateEvent));
    }

    let person = identity.canonicalize(user_id)?.code;
    Ok(Ok(PendingEvent {
        timestamp_ms,
        person,
        payload,
        event_id: row.event_id,
    }))
}

fn resolve<'a>(span: ByteSpan, arena: &'a [u8], field: &str) -> Result<&'a [u8], PipelineError> {
    span.resolve(arena).ok_or_else(|| {
        PipelineError::new(
            PipelineErrorKind::InputContract,
            format!("event {field} span is outside its parser arena"),
        )
    })
}

fn parse_price(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut value = 0_u32;
    for &digit in bytes {
        value = value
            .checked_mul(10)?
            .checked_add(u32::from(digit - b'0'))?;
    }
    (1..=10_000_000).contains(&value).then_some(value)
}

fn increment_reason(reasons: &mut HashMap<DropReason, u64>, reason: DropReason) {
    let count = reasons.entry(reason).or_default();
    *count += 1;
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{BuildHasherDefault, Hasher};

    use shopping_sessions_domain::{ByteHandle, CanonicalPerson, ParseStatus, PersonCode};

    use super::*;

    #[derive(Default)]
    struct Arena {
        bytes: Vec<u8>,
    }

    impl Arena {
        fn field(&mut self, bytes: &[u8]) -> ByteSpan {
            let offset = u32::try_from(self.bytes.len()).unwrap();
            let length = u32::try_from(bytes.len()).unwrap();
            self.bytes.extend_from_slice(bytes);
            ByteSpan::new(offset, length).unwrap()
        }

        #[allow(clippy::too_many_arguments)]
        fn row(
            &mut self,
            event_id: &[u8],
            user_id: &[u8],
            timestamp: &[u8],
            ingest: &[u8],
            event_type: &[u8],
            product_id: &[u8],
            price: &[u8],
        ) -> RawEvent {
            RawEvent {
                event_id: self.field(event_id),
                user_id: self.field(user_id),
                timestamp: self.field(timestamp),
                ingest_timestamp: self.field(ingest),
                event_type: self.field(event_type),
                product_id: self.field(product_id),
                price_cents: self.field(price),
            }
        }
    }

    struct ValidationSpec<'a> {
        event_id: &'a [u8],
        user: &'a [u8],
        timestamp: &'a [u8],
        kind: &'a [u8],
        product: &'a [u8],
        price: &'a [u8],
    }

    impl<'a> ValidationSpec<'a> {
        fn new(
            event_id: &'a [u8],
            user: &'a [u8],
            timestamp: &'a [u8],
            kind: &'a [u8],
            product: &'a [u8],
            price: &'a [u8],
        ) -> Self {
            Self {
                event_id,
                user,
                timestamp,
                kind,
                product,
                price,
            }
        }
    }

    fn validation_rows(arena: &mut Arena) -> Vec<ParseOutcome<RawEvent>> {
        let specifications = [
            ValidationSpec::new(b"empty-user", b"", b"2000", b"view", b"p1", b""),
            ValidationSpec::new(b"bad-ts", b"u1", b"bad", b"view", b"p1", b""),
            ValidationSpec::new(b"before", b"u1", b"999", b"view", b"p1", b""),
            ValidationSpec::new(b"at-end", b"u1", b"10000", b"view", b"p1", b""),
            ValidationSpec::new(b"bad-type", b"u1", b"2000", b"Purchase", b"p1", b"100"),
            ValidationSpec::new(b"empty-product", b"u1", b"2000", b"view", b"", b""),
            ValidationSpec::new(b"decimal", b"u1", b"2000", b"purchase", b"p1", b"19.99"),
            ValidationSpec::new(b"zero", b"u1", b"2000", b"purchase", b"p1", b"0"),
            ValidationSpec::new(b"too-high", b"u1", b"2000", b"purchase", b"p1", b"10000001"),
            ValidationSpec::new(b"signed", b"u1", b"2000", b"purchase", b"p1", b"-5"),
            ValidationSpec::new(b"empty-price", b"u1", b"2000", b"purchase", b"p1", b""),
            ValidationSpec::new(
                b"unicode-price",
                b"u1",
                b"2000",
                b"purchase",
                b"p1",
                "２５０".as_bytes(),
            ),
            ValidationSpec::new(
                b"overflow-price",
                b"u1",
                b"2000",
                b"purchase",
                b"p1",
                b"99999999999999999999",
            ),
        ];
        specifications
            .into_iter()
            .map(|specification| {
                ParseOutcome::Accepted(arena.row(
                    specification.event_id,
                    specification.user,
                    specification.timestamp,
                    b"2100",
                    specification.kind,
                    specification.product,
                    specification.price,
                ))
            })
            .collect()
    }

    struct IntegerTimestamps;

    impl TimestampParser for IntegerTimestamps {
        fn parse_timestamp(&self, bytes: &[u8]) -> Option<i64> {
            if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
                return None;
            }
            bytes.iter().try_fold(0_i64, |value, digit| {
                value.checked_mul(10)?.checked_add(i64::from(digit - b'0'))
            })
        }
    }

    struct ExactIdentity;

    impl Canonicalize for ExactIdentity {
        fn canonicalize(&self, user_bytes: &[u8]) -> Result<CanonicalPerson, PipelineError> {
            let code = match user_bytes {
                b"U1" => 1,
                b"u1" => 2,
                b"u2" => 3,
                b" u1" => 4,
                b"u1 " => 5,
                _ => 99,
            };
            Ok(CanonicalPerson {
                code: PersonCode(code),
                canonical_name: ByteHandle(code),
            })
        }
    }

    struct TestProducts;

    impl ProductCatalog for TestProducts {
        fn resolve_product(&self, product_id: &[u8]) -> ResolvedProduct {
            match product_id {
                b"p1" => ResolvedProduct {
                    product: Some(ProductCode(1)),
                    category: CategoryCode(10),
                },
                b"p2" => ResolvedProduct {
                    product: Some(ProductCode(2)),
                    category: CategoryCode(20),
                },
                _ => ResolvedProduct {
                    product: None,
                    category: CategoryCode(99),
                },
            }
        }
    }

    fn window() -> EventWindow {
        EventWindow::new(1_000, 10_000).unwrap()
    }

    fn reduce(arena: &Arena, rows: Vec<ParseOutcome<RawEvent>>) -> EventReduction {
        reduce_events(
            rows,
            &arena.bytes,
            &IntegerTimestamps,
            &ExactIdentity,
            &TestProducts,
            window(),
        )
        .unwrap()
    }

    fn payloads(reduction: &EventReduction) -> Vec<EventPayload> {
        reduction
            .batch
            .records
            .iter()
            .map(|record| reduction.batch.payloads[record.payload.0 as usize])
            .collect()
    }

    #[test]
    fn malformed_copy_is_removed_before_dedup_and_invalid_earliest_winner_is_not_rescued() {
        let mut arena = Arena::default();
        let valid_after_malformed = arena.row(b"e1", b"u1", b"2000", b"2100", b"view", b"p1", b"");
        let later_valid = arena.row(b"e2", b"u2", b"2000", b"2500", b"view", b"p1", b"");
        let earliest_invalid = arena.row(b"e2", b"u1", b"bad", b"2100", b"view", b"p1", b"");
        let rows = vec![
            ParseOutcome::Rejected(ParseStatus::WrongFieldCount),
            ParseOutcome::Accepted(valid_after_malformed),
            ParseOutcome::Accepted(later_valid),
            ParseOutcome::Accepted(earliest_invalid),
        ];

        let result = reduce(&arena, rows);
        assert_eq!(result.batch.records.len(), 1);
        assert_eq!(result.batch.records[0].person, PersonCode(2));
        assert_eq!(result.dropped_for(DropReason::InvalidTimestamp), 1);
        assert_eq!(result.counters.malformed_rows, 1);
        assert_eq!(result.counters.duplicate_rows, 1);
        assert!(result.counters.reconciles());
    }

    #[derive(Default)]
    struct AlwaysCollideHasher;

    impl Hasher for AlwaysCollideHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    #[test]
    fn hash_collisions_do_not_merge_distinct_exact_event_ids() {
        let mut arena = Arena::default();
        let first = arena.row(b"different-a", b"u1", b"2000", b"2100", b"view", b"p1", b"");
        let second = arena.row(b"different-b", b"u2", b"2000", b"2100", b"cart", b"p1", b"");
        let result = reduce_events_with_hasher(
            [
                ParseOutcome::Accepted(first),
                ParseOutcome::Accepted(second),
            ],
            &arena.bytes,
            &IntegerTimestamps,
            &ExactIdentity,
            &TestProducts,
            window(),
            BuildHasherDefault::<AlwaysCollideHasher>::default(),
        )
        .unwrap();

        assert_eq!(result.batch.records.len(), 2);
        assert_eq!(result.counters.duplicate_rows, 0);
        assert_eq!(result.batch.records[0].person, PersonCode(2));
        assert_eq!(result.batch.records[1].person, PersonCode(3));
    }

    #[test]
    fn minimum_ingest_wins_across_changed_users_and_equal_ingest_is_a_duplicate() {
        let mut arena = Arena::default();
        let later_changed_user = arena.row(b"e1", b"u2", b"2000", b"2300", b"view", b"p1", b"");
        let earliest = arena.row(b"e1", b"u1", b"2000", b"2100", b"view", b"p1", b"");
        let equal_copy = arena.row(b"e1", b"u1", b"2000", b"2100", b"view", b"p1", b"");
        let result = reduce(
            &arena,
            vec![
                ParseOutcome::Accepted(later_changed_user),
                ParseOutcome::Accepted(earliest),
                ParseOutcome::Accepted(equal_copy),
            ],
        );

        assert_eq!(result.batch.records.len(), 1);
        assert_eq!(result.batch.records[0].person, PersonCode(2));
        assert_eq!(result.counters.duplicate_rows, 2);
        assert_eq!(result.dropped_for(DropReason::Duplicate), 2);
    }

    #[test]
    fn winner_validation_covers_required_fields_types_prices_and_window_edges() {
        let mut arena = Arena::default();
        let mut rows = validation_rows(&mut arena);
        let start = arena.row(b"at-start", b"u1", b"1000", b"1000", b"view", b"p1", b"-5");
        let last = arena.row(b"last", b"u1", b"9999", b"9999", b"cart", b"p2", b"garbage");
        let leading_zero_price = arena.row(
            b"leading-zero",
            b"u1",
            b"2000",
            b"2100",
            b"purchase",
            b"p1",
            b"0250",
        );
        rows.extend([
            ParseOutcome::Accepted(start),
            ParseOutcome::Accepted(last),
            ParseOutcome::Accepted(leading_zero_price),
        ]);

        let result = reduce(&arena, rows);
        assert_eq!(result.batch.records.len(), 3);
        assert_eq!(result.dropped_for(DropReason::EmptyUserId), 1);
        assert_eq!(result.dropped_for(DropReason::InvalidTimestamp), 1);
        assert_eq!(result.dropped_for(DropReason::OutsideWindow), 2);
        assert_eq!(result.dropped_for(DropReason::InvalidEventType), 1);
        assert_eq!(result.dropped_for(DropReason::EmptyProductId), 1);
        assert_eq!(result.dropped_for(DropReason::InvalidPrice), 7);
        assert_eq!(payloads(&result)[1].price_cents(), Some(250));
        assert!(result.counters.reconciles());
    }

    #[test]
    fn late_cutoff_is_strict_and_negative_lateness_is_kept() {
        let mut arena = Arena::default();
        let exactly = arena.row(
            b"exactly-24h",
            b"u1",
            b"2000",
            b"86402000",
            b"view",
            b"p1",
            b"",
        );
        let over = arena.row(
            b"over-24h",
            b"u1",
            b"2000",
            b"86402001",
            b"view",
            b"p1",
            b"",
        );
        let before_event = arena.row(
            b"before-event",
            b"u1",
            b"3000",
            b"2000",
            b"view",
            b"p1",
            b"",
        );
        let result = reduce(
            &arena,
            vec![
                ParseOutcome::Accepted(exactly),
                ParseOutcome::Accepted(over),
                ParseOutcome::Accepted(before_event),
            ],
        );

        assert_eq!(result.batch.records.len(), 2);
        assert_eq!(result.dropped_for(DropReason::LateEvent), 1);
    }

    #[test]
    fn products_categories_prices_and_exact_user_bytes_reach_compact_payloads() {
        let mut arena = Arena::default();
        let known = arena.row(
            b"known",
            b"U1",
            b"2000",
            b"2100",
            b"purchase",
            b"p1",
            b"10000000",
        );
        let unknown = arena.row(
            b"unknown",
            b" u1",
            b"2000",
            b"2100",
            b"purchase",
            b"p9",
            b"2000",
        );
        let spaced_product = arena.row(
            b"spaced",
            b"u1 ",
            b"2000",
            b"2100",
            b"purchase",
            b" p1",
            b"1500",
        );
        let result = reduce(
            &arena,
            vec![
                ParseOutcome::Accepted(unknown),
                ParseOutcome::Accepted(spaced_product),
                ParseOutcome::Accepted(known),
            ],
        );

        assert_eq!(
            result
                .batch
                .records
                .iter()
                .map(|record| record.person)
                .collect::<Vec<_>>(),
            vec![PersonCode(1), PersonCode(4), PersonCode(5)]
        );
        let payloads = payloads(&result);
        assert_eq!(payloads[0].category(), Some(CategoryCode(10)));
        assert_eq!(payloads[0].price_cents(), Some(10_000_000));
        assert_eq!(payloads[1].category(), Some(CategoryCode(99)));
        assert_eq!(payloads[2].category(), Some(CategoryCode(99)));
    }

    #[test]
    fn compact_rows_are_deterministically_sorted_with_equal_timestamp_ties() {
        let mut arena = Arena::default();
        let z = arena.row(b"z", b"u1", b"2000", b"2100", b"cart", b"p1", b"");
        let a = arena.row(b"a", b"u1", b"2000", b"2100", b"view", b"p1", b"");
        let earlier_other_person = arena.row(b"x", b"U1", b"3000", b"3100", b"view", b"p1", b"");
        let result = reduce(
            &arena,
            vec![
                ParseOutcome::Accepted(z),
                ParseOutcome::Accepted(earlier_other_person),
                ParseOutcome::Accepted(a),
            ],
        );

        assert_eq!(std::mem::size_of::<CompactEvent>(), 16);
        assert_eq!(
            result
                .batch
                .records
                .iter()
                .map(|record| (record.person, record.timestamp_ms))
                .collect::<Vec<_>>(),
            vec![
                (PersonCode(1), 3000),
                (PersonCode(2), 2000),
                (PersonCode(2), 2000)
            ]
        );
        let payloads = payloads(&result);
        assert_eq!(payloads[1].kind(), EventKind::View);
        assert_eq!(payloads[2].kind(), EventKind::Cart);
    }

    #[test]
    fn empty_ids_and_every_parser_rejection_reconcile_without_entering_winner_state() {
        let mut arena = Arena::default();
        let empty = arena.row(b"", b"u1", b"2000", b"2100", b"view", b"p1", b"");
        let statuses = [
            ParseStatus::EmptyLine,
            ParseStatus::WrongFieldCount,
            ParseStatus::MisplacedQuote,
            ParseStatus::InvalidUtf8,
            ParseStatus::ArenaOverflow,
        ];
        let mut rows = statuses
            .into_iter()
            .map(ParseOutcome::Rejected)
            .collect::<Vec<_>>();
        rows.push(ParseOutcome::Accepted(empty));

        let result = reduce(&arena, rows);
        assert!(result.batch.records.is_empty());
        assert_eq!(result.counters.input_rows, 6);
        assert_eq!(result.counters.malformed_rows, 5);
        assert_eq!(result.counters.empty_event_id_rows, 1);
        assert_eq!(result.counters.winning_rows, 0);
        assert!(result.counters.reconciles());
    }

    #[test]
    fn invalid_window_and_invalid_ingest_fail_closed() {
        assert!(EventWindow::new(1, 1).is_err());

        let mut arena = Arena::default();
        let row = arena.row(b"e1", b"u1", b"2000", b"bad", b"view", b"p1", b"");
        let error = reduce_events(
            [ParseOutcome::Accepted(row)],
            &arena.bytes,
            &IntegerTimestamps,
            &ExactIdentity,
            &TestProducts,
            window(),
        )
        .unwrap_err();
        assert_eq!(error.kind, PipelineErrorKind::InputContract);
    }

    #[test]
    fn invalid_arena_spans_fail_closed() {
        let span = ByteSpan::new(100, 1).unwrap();
        let row = RawEvent {
            event_id: span,
            user_id: span,
            timestamp: span,
            ingest_timestamp: span,
            event_type: span,
            product_id: span,
            price_cents: span,
        };
        let error = reduce_events(
            [ParseOutcome::Accepted(row)],
            b"short",
            &IntegerTimestamps,
            &ExactIdentity,
            &TestProducts,
            window(),
        )
        .unwrap_err();
        assert_eq!(error.kind, PipelineErrorKind::InputContract);
    }

    #[test]
    fn standard_hasher_still_builds_normally() {
        let _: BuildHasherDefault<DefaultHasher> = BuildHasherDefault::default();
    }
}
