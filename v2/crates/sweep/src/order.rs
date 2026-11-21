use std::cell::Cell;

use shopping_sessions_domain::{CompactClickBatch, CompactEvent, PipelineError};

/// Measurements retained so a future radix implementation can be compared with
/// the initial stable comparison sort without changing the sweep interface.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SortCounters {
    pub event_records: u64,
    pub click_records: u64,
    pub event_comparisons: u64,
    pub click_comparisons: u64,
    pub equal_event_timestamps: u64,
    pub equal_click_timestamps: u64,
    pub max_events_per_person: u64,
    pub max_clicks_per_person: u64,
}

/// Index permutations over immutable reducer batches.
///
/// Click indices deliberately remain tied to their index-aligned click-ID
/// sidecar. Equal event timestamps retain their input order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OrderedRecords {
    pub event_indices: Vec<usize>,
    pub click_indices: Vec<usize>,
    pub counters: SortCounters,
}

/// Interchangeable person/time ordering strategy.
pub trait RecordOrderer {
    /// Produces person/time order without changing either reducer batch.
    ///
    /// # Errors
    ///
    /// Returns an invariant error if the click batch is internally invalid.
    fn order(
        &self,
        events: &[CompactEvent],
        clicks: &CompactClickBatch,
    ) -> Result<OrderedRecords, PipelineError>;
}

/// The correctness-first stable comparison sort.
#[derive(Clone, Copy, Debug, Default)]
pub struct StablePersonTimeOrder;

impl RecordOrderer for StablePersonTimeOrder {
    fn order(
        &self,
        events: &[CompactEvent],
        clicks: &CompactClickBatch,
    ) -> Result<OrderedRecords, PipelineError> {
        clicks.validate()?;

        let mut event_indices: Vec<usize> = (0..events.len()).collect();
        let event_comparisons = Cell::new(0_u64);
        event_indices.sort_by(|left, right| {
            event_comparisons.set(event_comparisons.get().saturating_add(1));
            let left = events[*left];
            let right = events[*right];
            (left.person, left.timestamp_ms).cmp(&(right.person, right.timestamp_ms))
        });

        let records = clicks.records();
        let mut click_indices: Vec<usize> = (0..records.len()).collect();
        let click_comparisons = Cell::new(0_u64);
        click_indices.sort_by(|left, right| {
            click_comparisons.set(click_comparisons.get().saturating_add(1));
            let left_record = records[*left];
            let right_record = records[*right];
            (left_record.person, left_record.timestamp_ms)
                .cmp(&(right_record.person, right_record.timestamp_ms))
                .then_with(|| {
                    clicks
                        .click_id(*left)
                        .unwrap_or_default()
                        .cmp(clicks.click_id(*right).unwrap_or_default())
                })
        });

        let mut counters = SortCounters {
            event_records: u64::try_from(events.len()).unwrap_or(u64::MAX),
            click_records: u64::try_from(records.len()).unwrap_or(u64::MAX),
            event_comparisons: event_comparisons.get(),
            click_comparisons: click_comparisons.get(),
            ..SortCounters::default()
        };
        measure_event_runs(events, &event_indices, &mut counters);
        measure_click_runs(records, &click_indices, &mut counters);

        Ok(OrderedRecords {
            event_indices,
            click_indices,
            counters,
        })
    }
}

fn measure_event_runs(records: &[CompactEvent], indices: &[usize], counters: &mut SortCounters) {
    let mut run = 0_u64;
    let mut previous_person = None;
    let mut previous_timestamp = None;
    for &index in indices {
        let record = records[index];
        if previous_person.is_some_and(|person| person == record.person) {
            run = run.saturating_add(1);
            if previous_timestamp == Some(record.timestamp_ms) {
                counters.equal_event_timestamps = counters.equal_event_timestamps.saturating_add(1);
            }
        } else {
            run = 1;
        }
        counters.max_events_per_person = counters.max_events_per_person.max(run);
        previous_person = Some(record.person);
        previous_timestamp = Some(record.timestamp_ms);
    }
}

fn measure_click_runs(
    records: &[shopping_sessions_domain::CompactClick],
    indices: &[usize],
    counters: &mut SortCounters,
) {
    let mut run = 0_u64;
    let mut previous_person = None;
    let mut previous_timestamp = None;
    for &index in indices {
        let record = records[index];
        if previous_person.is_some_and(|person| person == record.person) {
            run = run.saturating_add(1);
            if previous_timestamp == Some(record.timestamp_ms) {
                counters.equal_click_timestamps = counters.equal_click_timestamps.saturating_add(1);
            }
        } else {
            run = 1;
        }
        counters.max_clicks_per_person = counters.max_clicks_per_person.max(run);
        previous_person = Some(record.person);
        previous_timestamp = Some(record.timestamp_ms);
    }
}
