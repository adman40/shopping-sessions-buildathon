#![doc = "Ordered per-person session, attribution, assist, and aggregate sweep."]
#![forbid(unsafe_code)]

mod order;

use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};

pub use order::{OrderedRecords, RecordOrderer, SortCounters, StablePersonTimeOrder};
use shopping_sessions_domain::time::{ATTRIBUTION_WINDOW_MS, DAY_MS, SESSION_GAP_MS};
use shopping_sessions_domain::{
    CampaignCode, CampaignDailyRow, CategoryCode, CompactClickBatch, CompactEventBatch, EpochDay,
    EventKind, OwnedUtf8, PersonCode, Phase, PipelineError, PipelineErrorKind, UserDailyRow,
};

/// Resolves dense codes into the byte-exact values owned by earlier slices.
pub trait DimensionLookup {
    fn person_name(&self, person: PersonCode) -> Option<&[u8]>;
    fn category_name(&self, category: CategoryCode) -> Option<&[u8]>;
    fn campaign_id(&self, campaign: CampaignCode) -> Option<&[u8]>;
    fn campaign_channel(&self, campaign: CampaignCode) -> Option<&[u8]>;
}

/// Counters from the fused scan, independent of the telemetry serialization contract.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SweepCounters {
    pub people: u64,
    pub sessions: u64,
    pub organic_sessions: u64,
    pub attributed_sessions: u64,
    pub assisted_campaign_sessions: u64,
}

/// Final deterministic rows for the output slice.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SweepOutput {
    pub user_daily: Vec<UserDailyRow>,
    pub campaign_daily: Vec<CampaignDailyRow>,
    pub sort_counters: SortCounters,
    pub sweep_counters: SweepCounters,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct UserTotals {
    sessions: i64,
    events: i64,
    revenue_cents: i64,
    category_revenue: BTreeMap<CategoryCode, i64>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CampaignKey {
    Organic,
    Paid(CampaignCode),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CampaignTotals {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

/// Aggregates private to one worker until deterministic integer reduction.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkerAggregate {
    users: BTreeMap<(PersonCode, EpochDay), UserTotals>,
    campaigns: BTreeMap<(CampaignKey, EpochDay), CampaignTotals>,
    sort_counters: SortCounters,
    sweep_counters: SweepCounters,
}

impl WorkerAggregate {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.users.is_empty() && self.campaigns.is_empty()
    }

    #[must_use]
    pub const fn sort_counters(&self) -> SortCounters {
        self.sort_counters
    }

    #[must_use]
    pub const fn sweep_counters(&self) -> SweepCounters {
        self.sweep_counters
    }
}

/// Sorts and scans one worker's complete-person partition.
///
/// A person's records must not be split across workers. Empty click-only people
/// are ignored because they cannot produce output.
///
/// # Errors
///
/// Returns an invariant error for invalid compact batches, missing payloads, or
/// arithmetic outside the output integer ranges.
pub fn sweep_worker<O: RecordOrderer>(
    events: &CompactEventBatch,
    clicks: &CompactClickBatch,
    orderer: &O,
) -> Result<WorkerAggregate, PipelineError> {
    events.validate().map_err(sweep_error)?;
    clicks.validate().map_err(sweep_error)?;
    let ordering = orderer.order(&events.records, clicks).map_err(sort_error)?;
    sweep_ordered(events, clicks, &ordering)
}

/// Scans one worker partition after its records have been ordered.
///
/// This boundary lets orchestration time ordering separately from the fused
/// session and attribution scan. The supplied ordering must come from a
/// [`RecordOrderer`] over these exact batches.
///
/// # Errors
///
/// Returns an invariant error for invalid compact batches, incomplete or
/// out-of-bounds permutations, missing payloads, or arithmetic outside the
/// output integer ranges.
pub fn sweep_ordered(
    events: &CompactEventBatch,
    clicks: &CompactClickBatch,
    ordering: &OrderedRecords,
) -> Result<WorkerAggregate, PipelineError> {
    events.validate().map_err(sweep_error)?;
    clicks.validate().map_err(sweep_error)?;
    validate_permutation(
        &ordering.event_indices,
        events.records.len(),
        "event ordering",
    )?;
    validate_permutation(
        &ordering.click_indices,
        clicks.records().len(),
        "click ordering",
    )?;
    let mut worker = WorkerAggregate {
        sort_counters: ordering.counters,
        ..WorkerAggregate::default()
    };

    let mut event_cursor = 0;
    let mut click_cursor = 0;
    while event_cursor < ordering.event_indices.len() {
        let person = events.records[ordering.event_indices[event_cursor]].person;
        let event_start = event_cursor;
        while event_cursor < ordering.event_indices.len()
            && events.records[ordering.event_indices[event_cursor]].person == person
        {
            event_cursor += 1;
        }

        while click_cursor < ordering.click_indices.len()
            && clicks.records()[ordering.click_indices[click_cursor]].person < person
        {
            click_cursor += 1;
        }
        let click_start = click_cursor;
        while click_cursor < ordering.click_indices.len()
            && clicks.records()[ordering.click_indices[click_cursor]].person == person
        {
            click_cursor += 1;
        }

        worker.sweep_counters.people = add_counter(worker.sweep_counters.people, 1)?;
        scan_person(
            events,
            clicks,
            &ordering.event_indices[event_start..event_cursor],
            &ordering.click_indices[click_start..click_cursor],
            &mut worker,
        )?;
    }
    Ok(worker)
}

fn validate_permutation(
    indices: &[usize],
    expected_length: usize,
    label: &str,
) -> Result<(), PipelineError> {
    if indices.len() != expected_length {
        return Err(invariant(format!(
            "{label} has {} indices for {expected_length} records",
            indices.len()
        )));
    }
    let mut seen = vec![false; expected_length];
    for &index in indices {
        let present = seen
            .get_mut(index)
            .ok_or_else(|| invariant(format!("{label} index {index} is out of bounds")))?;
        if *present {
            return Err(invariant(format!("{label} repeats index {index}")));
        }
        *present = true;
    }
    Ok(())
}

/// Deterministically reduces worker-private aggregates and resolves output text.
///
/// # Errors
///
/// Returns an invariant error for missing dimensions, invalid output UTF-8, or
/// integer overflow.
pub fn reduce_workers<D: DimensionLookup>(
    workers: impl IntoIterator<Item = WorkerAggregate>,
    dimensions: &D,
) -> Result<SweepOutput, PipelineError> {
    let mut combined = WorkerAggregate::default();
    for worker in workers {
        merge_worker(&mut combined, worker)?;
    }

    let mut user_daily = Vec::with_capacity(combined.users.len());
    for ((person, day), totals) in combined.users {
        let user_id = owned_dimension(
            dimensions.person_name(person),
            format!("missing name for person code {}", person.0),
        )?;
        let top_category = choose_top_category(&totals.category_revenue, dimensions)?;
        user_daily.push(UserDailyRow {
            user_id,
            day,
            sessions: totals.sessions,
            events: totals.events,
            revenue_cents: totals.revenue_cents,
            top_category,
        });
    }
    user_daily.sort_by(|left, right| {
        left.user_id
            .as_bytes()
            .cmp(right.user_id.as_bytes())
            .then(left.day.cmp(&right.day))
    });

    let mut campaign_daily = Vec::with_capacity(combined.campaigns.len());
    for ((campaign, day), totals) in combined.campaigns {
        let (campaign_id, channel) = match campaign {
            CampaignKey::Organic => (
                OwnedUtf8::from_static("organic"),
                OwnedUtf8::from_static("organic"),
            ),
            CampaignKey::Paid(code) => (
                owned_dimension(
                    dimensions.campaign_id(code),
                    format!("missing ID for campaign code {}", code.0),
                )?,
                owned_dimension(
                    dimensions.campaign_channel(code),
                    format!("missing channel for campaign code {}", code.0),
                )?,
            ),
        };
        campaign_daily.push(CampaignDailyRow {
            campaign_id,
            channel,
            day,
            sessions: totals.sessions,
            purchasing_sessions: totals.purchasing_sessions,
            revenue_cents: totals.revenue_cents,
            assisted_sessions: totals.assisted_sessions,
        });
    }
    campaign_daily.sort_by(|left, right| {
        left.campaign_id
            .as_bytes()
            .cmp(right.campaign_id.as_bytes())
            .then(left.day.cmp(&right.day))
    });

    Ok(SweepOutput {
        user_daily,
        campaign_daily,
        sort_counters: combined.sort_counters,
        sweep_counters: combined.sweep_counters,
    })
}

/// Convenience path for one in-memory worker.
///
/// # Errors
///
/// Propagates compact-batch, dimension, UTF-8, and arithmetic errors.
pub fn sweep<D: DimensionLookup>(
    events: &CompactEventBatch,
    clicks: &CompactClickBatch,
    dimensions: &D,
) -> Result<SweepOutput, PipelineError> {
    let worker = sweep_worker(events, clicks, &StablePersonTimeOrder)?;
    reduce_workers([worker], dimensions)
}

#[derive(Debug)]
struct Session {
    start_ms: i64,
    last_event_ms: i64,
    events: i64,
    revenue_cents: i64,
    category_revenue: BTreeMap<CategoryCode, i64>,
    credited: CampaignKey,
    assists: Vec<CampaignCode>,
}

fn scan_person(
    events: &CompactEventBatch,
    clicks: &CompactClickBatch,
    event_indices: &[usize],
    click_indices: &[usize],
    worker: &mut WorkerAggregate,
) -> Result<(), PipelineError> {
    let person = events.records[event_indices[0]].person;
    let mut click_window = ClickWindow::new(clicks, click_indices);
    let mut session = None;

    for &event_index in event_indices {
        let event = events.records[event_index];
        let starts_new = session.as_ref().is_none_or(|current: &Session| {
            event.timestamp_ms.saturating_sub(current.last_event_ms) > SESSION_GAP_MS
        });
        if starts_new {
            if let Some(completed) = session.take() {
                record_session(person, completed, worker)?;
            }
            let (credited, assists) = click_window.at(event.timestamp_ms)?;
            session = Some(Session {
                start_ms: event.timestamp_ms,
                last_event_ms: event.timestamp_ms,
                events: 0,
                revenue_cents: 0,
                category_revenue: BTreeMap::new(),
                credited,
                assists,
            });
        }

        let current = session
            .as_mut()
            .ok_or_else(|| invariant("session state was not initialized for an event"))?;
        current.last_event_ms = event.timestamp_ms;
        current.events = add_i64(current.events, 1, "session event count")?;
        let payload_index = usize::try_from(event.payload.0)
            .map_err(|_| invariant("event payload index does not fit usize"))?;
        let payload = events
            .payloads
            .get(payload_index)
            .ok_or_else(|| invariant("event payload index is out of bounds"))?;
        if payload.kind() == EventKind::Purchase {
            let price = i64::from(
                payload
                    .price_cents()
                    .ok_or_else(|| invariant("purchase payload has no price"))?,
            );
            let category = payload
                .category()
                .ok_or_else(|| invariant("purchase payload has no category"))?;
            current.revenue_cents = add_i64(current.revenue_cents, price, "session revenue")?;
            let category_total = current.category_revenue.entry(category).or_default();
            *category_total = add_i64(*category_total, price, "category revenue")?;
        }
    }

    if let Some(completed) = session {
        record_session(person, completed, worker)?;
    }
    Ok(())
}

struct ClickWindow<'a> {
    batch: &'a CompactClickBatch,
    ordered: &'a [usize],
    cursor: usize,
    active: VecDeque<usize>,
    campaign_counts: BTreeMap<CampaignCode, u64>,
}

impl<'a> ClickWindow<'a> {
    fn new(batch: &'a CompactClickBatch, ordered: &'a [usize]) -> Self {
        Self {
            batch,
            ordered,
            cursor: 0,
            active: VecDeque::new(),
            campaign_counts: BTreeMap::new(),
        }
    }

    fn at(
        &mut self,
        session_start: i64,
    ) -> Result<(CampaignKey, Vec<CampaignCode>), PipelineError> {
        while self.cursor < self.ordered.len() {
            let index = self.ordered[self.cursor];
            let click = self.batch.records()[index];
            if click.timestamp_ms > session_start {
                break;
            }
            self.active.push_back(index);
            let count = self.campaign_counts.entry(click.campaign).or_default();
            *count = add_counter(*count, 1)?;
            self.cursor += 1;
        }

        let cutoff = session_start.saturating_sub(ATTRIBUTION_WINDOW_MS);
        while self
            .active
            .front()
            .is_some_and(|index| self.batch.records()[*index].timestamp_ms < cutoff)
        {
            let index = self
                .active
                .pop_front()
                .ok_or_else(|| invariant("active click disappeared"))?;
            let campaign = self.batch.records()[index].campaign;
            let count = self
                .campaign_counts
                .get_mut(&campaign)
                .ok_or_else(|| invariant("active campaign count is missing"))?;
            *count = count
                .checked_sub(1)
                .ok_or_else(|| invariant("active campaign count underflowed"))?;
            if *count == 0 {
                self.campaign_counts.remove(&campaign);
            }
        }

        let credited = self.latest_click()?.map_or(CampaignKey::Organic, |index| {
            CampaignKey::Paid(self.batch.records()[index].campaign)
        });
        Ok((credited, self.campaign_counts.keys().copied().collect()))
    }

    fn latest_click(&self) -> Result<Option<usize>, PipelineError> {
        let Some(&last_index) = self.active.back() else {
            return Ok(None);
        };
        let latest_timestamp = self.batch.records()[last_index].timestamp_ms;
        let mut winner = last_index;
        for &index in self.active.iter().rev() {
            if self.batch.records()[index].timestamp_ms != latest_timestamp {
                break;
            }
            let click_id = self
                .batch
                .click_id(index)
                .ok_or_else(|| invariant("click ID sidecar is out of bounds"))?;
            let winner_id = self
                .batch
                .click_id(winner)
                .ok_or_else(|| invariant("click ID sidecar is out of bounds"))?;
            if click_id < winner_id {
                winner = index;
            }
        }
        Ok(Some(winner))
    }
}

fn record_session(
    person: PersonCode,
    session: Session,
    worker: &mut WorkerAggregate,
) -> Result<(), PipelineError> {
    let epoch_day = session.start_ms.div_euclid(DAY_MS);
    let day = EpochDay(
        i32::try_from(epoch_day).map_err(|_| invariant("session day does not fit EpochDay"))?,
    );
    let user = worker.users.entry((person, day)).or_default();
    user.sessions = add_i64(user.sessions, 1, "daily sessions")?;
    user.events = add_i64(user.events, session.events, "daily events")?;
    user.revenue_cents = add_i64(
        user.revenue_cents,
        session.revenue_cents,
        "daily user revenue",
    )?;
    for (category, revenue) in session.category_revenue {
        let total = user.category_revenue.entry(category).or_default();
        *total = add_i64(*total, revenue, "daily category revenue")?;
    }

    let campaign = worker.campaigns.entry((session.credited, day)).or_default();
    campaign.sessions = add_i64(campaign.sessions, 1, "daily credited sessions")?;
    if session.revenue_cents > 0 {
        campaign.purchasing_sessions =
            add_i64(campaign.purchasing_sessions, 1, "daily purchasing sessions")?;
    }
    campaign.revenue_cents = add_i64(
        campaign.revenue_cents,
        session.revenue_cents,
        "daily campaign revenue",
    )?;

    for assisting in session.assists {
        let assist = worker
            .campaigns
            .entry((CampaignKey::Paid(assisting), day))
            .or_default();
        assist.assisted_sessions = add_i64(assist.assisted_sessions, 1, "daily assisted sessions")?;
        worker.sweep_counters.assisted_campaign_sessions =
            add_counter(worker.sweep_counters.assisted_campaign_sessions, 1)?;
    }

    worker.sweep_counters.sessions = add_counter(worker.sweep_counters.sessions, 1)?;
    match session.credited {
        CampaignKey::Organic => {
            worker.sweep_counters.organic_sessions =
                add_counter(worker.sweep_counters.organic_sessions, 1)?;
        }
        CampaignKey::Paid(_) => {
            worker.sweep_counters.attributed_sessions =
                add_counter(worker.sweep_counters.attributed_sessions, 1)?;
        }
    }
    Ok(())
}

fn merge_worker(
    target: &mut WorkerAggregate,
    source: WorkerAggregate,
) -> Result<(), PipelineError> {
    for (key, source_totals) in source.users {
        let totals = target.users.entry(key).or_default();
        totals.sessions = add_i64(totals.sessions, source_totals.sessions, "reduced sessions")?;
        totals.events = add_i64(totals.events, source_totals.events, "reduced events")?;
        totals.revenue_cents = add_i64(
            totals.revenue_cents,
            source_totals.revenue_cents,
            "reduced user revenue",
        )?;
        for (category, revenue) in source_totals.category_revenue {
            let total = totals.category_revenue.entry(category).or_default();
            *total = add_i64(*total, revenue, "reduced category revenue")?;
        }
    }
    for (key, source_totals) in source.campaigns {
        let totals = target.campaigns.entry(key).or_default();
        totals.sessions = add_i64(
            totals.sessions,
            source_totals.sessions,
            "reduced campaign sessions",
        )?;
        totals.purchasing_sessions = add_i64(
            totals.purchasing_sessions,
            source_totals.purchasing_sessions,
            "reduced purchasing sessions",
        )?;
        totals.revenue_cents = add_i64(
            totals.revenue_cents,
            source_totals.revenue_cents,
            "reduced campaign revenue",
        )?;
        totals.assisted_sessions = add_i64(
            totals.assisted_sessions,
            source_totals.assisted_sessions,
            "reduced assisted sessions",
        )?;
    }
    merge_sort_counters(&mut target.sort_counters, source.sort_counters)?;
    merge_sweep_counters(&mut target.sweep_counters, source.sweep_counters)?;
    Ok(())
}

fn merge_sort_counters(
    target: &mut SortCounters,
    source: SortCounters,
) -> Result<(), PipelineError> {
    target.event_records = add_counter(target.event_records, source.event_records)?;
    target.click_records = add_counter(target.click_records, source.click_records)?;
    target.event_comparisons = add_counter(target.event_comparisons, source.event_comparisons)?;
    target.click_comparisons = add_counter(target.click_comparisons, source.click_comparisons)?;
    target.equal_event_timestamps =
        add_counter(target.equal_event_timestamps, source.equal_event_timestamps)?;
    target.equal_click_timestamps =
        add_counter(target.equal_click_timestamps, source.equal_click_timestamps)?;
    target.max_events_per_person = target
        .max_events_per_person
        .max(source.max_events_per_person);
    target.max_clicks_per_person = target
        .max_clicks_per_person
        .max(source.max_clicks_per_person);
    Ok(())
}

fn merge_sweep_counters(
    target: &mut SweepCounters,
    source: SweepCounters,
) -> Result<(), PipelineError> {
    target.people = add_counter(target.people, source.people)?;
    target.sessions = add_counter(target.sessions, source.sessions)?;
    target.organic_sessions = add_counter(target.organic_sessions, source.organic_sessions)?;
    target.attributed_sessions =
        add_counter(target.attributed_sessions, source.attributed_sessions)?;
    target.assisted_campaign_sessions = add_counter(
        target.assisted_campaign_sessions,
        source.assisted_campaign_sessions,
    )?;
    Ok(())
}

fn choose_top_category<D: DimensionLookup>(
    category_revenue: &BTreeMap<CategoryCode, i64>,
    dimensions: &D,
) -> Result<Option<OwnedUtf8>, PipelineError> {
    let mut winner: Option<(i64, &[u8])> = None;
    for (&category, &revenue) in category_revenue {
        let name = dimensions
            .category_name(category)
            .ok_or_else(|| invariant(format!("missing name for category code {}", category.0)))?;
        if winner.is_none_or(|(winning_revenue, winning_name)| {
            revenue > winning_revenue
                || (revenue == winning_revenue && name.cmp(winning_name) == Ordering::Less)
        }) {
            winner = Some((revenue, name));
        }
    }
    winner
        .map(|(_, name)| owned_dimension(Some(name), "category name is missing"))
        .transpose()
}

fn owned_dimension(
    bytes: Option<&[u8]>,
    missing_message: impl Into<String>,
) -> Result<OwnedUtf8, PipelineError> {
    let bytes = bytes.ok_or_else(|| invariant(missing_message.into()))?;
    OwnedUtf8::from_bytes(bytes)
        .map_err(|error| invariant(format!("output dimension is not valid UTF-8: {error}")))
}

fn add_i64(left: i64, right: i64, label: &str) -> Result<i64, PipelineError> {
    left.checked_add(right)
        .ok_or_else(|| invariant(format!("{label} overflowed i64")))
}

fn add_counter(left: u64, right: u64) -> Result<u64, PipelineError> {
    left.checked_add(right)
        .ok_or_else(|| invariant("sweep counter overflowed u64"))
}

fn invariant(message: impl Into<Box<str>>) -> PipelineError {
    PipelineError::new(PipelineErrorKind::Invariant, message).in_phase(Phase::Sweep)
}

fn sweep_error(mut error: PipelineError) -> PipelineError {
    error.phase = Some(Phase::Sweep);
    error
}

fn sort_error(mut error: PipelineError) -> PipelineError {
    error.phase = Some(Phase::Sort);
    error
}
