use std::collections::BTreeMap;

use shopping_sessions_domain::time::{ATTRIBUTION_WINDOW_MS, DAY_MS, SESSION_GAP_MS};
use shopping_sessions_domain::{
    ByteSpan, CampaignCode, CategoryCode, CompactClick, CompactClickBatch, CompactEvent,
    CompactEventBatch, EpochDay, EventKind, EventPayload, EventPayloadCode, OwnedUtf8, PersonCode,
};
use shopping_sessions_sweep::{
    DimensionLookup, OrderedRecords, RecordOrderer, StablePersonTimeOrder, reduce_workers, sweep,
    sweep_ordered, sweep_worker,
};

#[derive(Default)]
struct Dimensions {
    people: BTreeMap<PersonCode, &'static [u8]>,
    categories: BTreeMap<CategoryCode, &'static [u8]>,
    campaign_ids: BTreeMap<CampaignCode, &'static [u8]>,
    campaign_channels: BTreeMap<CampaignCode, &'static [u8]>,
}

impl DimensionLookup for Dimensions {
    fn person_name(&self, person: PersonCode) -> Option<&[u8]> {
        self.people.get(&person).copied()
    }

    fn category_name(&self, category: CategoryCode) -> Option<&[u8]> {
        self.categories.get(&category).copied()
    }

    fn campaign_id(&self, campaign: CampaignCode) -> Option<&[u8]> {
        self.campaign_ids.get(&campaign).copied()
    }

    fn campaign_channel(&self, campaign: CampaignCode) -> Option<&[u8]> {
        self.campaign_channels.get(&campaign).copied()
    }
}

fn dimensions() -> Dimensions {
    Dimensions {
        people: BTreeMap::from([
            (PersonCode(0), &b"u0"[..]),
            (PersonCode(1), &b"u1"[..]),
            (PersonCode(7), &b"U7"[..]),
        ]),
        categories: BTreeMap::from([
            (CategoryCode(0), &b"shoes"[..]),
            (CategoryCode(1), &b"books"[..]),
        ]),
        campaign_ids: BTreeMap::from([
            (CampaignCode(0), &b"c_1"[..]),
            (CampaignCode(1), &b"c_2"[..]),
            (CampaignCode(2), &b"c_3"[..]),
        ]),
        campaign_channels: BTreeMap::from([
            (CampaignCode(0), &b"search"[..]),
            (CampaignCode(1), &b"social"[..]),
            (CampaignCode(2), &b"display"[..]),
        ]),
    }
}

fn event_batch(rows: &[(PersonCode, i64, EventPayload)]) -> CompactEventBatch {
    let mut records = Vec::with_capacity(rows.len());
    let mut payloads = Vec::with_capacity(rows.len());
    for &(person, timestamp_ms, payload) in rows {
        let payload_code =
            EventPayloadCode(u32::try_from(payloads.len()).expect("small test batch"));
        payloads.push(payload);
        records.push(CompactEvent {
            timestamp_ms,
            person,
            payload: payload_code,
        });
    }
    CompactEventBatch { records, payloads }
}

fn click_batch(rows: &[(PersonCode, i64, CampaignCode, &[u8])]) -> CompactClickBatch {
    let mut records = Vec::with_capacity(rows.len());
    let mut spans = Vec::with_capacity(rows.len());
    let mut bytes = Vec::new();
    for &(person, timestamp_ms, campaign, click_id) in rows {
        let offset = u32::try_from(bytes.len()).expect("small test arena");
        let length = u32::try_from(click_id.len()).expect("small click ID");
        bytes.extend_from_slice(click_id);
        spans.push(ByteSpan::new(offset, length).expect("valid test span"));
        records.push(CompactClick {
            timestamp_ms,
            person,
            campaign,
        });
    }
    CompactClickBatch::new(records, spans, bytes).expect("valid synthetic click batch")
}

fn view() -> EventPayload {
    EventPayload::non_purchase(EventKind::View).expect("view payload")
}

fn purchase(category: u32, price: u32) -> EventPayload {
    EventPayload::purchase(CategoryCode(category), price).expect("valid purchase")
}

#[test]
fn e_boundaries_equal_times_utc_start_day_and_category_ties() {
    let first = 10 * DAY_MS + 23 * 60 * 60 * 1_000 + 40 * 60 * 1_000;
    let midnight_purchase = first + 20 * 60 * 1_000;
    let exact_thirty = midnight_purchase + SESSION_GAP_MS;
    let split = exact_thirty + SESSION_GAP_MS + 1;
    let events = event_batch(&[
        (PersonCode(0), split, purchase(0, 500)),
        (PersonCode(0), first, view()),
        (PersonCode(0), split, view()),
        (PersonCode(0), midnight_purchase, purchase(0, 500)),
        (PersonCode(0), split, purchase(1, 500)),
        (PersonCode(0), exact_thirty, view()),
    ]);

    let output = sweep(&events, &click_batch(&[]), &dimensions()).expect("sweep succeeds");

    assert_eq!(output.user_daily.len(), 2);
    assert_eq!(output.user_daily[0].day, EpochDay(10));
    assert_eq!(output.user_daily[0].sessions, 1);
    assert_eq!(output.user_daily[0].events, 3);
    assert_eq!(output.user_daily[0].revenue_cents, 500);
    assert_eq!(
        output.user_daily[0]
            .top_category
            .as_ref()
            .map(OwnedUtf8::as_str),
        Some("shoes")
    );
    assert_eq!(output.user_daily[1].day, EpochDay(11));
    assert_eq!(output.user_daily[1].sessions, 1);
    assert_eq!(output.user_daily[1].events, 3);
    assert_eq!(output.user_daily[1].revenue_cents, 1_000);
    assert_eq!(
        output.user_daily[1]
            .top_category
            .as_ref()
            .map(OwnedUtf8::as_str),
        Some("books")
    );
    assert_eq!(output.sweep_counters.sessions, 2);
    assert_eq!(output.sweep_counters.organic_sessions, 2);
    assert_eq!(output.sort_counters.equal_event_timestamps, 2);
}

#[test]
fn a_boundaries_use_inclusive_window_click_id_tie_and_distinct_assists() {
    let start = 20 * DAY_MS + 10 * 60 * 60 * 1_000;
    let second_start = start + SESSION_GAP_MS + 1;
    let events = event_batch(&[
        (PersonCode(1), start, purchase(0, 1_000)),
        (PersonCode(0), second_start, view()),
        (PersonCode(0), start, view()),
    ]);
    let clicks = click_batch(&[
        (
            PersonCode(0),
            start - ATTRIBUTION_WINDOW_MS - 1,
            CampaignCode(2),
            b"expired",
        ),
        (
            PersonCode(0),
            start - ATTRIBUTION_WINDOW_MS,
            CampaignCode(1),
            b"inclusive",
        ),
        (PersonCode(0), start, CampaignCode(1), b"b"),
        (PersonCode(0), start, CampaignCode(0), b"a"),
        (PersonCode(0), start - 100, CampaignCode(0), b"older"),
        (PersonCode(0), start + 1, CampaignCode(1), b"after"),
    ]);

    let output = sweep(&events, &clicks, &dimensions()).expect("sweep succeeds");
    let c1 = output
        .campaign_daily
        .iter()
        .find(|row| row.campaign_id.as_str() == "c_1")
        .expect("c_1 row");
    let c2 = output
        .campaign_daily
        .iter()
        .find(|row| row.campaign_id.as_str() == "c_2")
        .expect("c_2 row");
    let organic = output
        .campaign_daily
        .iter()
        .find(|row| row.campaign_id.as_str() == "organic")
        .expect("organic row");

    assert_eq!(
        (
            c1.sessions,
            c1.purchasing_sessions,
            c1.revenue_cents,
            c1.assisted_sessions,
        ),
        (1, 0, 0, 2)
    );
    assert_eq!((c2.sessions, c2.assisted_sessions), (1, 2));
    assert_eq!(
        (
            organic.sessions,
            organic.purchasing_sessions,
            organic.revenue_cents,
            organic.assisted_sessions,
        ),
        (1, 1, 1_000, 0)
    );
    assert!(
        output
            .campaign_daily
            .iter()
            .all(|row| row.campaign_id.as_str() != "c_3")
    );
    assert_eq!(output.sweep_counters.attributed_sessions, 2);
    assert_eq!(output.sweep_counters.organic_sessions, 1);
    assert_eq!(output.sweep_counters.assisted_campaign_sessions, 4);
    assert_eq!(output.sort_counters.equal_click_timestamps, 1);
}

#[test]
fn l_compacts_for_one_linked_person_share_sessions_clicks_and_canonical_name() {
    let start = 5 * DAY_MS + 10 * 60 * 60 * 1_000;
    let events = event_batch(&[
        (PersonCode(7), start + 50 * 60 * 1_000, view()),
        (PersonCode(7), start, view()),
        (PersonCode(7), start + 25 * 60 * 1_000, purchase(1, 800)),
    ]);
    let clicks = click_batch(&[
        (PersonCode(7), start - 2_000, CampaignCode(1), b"k2"),
        (PersonCode(7), start - 1_000, CampaignCode(0), b"k1"),
    ]);

    let output = sweep(&events, &clicks, &dimensions()).expect("sweep succeeds");

    assert_eq!(output.user_daily.len(), 1);
    assert_eq!(output.user_daily[0].user_id.as_str(), "U7");
    assert_eq!(output.user_daily[0].sessions, 1);
    assert_eq!(output.user_daily[0].events, 3);
    assert_eq!(output.user_daily[0].revenue_cents, 800);
    let c1 = output
        .campaign_daily
        .iter()
        .find(|row| row.campaign_id.as_str() == "c_1")
        .expect("credit row");
    let c2 = output
        .campaign_daily
        .iter()
        .find(|row| row.campaign_id.as_str() == "c_2")
        .expect("assist-only row");
    assert_eq!(
        (
            c1.sessions,
            c1.purchasing_sessions,
            c1.revenue_cents,
            c1.assisted_sessions,
        ),
        (1, 1, 800, 1)
    );
    assert_eq!((c2.sessions, c2.assisted_sessions), (0, 1));
}

#[test]
fn ordering_and_sweep_can_be_timed_at_separate_boundaries() {
    let events = event_batch(&[
        (PersonCode(1), 20, view()),
        (PersonCode(0), 10, purchase(0, 250)),
    ]);
    let clicks = click_batch(&[(PersonCode(0), 9, CampaignCode(0), b"first")]);
    let ordering = StablePersonTimeOrder
        .order(&events.records, &clicks)
        .expect("ordering succeeds");
    let separated = sweep_ordered(&events, &clicks, &ordering).expect("ordered sweep succeeds");
    let combined =
        sweep_worker(&events, &clicks, &StablePersonTimeOrder).expect("combined sweep succeeds");

    assert_eq!(separated, combined);

    let invalid = OrderedRecords {
        event_indices: vec![0, 0],
        click_indices: vec![0],
        counters: ordering.counters,
    };
    assert!(sweep_ordered(&events, &clicks, &invalid).is_err());
}

#[test]
fn worker_reduction_is_integer_deterministic_in_any_worker_order() {
    let start = 4 * DAY_MS;
    let first_events = event_batch(&[(PersonCode(0), start, purchase(0, 250))]);
    let second_events = event_batch(&[(PersonCode(1), start, view())]);
    let first_clicks = click_batch(&[(PersonCode(0), start, CampaignCode(0), b"first")]);
    let second_clicks = click_batch(&[]);
    let first = sweep_worker(&first_events, &first_clicks, &StablePersonTimeOrder)
        .expect("first worker succeeds");
    let second = sweep_worker(&second_events, &second_clicks, &StablePersonTimeOrder)
        .expect("second worker succeeds");

    let forward = reduce_workers([first.clone(), second.clone()], &dimensions())
        .expect("forward reduction succeeds");
    let reverse =
        reduce_workers([second, first], &dimensions()).expect("reverse reduction succeeds");

    assert_eq!(forward, reverse);
    assert_eq!(forward.user_daily.len(), 2);
    assert_eq!(forward.sort_counters.event_records, 2);
    assert_eq!(forward.sweep_counters.people, 2);
}
