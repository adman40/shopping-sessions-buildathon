/// Milliseconds in one second.
pub const SECOND_MS: i64 = 1_000;
/// Milliseconds in one minute.
pub const MINUTE_MS: i64 = 60 * SECOND_MS;
/// Milliseconds in one UTC day.
pub const DAY_MS: i64 = 24 * 60 * MINUTE_MS;
/// Inclusive attribution and assist lookback.
pub const ATTRIBUTION_WINDOW_MS: i64 = 7 * DAY_MS;
/// A gap strictly greater than this starts a new session.
pub const SESSION_GAP_MS: i64 = 30 * MINUTE_MS;
/// An event is late only when ingest minus event time is strictly greater than this.
pub const LATE_EVENT_MS: i64 = DAY_MS;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_constants_are_exact() {
        assert_eq!(DAY_MS, 86_400_000);
        assert_eq!(ATTRIBUTION_WINDOW_MS, 604_800_000);
        assert_eq!(SESSION_GAP_MS, 1_800_000);
        assert_eq!(LATE_EVENT_MS, 86_400_000);
    }
}
