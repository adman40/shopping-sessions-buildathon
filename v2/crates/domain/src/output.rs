use serde::{Deserialize, Serialize};

use crate::OwnedUtf8;

/// Days since 1970-01-01 UTC, matching the physical Parquet DATE representation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(transparent)]
pub struct EpochDay(pub i32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalType {
    Utf8,
    Date,
    SignedInteger,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnSchema {
    pub name: &'static str,
    pub logical_type: LogicalType,
    pub nullable: bool,
}

pub const USER_DAILY_SCHEMA: [ColumnSchema; 6] = [
    ColumnSchema {
        name: "user_id",
        logical_type: LogicalType::Utf8,
        nullable: false,
    },
    ColumnSchema {
        name: "day",
        logical_type: LogicalType::Date,
        nullable: false,
    },
    ColumnSchema {
        name: "sessions",
        logical_type: LogicalType::SignedInteger,
        nullable: false,
    },
    ColumnSchema {
        name: "events",
        logical_type: LogicalType::SignedInteger,
        nullable: false,
    },
    ColumnSchema {
        name: "revenue_cents",
        logical_type: LogicalType::SignedInteger,
        nullable: false,
    },
    ColumnSchema {
        name: "top_category",
        logical_type: LogicalType::Utf8,
        nullable: true,
    },
];

pub const CAMPAIGN_DAILY_SCHEMA: [ColumnSchema; 7] = [
    ColumnSchema {
        name: "campaign_id",
        logical_type: LogicalType::Utf8,
        nullable: false,
    },
    ColumnSchema {
        name: "channel",
        logical_type: LogicalType::Utf8,
        nullable: false,
    },
    ColumnSchema {
        name: "day",
        logical_type: LogicalType::Date,
        nullable: false,
    },
    ColumnSchema {
        name: "sessions",
        logical_type: LogicalType::SignedInteger,
        nullable: false,
    },
    ColumnSchema {
        name: "purchasing_sessions",
        logical_type: LogicalType::SignedInteger,
        nullable: false,
    },
    ColumnSchema {
        name: "revenue_cents",
        logical_type: LogicalType::SignedInteger,
        nullable: false,
    },
    ColumnSchema {
        name: "assisted_sessions",
        logical_type: LogicalType::SignedInteger,
        nullable: false,
    },
];

/// Fully owned sweep-to-output user aggregate.
///
/// Rows may move between worker and writer threads without borrowing worker
/// arenas. The output slice writes these fields exactly as `USER_DAILY_SCHEMA`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserDailyRow {
    pub user_id: OwnedUtf8,
    pub day: EpochDay,
    pub sessions: i64,
    pub events: i64,
    pub revenue_cents: i64,
    pub top_category: Option<OwnedUtf8>,
}

/// Fully owned sweep-to-output campaign aggregate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CampaignDailyRow {
    pub campaign_id: OwnedUtf8,
    pub channel: OwnedUtf8,
    pub day: EpochDay,
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue_cents: i64,
    pub assisted_sessions: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemas_freeze_names_types_and_nullability() {
        assert_eq!(
            USER_DAILY_SCHEMA.map(|column| column.name),
            [
                "user_id",
                "day",
                "sessions",
                "events",
                "revenue_cents",
                "top_category"
            ]
        );
        assert_eq!(
            USER_DAILY_SCHEMA
                .iter()
                .filter(|column| column.nullable)
                .count(),
            1
        );
        assert_eq!(USER_DAILY_SCHEMA[1].logical_type, LogicalType::Date);
        assert_eq!(
            CAMPAIGN_DAILY_SCHEMA.map(|column| column.name),
            [
                "campaign_id",
                "channel",
                "day",
                "sessions",
                "purchasing_sessions",
                "revenue_cents",
                "assisted_sessions"
            ]
        );
        assert!(CAMPAIGN_DAILY_SCHEMA.iter().all(|column| !column.nullable));
    }

    #[test]
    fn top_category_is_the_only_nullable_row_value() {
        let row = UserDailyRow {
            user_id: OwnedUtf8::from_static("u1"),
            day: EpochDay(0),
            sessions: 1,
            events: 1,
            revenue_cents: 0,
            top_category: None,
        };
        assert!(row.top_category.is_none());
    }
}
