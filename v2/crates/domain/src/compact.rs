use serde::{Deserialize, Serialize};

use crate::{ByteHandle, PipelineError, PipelineErrorKind};

macro_rules! code_type {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        #[repr(transparent)]
        pub struct $name(pub u32);
    };
}

code_type!(
    PersonCode,
    "Dense person code assigned by the identity slice."
);
code_type!(
    ProductCode,
    "Dense product code assigned during dimension loading."
);
code_type!(
    CategoryCode,
    "Dense category code assigned during dimension loading."
);
code_type!(
    CampaignCode,
    "Dense campaign code assigned during dimension loading."
);
code_type!(
    EventPayloadCode,
    "Index into a CompactEventBatch payload table."
);

/// Canonical identity result. The identity slice owns the name arena.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalPerson {
    pub code: PersonCode,
    pub canonical_name: ByteHandle,
}

/// Read-only identity boundary used by reducers.
///
/// Implementations must return the byte-minimum linked name. Unseen IDs become
/// byte-exact singleton people. Returned handles remain valid for the run.
pub trait Canonicalize {
    /// Resolves one exact user ID.
    ///
    /// # Errors
    ///
    /// Returns an invariant or resource error if identity state cannot supply a stable result.
    fn canonicalize(&self, user_bytes: &[u8]) -> Result<CanonicalPerson, PipelineError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum EventKind {
    View = 0,
    Cart = 1,
    Purchase = 2,
}

/// Expanded event payload owned by a compact-event batch.
///
/// Purchases carry a category and a price in the rules' validated range.
/// Views and carts carry neither. Constructors enforce this invariant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventPayload {
    pub kind: EventKind,
    pub category: Option<CategoryCode>,
    pub price_cents: Option<u32>,
}

impl EventPayload {
    #[must_use]
    pub const fn non_purchase(kind: EventKind) -> Option<Self> {
        match kind {
            EventKind::View | EventKind::Cart => Some(Self {
                kind,
                category: None,
                price_cents: None,
            }),
            EventKind::Purchase => None,
        }
    }

    #[must_use]
    pub fn purchase(category: CategoryCode, price_cents: u32) -> Option<Self> {
        (1..=10_000_000).contains(&price_cents).then_some(Self {
            kind: EventKind::Purchase,
            category: Some(category),
            price_cents: Some(price_cents),
        })
    }
}

/// The reducer-to-sweep event record, fixed at 16 bytes.
///
/// `payload` indexes the owning batch's immutable payload table. Records are
/// sorted by `(person, timestamp_ms)` before the sweep boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct CompactEvent {
    pub timestamp_ms: i64,
    pub person: PersonCode,
    pub payload: EventPayloadCode,
}

/// The reducer-to-sweep click record, fixed at 16 bytes.
///
/// Its click ID is retained in the owning batch's parallel `click_ids` sidecar,
/// solely for exact same-timestamp bytewise tie resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct CompactClick {
    pub timestamp_ms: i64,
    pub person: PersonCode,
    pub campaign: CampaignCode,
}

/// Owns compact events and the payload table referenced by each record.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactEventBatch {
    pub records: Vec<CompactEvent>,
    pub payloads: Vec<EventPayload>,
}

impl CompactEventBatch {
    /// Validates every payload index.
    ///
    /// # Errors
    ///
    /// Returns an invariant error for an out-of-bounds payload code.
    pub fn validate(&self) -> Result<(), PipelineError> {
        if let Some(record) = self.records.iter().find(|record| {
            usize::try_from(record.payload.0).map_or(true, |at| at >= self.payloads.len())
        }) {
            return Err(PipelineError::new(
                PipelineErrorKind::Invariant,
                format!("event payload index {} is out of bounds", record.payload.0),
            ));
        }
        Ok(())
    }
}

/// Owns compact clicks and their index-aligned click-ID handles.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactClickBatch {
    pub records: Vec<CompactClick>,
    pub click_ids: Vec<ByteHandle>,
}

impl CompactClickBatch {
    /// Validates the index-aligned click-ID sidecar.
    ///
    /// # Errors
    ///
    /// Returns an invariant error when sidecar and record lengths differ.
    pub fn validate(&self) -> Result<(), PipelineError> {
        if self.records.len() != self.click_ids.len() {
            return Err(PipelineError::new(
                PipelineErrorKind::Invariant,
                format!(
                    "{} click records have {} click-ID handles",
                    self.records.len(),
                    self.click_ids.len()
                ),
            ));
        }
        Ok(())
    }
}

const _: [(); 16] = [(); std::mem::size_of::<CompactEvent>()];
const _: [(); 16] = [(); std::mem::size_of::<CompactClick>()];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_records_are_sixteen_bytes() {
        assert_eq!(std::mem::size_of::<CompactEvent>(), 16);
        assert_eq!(std::mem::size_of::<CompactClick>(), 16);
    }

    #[test]
    fn payload_invariants_are_explicit() {
        assert!(EventPayload::non_purchase(EventKind::Purchase).is_none());
        assert!(EventPayload::purchase(CategoryCode(1), 0).is_none());
        assert!(EventPayload::purchase(CategoryCode(1), 10_000_000).is_some());
    }

    #[test]
    fn batches_validate_side_tables() {
        let events = CompactEventBatch {
            records: vec![CompactEvent {
                timestamp_ms: 0,
                person: PersonCode(0),
                payload: EventPayloadCode(1),
            }],
            payloads: vec![EventPayload::non_purchase(EventKind::View).unwrap()],
        };
        assert!(events.validate().is_err());

        let clicks = CompactClickBatch {
            records: vec![CompactClick {
                timestamp_ms: 0,
                person: PersonCode(0),
                campaign: CampaignCode(0),
            }],
            click_ids: vec![],
        };
        assert!(clicks.validate().is_err());
    }
}
