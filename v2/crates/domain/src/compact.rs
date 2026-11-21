use serde::{Deserialize, Serialize};

use crate::{ByteHandle, ByteSpan, PipelineError, PipelineErrorKind};

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
    kind: EventKind,
    category: Option<CategoryCode>,
    price_cents: Option<u32>,
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

    #[must_use]
    pub const fn kind(&self) -> EventKind {
        self.kind
    }

    #[must_use]
    pub const fn category(&self) -> Option<CategoryCode> {
        self.category
    }

    #[must_use]
    pub const fn price_cents(&self) -> Option<u32> {
        self.price_cents
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
/// Its click-ID span is retained in the owning batch's parallel `click_ids` sidecar
/// and resolves into batch-owned bytes solely for exact same-timestamp tie resolution.
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

/// Owns compact clicks and their index-aligned click IDs in a contiguous byte arena.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactClickBatch {
    records: Box<[CompactClick]>,
    click_ids: Box<[ByteSpan]>,
    click_id_bytes: Box<[u8]>,
}

impl CompactClickBatch {
    /// Builds a batch whose click-ID spans all resolve in the supplied arena.
    ///
    /// # Errors
    ///
    /// Returns an invariant error when sidecar lengths differ or a span is out of bounds.
    pub fn new(
        records: Vec<CompactClick>,
        click_ids: Vec<ByteSpan>,
        click_id_bytes: Vec<u8>,
    ) -> Result<Self, PipelineError> {
        let batch = Self {
            records: records.into_boxed_slice(),
            click_ids: click_ids.into_boxed_slice(),
            click_id_bytes: click_id_bytes.into_boxed_slice(),
        };
        batch.validate()?;
        Ok(batch)
    }

    #[must_use]
    pub fn records(&self) -> &[CompactClick] {
        &self.records
    }

    #[must_use]
    pub fn click_id(&self, record_index: usize) -> Option<&[u8]> {
        self.click_ids
            .get(record_index)
            .and_then(|span| span.resolve(&self.click_id_bytes))
    }

    /// Validates the index-aligned click-ID sidecar.
    ///
    /// # Errors
    ///
    /// Returns an invariant error when sidecar lengths differ or a span is out of bounds.
    pub fn validate(&self) -> Result<(), PipelineError> {
        if self.records.len() != self.click_ids.len() {
            return Err(PipelineError::new(
                PipelineErrorKind::Invariant,
                format!(
                    "{} click records have {} click-ID spans",
                    self.records.len(),
                    self.click_ids.len()
                ),
            ));
        }
        if let Some((index, _)) = self
            .click_ids
            .iter()
            .enumerate()
            .find(|(_, span)| span.resolve(&self.click_id_bytes).is_none())
        {
            return Err(PipelineError::new(
                PipelineErrorKind::Invariant,
                format!("click-ID span at index {index} is out of bounds"),
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

        let view = EventPayload::non_purchase(EventKind::View).unwrap();
        assert_eq!(view.kind(), EventKind::View);
        assert_eq!(view.category(), None);
        assert_eq!(view.price_cents(), None);

        let purchase = EventPayload::purchase(CategoryCode(1), 10_000_000).unwrap();
        assert_eq!(purchase.kind(), EventKind::Purchase);
        assert_eq!(purchase.category(), Some(CategoryCode(1)));
        assert_eq!(purchase.price_cents(), Some(10_000_000));
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

        let click = CompactClick {
            timestamp_ms: 0,
            person: PersonCode(0),
            campaign: CampaignCode(0),
        };
        assert!(CompactClickBatch::new(vec![click], vec![], vec![]).is_err());
        assert!(
            CompactClickBatch::new(
                vec![click],
                vec![ByteSpan::new(2, 2).unwrap()],
                b"abc".to_vec(),
            )
            .is_err()
        );

        let clicks = CompactClickBatch::new(
            vec![click],
            vec![ByteSpan::new(1, 2).unwrap()],
            b"zab".to_vec(),
        )
        .unwrap();
        assert_eq!(clicks.records(), &[click]);
        assert_eq!(clicks.click_id(0), Some(&b"ab"[..]));
        assert_eq!(clicks.click_id(1), None);
    }
}
