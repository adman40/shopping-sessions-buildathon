#![doc = "Frozen cross-slice contracts for the Shopping Sessions V2 pipeline."]
#![forbid(unsafe_code)]

pub mod bytes;
pub mod compact;
pub mod error;
pub mod output;
pub mod parsed;
pub mod status;
pub mod telemetry;
pub mod time;

pub use bytes::{ByteHandle, ByteSpan, OwnedUtf8, compare_bytes};
pub use compact::{
    CampaignCode, CanonicalPerson, Canonicalize, CategoryCode, CompactClick, CompactClickBatch,
    CompactEvent, CompactEventBatch, EventKind, EventPayload, EventPayloadCode, PersonCode,
    ProductCode,
};
pub use error::{PipelineError, PipelineErrorKind};
pub use output::{
    CAMPAIGN_DAILY_SCHEMA, CampaignDailyRow, ColumnSchema, EpochDay, LogicalType,
    USER_DAILY_SCHEMA, UserDailyRow,
};
pub use parsed::{ParseOutcome, ParseStatus, RawClick, RawEvent, RawLink};
pub use status::{DropReason, RecordStatus};
pub use telemetry::{
    Phase, PhaseCounters, PhaseTelemetry, ResourceTelemetry, RunOutcome, RunTelemetry,
    ScratchTracker, TelemetryRecorder,
};
