#![doc = "Exact scalar input parser for the Shopping Sessions V2 pipeline."]
#![forbid(unsafe_code)]

mod csv;
mod discovery;
mod input;
mod timestamp;

pub use csv::{ParsedBatch, parse_fields};
pub use discovery::{DiscoveredInput, discover_input};
pub use input::{
    ByteCatalog, Manifest, load_campaigns, load_manifest, load_products, parse_clicks,
    parse_events, parse_links,
};
pub use timestamp::{
    IntegerError, ParsedTimestamp, TimestampError, TimestampWindow, WindowClassification,
    parse_ascii_u64, parse_f1_timestamp, parse_timestamp,
};
