use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde_json::{Value, json};
use shopping_sessions_domain::telemetry::read_cgroup_memory_peak;
use shopping_sessions_domain::{
    ByteSpan, CampaignCode, Canonicalize, CategoryCode, ParseOutcome, Phase, PipelineError,
    PipelineErrorKind, ProductCode, RawClick, RawEvent, ResourceTelemetry, RunOutcome,
    ScratchTracker, TelemetryRecorder,
};
use shopping_sessions_identity::{IdentityBuilder, IdentityGraph};
use shopping_sessions_parser::{
    ByteCatalog, ParsedBatch, ParsedTimestamp, discover_input, load_campaigns, load_manifest,
    load_products, parse_clicks, parse_events, parse_links, parse_timestamp,
};
use shopping_sessions_reduce::click::{CampaignCatalog, reduce_click_segments};
use shopping_sessions_reduce::event::{
    EventWindow, ProductCatalog, ResolvedProduct, TimestampParser, reduce_event_segments,
};
use shopping_sessions_sweep::{
    DimensionLookup, RecordOrderer, StablePersonTimeOrder, reduce_workers, sweep_ordered,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if let Err(log_error) = write_failure_log(&error) {
                eprintln!("shopping-sessions-v2: could not write failure log: {log_error}");
            }
            eprintln!("shopping-sessions-v2: {error}");
            ExitCode::FAILURE
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run() -> Result<(), PipelineError> {
    let data_root = required_path("DATA_DIR")?;
    let _output_root = required_path("OUTPUT_DIR")?;
    let scratch_root = required_path("SCRATCH_DIR")?;
    let telemetry_path = std::env::var_os("V2_TELEMETRY_PATH")
        .map_or_else(|| scratch_root.join("v2-telemetry.json"), PathBuf::from);

    let mut telemetry = TelemetryRecorder::new();
    let mut scratch = ScratchTracker::new(&scratch_root);

    telemetry.start_phase(Phase::Discovery)?;
    let discovered = discover_input(&data_root)?;
    let manifest = load_manifest(&discovered.manifest)?;
    let products = ProductIndex::from_catalog(&load_products(&discovered.products)?)?;
    let campaigns = CampaignIndex::from_catalog(&load_campaigns(&discovered.campaigns)?)?;
    let discovered_paths = all_input_paths(&discovered);
    {
        let counters = telemetry.counters_mut(Phase::Discovery)?;
        counters.discovered_files = to_u64(discovered_paths.len());
        counters.bytes_read = paths_size(&discovered_paths, Phase::Discovery)?;
        counters.accepted_rows = 3;
        counters.input_rows = 3;
    }
    telemetry.finish_phase(Phase::Discovery)?;
    sample_scratch(&mut scratch, Phase::Discovery)?;

    telemetry.start_phase(Phase::Links)?;
    let link_batches = parse_links(&discovered.link_files)?;
    let mut identities = IdentityBuilder::new();
    let mut known_ids = HashSet::<Box<[u8]>>::new();
    let mut accepted_links = 0_u64;
    let mut link_rows = 0_u64;
    for batch in &link_batches {
        link_rows = link_rows.saturating_add(to_u64(batch.rows.len()));
        for outcome in &batch.rows {
            let ParseOutcome::Accepted(row) = outcome else {
                continue;
            };
            let left = resolve(batch, row.user_id_a, "link user_id_a")?;
            let right = resolve(batch, row.user_id_b, "link user_id_b")?;
            if identities.ingest_link(left, right)? {
                known_ids.insert(left.into());
                known_ids.insert(right.into());
                accepted_links = accepted_links.saturating_add(1);
            }
        }
    }
    {
        let counters = telemetry.counters_mut(Phase::Links)?;
        counters.input_rows = link_rows;
        counters.accepted_rows = accepted_links;
        counters.dropped_rows = link_rows.saturating_sub(accepted_links);
        counters.output_rows = accepted_links;
        counters.bytes_read = paths_size(&discovered.link_files, Phase::Links)?;
    }
    telemetry.finish_phase(Phase::Links)?;
    sample_scratch(&mut scratch, Phase::Links)?;

    telemetry.start_phase(Phase::Events)?;
    let event_batches = parse_events(&discovered.event_files)?;
    let click_batches = parse_clicks(&discovered.click_files)?;
    register_event_ids(&event_batches, &mut identities, &mut known_ids)?;
    register_click_ids(&click_batches, &mut identities, &mut known_ids)?;
    let identity = identities.finish()?;
    let person_names = build_person_names(&identity, &known_ids)?;
    let timestamps = ExactTimestamps;
    let event_window = EventWindow::new(manifest.window_start_ms, manifest.window_end_ms)?;
    let event_reduction = reduce_event_segments(
        event_batches
            .iter()
            .map(|batch| (batch.arena(), batch.rows.as_slice())),
        &timestamps,
        &identity,
        &products,
        event_window,
    )?;
    drop(event_batches);
    if !event_reduction.counters.reconciles() {
        return Err(invariant("event reducer counters do not reconcile").in_phase(Phase::Events));
    }
    {
        let counters = telemetry.counters_mut(Phase::Events)?;
        counters.input_rows = event_reduction.counters.input_rows;
        counters.accepted_rows = event_reduction.counters.output_rows;
        counters.dropped_rows = event_reduction
            .counters
            .input_rows
            .saturating_sub(event_reduction.counters.output_rows);
        counters.output_rows = event_reduction.counters.output_rows;
        counters.bytes_read = paths_size(&discovered.event_files, Phase::Events)?;
    }
    telemetry.finish_phase(Phase::Events)?;
    sample_scratch(&mut scratch, Phase::Events)?;

    telemetry.start_phase(Phase::Clicks)?;
    let click_reduction = reduce_click_segments(
        click_batches
            .iter()
            .map(|batch| (batch.arena(), batch.rows.as_slice())),
        manifest.window_start_ms,
        manifest.window_end_ms,
        parse_timestamp_ms,
        &identity,
        &campaigns.catalog,
    )?;
    drop(click_batches);
    let accepted_clicks = to_u64(click_reduction.batch.records().len());
    let input_clicks = to_u64(click_reduction.statuses.len());
    {
        let counters = telemetry.counters_mut(Phase::Clicks)?;
        counters.input_rows = input_clicks;
        counters.accepted_rows = accepted_clicks;
        counters.dropped_rows = input_clicks.saturating_sub(accepted_clicks);
        counters.output_rows = accepted_clicks;
        counters.bytes_read = paths_size(&discovered.click_files, Phase::Clicks)?;
    }
    telemetry.finish_phase(Phase::Clicks)?;
    sample_scratch(&mut scratch, Phase::Clicks)?;

    telemetry.start_phase(Phase::Sort)?;
    let event_count = to_u64(event_reduction.batch.records.len());
    let click_count = to_u64(click_reduction.batch.records().len());
    let ordering = StablePersonTimeOrder
        .order(&event_reduction.batch.records, &click_reduction.batch)
        .map_err(|error| error.in_phase(Phase::Sort))?;
    {
        let counters = telemetry.counters_mut(Phase::Sort)?;
        counters.input_rows = event_count.saturating_add(click_count);
        counters.accepted_rows = counters.input_rows;
        counters.output_rows = counters.input_rows;
    }
    telemetry.finish_phase(Phase::Sort)?;
    sample_scratch(&mut scratch, Phase::Sort)?;

    telemetry.start_phase(Phase::Sweep)?;
    let worker = sweep_ordered(&event_reduction.batch, &click_reduction.batch, &ordering)?;
    let dimensions = Dimensions {
        people: person_names,
        categories: products.categories.clone(),
        campaign_ids: campaigns.ids,
        campaign_channels: campaigns.channels,
    };
    let output = reduce_workers([worker], &dimensions)?;
    {
        let counters = telemetry.counters_mut(Phase::Sweep)?;
        counters.input_rows = event_count.saturating_add(click_count);
        counters.accepted_rows = event_count;
        counters.output_rows = to_u64(
            output
                .user_daily
                .len()
                .saturating_add(output.campaign_daily.len()),
        );
    }
    telemetry.finish_phase(Phase::Sweep)?;
    sample_scratch(&mut scratch, Phase::Sweep)?;

    telemetry.start_phase(Phase::Write)?;
    shopping_sessions_output::publish(&output.user_daily, &output.campaign_daily)?;
    let output_rows = to_u64(
        output
            .user_daily
            .len()
            .saturating_add(output.campaign_daily.len()),
    );
    {
        let counters = telemetry.counters_mut(Phase::Write)?;
        counters.input_rows = output_rows;
        counters.accepted_rows = output_rows;
        counters.output_rows = output_rows;
        counters.bytes_written = paths_size(
            &[
                PathBuf::from("/output/user_daily/part-0.parquet"),
                PathBuf::from("/output/campaign_daily/part-0.parquet"),
            ],
            Phase::Write,
        )?;
    }
    telemetry.finish_phase(Phase::Write)?;
    sample_scratch(&mut scratch, Phase::Write)?;

    let memory_peak = read_cgroup_memory_peak(Path::new("/sys/fs/cgroup")).map_err(|error| {
        PipelineError::new(
            PipelineErrorKind::Io,
            format!("read cgroup memory peak: {error}"),
        )
    })?;
    telemetry.set_resources(ResourceTelemetry {
        cgroup_memory_peak_bytes: memory_peak,
        scratch_high_water_bytes: Some(scratch.high_water_bytes()),
    });
    let completed = telemetry.finish(RunOutcome::Success)?;
    let body = completed.to_json_line().map_err(|error| {
        PipelineError::new(
            PipelineErrorKind::Invariant,
            format!("serialize telemetry: {error}"),
        )
    })?;
    if let Some(parent) = telemetry_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| io_pipeline("create telemetry directory", &error))?;
    }
    fs::write(&telemetry_path, body).map_err(|error| io_pipeline("write telemetry", &error))?;
    Ok(())
}

struct ExactTimestamps;

impl TimestampParser for ExactTimestamps {
    fn parse_timestamp(&self, bytes: &[u8]) -> Option<i64> {
        parse_timestamp_ms(bytes)
    }
}

fn parse_timestamp_ms(bytes: &[u8]) -> Option<i64> {
    match parse_timestamp(bytes) {
        Ok(ParsedTimestamp::Milliseconds(value)) => Some(value),
        Ok(ParsedTimestamp::Overflow) => Some(i64::MAX),
        Err(_) => None,
    }
}

struct ProductIndex {
    by_id: HashMap<Box<[u8]>, ResolvedProduct>,
    categories: Vec<Box<[u8]>>,
    unknown: CategoryCode,
}

impl ProductIndex {
    fn from_catalog(catalog: &ByteCatalog) -> Result<Self, PipelineError> {
        let unknown = CategoryCode(0);
        let mut categories = vec![Box::<[u8]>::from(&b"unknown"[..])];
        let mut category_codes = HashMap::<Box<[u8]>, CategoryCode>::new();
        category_codes.insert(Box::from(&b"unknown"[..]), unknown);
        let mut by_id = HashMap::new();

        for (index, &(product_span, category_span)) in catalog.entries.iter().enumerate() {
            let product_id = catalog.resolve(product_span).ok_or_else(|| {
                invariant("product ID span is outside the catalog arena").in_phase(Phase::Discovery)
            })?;
            let category = catalog.resolve(category_span).ok_or_else(|| {
                invariant("category span is outside the catalog arena").in_phase(Phase::Discovery)
            })?;
            if product_id.is_empty() || category.is_empty() {
                return Err(input("product catalog contains an empty field"));
            }
            let category_code = if let Some(code) = category_codes.get(category) {
                *code
            } else {
                let code = CategoryCode(to_u32(categories.len(), "too many categories")?);
                categories.push(category.into());
                category_codes.insert(category.into(), code);
                code
            };
            let product_code = ProductCode(to_u32(index, "too many products")?);
            if by_id
                .insert(
                    product_id.into(),
                    ResolvedProduct {
                        product: Some(product_code),
                        category: category_code,
                    },
                )
                .is_some()
            {
                return Err(input("product catalog contains a duplicate ID"));
            }
        }
        Ok(Self {
            by_id,
            categories,
            unknown,
        })
    }
}

impl ProductCatalog for ProductIndex {
    fn resolve_product(&self, product_id: &[u8]) -> ResolvedProduct {
        self.by_id
            .get(product_id)
            .copied()
            .unwrap_or(ResolvedProduct {
                product: None,
                category: self.unknown,
            })
    }
}

struct CampaignIndex {
    catalog: CampaignCatalog,
    ids: Vec<Box<[u8]>>,
    channels: Vec<Box<[u8]>>,
}

impl CampaignIndex {
    fn from_catalog(catalog: &ByteCatalog) -> Result<Self, PipelineError> {
        let mut entries = Vec::with_capacity(catalog.entries.len());
        let mut ids = Vec::with_capacity(catalog.entries.len());
        let mut channels = Vec::with_capacity(catalog.entries.len());
        for (index, &(id_span, channel_span)) in catalog.entries.iter().enumerate() {
            let id = catalog.resolve(id_span).ok_or_else(|| {
                invariant("campaign ID span is outside the catalog arena")
                    .in_phase(Phase::Discovery)
            })?;
            let channel = catalog.resolve(channel_span).ok_or_else(|| {
                invariant("campaign channel span is outside the catalog arena")
                    .in_phase(Phase::Discovery)
            })?;
            if channel.is_empty() {
                return Err(input("campaign catalog contains an empty channel"));
            }
            let code = CampaignCode(to_u32(index, "too many campaigns")?);
            entries.push((id.to_vec(), code));
            ids.push(id.into());
            channels.push(channel.into());
        }
        Ok(Self {
            catalog: CampaignCatalog::new(entries)?,
            ids,
            channels,
        })
    }
}

struct Dimensions {
    people: Vec<Box<[u8]>>,
    categories: Vec<Box<[u8]>>,
    campaign_ids: Vec<Box<[u8]>>,
    campaign_channels: Vec<Box<[u8]>>,
}

impl DimensionLookup for Dimensions {
    fn person_name(&self, person: shopping_sessions_domain::PersonCode) -> Option<&[u8]> {
        self.people.get(person.0 as usize).map(AsRef::as_ref)
    }

    fn category_name(&self, category: CategoryCode) -> Option<&[u8]> {
        self.categories.get(category.0 as usize).map(AsRef::as_ref)
    }

    fn campaign_id(&self, campaign: CampaignCode) -> Option<&[u8]> {
        self.campaign_ids
            .get(campaign.0 as usize)
            .map(AsRef::as_ref)
    }

    fn campaign_channel(&self, campaign: CampaignCode) -> Option<&[u8]> {
        self.campaign_channels
            .get(campaign.0 as usize)
            .map(AsRef::as_ref)
    }
}

fn register_event_ids(
    batches: &[ParsedBatch<RawEvent>],
    identities: &mut IdentityBuilder,
    known_ids: &mut HashSet<Box<[u8]>>,
) -> Result<(), PipelineError> {
    for batch in batches {
        for outcome in &batch.rows {
            if let ParseOutcome::Accepted(row) = outcome {
                register_id(
                    resolve(batch, row.user_id, "event user_id")?,
                    identities,
                    known_ids,
                )?;
            }
        }
    }
    Ok(())
}

fn register_click_ids(
    batches: &[ParsedBatch<RawClick>],
    identities: &mut IdentityBuilder,
    known_ids: &mut HashSet<Box<[u8]>>,
) -> Result<(), PipelineError> {
    for batch in batches {
        for outcome in &batch.rows {
            if let ParseOutcome::Accepted(row) = outcome {
                register_id(
                    resolve(batch, row.user_id, "click user_id")?,
                    identities,
                    known_ids,
                )?;
            }
        }
    }
    Ok(())
}

fn register_id(
    id: &[u8],
    identities: &mut IdentityBuilder,
    known_ids: &mut HashSet<Box<[u8]>>,
) -> Result<(), PipelineError> {
    if identities.ingest_id(id)? {
        known_ids.insert(id.into());
    }
    Ok(())
}

fn build_person_names(
    identity: &IdentityGraph,
    known_ids: &HashSet<Box<[u8]>>,
) -> Result<Vec<Box<[u8]>>, PipelineError> {
    let mut names = vec![None; identity.person_count() as usize];
    for id in known_ids {
        let person = identity.canonicalize(id)?;
        let name = identity
            .resolve(person.canonical_name)
            .ok_or_else(|| invariant("identity canonical-name handle is outside its arena"))?;
        let slot = names
            .get_mut(person.code.0 as usize)
            .ok_or_else(|| invariant("identity person code is outside the dense person range"))?;
        if let Some(existing) = slot.as_deref() {
            if existing != name {
                return Err(invariant("identity person code has inconsistent names"));
            }
        } else {
            *slot = Some(Box::from(name));
        }
    }
    names
        .into_iter()
        .map(|name| name.ok_or_else(|| invariant("identity person code has no canonical name")))
        .collect()
}

fn resolve<'a, T>(
    batch: &'a ParsedBatch<T>,
    span: ByteSpan,
    field: &str,
) -> Result<&'a [u8], PipelineError> {
    batch.resolve(span).ok_or_else(|| {
        PipelineError::new(
            PipelineErrorKind::Invariant,
            format!("{field} span is outside its parser arena"),
        )
    })
}

fn all_input_paths(input: &shopping_sessions_parser::DiscoveredInput) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    paths.extend(input.event_files.iter().cloned());
    paths.extend(input.click_files.iter().cloned());
    paths.extend(input.link_files.iter().cloned());
    paths.extend([
        input.products.clone(),
        input.campaigns.clone(),
        input.manifest.clone(),
    ]);
    paths
}

fn paths_size(paths: &[PathBuf], phase: Phase) -> Result<u64, PipelineError> {
    paths.iter().try_fold(0_u64, |total, path| {
        let length = fs::metadata(path)
            .map_err(|error| {
                PipelineError::new(
                    PipelineErrorKind::Io,
                    format!("inspect {}: {error}", path.display()),
                )
                .in_phase(phase)
            })?
            .len();
        Ok(total.saturating_add(length))
    })
}

fn sample_scratch(tracker: &mut ScratchTracker, phase: Phase) -> Result<(), PipelineError> {
    tracker.sample().map(|_| ()).map_err(|error| {
        PipelineError::new(
            PipelineErrorKind::Io,
            format!("sample scratch usage: {error}"),
        )
        .in_phase(phase)
    })
}

fn required_path(variable: &str) -> Result<PathBuf, PipelineError> {
    std::env::var_os(variable)
        .map(PathBuf::from)
        .ok_or_else(|| {
            PipelineError::new(
                PipelineErrorKind::InputContract,
                format!("required environment variable {variable} is not set"),
            )
        })
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn to_u32(value: usize, message: &str) -> Result<u32, PipelineError> {
    u32::try_from(value).map_err(|_| PipelineError::new(PipelineErrorKind::ResourceLimit, message))
}

fn input(message: impl Into<Box<str>>) -> PipelineError {
    PipelineError::new(PipelineErrorKind::InputContract, message).in_phase(Phase::Discovery)
}

fn invariant(message: impl Into<Box<str>>) -> PipelineError {
    PipelineError::new(PipelineErrorKind::Invariant, message)
}

fn io_pipeline(context: &str, error: &io::Error) -> PipelineError {
    PipelineError::new(PipelineErrorKind::Io, format!("{context}: {error}"))
}

fn failure_record(error: &PipelineError) -> Value {
    json!({
        "format_version": 1,
        "outcome": "failed",
        "error": {
            "kind": error.kind,
            "phase": error.phase,
            "message": error.message,
        },
    })
}

fn failure_log_path() -> Option<PathBuf> {
    std::env::var_os("V2_FAILURE_LOG_PATH")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("SCRATCH_DIR")
                .map(PathBuf::from)
                .map(|scratch| scratch.join("v2-failures/app.json"))
        })
}

fn write_failure_log(error: &PipelineError) -> io::Result<()> {
    let Some(path) = failure_log_path() else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut body = serde_json::to_vec_pretty(&failure_record(error)).map_err(io::Error::other)?;
    body.push(b'\n');
    fs::write(path, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_is_machine_readable() {
        let error =
            PipelineError::new(PipelineErrorKind::Invariant, "broken").in_phase(Phase::Sweep);
        let record = failure_record(&error);
        assert_eq!(record["format_version"], 1);
        assert_eq!(record["outcome"], "failed");
        assert_eq!(record["error"]["kind"], "invariant");
        assert_eq!(record["error"]["phase"], "sweep");
        assert_eq!(record["error"]["message"], "broken");
    }

    #[test]
    fn timestamp_adapter_retains_overflow_as_outside() {
        assert_eq!(
            parse_timestamp_ms(b"1791071890000"),
            Some(1_791_071_890_000)
        );
        assert_eq!(parse_timestamp_ms(b"99999999999999999999"), Some(i64::MAX));
        assert_eq!(parse_timestamp_ms(b"bad"), None);
    }

}
