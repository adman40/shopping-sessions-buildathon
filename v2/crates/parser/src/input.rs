use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use shopping_sessions_domain::{
    ByteSpan, ParseOutcome, ParseStatus, Phase, PipelineError, PipelineErrorKind, RawClick,
    RawEvent, RawLink,
};

use crate::csv::{ParsedBatch, append_fixed_fields, parse_fixed_fields};
use crate::timestamp::{TimestampWindow, parse_f1_timestamp};

/// A byte-exact two-column catalog in file order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ByteCatalog {
    arena: Box<[u8]>,
    pub entries: Vec<(ByteSpan, ByteSpan)>,
}

impl ByteCatalog {
    #[must_use]
    pub fn arena(&self) -> &[u8] {
        &self.arena
    }

    #[must_use]
    pub fn resolve(&self, span: ByteSpan) -> Option<&[u8]> {
        span.resolve(&self.arena)
    }
}

/// Validated manifest boundaries and their derived event/click window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Manifest {
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub window: TimestampWindow,
}

/// Parses link files in the supplied order, skipping each file's first physical line.
///
/// # Errors
///
/// Returns an I/O error if any file cannot be read.
pub fn parse_links(paths: &[PathBuf]) -> Result<ParsedBatch<RawLink>, PipelineError> {
    parse_typed_rows::<_, _, 2>(paths, Phase::Links, |spans| RawLink {
        user_id_a: spans[0],
        user_id_b: spans[1],
    })
}

/// Parses event files while retaining raw semantic fields for dedup-before-validation.
///
/// # Errors
///
/// Returns an I/O error if any file cannot be read.
pub fn parse_events(paths: &[PathBuf]) -> Result<ParsedBatch<RawEvent>, PipelineError> {
    parse_typed_rows::<_, _, 7>(paths, Phase::Events, |spans| RawEvent {
        event_id: spans[0],
        user_id: spans[1],
        timestamp: spans[2],
        ingest_timestamp: spans[3],
        event_type: spans[4],
        product_id: spans[5],
        price_cents: spans[6],
    })
}

/// Parses click files in the supplied order, preserving exact field bytes.
///
/// # Errors
///
/// Returns an I/O error if any file cannot be read.
pub fn parse_clicks(paths: &[PathBuf]) -> Result<ParsedBatch<RawClick>, PipelineError> {
    parse_typed_rows::<_, _, 4>(paths, Phase::Clicks, |spans| RawClick {
        click_id: spans[0],
        user_id: spans[1],
        campaign_id: spans[2],
        timestamp: spans[3],
    })
}

/// Loads the clean product catalog using the restricted CSV grammar.
///
/// # Errors
///
/// Returns an error for I/O failure or any malformed catalog row.
pub fn load_products(path: &Path) -> Result<ByteCatalog, PipelineError> {
    load_catalog(path, "products")
}

/// Loads the clean campaign catalog using the restricted CSV grammar.
///
/// # Errors
///
/// Returns an error for I/O failure or any malformed catalog row.
pub fn load_campaigns(path: &Path) -> Result<ByteCatalog, PipelineError> {
    load_catalog(path, "campaigns")
}

/// Loads exact F1 manifest bounds and validates the derived windows.
///
/// # Errors
///
/// Returns an error for I/O failure, malformed JSON, unexpected fields, invalid
/// F1 bounds, an empty/reversed window, or click-lookback overflow.
pub fn load_manifest(path: &Path) -> Result<Manifest, PipelineError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RawManifest {
        window_start: String,
        window_end: String,
    }

    let bytes = fs::read(path).map_err(|error| io_error(Phase::Discovery, path, &error))?;
    let raw: RawManifest = serde_json::from_slice(&bytes).map_err(|error| {
        input_error(
            Phase::Discovery,
            format!("invalid manifest {}: {error}", path.display()),
        )
    })?;
    let window_start_ms = parse_f1_timestamp(raw.window_start.as_bytes()).map_err(|error| {
        input_error(
            Phase::Discovery,
            format!(
                "invalid manifest window_start in {}: {error:?}",
                path.display()
            ),
        )
    })?;
    let window_end_ms = parse_f1_timestamp(raw.window_end.as_bytes()).map_err(|error| {
        input_error(
            Phase::Discovery,
            format!(
                "invalid manifest window_end in {}: {error:?}",
                path.display()
            ),
        )
    })?;
    let window = TimestampWindow::new(window_start_ms, window_end_ms).ok_or_else(|| {
        input_error(
            Phase::Discovery,
            format!(
                "manifest window must be nonempty and representable: {}",
                path.display()
            ),
        )
    })?;
    Ok(Manifest {
        window_start_ms,
        window_end_ms,
        window,
    })
}

fn parse_typed_rows<T, F, const N: usize>(
    paths: &[PathBuf],
    phase: Phase,
    build: F,
) -> Result<ParsedBatch<T>, PipelineError>
where
    F: Fn(&[ByteSpan; N]) -> T,
{
    let mut arena = Vec::new();
    let mut rows = Vec::new();
    for path in paths {
        let bytes = fs::read(path).map_err(|error| io_error(phase, path, &error))?;
        for_each_data_line(&bytes, |line| match parse_fixed_fields::<N>(line) {
            ParseOutcome::Accepted(fields) => {
                if let Some(spans) = append_fixed_fields(&mut arena, &fields) {
                    rows.push(ParseOutcome::Accepted(build(&spans)));
                } else {
                    rows.push(ParseOutcome::Rejected(ParseStatus::ArenaOverflow));
                }
            }
            ParseOutcome::Rejected(status) => rows.push(ParseOutcome::Rejected(status)),
        });
    }
    Ok(ParsedBatch::new(arena, rows))
}

fn load_catalog(path: &Path, name: &str) -> Result<ByteCatalog, PipelineError> {
    let bytes = fs::read(path).map_err(|error| io_error(Phase::Discovery, path, &error))?;
    let mut arena = Vec::new();
    let mut entries = Vec::new();
    let mut failure = None;
    let mut row_number = 1_u64;
    for_each_data_line(&bytes, |line| {
        if failure.is_some() {
            return;
        }
        row_number += 1;
        match parse_fixed_fields::<2>(line) {
            ParseOutcome::Accepted(fields) => match append_fixed_fields(&mut arena, &fields) {
                Some(spans) => entries.push((spans[0], spans[1])),
                None => failure = Some((ParseStatus::ArenaOverflow, row_number)),
            },
            ParseOutcome::Rejected(status) => failure = Some((status, row_number)),
        }
    });
    if let Some((status, failed_row)) = failure {
        return Err(input_error(
            Phase::Discovery,
            format!(
                "malformed {name} catalog row {failed_row} in {}: {status:?}",
                path.display()
            ),
        ));
    }
    Ok(ByteCatalog {
        arena: arena.into_boxed_slice(),
        entries,
    })
}

fn for_each_data_line(mut bytes: &[u8], mut visit: impl FnMut(&[u8])) {
    let mut is_header = true;
    while !bytes.is_empty() {
        let (line, remainder) = match bytes.iter().position(|byte| *byte == b'\n') {
            Some(index) => (&bytes[..index], &bytes[index + 1..]),
            None => (bytes, &[][..]),
        };
        if is_header {
            is_header = false;
        } else {
            visit(line);
        }
        bytes = remainder;
    }
}

fn io_error(phase: Phase, path: &Path, error: &std::io::Error) -> PipelineError {
    PipelineError::new(
        PipelineErrorKind::Io,
        format!("read {}: {error}", path.display()),
    )
    .in_phase(phase)
}

fn input_error(phase: Phase, message: impl Into<Box<str>>) -> PipelineError {
    PipelineError::new(PipelineErrorKind::InputContract, message).in_phase(phase)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lf_is_the_only_line_ending_and_header_is_per_file() {
        let mut lines = Vec::new();
        for_each_data_line(b"header\na,b\r\n\nlast,row", |line| {
            lines.push(line.to_vec());
        });
        assert_eq!(lines, [b"a,b\r".to_vec(), Vec::new(), b"last,row".to_vec()]);

        let mut header_only = Vec::new();
        for_each_data_line(b"header\n", |line| header_only.push(line.to_vec()));
        assert!(header_only.is_empty());
    }
}
