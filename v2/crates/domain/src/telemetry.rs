use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::{PipelineError, PipelineErrorKind};

pub const TELEMETRY_FORMAT_VERSION: u32 = 1;
pub const PHASE_COUNT: usize = 7;

/// Frozen phase names and order for machine-readable telemetry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Discovery,
    Links,
    Events,
    Clicks,
    Sort,
    Sweep,
    Write,
}

impl Phase {
    pub const ALL: [Self; PHASE_COUNT] = [
        Self::Discovery,
        Self::Links,
        Self::Events,
        Self::Clicks,
        Self::Sort,
        Self::Sweep,
        Self::Write,
    ];

    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::Discovery => 0,
            Self::Links => 1,
            Self::Events => 2,
            Self::Clicks => 3,
            Self::Sort => 4,
            Self::Sweep => 5,
            Self::Write => 6,
        }
    }
}

/// Common counters emitted for every phase. Unused counters remain zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhaseCounters {
    pub discovered_files: u64,
    pub input_rows: u64,
    pub accepted_rows: u64,
    pub dropped_rows: u64,
    pub output_rows: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhaseTelemetry {
    pub phase: Phase,
    pub elapsed_ns: u64,
    pub counters: PhaseCounters,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceTelemetry {
    pub cgroup_memory_peak_bytes: Option<u64>,
    pub scratch_high_water_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    Success,
    Failed,
}

/// One JSON object is the telemetry wire format; version 1 freezes these fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunTelemetry {
    pub format_version: u32,
    pub outcome: RunOutcome,
    pub phases: [PhaseTelemetry; PHASE_COUNT],
    pub resources: ResourceTelemetry,
}

impl RunTelemetry {
    /// Serializes the versioned telemetry object and one trailing newline.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the wire object cannot be encoded.
    pub fn to_json_line(&self) -> Result<String, serde_json::Error> {
        let mut line = serde_json::to_string(self)?;
        line.push('\n');
        Ok(line)
    }
}

/// Sequential phase timer. It rejects missing, repeated, or out-of-order phases.
pub struct TelemetryRecorder {
    phases: [PhaseTelemetry; PHASE_COUNT],
    next_phase: usize,
    active: Option<(Phase, Instant)>,
    resources: ResourceTelemetry,
}

impl Default for TelemetryRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetryRecorder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            phases: Phase::ALL.map(|phase| PhaseTelemetry {
                phase,
                elapsed_ns: 0,
                counters: PhaseCounters::default(),
            }),
            next_phase: 0,
            active: None,
            resources: ResourceTelemetry::default(),
        }
    }

    /// Starts the next required phase.
    ///
    /// # Errors
    ///
    /// Returns an invariant error for overlapping or out-of-order phases.
    pub fn start_phase(&mut self, phase: Phase) -> Result<(), PipelineError> {
        if self.active.is_some() || Phase::ALL.get(self.next_phase) != Some(&phase) {
            return Err(PipelineError::new(
                PipelineErrorKind::Invariant,
                format!("phase {phase:?} started out of order"),
            ));
        }
        self.active = Some((phase, Instant::now()));
        Ok(())
    }

    /// Returns counters for the active phase.
    ///
    /// # Errors
    ///
    /// Returns an invariant error if `phase` is not active.
    pub fn counters_mut(&mut self, phase: Phase) -> Result<&mut PhaseCounters, PipelineError> {
        if self.active.map(|(active, _)| active) != Some(phase) {
            return Err(PipelineError::new(
                PipelineErrorKind::Invariant,
                format!("phase {phase:?} is not active"),
            ));
        }
        Ok(&mut self.phases[phase.index()].counters)
    }

    /// Stops the active phase timer.
    ///
    /// # Errors
    ///
    /// Returns an invariant error if no phase or another phase is active.
    pub fn finish_phase(&mut self, phase: Phase) -> Result<(), PipelineError> {
        let Some((active, started)) = self.active.take() else {
            return Err(PipelineError::new(
                PipelineErrorKind::Invariant,
                "no telemetry phase is active",
            ));
        };
        if active != phase {
            self.active = Some((active, started));
            return Err(PipelineError::new(
                PipelineErrorKind::Invariant,
                format!("cannot finish {phase:?} while {active:?} is active"),
            ));
        }
        self.phases[phase.index()].elapsed_ns =
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.next_phase += 1;
        Ok(())
    }

    pub fn set_resources(&mut self, resources: ResourceTelemetry) {
        self.resources = resources;
    }

    /// Finalizes the wire object after all seven phases complete.
    ///
    /// # Errors
    ///
    /// Returns an invariant error if any required phase is incomplete.
    pub fn finish(self, outcome: RunOutcome) -> Result<RunTelemetry, PipelineError> {
        if self.active.is_some() || self.next_phase != PHASE_COUNT {
            return Err(PipelineError::new(
                PipelineErrorKind::Invariant,
                "all telemetry phases must finish before the run",
            ));
        }
        Ok(RunTelemetry {
            format_version: TELEMETRY_FORMAT_VERSION,
            outcome,
            phases: self.phases,
            resources: self.resources,
        })
    }
}

/// Samples scratch usage at phase boundaries and retains the maximum observed size.
#[derive(Clone, Debug)]
pub struct ScratchTracker {
    root: PathBuf,
    high_water_bytes: u64,
}

impl ScratchTracker {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            high_water_bytes: 0,
        }
    }

    /// Samples current scratch bytes and updates the high-water mark.
    ///
    /// # Errors
    ///
    /// Returns filesystem traversal errors other than concurrent file removal.
    pub fn sample(&mut self) -> io::Result<u64> {
        let current = directory_size(&self.root)?;
        self.high_water_bytes = self.high_water_bytes.max(current);
        Ok(current)
    }

    #[must_use]
    pub const fn high_water_bytes(&self) -> u64 {
        self.high_water_bytes
    }
}

/// Reads cgroup v2 first, then the legacy v1 peak file.
///
/// # Errors
///
/// Returns I/O or integer-format errors from an available cgroup peak file.
pub fn read_cgroup_memory_peak(cgroup_root: &Path) -> io::Result<Option<u64>> {
    for relative in [
        "memory.peak",
        "memory.max_usage_in_bytes",
        "memory/memory.max_usage_in_bytes",
    ] {
        let path = cgroup_root.join(relative);
        match fs::read_to_string(&path) {
            Ok(value) => {
                let parsed = value.trim().parse::<u64>().map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{}: {error}", path.display()),
                    )
                })?;
                return Ok(Some(parsed));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

fn directory_size(root: &Path) -> io::Result<u64> {
    let mut total = 0_u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {}
            Ok(metadata) if metadata.is_file() => {
                total = total.saturating_add(metadata.len());
            }
            Ok(metadata) if metadata.is_dir() => {
                for entry in fs::read_dir(path)? {
                    pending.push(entry?.path());
                }
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorder_requires_all_phases_in_order() {
        let mut recorder = TelemetryRecorder::new();
        assert!(recorder.start_phase(Phase::Links).is_err());
        for phase in Phase::ALL {
            recorder.start_phase(phase).unwrap();
            recorder.counters_mut(phase).unwrap().input_rows = 1;
            recorder.finish_phase(phase).unwrap();
        }
        let telemetry = recorder.finish(RunOutcome::Success).unwrap();
        assert_eq!(telemetry.phases.map(|item| item.phase), Phase::ALL);
        assert!(
            telemetry
                .phases
                .iter()
                .all(|item| item.counters.input_rows == 1)
        );
    }

    #[test]
    fn telemetry_json_contract_is_stable() {
        let phases = Phase::ALL.map(|phase| PhaseTelemetry {
            phase,
            elapsed_ns: 1,
            counters: PhaseCounters::default(),
        });
        let telemetry = RunTelemetry {
            format_version: TELEMETRY_FORMAT_VERSION,
            outcome: RunOutcome::Success,
            phases,
            resources: ResourceTelemetry {
                cgroup_memory_peak_bytes: Some(42),
                scratch_high_water_bytes: Some(7),
            },
        };
        let value: serde_json::Value =
            serde_json::from_str(&telemetry.to_json_line().unwrap()).unwrap();
        assert_eq!(value["format_version"], 1);
        assert_eq!(value["phases"][0]["phase"], "discovery");
        assert_eq!(value["phases"][6]["phase"], "write");
        assert_eq!(value["resources"]["cgroup_memory_peak_bytes"], 42);
    }

    #[test]
    fn scratch_tracker_keeps_a_high_water_mark() {
        let root = std::env::temp_dir().join(format!("v2-scratch-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("nested")).unwrap();
        fs::write(root.join("nested/data"), b"1234").unwrap();
        let mut tracker = ScratchTracker::new(&root);
        assert_eq!(tracker.sample().unwrap(), 4);
        fs::remove_file(root.join("nested/data")).unwrap();
        assert_eq!(tracker.sample().unwrap(), 0);
        assert_eq!(tracker.high_water_bytes(), 4);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cgroup_reader_supports_v2_format() {
        let root = std::env::temp_dir().join(format!("v2-cgroup-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("memory.peak"), "123\n").unwrap();
        assert_eq!(read_cgroup_memory_peak(&root).unwrap(), Some(123));
        fs::remove_dir_all(root).unwrap();
    }
}
