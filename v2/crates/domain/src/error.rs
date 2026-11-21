use std::error::Error;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::telemetry::Phase;

/// Stable top-level failure categories. Detailed parser drops use `ParseStatus`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineErrorKind {
    Io,
    InputContract,
    Invariant,
    ResourceLimit,
    OutputValidation,
    NotImplemented,
}

/// Owned error crossing slice boundaries.
///
/// Messages are diagnostics rather than a machine-readable protocol; callers
/// branch on `kind` and optional `phase` only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PipelineError {
    pub kind: PipelineErrorKind,
    pub phase: Option<Phase>,
    pub message: Box<str>,
}

impl PipelineError {
    pub fn new(kind: PipelineErrorKind, message: impl Into<Box<str>>) -> Self {
        Self {
            kind,
            phase: None,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn in_phase(mut self, phase: Phase) -> Self {
        self.phase = Some(phase);
        self
    }
}

impl fmt::Display for PipelineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(phase) = self.phase {
            write!(formatter, "{phase:?}: {}", self.message)
        } else {
            formatter.write_str(&self.message)
        }
    }
}

impl Error for PipelineError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_contract_separates_kind_phase_and_diagnostic() {
        let error =
            PipelineError::new(PipelineErrorKind::Io, "cannot read").in_phase(Phase::Discovery);
        assert_eq!(error.kind, PipelineErrorKind::Io);
        assert_eq!(error.phase, Some(Phase::Discovery));
        assert_eq!(error.to_string(), "Discovery: cannot read");
    }
}
