use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

use serde_json::{Value, json};
use shopping_sessions_domain::{PipelineError, PipelineErrorKind};
use shopping_sessions_identity as _;
use shopping_sessions_output as _;
use shopping_sessions_parser as _;
use shopping_sessions_reduce as _;
use shopping_sessions_sweep as _;

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

fn run() -> Result<(), PipelineError> {
    for variable in ["DATA_DIR", "OUTPUT_DIR", "SCRATCH_DIR"] {
        std::env::var_os(variable).ok_or_else(|| {
            PipelineError::new(
                PipelineErrorKind::InputContract,
                format!("required environment variable {variable} is not set"),
            )
        })?;
    }
    Err(PipelineError::new(
        PipelineErrorKind::NotImplemented,
        "shadow integration is incomplete: Slice 3 event reduction is not integrated",
    ))
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
    fn incomplete_integration_failure_is_machine_readable() {
        let error = PipelineError::new(PipelineErrorKind::NotImplemented, "not integrated");
        let record = failure_record(&error);
        assert_eq!(record["format_version"], 1);
        assert_eq!(record["outcome"], "failed");
        assert_eq!(record["error"]["kind"], "not_implemented");
        assert!(record["error"]["phase"].is_null());
        assert_eq!(record["error"]["message"], "not integrated");
    }
}
