use std::process::ExitCode;

use shopping_sessions_domain::{PipelineError, PipelineErrorKind};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
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
        "Slice 0 freezes contracts only; execution slices are not integrated",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_fails_closed() {
        let error = PipelineError::new(PipelineErrorKind::NotImplemented, "not integrated");
        assert_eq!(error.kind, PipelineErrorKind::NotImplemented);
    }
}
