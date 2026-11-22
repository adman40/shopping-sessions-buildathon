use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use shopping_sessions_domain::{Phase, PipelineError, PipelineErrorKind};

/// Complete deterministic inventory of one input root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredInput {
    pub event_files: Vec<PathBuf>,
    pub click_files: Vec<PathBuf>,
    pub link_files: Vec<PathBuf>,
    pub products: PathBuf,
    pub campaigns: PathBuf,
    pub manifest: PathBuf,
}

/// Discovers all direct `*.csv` children and required singleton inputs.
///
/// Returned part files are ordered by their exact platform path ordering.
/// Nested files and names whose extension is not exactly lowercase `csv` are ignored.
///
/// # Errors
///
/// Returns an I/O error when an input directory cannot be read or an input-contract
/// error when a required singleton path is not a regular file.
pub fn discover_input(root: &Path) -> Result<DiscoveredInput, PipelineError> {
    Ok(DiscoveredInput {
        event_files: discover_parts(&root.join("events"))?,
        click_files: discover_parts(&root.join("clicks"))?,
        link_files: discover_parts(&root.join("links"))?,
        products: required_file(root, "products.csv")?,
        campaigns: required_file(root, "campaigns.csv")?,
        manifest: required_file(root, "manifest.json")?,
    })
}

fn discover_parts(directory: &Path) -> Result<Vec<PathBuf>, PipelineError> {
    let entries =
        fs::read_dir(directory).map_err(|error| io_error("read directory", directory, &error))?;
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| io_error("read directory entry", directory, &error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("inspect directory entry", &entry.path(), &error))?;
        let path = entry.path();
        if file_type.is_file() && path.extension() == Some(OsStr::new("csv")) {
            files.push(path);
        }
    }
    files.sort_unstable();
    Ok(files)
}

fn required_file(root: &Path, name: &str) -> Result<PathBuf, PipelineError> {
    let path = root.join(name);
    let metadata =
        fs::metadata(&path).map_err(|error| io_error("inspect required file", &path, &error))?;
    if !metadata.is_file() {
        return Err(PipelineError::new(
            PipelineErrorKind::InputContract,
            format!("required input is not a regular file: {}", path.display()),
        )
        .in_phase(Phase::Discovery));
    }
    Ok(path)
}

fn io_error(action: &str, path: &Path, error: &std::io::Error) -> PipelineError {
    PipelineError::new(
        PipelineErrorKind::Io,
        format!("{action} {}: {error}", path.display()),
    )
    .in_phase(Phase::Discovery)
}
