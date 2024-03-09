use serde::Serialize;
use std::convert::AsRef;
use std::num::ParseIntError;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

pub mod lane;
pub mod manager;
pub mod run_completion;

pub use manager::DirManager;
pub use manager::SeqDirState;
pub use run_completion::CompletionStatus;
pub use run_completion::Message;

use crate::run_completion::parse_run_completion;

pub const COPY_COMPLETE_TXT: &str = "CopyComplete.txt";
pub const RTA_COMPLETE_TXT: &str = "RTAComplete.txt";
pub const SEQUENCE_COMPLETE_TXT: &str = "SequenceComplete.txt";
pub const SAMPLESHEET_CSV: &str = "SampleSheet.csv";
pub const RUN_INFO_XML: &str = "RunInfo.xml";
pub const RUN_COMPLETION_STATUS_XML: &str = "RunCompletionStatus.xml";
pub const RUN_PARAMS_XML: &str = "RunParameters.xml";

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SeqDirError {
    #[error("cannot find {0} or it is not readable")]
    NotFound(PathBuf),
    #[error("cannot find lane directories")]
    MissingLaneDirs,
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("found no cycles")]
    MissingCycles,
    #[error("found no bcls for cycle {0}")]
    MissingBcls(u16),
    #[error("expected cycle directory in format of C###.#, found: {0}")]
    BadCycle(PathBuf),
    #[error(transparent)]
    ParseIntError(#[from] ParseIntError),
    #[error("unexpected run completion status: {0}")]
    CompletionStatus(CompletionStatus),
}

#[derive(Clone, Debug, Serialize, PartialEq)]
/// An Illumina sequencing directory
pub struct SeqDir {
    root: PathBuf,
    #[serde(skip)]
    samplesheet: PathBuf,
    #[serde(skip)]
    run_info: PathBuf,
    #[serde(skip)]
    run_params: PathBuf,
    #[serde(skip)]
    run_completion: PathBuf,
}

impl SeqDir {
    /// Create a new SeqDir
    ///
    /// Succeeds as long as `path` is readable and is a directory.
    /// To enforce that the directory is a well-formed, completed sequencing directory, use
    /// `from_completed`.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, SeqDirError> {
        if path.as_ref().is_dir() {
            Ok(SeqDir {
                root: path.as_ref().to_path_buf(),
                samplesheet: path.as_ref().join(SAMPLESHEET_CSV),
                run_info: path.as_ref().join(RUN_INFO_XML),
                run_params: path.as_ref().join(RUN_PARAMS_XML),
                run_completion: path.as_ref().join(RUN_COMPLETION_STATUS_XML),
            })
        } else {
            Err(SeqDirError::NotFound(path.as_ref().to_path_buf()))
        }
    }

    /// Create a new SeqDir from a completed sequencing directory.
    ///
    /// Errors if the sequencing directory is not complete.
    /// Completion is determined by the following:
    /// 1. CopyComplete.txt is present
    /// 2. RunCompletionStatus (if present) is CompletedAsPlanned
    pub fn from_completed<P: AsRef<Path>>(path: P) -> Result<Self, SeqDirError> {
        let seq_dir = Self::from_path(&path)?;
        seq_dir
            .is_copy_complete()
            .then(|| Ok::<(), SeqDirError>(()))
            .ok_or_else(|| SeqDirError::NotFound(seq_dir.root().join(COPY_COMPLETE_TXT)))??;

        // If RunCompletionStatus exists, verify it, but cannot rely on this
        // since not all platforms output this file
        match seq_dir.get_completion_status() {
            None => {}
            Some(Ok(status)) => match status {
                CompletionStatus::CompletedAsPlanned(..) => {}
                _ => return Err(SeqDirError::CompletionStatus(status)),
            },
            Some(Err(e)) => return Err(e),
        };

        Ok(seq_dir)
    }

    /// Try to get the root of the sequencing directory.
    ///
    /// Returns SeqDirError::NotFound if directory is inaccessible.
    pub fn try_root(&self) -> Result<&Path, SeqDirError> {
        self.root()
            .is_dir()
            .then(|| self.root())
            .ok_or_else(|| SeqDirError::NotFound(self.root().to_owned()))
    }

    /// Returns true if CopyComplete.txt exists.
    pub fn is_copy_complete(&self) -> bool {
        self.root().join(COPY_COMPLETE_TXT).exists()
    }

    /// Returns true if RTAComplete.txt exists.
    pub fn is_rta_complete(&self) -> bool {
        self.root().join(RTA_COMPLETE_TXT).exists()
    }

    /// Returns true if SequenceComplete.txt exists.
    pub fn is_sequence_complete(&self) -> bool {
        self.root().join(SEQUENCE_COMPLETE_TXT).exists()
    }

    /// Get an arbitrary file rooted at the base of the sequencing directory.
    ///
    /// Returns SeqDirError::NotFound if file does not exist or is inaccessible.
    pub fn get_file<P: AsRef<Path>>(&self, path: P) -> Result<PathBuf, SeqDirError> {
        self.root()
            .join(&path)
            .is_file()
            .then(|| self.root().join(&path))
            .ok_or_else(|| SeqDirError::NotFound(self.root().join(&path)))
    }

    /// Returns true if the root directory is readable.
    pub fn is_available(&self) -> bool {
        self.try_root().is_ok()
    }

    /// Returns true if the root directory cannot be read
    pub fn is_unavailable(&self) -> bool {
        self.try_root().is_err()
    }

    /// Attempt to parse RunCompletionStatus.xml and return a
    /// Option<Result<[CompletionStatus]>>
    pub fn get_completion_status(&self) -> Option<Result<CompletionStatus, SeqDirError>> {
        Some(parse_run_completion(self.run_completion_status()?).map_err(SeqDirError::from))
    }

    /// Attempt to determine if a run has failed sequencing.
    ///
    /// Uses RunCompletionStatus.xml. If RunCompletionStatus is not available, returns false.
    /// unlike other `is_` library methods, this is fallible because it must parse a file.
    pub fn is_failed(&self) -> Result<bool, SeqDirError> {
        match self.get_completion_status() {
            None => Ok(false),
            Some(Err(e)) => Err(e),
            Some(Ok(res)) => match res {
                CompletionStatus::CompletedAsPlanned(..) => Ok(false),
                _ => Ok(true),
            },
        }
    }

    /// Returns true if SequenceComplete.txt is not present
    ///
    /// Convenience method, inverts `is_sequence_complete`
    pub fn is_sequencing(&self) -> bool {
        !self.is_sequence_complete()
    }

    /// Returns reference to seqdir root
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get the path to SampleSheet.csv
    ///
    /// Returns SeqDirError::NotFound if path does not exist or is inaccessible.
    pub fn samplesheet(&self) -> Result<&Path, SeqDirError> {
        self.samplesheet
            .is_file()
            .then_some(self.samplesheet.as_path())
            .ok_or_else(|| SeqDirError::NotFound(self.samplesheet.clone()))
    }

    /// Get the path to RunInfo.xml
    ///
    /// Returns SeqDirError::NotFound if path does not exist or is inaccessible.
    pub fn run_info(&self) -> Result<&Path, SeqDirError> {
        self.run_info
            .is_file()
            .then_some(self.run_info.as_path())
            .ok_or_else(|| SeqDirError::NotFound(self.run_info.clone()))
    }

    /// Get the path to RunParameters.xml
    ///
    /// Returns SeqDirError::NotFound if path does not exist or is inaccessible.
    pub fn run_params(&self) -> Result<&Path, SeqDirError> {
        self.run_params
            .is_file()
            .then_some(self.run_params.as_path())
            .ok_or_else(|| SeqDirError::NotFound(self.run_params.clone()))
    }

    /// Get the path to RunCompletionStatus.xml
    ///
    /// Returns Option because not all illumina sequencers / platform versions generate this file.
    /// To actually parse RunCompletionStatus.xml, see
    /// [get_completion_status](crate::SeqDir.get_completion_status)
    pub fn run_completion_status(&self) -> Option<&Path> {
        self.run_completion
            .is_file()
            .then_some(self.run_completion.as_path())
            .or(None)
    }
}

#[cfg(test)]
mod tests {

    use crate::{SeqDir, SeqDirError};

    const COMPLETE: &str = "test_data/seq_complete/";
    const FAILED: &str = "test_data/seq_failed/";
    const TRANSFERRING: &str = "test_data/seq_transferring/";
    const SEQUENCING: &str = "test_data/seq_sequencing/";

    #[test]
    fn complete_seqdir() {
        let seq_dir = SeqDir::from_completed(COMPLETE).unwrap();
        seq_dir.samplesheet().unwrap();
        seq_dir.run_info().unwrap();
        seq_dir.run_params().unwrap();
        assert!(seq_dir.is_available());
        assert!(seq_dir.is_sequence_complete());
        assert!(seq_dir.is_copy_complete());
        assert!(seq_dir.is_rta_complete());
        assert!(!seq_dir.is_sequencing());
    }

    #[test]
    fn failed_seqdir() {
        let seq_dir = SeqDir::from_path(FAILED).unwrap();
        assert!(seq_dir.is_failed().unwrap());
        assert!(matches!(
            SeqDir::from_completed(FAILED),
            Err(SeqDirError::CompletionStatus(..))
        ));
    }

    #[test]
    fn transferring_seqdir() {
        let seq_dir = SeqDir::from_path(TRANSFERRING).unwrap();
        assert!(seq_dir.is_available());
        assert!(seq_dir.is_sequence_complete());
        assert!(!seq_dir.is_sequencing());
        assert!(!seq_dir.is_failed().unwrap());
        assert!(!seq_dir.is_copy_complete());
        assert!(seq_dir.is_rta_complete());
    }

    #[test]
    fn sequencing_seqdir() {
        let seq_dir = SeqDir::from_path(SEQUENCING).unwrap();
        assert!(seq_dir.is_available());
        assert!(!seq_dir.is_sequence_complete());
        assert!(seq_dir.is_sequencing());
        assert!(!seq_dir.is_failed().unwrap());
        assert!(!seq_dir.is_copy_complete());
        assert!(seq_dir.is_rta_complete());
    }
}
