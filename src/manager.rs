use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{SeqDir, SeqDirError};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state")]
pub enum SeqDirState {
    Complete(CompleteSeqDir),
    Transferring(TransferringSeqDir),
    Sequencing(SequencingSeqDir),
    Failed(FailedSeqDir),
}

pub trait Transition {
    fn transition(self) -> SeqDirState;
}

#[derive(Debug, Clone, Serialize)]
pub struct CompleteSeqDir {
    #[serde(flatten)]
    seq_dir: SeqDir,
    since: DateTime<Utc>,
    available: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnavailableSeqDir {
    #[serde(flatten)]
    seq_dir: SeqDir,
    since: DateTime<Utc>,
    available: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SequencingSeqDir {
    #[serde(flatten)]
    seq_dir: SeqDir,
    since: DateTime<Utc>,
    available: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedSeqDir {
    #[serde(flatten)]
    seq_dir: SeqDir,
    since: DateTime<Utc>,
    available: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransferringSeqDir {
    #[serde(flatten)]
    seq_dir: SeqDir,
    since: DateTime<Utc>,
    available: bool,
}

impl Transition for CompleteSeqDir {
    fn transition(self) -> SeqDirState {
        SeqDirState::Complete(CompleteSeqDir {
            available: self.seq_dir.is_available(),
            ..self
        })
    }
}

impl Transition for TransferringSeqDir {
    fn transition(self) -> SeqDirState {
        if self.seq_dir.is_unavailable() {
            return SeqDirState::Transferring(TransferringSeqDir {
                available: false,
                ..self
            });
        }
        if self.seq_dir.is_copy_complete() {
            SeqDirState::Complete(CompleteSeqDir::from(self))
        } else if self.seq_dir.is_failed().unwrap_or(false) {
            SeqDirState::Failed(FailedSeqDir::from(self))
        } else {
            SeqDirState::Transferring(self)
        }
    }
}

impl Transition for SequencingSeqDir {
    fn transition(self) -> SeqDirState {
        if self.seq_dir.is_unavailable() {
            return SeqDirState::Sequencing(SequencingSeqDir {
                available: false,
                ..self
            });
        }
        if self.seq_dir.is_failed().unwrap_or(false) {
            SeqDirState::Failed(FailedSeqDir::from(self))
        } else if self.seq_dir.is_sequencing() {
            return SeqDirState::Sequencing(self);
        } else if self.seq_dir.is_copy_complete() {
            SeqDirState::Complete(CompleteSeqDir::from(self))
        } else {
            SeqDirState::Transferring(TransferringSeqDir::from(self))
        }
    }
}

impl Transition for FailedSeqDir {
    fn transition(self) -> SeqDirState {
        SeqDirState::Failed(FailedSeqDir {
            available: self.seq_dir.is_available(),
            ..self
        })
    }
}

impl From<SequencingSeqDir> for CompleteSeqDir {
    /// Sequencing -> Available
    fn from(value: SequencingSeqDir) -> Self {
        CompleteSeqDir {
            available: value.seq_dir.is_available(),
            seq_dir: value.seq_dir,
            since: Utc::now(),
        }
    }
}

impl From<SequencingSeqDir> for FailedSeqDir {
    /// Sequencing -> Failed
    fn from(value: SequencingSeqDir) -> Self {
        FailedSeqDir {
            available: value.seq_dir.is_available(),
            seq_dir: value.seq_dir,
            since: Utc::now(),
        }
    }
}

impl From<SequencingSeqDir> for TransferringSeqDir {
    /// Sequencing -> Transferring
    fn from(value: SequencingSeqDir) -> Self {
        TransferringSeqDir {
            available: value.seq_dir.is_available(),
            seq_dir: value.seq_dir,
            since: Utc::now(),
        }
    }
}

impl From<TransferringSeqDir> for CompleteSeqDir {
    /// Transferring -> Available
    fn from(value: TransferringSeqDir) -> Self {
        CompleteSeqDir {
            available: value.seq_dir.is_available(),
            seq_dir: value.seq_dir,
            since: Utc::now(),
        }
    }
}

impl From<TransferringSeqDir> for FailedSeqDir {
    /// Transferring -> Failed
    fn from(value: TransferringSeqDir) -> Self {
        FailedSeqDir {
            available: value.seq_dir.is_available(),
            seq_dir: value.seq_dir,
            since: Utc::now(),
        }
    }
}

impl SeqDirState {
    /// Reference to inner SeqDir
    fn dir(&self) -> &SeqDir {
        match self {
            SeqDirState::Failed(dir) => &dir.seq_dir,
            SeqDirState::Complete(dir) => &dir.seq_dir,
            SeqDirState::Sequencing(dir) => &dir.seq_dir,
            SeqDirState::Transferring(dir) => &dir.seq_dir,
        }
    }

    /// Timestamp of when state was entered
    fn since(&self) -> &DateTime<Utc> {
        match self {
            SeqDirState::Failed(dir) => &dir.since,
            SeqDirState::Complete(dir) => &dir.since,
            SeqDirState::Sequencing(dir) => &dir.since,
            SeqDirState::Transferring(dir) => &dir.since,
        }
    }

    /// Mutable reference to inner SeqDir
    fn dir_mut(&mut self) -> &mut SeqDir {
        match self {
            SeqDirState::Failed(dir) => &mut dir.seq_dir,
            SeqDirState::Complete(dir) => &mut dir.seq_dir,
            SeqDirState::Sequencing(dir) => &mut dir.seq_dir,
            SeqDirState::Transferring(dir) => &mut dir.seq_dir,
        }
    }

    fn transition(self) -> Self {
        match self {
            SeqDirState::Complete(dir) => dir.transition(),
            SeqDirState::Failed(dir) => dir.transition(),
            SeqDirState::Sequencing(dir) => dir.transition(),
            SeqDirState::Transferring(dir) => dir.transition(),
        }
    }
}

#[derive(Clone)]
struct DirManager {
    seq_dir: SeqDirState,
}

impl DirManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, SeqDirError> {
        let mut dir_manager = DirManager {
            seq_dir: SeqDirState::Sequencing(SequencingSeqDir {
                seq_dir: SeqDir::from_path(&path)?,
                since: Utc::now(),
                available: path.as_ref().exists(),
            }),
        };
        dir_manager.poll();
        Ok(dir_manager)
    }

    /// Consume the DirManager, returning contained SeqDir, regardless of state.
    /// Discards associated timestamp.
    pub fn into_inner(self) -> Result<SeqDir, SeqDirError> {
        match self.seq_dir {
            SeqDirState::Complete(dir) => Ok(dir.seq_dir),
            SeqDirState::Sequencing(dir) => Ok(dir.seq_dir),
            SeqDirState::Failed(dir) => Ok(dir.seq_dir),
            SeqDirState::Transferring(dir) => Ok(dir.seq_dir),
        }
    }

    /// Reference to the inner SeqDir being managed
    pub fn inner(&self) -> &SeqDir {
        self.seq_dir.dir()
    }

    /// Mutable reference to inner SeqDir being managed
    /// It is not recommended that you mutate the inner Seqdir directly unless you have a very good
    /// reason to. Doing so can cause unexpected behavior.
    pub fn inner_mut(&mut self) -> &mut SeqDir {
        self.seq_dir.dir_mut()
    }

    /// Current state
    pub fn state(&self) -> &SeqDirState {
        &self.seq_dir
    }

    /// Check if the contained SeqDir should be moved to a new state, and transition if so
    pub fn poll(&mut self) -> &SeqDirState {
        *self = match std::mem::replace(&mut self.seq_dir, _default()) {
            state => DirManager {
                seq_dir: state.transition(),
                ..*self
            },
        };
        self.state()
    }

    /// Timestamp of when the DirManager's SeqDir entered its current state
    pub fn since(&self) -> &DateTime<Utc> {
        self.seq_dir.since()
    }
}

/// This SeqDirState contains a completely invalid SeqDir and is only used as a placeholder when
/// `poll`ing for updated state. This really should not be used anywhere else.
fn _default() -> SeqDirState {
    // TODO the overhead of reconstructing this every time isn't great
    SeqDirState::Sequencing(SequencingSeqDir {
        seq_dir: SeqDir {
            root: Path::new("").to_owned(),
            samplesheet: Path::new("").to_owned(),
            run_info: Path::new("").to_owned(),
            run_params: Path::new("").to_owned(),
            run_completion: Path::new("").to_owned(),
        },
        since: Utc::now(),
        available: false,
    })
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr};

    use super::{DirManager, SeqDirState};

    const COMPLETE: &str = "test_data/seq_complete/";
    const FAILED: &str = "test_data/seq_failed/";
    const TRANSFERRING: &str = "test_data/seq_transferring/";

    #[test]
    fn goes_to_complete() {
        let mut manager = DirManager::new(COMPLETE).unwrap();
        match manager.state() {
            SeqDirState::Complete(..) => {}
            x => panic!("expected SeqDirState::Available, got {x:?}"),
        };
        manager.poll();
        match manager.state() {
            SeqDirState::Complete(..) => {}
            x => panic!("expected SeqDirState::Available, got {x:?}"),
        };
    }

    #[test]
    fn goes_to_failed() {
        let mut manager = DirManager::new(FAILED).unwrap();
        match manager.state() {
            SeqDirState::Failed(..) => {}
            x => panic!("expected SeqDirState::Failed, got {x:?}"),
        };
        manager.poll();
        match manager.state() {
            SeqDirState::Failed(..) => {}
            x => panic!("expected SeqDirState::Failed, got {x:?}"),
        };
    }

    #[test]
    fn goes_to_unavailable() {
        // you cannot manage a directory that doesn't exist
        let mut manager = DirManager::new(COMPLETE).unwrap();
        match manager.state() {
            SeqDirState::Complete(..) => {}
            x => panic!("expected SeqDirState::Available, got {x:?}"),
        };
        manager.inner_mut().root = PathBuf::from_str("/dev/null").unwrap();
        manager.poll();
        match manager.seq_dir.dir().is_available() {
            false => {}
            true => panic!("expected false"),
        };
        manager.inner_mut().root = PathBuf::from_str(COMPLETE).unwrap();
        manager.poll();
        match manager.state() {
            SeqDirState::Complete(..) => {}
            x => panic!("expected SeqDirState::Available, got {x:?}"),
        };
    }

    #[test]
    fn transferring_to_complete() {
        let copy_complete = PathBuf::from_str(TRANSFERRING)
            .unwrap()
            .join("CopyComplete.txt");
        let mut manager = DirManager::new(TRANSFERRING).unwrap();
        match manager.state() {
            SeqDirState::Transferring(..) => {}
            x => panic!("expected SeqDirState::Transferring, got {x:?}"),
        };
        std::fs::File::create(&copy_complete).unwrap();
        manager.poll();
        std::fs::remove_file(&copy_complete).unwrap();
        match manager.state() {
            SeqDirState::Complete(..) => {}
            x => panic!("expected SeqDirState::Available, got {x:?}"),
        };
    }

    #[test]
    fn test_serialize_to_json() {
        use serde_json;

        let mut manager = DirManager::new(COMPLETE).unwrap();
        match manager.state() {
            SeqDirState::Complete(..) => {}
            x => panic!("expected SeqDirState::Available, got {x:?}"),
        };
        manager.poll();

        serde_json::to_string(manager.state()).unwrap();
    }
}
