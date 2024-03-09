//! Monitor the state of a sequencing directory
//!
//! The manager implements a state machine with the following transitions:
//!
//!```none
//!        ┌──────────────────────┐
//!        ▼                      │
//!      ┌──────────────┐         │
//!      │              │ ───┐    │
//!      │    Failed    │    │    │
//!      │              │ ◀──┘    │
//!      └──────────────┘         │
//!        ▲                      │
//!        │                      │
//!        │                      │
//!      ┌──────────────┐         │
//! ┌─── │              │         │
//! │    │  Sequencing  │         │
//! └──▶ │              │ ─┐      │
//!      └──────────────┘  │      │
//!        │               │      │
//!        │               │      │
//!        ▼               │      │
//!      ┌──────────────┐  │      │
//! ┌─── │              │  │      │
//! │    │ Transferring │  │      │
//! └──▶ │              │ ─┼──────┘
//!      └──────────────┘  │
//!        │               │
//!        │               │
//!        ▼               │
//!      ┌──────────────┐  │
//! ┌─── │              │  │
//! │    │   Complete   │  │
//! └──▶ │              │ ◀┘
//!      └──────────────┘
//!```
//! Self-transitions are explicitly defined because even terminal states
//! ([Complete](SeqDirState::Complete) and [Failed](SeqDirState::Failed)) may still update their
//! [Availability] on every call to [poll](DirManager::poll()).
//!
//! Each state is represented by both an enum variant ([SeqDirState]) and a corresponding struct
//! wrapped by the variant. [Transition] is implemented for each struct, which has a single method
//! [transition](Transition::transition()) that defines when and how the current state should be
//! consumed to produce another valid state. The logic for determining these transitions is
//! explained in the docstring of each impl block, but they rely entirely on basic methods provided
//! by the inner [SeqDir] itself, there is no magic.
//!
//! The state machine may only be updated as frequently as it is polled, it will not progress on
//! its own.
//!
//! All states are serializable so that they may be treated as emitted events.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{SeqDir, SeqDirError};

pub(crate) mod sealed {
    pub trait Sealed {}
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "state")]
/// The current state of the SeqDir.
///
/// Each variant wraps the corresponding struct.
pub enum SeqDirState {
    Complete(CompleteSeqDir),
    Transferring(TransferringSeqDir),
    Sequencing(SequencingSeqDir),
    Failed(FailedSeqDir),
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
/// The availability of a directory.
///
/// Determined by whether it can be read or not.
/// Contains a [DateTime] in UTC of when the availability last changed.
pub enum Availability {
    Available(DateTime<Utc>),
    Unavailable(DateTime<Utc>),
}

impl Availability {
    /// Compares self to updated availability. If it differs, emit
    /// the correct variant with updated timestamp. Otherwise,
    /// return self with original timestamp.
    pub fn check<P: AsRef<Path>>(self, path: P) -> Availability {
        let exists = path.as_ref().exists();
        match self {
            Availability::Available(..) => {
                if exists {
                    self
                } else {
                    Availability::Unavailable(Utc::now())
                }
            }
            Availability::Unavailable(..) => {
                if exists {
                    Availability::Available(Utc::now())
                } else {
                    self
                }
            }
        }
    }
}

/// Implemented for structs that can transition to another state.
pub trait Transition: sealed::Sealed {
    /// Attempt to perform a state transition.
    ///
    /// On transition, struct is consumed and wrapped by the appropriate [SeqDirState]
    fn transition(self) -> SeqDirState;
}

#[derive(Debug, Clone, Serialize, PartialEq)]
/// A directory whose run has completed sequencing.
pub struct CompleteSeqDir {
    #[serde(flatten)]
    seq_dir: SeqDir,
    since: DateTime<Utc>,
    availability: Availability,
}

/// A directory whose run is actively sequencing
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SequencingSeqDir {
    #[serde(flatten)]
    seq_dir: SeqDir,
    since: DateTime<Utc>,
    availability: Availability,
}

/// A directory whose run has failed sequencing.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FailedSeqDir {
    #[serde(flatten)]
    seq_dir: SeqDir,
    since: DateTime<Utc>,
    availability: Availability,
}

/// A directory whose run is transferring.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TransferringSeqDir {
    #[serde(flatten)]
    seq_dir: SeqDir,
    since: DateTime<Utc>,
    availability: Availability,
}

impl sealed::Sealed for CompleteSeqDir {}
impl sealed::Sealed for TransferringSeqDir {}
impl sealed::Sealed for FailedSeqDir {}
impl sealed::Sealed for SequencingSeqDir {}

/// Completed must only transition to itself, possibly updating its [Availability]
impl Transition for CompleteSeqDir {
    fn transition(self) -> SeqDirState {
        SeqDirState::Complete(CompleteSeqDir {
            availability: self.availability.check(self.seq_dir.root()),
            ..self
        })
    }
}

/// Transferring may transition to itself, Failed, or Complete
///
/// Availability is checked first. If the directory is Unavailable, no transition will occur.
/// If CopyComplete.txt is found, transitions to Completed.
/// If [is_failed](SeqDir::is_failed()) returns true, transitions to Failed.
/// Otherwise, availability is updated and returns self.
impl Transition for TransferringSeqDir {
    fn transition(self) -> SeqDirState {
        if self.seq_dir.is_unavailable() {
            return SeqDirState::Transferring(TransferringSeqDir {
                availability: self.availability.check(self.seq_dir.root()),
                ..self
            });
        }
        if self.seq_dir.is_copy_complete() {
            SeqDirState::Complete(CompleteSeqDir::from(self))
        } else if self.seq_dir.is_failed().unwrap_or(false) {
            SeqDirState::Failed(FailedSeqDir::from(self))
        } else {
            SeqDirState::Transferring(TransferringSeqDir {
                availability: self.availability.check(self.seq_dir.root()),
                ..self
            })
        }
    }
}

/// Sequencing may transfer to any other state
///
/// Availability is checked first. If the directory is Unavailable, no transition will occur.
/// If [is_failed](SeqDir::is_failed()) returns true, transitions to Failed.
/// If SequenceComplete.txt is not found, availablility is updated and returns self.
/// If CopyComplete.txt is found, transitions to Completed.
/// Otherwise, is assumed to be Transferring (as SequenceComplete is present but not CopyComplete).
impl Transition for SequencingSeqDir {
    fn transition(self) -> SeqDirState {
        if self.seq_dir.is_unavailable() {
            return SeqDirState::Sequencing(SequencingSeqDir {
                availability: self.availability.check(self.seq_dir.root()),
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

/// Failed must only transition to itself, possibly updating its [Availability].
impl Transition for FailedSeqDir {
    fn transition(self) -> SeqDirState {
        SeqDirState::Failed(FailedSeqDir {
            availability: self.availability.check(self.seq_dir.root()),
            ..self
        })
    }
}

impl From<SequencingSeqDir> for CompleteSeqDir {
    /// Sequencing -> Available
    fn from(value: SequencingSeqDir) -> Self {
        CompleteSeqDir {
            availability: value.availability.check(value.seq_dir.root()),
            seq_dir: value.seq_dir,
            since: Utc::now(),
        }
    }
}

impl From<SequencingSeqDir> for FailedSeqDir {
    /// Sequencing -> Failed
    fn from(value: SequencingSeqDir) -> Self {
        FailedSeqDir {
            availability: value.availability.check(value.seq_dir.root()),
            seq_dir: value.seq_dir,
            since: Utc::now(),
        }
    }
}

impl From<SequencingSeqDir> for TransferringSeqDir {
    /// Sequencing -> Transferring
    fn from(value: SequencingSeqDir) -> Self {
        TransferringSeqDir {
            availability: value.availability.check(value.seq_dir.root()),
            seq_dir: value.seq_dir,
            since: Utc::now(),
        }
    }
}

impl From<TransferringSeqDir> for CompleteSeqDir {
    /// Transferring -> Available
    fn from(value: TransferringSeqDir) -> Self {
        CompleteSeqDir {
            availability: value.availability.check(value.seq_dir.root()),
            seq_dir: value.seq_dir,
            since: Utc::now(),
        }
    }
}

impl From<TransferringSeqDir> for FailedSeqDir {
    /// Transferring -> Failed
    fn from(value: TransferringSeqDir) -> Self {
        FailedSeqDir {
            availability: value.availability.check(value.seq_dir.root()),
            seq_dir: value.seq_dir,
            since: Utc::now(),
        }
    }
}

impl SeqDirState {
    /// Returns a reference to the inner SeqDir
    pub fn dir(&self) -> &SeqDir {
        match self {
            SeqDirState::Failed(dir) => &dir.seq_dir,
            SeqDirState::Complete(dir) => &dir.seq_dir,
            SeqDirState::Sequencing(dir) => &dir.seq_dir,
            SeqDirState::Transferring(dir) => &dir.seq_dir,
        }
    }

    /// Timestamp of when state was entered.
    pub fn since(&self) -> &DateTime<Utc> {
        match self {
            SeqDirState::Failed(dir) => &dir.since,
            SeqDirState::Complete(dir) => &dir.since,
            SeqDirState::Sequencing(dir) => &dir.since,
            SeqDirState::Transferring(dir) => &dir.since,
        }
    }

    /// Mutable reference to inner SeqDir
    #[cfg(test)]
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

    /// Returns reference to the current [Availability] of the sequencing directory
    ///
    /// Does *not* re-evaluate availablity. It is not recommended that you keep
    /// long-lasting references to the returned value.
    /// See also [available](SeqDirState::available()) and
    /// [check_available](SeqDirState::available()).
    pub fn availablity(&self) -> &Availability {
        match self {
            SeqDirState::Complete(dir) => &dir.availability,
            SeqDirState::Failed(dir) => &dir.availability,
            SeqDirState::Sequencing(dir) => &dir.availability,
            SeqDirState::Transferring(dir) => &dir.availability,
        }
    }

    /// Obtain a mutable reference to the seqdir's [Availability]
    fn availability_mut(&mut self) -> &mut Availability {
        match self {
            SeqDirState::Complete(dir) => &mut dir.availability,
            SeqDirState::Failed(dir) => &mut dir.availability,
            SeqDirState::Sequencing(dir) => &mut dir.availability,
            SeqDirState::Transferring(dir) => &mut dir.availability,
        }
    }

    /// Returns true if current [Availability] is Available variant, and false otherwise
    pub fn available(&self) -> bool {
        matches!(self.availablity(), Availability::Available(..))
    }

    /// Check the current availablity, possibly updating it, and return true if available
    ///
    /// See [available](SeqDirState::available()) for an immutable alternative.
    pub fn check_available(&mut self) -> bool {
        *self.availability_mut() = self.availability_mut().check(self.dir().root());
        self.available()
    }
}

#[derive(Clone)]
/// Implements a state machine for managing the state of a [SeqDir].
///
/// Once a directory has gone to either [Complete](SeqDirState::Complete) or
/// [Failed](SeqDirState::Failed), it cannot transition back to another state.
/// However, the [Availability] of the dir may still update on every call to [poll](DirManager::poll()).
pub struct DirManager {
    seq_dir: SeqDirState,
}

impl DirManager {
    /// Construct a new DirManager from a path.
    ///
    /// The initial state will always be Sequencing', but `poll` is called
    /// automatically before returning, so the state will be accurate.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, SeqDirError> {
        let seq_dir = SeqDir::from_path(&path)?;
        let mut dir_manager = DirManager {
            seq_dir: SeqDirState::Sequencing(SequencingSeqDir {
                seq_dir,
                since: Utc::now(),
                availability: Availability::Available(Utc::now()),
            }),
        };
        dir_manager.poll();
        Ok(dir_manager)
    }

    /// Consume the DirManager, returning contained SeqDir, regardless of state.
    ///
    /// Discards associated timestamp.
    pub fn into_inner(self) -> Result<SeqDir, SeqDirError> {
        match self.seq_dir {
            SeqDirState::Complete(dir) => Ok(dir.seq_dir),
            SeqDirState::Sequencing(dir) => Ok(dir.seq_dir),
            SeqDirState::Failed(dir) => Ok(dir.seq_dir),
            SeqDirState::Transferring(dir) => Ok(dir.seq_dir),
        }
    }

    /// Returns reference to the inner SeqDir being managed.
    pub fn inner(&self) -> &SeqDir {
        self.seq_dir.dir()
    }

    /// Mutable reference to inner SeqDir being managed.
    #[cfg(test)]
    fn inner_mut(&mut self) -> &mut SeqDir {
        self.seq_dir.dir_mut()
    }

    /// Returns a reference to inner state
    pub fn state(&self) -> &SeqDirState {
        &self.seq_dir
    }

    /// Returns a mutable reference to inner state
    pub fn state_mut(&mut self) -> &mut SeqDirState {
        &mut self.seq_dir
    }

    /// Attempt to perform a transition, possibly updating the state.
    ///
    /// Returns reference to current state.
    pub fn poll(&mut self) -> &SeqDirState {
        let state = std::mem::replace(&mut self.seq_dir, _default());
        self.seq_dir = state.transition();
        self.state()
    }

    /// Attempt to perform a transition, possibly updating the state.
    ///
    /// Returns mutable reference to current state.
    /// CAUTION: poll_mut should be used judiciously.
    pub fn poll_mut(&mut self) -> &mut SeqDirState {
        let state = std::mem::replace(&mut self.seq_dir, _default());
        self.seq_dir = state.transition();
        self.state_mut()
    }

    /// Timestamp of when the DirManager's SeqDir entered its current state
    pub fn since(&self) -> &DateTime<Utc> {
        self.seq_dir.since()
    }
}

#[doc(hidden)]
/// This SeqDirState contains a completely invalid SeqDir and is only used as a placeholder when
/// polling for updated state. This really should not be used anywhere else.
/// This should be a very lightweight operation because none of the struct fields allocate
fn _default() -> SeqDirState {
    let seq_dir = SeqDir {
        root: PathBuf::new(),
        samplesheet: PathBuf::new(),
        run_info: PathBuf::new(),
        run_params: PathBuf::new(),
        run_completion: PathBuf::new(),
    };
    SeqDirState::Sequencing(SequencingSeqDir {
        seq_dir,
        since: DateTime::<Utc>::MIN_UTC,
        availability: Availability::Unavailable(DateTime::<Utc>::MIN_UTC),
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

        dbg!(serde_json::to_string(manager.state()).unwrap());
    }
}
