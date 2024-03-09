use serde::Serialize;
use std::ffi::OsStr;
use std::fs::read_dir;
use std::path::{Path, PathBuf};

use crate::SeqDirError;

// may as well future proof against S8 flowcells
const LANES: [&str; 8] = [
    "L001", "L002", "L003", "L004", "L005", "L006", "L007", "L008",
];
const BASECALLS: &str = "Data/Intensities/BaseCalls/";
const FILTER_EXT: &str = "filter";
const CBCL: &str = "cbcl";
const CBCL_GZ: &str = "cbcl.gz";
const BCL: &str = "bcl";
const BCL_GZ: &str = "bcl.gz";
const CYCLE_PREFIX: &str = "C";

/// A BCL or a CBCL
#[derive(Clone, Debug, Serialize, PartialEq)]
pub enum Bcl {
    Bcl(PathBuf),
    CBcl(PathBuf),
}

impl Bcl {
    /// Construct Bcl variant from a path.
    ///
    /// Paths ending in 'bcl' or 'bcl.gz' are mapped to `Bcl`.
    /// Paths ending in 'cbcl' or 'cbcl.gz' are mapped to `Cbcl`.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Option<Self> {
        let path_str = path.as_ref().to_str()?;
        if path_str.ends_with(CBCL) || path_str.ends_with(CBCL_GZ) {
            Some(Self::CBcl(path.as_ref().to_owned()))
        } else if path_str.ends_with(BCL) || path_str.ends_with(BCL_GZ) {
            Some(Self::Bcl(path.as_ref().to_owned()))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
/// A cycle consists of a cycle number and any number of (C)BCLs
pub struct Cycle<P: AsRef<Path>> {
    pub cycle_num: u16,
    pub root: P,
    pub bcls: Vec<Bcl>,
}

impl<P: AsRef<Path>> Cycle<P> {
    /// Attempt to read the provided directory as a Cycle
    ///
    /// Parses the cycle number from the directory name and finds [Bcls](Bcl).
    ///
    /// Returns None if:
    /// 1. the directory does no start with 'C' or is not followed by a cycle number
    /// 2. the directory does not contain any (C)Bcls
    pub fn from_path(path: P) -> Result<Cycle<P>, SeqDirError> {
        let cycle_num = path
            .as_ref()
            .file_stem()
            .ok_or(SeqDirError::BadCycle(path.as_ref().to_owned()))?
            .to_owned()
            .to_string_lossy()
            .strip_prefix(CYCLE_PREFIX)
            .ok_or(SeqDirError::BadCycle(path.as_ref().to_owned()))?
            .parse::<u16>()?;

        // collect any BCLs. Return None if no BCLs
        let bcls: Vec<Bcl> = read_dir(&path)?
            .filter_map(|p| p.ok())
            .map(|p| p.path())
            .filter_map(Bcl::from_path)
            .collect();
        if bcls.is_empty() {
            return Err(SeqDirError::MissingBcls(cycle_num));
        }

        Ok(Cycle {
            cycle_num,
            root: path,
            bcls,
        })
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
/// A lane consists of any number of cycles and any number of filters
pub struct Lane<P: AsRef<Path>> {
    pub lane_num: u8,
    cycles: Vec<Cycle<P>>,
    filters: Vec<P>,
}

impl<P> Lane<P>
where
    P: AsRef<Path>,
{
    /// Attempt to read the provided directory as a Lane
    ///
    /// This will try to construct valid [Cycle] objects from matching directories in the provided
    /// directory. It will also attempt to find all filter files in the directory.
    pub fn from_path(path: P) -> Result<Lane<PathBuf>, SeqDirError> {
        let lane_num = path
            .as_ref()
            .file_stem()
            .ok_or_else(|| SeqDirError::MissingLaneDirs)?
            .to_str()
            .ok_or_else(|| SeqDirError::MissingLaneDirs)?
            .strip_prefix('L')
            .ok_or_else(|| SeqDirError::MissingLaneDirs)?
            .parse::<u8>()?;

        // collect any cycles we can find. Error if we don't find any, or any are malformed.
        let cycles = read_dir(&path)?
            .filter_map(|p| p.ok())
            .map(|p| p.path())
            .filter(|p| {
                p.is_dir()
                    && p.file_name()
                        .unwrap_or(OsStr::new(""))
                        .to_str()
                        .unwrap_or("")
                        .starts_with(CYCLE_PREFIX)
            })
            .map(|p| Cycle::from_path(p.as_path().to_owned()))
            .collect::<Result<Vec<Cycle<PathBuf>>, SeqDirError>>()?;
        if cycles.is_empty() {
            return Err(SeqDirError::MissingCycles);
        }

        // now collect any filters. It's okay to not find any.
        let filters: Vec<PathBuf> = read_dir(&path)?
            .filter_map(|p| p.ok())
            .map(|p| p.path())
            .filter(|p| {
                p.is_file() && p.extension().unwrap_or_else(|| OsStr::new("")) == FILTER_EXT
            })
            .collect();

        Ok(Lane {
            lane_num,
            cycles,
            filters,
        })
    }

    /// Returns a reference to the vector of cycles
    pub fn cycles(&self) -> &Vec<Cycle<P>> {
        &self.cycles
    }

    /// Returns an iterator over associated [Cycles](Cycle)
    pub fn iter_cycles(&self) -> std::slice::Iter<'_, Cycle<P>> {
        self.cycles.iter()
    }

    /// Returns a reference to the vector of filters
    pub fn filters(&self) -> &Vec<P> {
        &self.filters
    }

    /// Returns an iterator over the associated filters
    pub fn iter_filters(&self) -> std::slice::Iter<'_, P> {
        self.filters.iter()
    }
}

/// Find outputs per-lane for a sequencing directory and construct `Lane` objects.
///
/// Errors on the following conditions:
/// 1. fails to parse lane number from any lane directory name
/// 2. any identified lane directory has no cycle directories
/// 3. any identified cycle directory has no (C)BCLs
pub fn detect_lanes<P: AsRef<Path>>(dir: P) -> Result<Vec<Lane<PathBuf>>, SeqDirError> {
    LANES
        .iter()
        .map(|l| dir.as_ref().join(BASECALLS).join(l))
        .filter(|l| l.exists())
        .map(Lane::from_path)
        .collect::<Result<Vec<Lane<PathBuf>>, SeqDirError>>()
}

#[cfg(test)]
mod tests {

    use crate::lane::detect_lanes;

    const COMPLETE: &str = "test_data/seq_complete/";
    const FAILED: &str = "test_data/seq_failed/";
    const TRANSFERRING: &str = "test_data/seq_transferring/";

    #[test]
    fn no_cycles_fails() {
        assert!(detect_lanes(TRANSFERRING).is_err())
    }

    #[test]
    fn no_lanes_ok() {
        assert!(detect_lanes(FAILED).is_ok())
    }

    #[test]
    fn completed_dir_succeeds() {
        detect_lanes(COMPLETE).unwrap();
    }
}
