const LANES: [&str; 4] = ["L001", "L002", "L003", "L004"];
const BASECALLS: &str = "Data/Intensities/BaseCalls/";
const FILTER_EXT: &str = "filter";
const CBCL: &str = "cbcl";
const CBCL_GZ: &str = "cbcl.gz";
const BCL: &str = "bcl";
const BCL_GZ: &str = "bcl.gz";
const CYCLE_PREFIX: &str = "C";

/// A BCL or a CBCL
#[derive(Clone, Debug, Serialize)]
pub enum Bcl {
    Bcl(PathBuf),
    CBcl(PathBuf),
}

impl Bcl {
    /// Construct Bcl variant from a path.
    ///
    /// Paths ending in 'bcl' or 'bcl.gz' are mapped to `Bcl`.
    /// Paths ending in 'cbcl' or 'cbcl.gz' are mapped to `Cbcl`.
    fn from_path<P: AsRef<Path>>(path: P) -> Option<Self> {
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

#[derive(Debug, Clone, Serialize)]
/// A cycle consists of a cycle number and any number of bcls
pub struct Cycle<P: AsRef<Path>> {
    pub cycle_num: u16,
    pub root: P,
}

impl<P: AsRef<Path>> Cycle<P> {
    /// Attempt to construct a cycle from the provided directory.
    ///
    /// Parses the cycle number from the directory name and finds [Bcls](Bcl).
    pub fn from_path(path: P) -> Result<Cycle<P>, SeqDirError> {
        let cycle_num = path
            .as_ref()
            .file_stem()
            .ok_or_else(|| SeqDirError::BadCycle(path.as_ref().to_owned()))?
            .to_string_lossy()
            .strip_prefix(CYCLE_PREFIX)
            .ok_or_else(|| SeqDirError::BadCycle(path.as_ref().to_owned()))?
            .parse::<u16>()?;

        Ok(Cycle {
            cycle_num,
            root: path,
        })
    }
}

impl<P: AsRef<Path>> Iterator for Cycle<P> {
    type Item = P;
    fn next(&mut self) -> Option<Self::Item> {}
}

#[derive(Clone, Debug, Serialize)]
/// A lane consists of any number of cycles and any number of filters
pub struct Lane<P: AsRef<Path>> {
    pub lane_num: u8,
    cycles: Vec<Cycle>,
    filters: Vec<P>,
}

impl<P> Lane<P>
where
    P: AsRef<Path>,
{
    /// Attempt to construct a Lane from a directory.
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
            .strip_prefix("L00")
            .ok_or_else(|| SeqDirError::MissingLaneDirs)?
            .parse::<u8>()?;
        let (cycle_paths, other_files): (Vec<PathBuf>, Vec<PathBuf>) = read_dir(path)?
            .filter_map(|p| p.ok())
            .map(|p| p.path())
            .partition(|p| {
                p.is_dir()
                    && p.file_name()
                        .unwrap_or_else(|| OsStr::new(""))
                        .to_str()
                        .unwrap_or("")
                        .starts_with(CYCLE_PREFIX)
            });

        let cycles: Vec<Cycle> = cycle_paths
            .iter()
            .map(Cycle::from_path)
            .collect::<Result<Vec<Cycle>, SeqDirError>>()?;
        if cycles.is_empty() {
            return Err(SeqDirError::MissingCycles);
        }

        let filters: Vec<PathBuf> = other_files
            .iter()
            .filter(|p| {
                p.is_file() && p.extension().unwrap_or_else(|| OsStr::new("")) == FILTER_EXT
            })
            .cloned()
            .collect();

        Ok(Lane {
            lane_num,
            cycles,
            filters,
        })
    }

    /// Returns a reference to the vector of cycles
    pub fn cycles(&self) -> &Vec<Cycle> {
        &self.cycles
    }

    /// Returns an iterator over associated [Cycles](Cycle)
    pub fn iter_cycles(&self) -> std::slice::Iter<'_, Cycle> {
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

// /// get lane data (if any) associated with the sequencing directory
// ///
// /// To keep SeqDir lightweight and to support incomplete sequencing runs, lanes are not stored
// /// within the struct.
// pub fn lanes(&self) -> Result<Vec<Lane<PathBuf>>, SeqDirError> {
//     detect_lanes(&self.root)
// }


/// Find outputs per-lane for a sequencing directory and construct `Lane` objects.
///
/// Will only find lanes 'L001' - 'L004', because those are the only ones that should exist.
pub fn detect_lanes<P: AsRef<Path>>(dir: P) -> Result<Vec<Lane<PathBuf>>, SeqDirError> {
    LANES
        .iter()
        .map(|l| dir.as_ref().join(BASECALLS).join(l))
        .filter(|l| l.exists())
        .map(|l| Lane::from_path(dir.as_ref().join(l)))
        .collect::<Result<Vec<Lane<PathBuf>>, SeqDirError>>()
}
