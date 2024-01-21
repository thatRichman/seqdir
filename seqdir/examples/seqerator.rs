use rand::prelude::*;
use seqdir::DirManager;

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{env, io};
use std::{thread, u16};

/// A quick-and-dirty example of what it looks like to use the library.
///
/// Provide a path to a sequencing directory (such as one of the test_data subdirs),
/// a maximum number of iterations, and a transition probability (0-100).
///
/// NOTE: This will create files on disk. If allowed to run to completion, it will clean up the
/// files it creates.

fn touch(path: &Path) -> io::Result<()> {
    match OpenOptions::new().create(true).write(true).open(path) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

fn main() {
    let args: Vec<_> = env::args().collect();

    let mut example_dir1 = PathBuf::new();
    example_dir1.push(args.get(1).unwrap());

    let transition_probability =
        str::parse::<u8>(args.get(2).unwrap_or(&String::from("50"))).unwrap();

    let abs = example_dir1.canonicalize().unwrap();

    let mut manager = DirManager::new(abs).unwrap();

    let mut to_remove = Vec::<PathBuf>::new();

    let max_iter_str = args.get(2).unwrap();
    let max_iter = max_iter_str.parse::<u16>().unwrap();
    let mut c = 0;
    while c < max_iter {
        let num = rand::thread_rng().gen_range(0..100);
        match manager.poll() {
            state @ seqdir::SeqDirState::Complete(..) => {
                println!("{}", serde_json::to_string_pretty(state).unwrap());
                if state.available() {
                    match state.dir().get_completion_status() {
                        Some(Ok(status)) => println!(
                            "completion status: {}",
                            serde_json::to_string(&status).unwrap()
                        ),
                        Some(Err(err)) => println!("failed to get completion status: {}", err),
                        None => println!(
                            "No completion status found for {}",
                            state.dir().root().display()
                        ),
                    }
                    break;
                }
            }
            state @ seqdir::SeqDirState::Sequencing(..) => {
                println!("{}", serde_json::to_string_pretty(state).unwrap());
                if num < transition_probability {
                    println!("Simulating transition Sequencing --> Transferring.");
                    let mut seq_complete = state.dir().root().to_owned();
                    seq_complete.push("SequenceComplete.txt");
                    to_remove.push(seq_complete.to_owned());
                    touch(seq_complete.as_path()).unwrap_or_else(|e| {
                        eprintln!("failed to transition sequencing --> transferring: {e}")
                    });
                }
            }
            state @ seqdir::SeqDirState::Transferring(..) => {
                println!("{}", serde_json::to_string_pretty(state).unwrap());
                if num < transition_probability {
                    println!("Simulating transition Transferring --> Complete.");
                    let mut copy_complete = state.dir().root().to_owned();
                    copy_complete.push("CopyComplete.txt");
                    to_remove.push(copy_complete.to_owned());
                    touch(copy_complete.as_path()).unwrap_or_else(|e| {
                        eprintln!("failed to transition transferring --> complete: {e}")
                    });
                }
            }
            _ => {}
        };
        c += 1;
        thread::sleep(Duration::from_secs(1));
    }

    for path in to_remove {
        match std::fs::remove_file(&path) {
            Ok(_) => {}
            Err(e) => eprintln!("failed to remove {} during cleanup: {e}", path.display()),
        }
    }
}
