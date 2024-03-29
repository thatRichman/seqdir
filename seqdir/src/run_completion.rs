//! Parse RunCompletionStatus.xml
//!
//! This module enables parsing on RunCompletionStatus.xml into [CompletionStatus] structs.
//! Each CompletionStatus variant wraps a [Message] that contains at minimum a the associated
//! sequencing run id, and may also include an optional message, which is parsed from the
//! ErrorDescription tag.
//!
//! CompletionStatus and Message are both readily serializable so they can be treated as
//! emitted events by higher-level implementations.

use std::fmt::Display;
use std::path::Path;
use std::{fs::File, io::Read};

use roxmltree;
use serde::Serialize;

const RUN_ID: &str = "RunId";
const COMPLETION_STATUS: &str = "CompletionStatus";
const ERROR_DESCRIPTION: &str = "ErrorDescription";

#[derive(Clone, Debug, Serialize, PartialEq)]
/// A RunCompletionStatus message.
///
/// Consists of a run_id and optional message content.
pub struct Message {
    pub run_id: String,
    pub message: Option<String>,
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} : {}",
            self.run_id,
            self.message.as_ref().unwrap_or(&"None".to_string())
        )
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "completion_status")]
/// The completion status of a run as extracted from RunCompletionStatus.xml
pub enum CompletionStatus {
    CompletedAsPlanned(Message),
    ExceptionEndedEarly(Message),
    UserEndedEarly(Message),
    Other(Message),
}

impl Display for CompletionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (var_str, message) = match self {
            Self::ExceptionEndedEarly(m) => ("ExceptionEndedEarly", m),
            Self::UserEndedEarly(m) => ("UserEndedEarly", m),
            Self::CompletedAsPlanned(m) => ("CompletedAsPlanned", m),
            Self::Other(m) => ("Other", m),
        };
        write!(f, "{} : {}", var_str, message)
    }
}

/// Attempts to parse a file in the format of RunCompletionStatus.xml
///
/// Returns a [CompletionStatus] wrapping the associated [Message]
pub fn parse_run_completion<P: AsRef<Path>>(path: P) -> Result<CompletionStatus, std::io::Error> {
    let mut handle = File::open(&path)?;
    let mut raw_contents = String::new();
    handle.read_to_string(&mut raw_contents)?;
    let doc = roxmltree::Document::parse(&raw_contents).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Could not parse as XML: {e}"),
        )
    })?;

    let run_id = match doc.descendants().find(|elem| elem.has_tag_name(RUN_ID)) {
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing RunId tag",
            ))
        }
        Some(node) => match node.text() {
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "RunId tag is empty",
                ))
            }
            Some(id) => id,
        },
    }
    .to_string();

    let message_txt = match doc
        .descendants()
        .find(|elem| elem.has_tag_name(ERROR_DESCRIPTION))
    {
        Some(node) => match node.text() {
            None => None,
            Some("None") => None,
            Some(text) => Some(text.to_string()),
        },
        None => None,
    };

    let message = Message {
        run_id,
        message: message_txt,
    };

    match doc
        .descendants()
        .find(|elem| elem.has_tag_name(COMPLETION_STATUS))
    {
        None => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "missing CompletionStatus tag",
        )),
        Some(node) => match node.text() {
            Some("CompletedAsPlanned") => Ok(CompletionStatus::CompletedAsPlanned(message)),
            Some("ExceptionEndedEarly") => Ok(CompletionStatus::ExceptionEndedEarly(message)),
            Some("UserEndedEarly") => Ok(CompletionStatus::UserEndedEarly(message)),
            Some(_) => Ok(CompletionStatus::Other(message)),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "CompletionStatus tag is empty",
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::parse_run_completion;
    use super::CompletionStatus;

    const COMPLETED_RCS: &str = "test_data/seq_complete/RunCompletionStatus.xml";
    const FAILED_RCS: &str = "test_data/seq_failed/RunCompletionStatus.xml";
    const GARBAGE_RCS: &str = "test_data/seq_corrupt/RunCompletionStatus.xml";

    #[test]
    fn parse_completed() {
        let completion_status = parse_run_completion(COMPLETED_RCS).unwrap();

        match completion_status {
            CompletionStatus::CompletedAsPlanned(message) => {
                assert_eq!(message.message, None);
                assert_eq!(message.run_id, "20231231_foo_ABCXYZ");
            }
            _ => panic!("expected CompletedAsPlanned variant"),
        }
    }

    #[test]
    fn parse_failed() {
        let completion_status = parse_run_completion(FAILED_RCS).unwrap();

        match completion_status {
            CompletionStatus::ExceptionEndedEarly(message) => {
                assert_ne!(message.message, None);
                assert_eq!(message.run_id, "20231231_bar_ABCXYZ");
            }
            _ => panic!("expected ExceptionEndedEarly variant"),
        }
    }

    // TODO fuzz
    #[test]
    fn bad_message_does_not_panic() {
        assert!(parse_run_completion(GARBAGE_RCS).is_err());
    }

    #[test]
    fn test_serialize() {
        use serde_json;

        let completion_status = parse_run_completion(COMPLETED_RCS).unwrap();
        serde_json::to_string(&completion_status).unwrap();
    }
}
