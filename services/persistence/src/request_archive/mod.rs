//! Capture and indexed payload for packed daily request archives.

mod capture;
pub mod indexed;
mod objects;

use crate::request_files::StableFile;
use capture::Capture;
use objects::InputBody;
use std::collections::HashMap;
use std::io;

pub const INBOUND_REQUEST_SLOT: u8 = 0;
pub const OUTBOUND_REQUEST_SLOT: u8 = 1;
pub const OUTBOUND_RESPONSE_SLOT: u8 = 2;
pub const INBOUND_RESPONSE_SLOT: u8 = 3;
pub const BODY_IS_DECODED: u8 = 1;
pub const BODY_IS_TEXT: u8 = 2;

pub(super) const FIELDS: [(&str, &str, &str); 4] = [
  ("request_downstream", "inbound_req_body", "inbound_req_headers"),
  ("request_upstream", "outbound_req_body", "outbound_req_headers"),
  ("request_upstream", "outbound_resp_body", "outbound_resp_headers"),
  ("request_downstream", "inbound_resp_body", "inbound_resp_headers"),
];
pub(super) const MAX_DEPTH: usize = 64;
pub(super) const MAX_REQUEST_ID_BYTES: usize = 64 * 1024;
const SESSION_WINDOW: usize = 8;

/// A captured snapshot ready to write or to compare against a pack.
/// The metadata snapshot and body bytes live in private temporary files.
pub struct RequestPack {
  capture: Capture,
}

/// Work and temporary output produced while capturing a request database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureProgress {
  /// The number of table copies, body column scans, and finalization steps.
  Started { tasks: u64 },
  /// One capture task completed successfully.
  TaskFinished,
  /// Bytes added to the body spool or finalized SQLite metadata snapshot.
  OutputBytes(u64),
}

impl RequestPack {
  /// Capture a checkpointed request database held stable by the caller.
  ///
  /// An unsupported schema or source outside the format limits returns
  /// `None` so maintenance can retain the whole-database archive path.
  /// Progress reports source body bytes processed and can interrupt capture.
  pub fn capture(source: &StableFile, progress: &mut impl FnMut(u64) -> io::Result<()>) -> io::Result<Option<Self>> {
    Self::capture_with_progress(source, progress, |_| {})
  }

  /// Report completed tasks and staged output alongside the cancellable source byte callback.
  pub fn capture_with_progress(
    source: &StableFile,
    progress: &mut impl FnMut(u64) -> io::Result<()>,
    mut on_capture: impl FnMut(CaptureProgress),
  ) -> io::Result<Option<Self>> {
    Ok(capture::load(source, progress, &mut on_capture)?.map(|capture| Self { capture }))
  }

  /// Total captured body bytes before deduplication and delta encoding.
  pub fn body_bytes(&self) -> u64 {
    self.capture.body_bytes()
  }

  fn inputs(&self) -> Vec<InputBody<'_>> {
    let capture = &self.capture;
    let mut seen = HashMap::new();
    let mut inputs = Vec::with_capacity(capture.bodies.len());
    for (index, body) in capture.bodies.iter().enumerate() {
      let paired_slot = match body.slot {
        OUTBOUND_REQUEST_SLOT => Some(INBOUND_REQUEST_SLOT),
        INBOUND_RESPONSE_SLOT => Some(OUTBOUND_RESPONSE_SLOT),
        _ => None,
      };
      let base = paired_slot.and_then(|slot| seen.get(&(body.request_id.as_str(), slot)).copied());
      seen.insert((body.request_id.as_str(), body.slot), index);
      inputs.push(InputBody {
        bytes: capture.bytes(body),
        base,
        session_bases: capture.candidates(index),
      });
    }
    inputs
  }
}

pub(super) fn invalid_data(message: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
  io::Error::new(io::ErrorKind::InvalidData, message)
}

fn codec_error(error: anyhow::Error) -> io::Error {
  error
    .downcast::<io::Error>()
    .unwrap_or_else(|error| invalid_data(format!("{error:#}")))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::requests::open_day_db;
  use std::fs;
  use std::io::Write;

  #[test]
  fn capture_propagates_cancellation_and_falls_back_for_non_sqlite_files() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let day = dir.path().join("source.db");
    open_day_db(&day)?.execute(
      "INSERT INTO request_downstream(request_id, inbound_req_body) VALUES ('r', x'aabb')",
      [],
    )?;
    let source = StableFile::open(&day)?;
    let captured = RequestPack::capture(&source, &mut |bytes| {
      if bytes == 0 {
        return Ok(());
      }
      Err(io::ErrorKind::Interrupted.into())
    });
    assert_eq!(captured.err().unwrap().kind(), io::ErrorKind::Interrupted);

    let unrecognized = dir.path().join("legacy.db");
    fs::write(&unrecognized, b"not a SQLite database")?;
    assert!(RequestPack::capture(&StableFile::open(&unrecognized)?, &mut |_| Ok(()))?.is_none());
    Ok(())
  }

  #[test]
  fn capture_reports_completed_tasks_and_normalized_output_bytes() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let day = dir.path().join("source.db");
    let body = b"normalized request body".repeat(32);
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&body)?;
    let wire = encoder.finish()?;
    open_day_db(&day)?.execute(
      "INSERT INTO request_downstream(request_id, inbound_req_headers, inbound_req_body) VALUES ('r', ?1, ?2)",
      (r#"{"content-encoding":"gzip"}"#, wire.as_slice()),
    )?;

    let mut source_bytes = 0;
    let mut events = Vec::new();
    let capture = RequestPack::capture_with_progress(
      &StableFile::open(&day)?,
      &mut |bytes| {
        source_bytes += bytes;
        Ok(())
      },
      |event| events.push(event),
    )?
    .expect("packable fixture");

    let tasks = 5 + FIELDS.len() + 1;
    assert_eq!(events.first(), Some(&CaptureProgress::Started { tasks: tasks as u64 }));
    assert_eq!(events.last(), Some(&CaptureProgress::TaskFinished));
    let completed = events
      .iter()
      .filter(|event| **event == CaptureProgress::TaskFinished)
      .count();
    assert_eq!(completed, tasks);

    let output: Vec<_> = events
      .into_iter()
      .filter_map(|event| match event {
        CaptureProgress::OutputBytes(bytes) => Some(bytes),
        _ => None,
      })
      .collect();
    let metadata_bytes = capture.capture.metadata_snapshot.as_file().metadata()?.len();
    assert_eq!(output, [body.len() as u64, metadata_bytes]);
    assert_eq!(source_bytes, wire.len() as u64);
    Ok(())
  }
}
