//! Versioned payload of a packed daily request archive.
//!
//! The v1 payload starts with its magic and a SQLite metadata snapshot with
//! empty body columns. A descriptor count and one descriptor per captured
//! body precede the object stream. The snapshot and request IDs are prefixed
//! with little-endian u32 lengths. Each descriptor also records its slot and
//! the decoded-body and SQLite TEXT flags.

mod capture;
pub mod indexed;
mod objects;
mod restore;

pub use restore::{restore, restore_metadata, restore_request};

use crate::request_files::StableFile;
use capture::Capture;
use objects::{InputBody, Sharing};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};

pub const MAGIC: [u8; 8] = *b"TOKNRPK\x01";
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

use capture::has_snapshot_schema;

/// A captured snapshot ready to write or to compare against a pack.
/// The metadata snapshot and body bytes live in private temporary files.
pub struct RequestPack {
  capture: Capture,
}

impl RequestPack {
  /// Capture a checkpointed request database held stable by the caller.
  ///
  /// An unsupported schema or source outside the format limits returns
  /// `None` so maintenance can retain the whole-database archive path.
  /// Progress reports source body bytes processed and can interrupt capture.
  pub fn capture(source: &StableFile, progress: &mut impl FnMut(u64) -> io::Result<()>) -> io::Result<Option<Self>> {
    Ok(capture::load(source, progress)?.map(|capture| Self { capture }))
  }

  /// Write the uncompressed v1 payload into the archive encoder.
  pub fn write(&self, mut writer: &mut dyn Write) -> io::Result<()> {
    let capture = &self.capture;
    let mut snapshot = File::open(capture.metadata_snapshot.path())?;
    let snapshot_len = u32::try_from(snapshot.metadata()?.len()).map_err(invalid_data)?;
    writer.write_all(&MAGIC)?;
    writer.write_all(&snapshot_len.to_le_bytes())?;
    io::copy(&mut snapshot, writer)?;
    write_count(writer, capture.bodies.len())?;

    for body in &capture.bodies {
      write_count(writer, body.request_id.len())?;
      writer.write_all(body.request_id.as_bytes())?;
      writer.write_all(&[body.slot, body.flags])?;
    }
    let stats = objects::write_objects(&mut writer, &self.inputs(), Sharing::Delta, MAX_DEPTH).map_err(codec_error)?;
    tracing::debug!(
      full = stats.full,
      references = stats.references,
      deltas = stats.deltas,
      session_deltas = stats.session_deltas,
      max_depth = stats.max_depth,
      "encoded request archive pack"
    );
    Ok(())
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

  /// Verify the archive against this capture, including metadata snapshot
  /// values and every body after applying the archive's references and deltas.
  pub fn verify(&self, mut reader: &mut dyn Read) -> io::Result<()> {
    verify_magic(reader)?;
    let snapshot_len = u64::from(read_u32(reader)?);
    let mut archived_metadata = tempfile::NamedTempFile::new()?;
    if io::copy(&mut (&mut *reader).take(snapshot_len), archived_metadata.as_file_mut())? != snapshot_len {
      return Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "incomplete request archive metadata snapshot",
      ));
    }
    capture::verify_metadata_snapshot(self.capture.metadata_snapshot.path(), archived_metadata.path())?;

    if read_u32(reader)? as usize != self.capture.bodies.len() {
      return Err(invalid_data("request archive body descriptor count differs"));
    }
    let mut inputs = Vec::with_capacity(self.capture.bodies.len());
    let mut id_buffer = Vec::new();
    for body in &self.capture.bodies {
      let id_len = read_u32(reader)? as usize;
      if id_len != body.request_id.len() {
        return Err(invalid_data("request archive identifier length differs"));
      }
      id_buffer.resize(id_len, 0);
      reader.read_exact(&mut id_buffer)?;
      if id_buffer != body.request_id.as_bytes() {
        return Err(invalid_data("request archive identifier differs"));
      }
      let mut descriptor = [0; 2];
      reader.read_exact(&mut descriptor)?;
      if descriptor != [body.slot, body.flags] {
        return Err(invalid_data("request archive body slot or flags differ"));
      }
      inputs.push(InputBody {
        bytes: self.capture.bytes(body),
        base: None,
        session_bases: &[],
      });
    }
    objects::verify_objects(&mut reader, &inputs, MAX_DEPTH).map_err(codec_error)?;
    if reader.read(&mut [0; 1])? != 0 {
      return Err(invalid_data("unexpected trailing request pack data"));
    }
    Ok(())
  }
}

pub(super) fn verify_magic(reader: &mut dyn Read) -> io::Result<()> {
  let mut magic = [0; MAGIC.len()];
  reader.read_exact(&mut magic)?;
  if magic != MAGIC {
    return Err(invalid_data("unsupported request archive pack version"));
  }
  Ok(())
}

fn write_count(writer: &mut dyn Write, count: usize) -> io::Result<()> {
  writer.write_all(&u32::try_from(count).map_err(invalid_data)?.to_le_bytes())
}

pub(super) fn read_u32(reader: &mut dyn Read) -> io::Result<u32> {
  let mut bytes = [0; 4];
  reader.read_exact(&mut bytes)?;
  Ok(u32::from_le_bytes(bytes))
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

  #[test]
  fn verifies_metadata_values_independent_of_sqlite_page_layout() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let day = dir.path().join("source.db");
    let conn = open_day_db(&day)?;
    conn.execute_batch(
      "INSERT INTO request_connection(rowid, request_id, ts) VALUES (42, 'turn-one', 100);
       INSERT INTO request_metadata(request_id, session_id, model) VALUES ('turn-one', 'session', 'model');
       INSERT INTO request_downstream(request_id, inbound_req_body) VALUES ('turn-one', 'first turn');",
    )?;
    drop(conn);

    let source = StableFile::open(&day)?;
    let snapshot = RequestPack::capture(&source, &mut |_| Ok(()))?.expect("standard requests schema");
    let mut encoded = Vec::new();
    snapshot.write(&mut encoded)?;
    snapshot.verify(&mut &encoded[..])?;

    let mut after_magic = &encoded[MAGIC.len()..];
    let length = read_u32(&mut after_magic)? as usize;
    let (metadata_bytes, body_data) = after_magic.split_at(length);
    let changed_snapshot = dir.path().join("metadata.db");
    fs::write(&changed_snapshot, metadata_bytes)?;
    let conn = rusqlite::Connection::open(&changed_snapshot)?;
    conn.execute_batch("PRAGMA page_size = 1024; VACUUM;")?;
    let replace_snapshot = || -> io::Result<Vec<u8>> {
      let metadata_bytes = fs::read(&changed_snapshot)?;
      let mut encoded = MAGIC.to_vec();
      write_count(&mut encoded, metadata_bytes.len())?;
      encoded.extend_from_slice(&metadata_bytes);
      encoded.extend_from_slice(body_data);
      Ok(encoded)
    };
    let rebuilt = replace_snapshot()?;
    assert_ne!(rebuilt, encoded);
    snapshot.verify(&mut &rebuilt[..])?;

    conn.execute("UPDATE request_metadata SET model = 'other model'", [])?;
    assert_eq!(
      snapshot.verify(&mut &replace_snapshot()?[..]).unwrap_err().kind(),
      io::ErrorKind::InvalidData
    );
    Ok(())
  }

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
}
