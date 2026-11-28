//! Seekable request pack payload. Compression is handled by the caller.
//!
//! The leading magic precedes the object stream. A SQLite metadata snapshot
//! follows the objects, with absolute logical offsets and SHA-256 body IDs.
//! The trailer contains its little-endian u64 length and the pack magic.

use super::objects::{self, ObjectIndex};
use super::{capture, codec_error, invalid_data, RequestPack, FIELDS, MAX_DEPTH};
use crate::request_files::begin_read;
use rusqlite::{params, Connection, OptionalExtension};
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Duration;

pub const MAGIC: [u8; 8] = *b"TOKNIDX\x01";
const VERSION: u8 = 1;
const HEADER_LEN: u64 = MAGIC.len() as u64;
const TRAILER_LEN: u64 = 8 + MAGIC.len() as u64;
const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";
const SCHEMA: &str = "
  CREATE TABLE tokn_archive_info (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    version INTEGER NOT NULL
  );
  CREATE TABLE tokn_archive_objects (
    ordinal INTEGER PRIMARY KEY,
    body_hash BLOB NOT NULL UNIQUE,
    logical_offset INTEGER NOT NULL,
    encoded_len INTEGER NOT NULL,
    body_len INTEGER NOT NULL,
    base INTEGER,
    depth INTEGER NOT NULL
  );
  CREATE TABLE tokn_archive_bodies (
    request_id TEXT NOT NULL,
    slot INTEGER NOT NULL,
    body_hash BLOB NOT NULL,
    flags INTEGER NOT NULL,
    PRIMARY KEY (request_id, slot),
    FOREIGN KEY (body_hash) REFERENCES tokn_archive_objects(body_hash)
  ) WITHOUT ROWID;
";

/// Serialize an indexed request pack without selecting a compression format.
pub fn write(
  snapshot: &RequestPack,
  output: &mut impl Write,
  progress: &mut impl FnMut(u64) -> io::Result<()>,
) -> io::Result<()> {
  progress(0)?;
  output.write_all(&MAGIC)?;
  let mut tracked = ProgressIo::new(&mut *output, progress);
  let result = objects::write_indexed_objects(&mut tracked, &snapshot.inputs(), MAX_DEPTH);
  let (index, canonical) = tracked.finish(result)?;

  let temp = tempfile::tempdir()?;
  let metadata = temp.path().join("metadata.db");
  {
    let mut source = File::open(snapshot.capture.metadata_snapshot.path())?;
    let source_len = source.metadata()?.len();
    copy_exact(&mut source, &mut create_private_file(&metadata)?, source_len, progress)?;
  }
  let mut conn = Connection::open(&metadata).map_err(invalid_data)?;
  write_indexes(&mut conn, snapshot, &index, &canonical, progress)?;
  conn.close().map_err(|(_, error)| invalid_data(error))?;

  let mut metadata = File::open(&metadata)?;
  let len = metadata.metadata()?.len();
  copy_exact(&mut metadata, output, len, progress)?;
  output.write_all(&len.to_le_bytes())?;
  output.write_all(&MAGIC)
}

fn write_indexes(
  conn: &mut Connection,
  snapshot: &RequestPack,
  index: &[ObjectIndex],
  canonical: &[usize],
  progress: &mut impl FnMut(u64) -> io::Result<()>,
) -> io::Result<()> {
  let tx = conn.transaction().map_err(invalid_data)?;
  tx.execute_batch(SCHEMA).map_err(invalid_data)?;
  tx.execute("INSERT INTO tokn_archive_info VALUES (1, ?1)", [VERSION])
    .map_err(invalid_data)?;
  {
    let mut insert_object = tx
      .prepare("INSERT INTO tokn_archive_objects VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")
      .map_err(invalid_data)?;
    let mut insert_body = tx
      .prepare("INSERT INTO tokn_archive_bodies VALUES (?1, ?2, ?3, ?4)")
      .map_err(invalid_data)?;
    for (ordinal, (body, &object)) in snapshot.capture.bodies.iter().zip(canonical).enumerate() {
      progress(0)?;
      let entry = &index[object];
      if object == ordinal {
        insert_object
          .execute(params![
            sql_int(object)?,
            entry.sha256.as_slice(),
            sql_int(entry.offset + HEADER_LEN)?,
            sql_int(entry.encoded_len)?,
            sql_int(entry.body_len)?,
            entry.base.map(sql_int).transpose()?,
            sql_int(entry.depth)?
          ])
          .map_err(invalid_data)?;
      }
      insert_body
        .execute(params![body.request_id, body.slot, entry.sha256.as_slice(), body.flags])
        .map_err(invalid_data)?;
    }
  }
  tx.commit().map_err(invalid_data)
}

/// Verify the entire indexed payload against the original capture.
pub fn verify(
  snapshot: &RequestPack,
  input: &mut (impl Read + Seek),
  progress: &mut impl FnMut(u64) -> io::Result<()>,
) -> io::Result<()> {
  let mut reader = IndexedPackReader::open_with_progress(input, Duration::ZERO, progress)?;
  capture::verify_metadata_rows(snapshot.capture.metadata_snapshot.path(), &reader.metadata)?;
  progress(0)?;
  let input = reader.input.get_mut();
  input.seek(SeekFrom::Start(0))?;
  let mut magic = [0; MAGIC.len()];
  input.read_exact(&mut magic)?;
  if magic != MAGIC {
    return Err(invalid_data("invalid indexed request archive header"));
  }
  let mut input = (&mut *input).take(reader.metadata_offset - HEADER_LEN);
  let mut tracked = ProgressIo::new(&mut input, progress);
  let result = objects::verify_indexed_objects(&mut tracked, &snapshot.inputs(), MAX_DEPTH);
  let (index, canonical) = tracked.finish(result)?;
  if input.limit() != 0 {
    return Err(invalid_data("indexed request archive object stream length differs"));
  }
  verify_indexes(&reader.metadata, snapshot, &index, &canonical, progress)
}

/// Query the unpacked metadata snapshot and seek to the requested body objects.
pub struct IndexedPackReader<R: Read + Seek> {
  input: RefCell<R>,
  metadata: Connection,
  metadata_offset: u64,
  // Close SQLite before removing its file.
  _temp: tempfile::TempDir,
}

impl<R: Read + Seek> IndexedPackReader<R> {
  pub fn open(input: R, timeout: Duration) -> io::Result<Self> {
    Self::open_with_progress(input, timeout, &mut |_| Ok(()))
  }

  fn open_with_progress(
    mut input: R,
    timeout: Duration,
    progress: &mut impl FnMut(u64) -> io::Result<()>,
  ) -> io::Result<Self> {
    progress(0)?;
    let len = input.seek(SeekFrom::End(0))?;
    let trailer = len
      .checked_sub(TRAILER_LEN)
      .filter(|&offset| offset >= HEADER_LEN)
      .ok_or_else(|| invalid_data("truncated indexed request archive"))?;
    input.seek(SeekFrom::Start(trailer))?;
    let mut metadata_len = [0; 8];
    let mut magic = [0; MAGIC.len()];
    input.read_exact(&mut metadata_len)?;
    input.read_exact(&mut magic)?;
    if magic != MAGIC {
      return Err(invalid_data("invalid indexed request archive trailer"));
    }
    let metadata_len = u64::from_le_bytes(metadata_len);
    let metadata_offset = trailer
      .checked_sub(metadata_len)
      .filter(|&offset| offset >= HEADER_LEN && metadata_len >= SQLITE_HEADER.len() as u64)
      .ok_or_else(|| invalid_data("invalid indexed request archive metadata bounds"))?;

    let temp = tempfile::tempdir()?;
    let path = temp.path().join("metadata.db");
    input.seek(SeekFrom::Start(metadata_offset))?;
    let mut signature = [0; SQLITE_HEADER.len()];
    input.read_exact(&mut signature)?;
    if &signature != SQLITE_HEADER {
      return Err(invalid_data("invalid indexed request archive metadata header"));
    }
    {
      let mut file = create_private_file(&path)?;
      file.write_all(&signature)?;
      copy_exact(&mut input, &mut file, metadata_len - signature.len() as u64, progress)?;
    }
    let metadata = crate::open_readonly(&path, timeout).map_err(io::Error::other)?;
    metadata
      .execute_batch("PRAGMA trusted_schema = OFF;")
      .map_err(invalid_data)?;
    begin_read(&metadata).map_err(io::Error::other)?;
    if !capture::has_snapshot_schema_with(&metadata, "main", SCHEMA).map_err(invalid_data)? {
      return Err(invalid_data("indexed request archive metadata schema differs"));
    }
    let version: u8 = metadata
      .query_row("SELECT version FROM tokn_archive_info WHERE singleton = 1", [], |row| {
        row.get(0)
      })
      .map_err(invalid_data)?;
    if version != VERSION {
      return Err(invalid_data("unsupported indexed request archive version"));
    }
    Ok(Self {
      input: RefCell::new(input),
      metadata,
      metadata_offset,
      _temp: temp,
    })
  }

  pub fn connection(&self) -> &Connection {
    &self.metadata
  }

  /// Read a body and its capture flags, following only its own delta chain.
  pub fn read_body(&self, request_id: &str, slot: u8) -> io::Result<Option<(Vec<u8>, u8)>> {
    if usize::from(slot) >= FIELDS.len() {
      return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid request body slot"));
    }
    let entry: Option<(Option<i64>, u8)> = self
      .metadata
      .query_row(
        "SELECT o.ordinal, b.flags FROM tokn_archive_bodies b
         LEFT JOIN tokn_archive_objects o ON o.body_hash = b.body_hash
         WHERE b.request_id = ?1 AND b.slot = ?2",
        (request_id, slot),
        |row| Ok((row.get(0)?, row.get(1)?)),
      )
      .optional()
      .map_err(invalid_data)?;
    let Some((ordinal, flags)) = entry else {
      return Ok(None);
    };
    let ordinal = usize::try_from(ordinal.ok_or_else(|| invalid_data("missing archived request body object"))?)
      .map_err(invalid_data)?;
    if flags & !(super::BODY_IS_DECODED | super::BODY_IS_TEXT) != 0 {
      return Err(invalid_data("invalid archived request body flags"));
    }
    let mut input = self
      .input
      .try_borrow_mut()
      .map_err(|_| io::Error::other("request archive reader is already in use"))?;
    let mut objects = self.metadata.prepare(LOOKUP_OBJECT).map_err(invalid_data)?;
    let body = objects::read_indexed_body(&mut *input, ordinal, MAX_DEPTH, |ordinal| {
      let entry = objects.query_row([sql_int(ordinal)?], read_object)?;
      if entry.offset < HEADER_LEN
        || entry
          .offset
          .checked_add(entry.encoded_len)
          .is_none_or(|end| end > self.metadata_offset)
      {
        return Err(invalid_data("indexed body lies outside the object stream").into());
      }
      Ok(entry)
    })
    .map_err(codec_error)?;
    Ok(Some((body, flags)))
  }
}

const LOOKUP_OBJECT: &str =
  "SELECT logical_offset, encoded_len, body_len, body_hash, base, depth FROM tokn_archive_objects WHERE ordinal = ?1";

fn read_object(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectIndex> {
  Ok(ObjectIndex {
    offset: number(row, 0)?,
    encoded_len: number(row, 1)?,
    body_len: number(row, 2)?,
    sha256: row.get(3)?,
    base: row
      .get::<_, Option<i64>>(4)?
      .map(|value| usize::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, value)))
      .transpose()?,
    depth: number(row, 5)?,
  })
}

fn verify_indexes(
  conn: &Connection,
  snapshot: &RequestPack,
  index: &[ObjectIndex],
  canonical: &[usize],
  progress: &mut impl FnMut(u64) -> io::Result<()>,
) -> io::Result<()> {
  let count = |table| {
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
      number::<usize>(row, 0)
    })
  };
  let canonical_count = canonical
    .iter()
    .enumerate()
    .filter(|&(ordinal, &object)| ordinal == object)
    .count();
  if count("tokn_archive_bodies").map_err(invalid_data)? != canonical.len()
    || count("tokn_archive_objects").map_err(invalid_data)? != canonical_count
  {
    return Err(invalid_data("indexed request archive index counts differ"));
  }
  let mut objects = conn.prepare(LOOKUP_OBJECT).map_err(invalid_data)?;
  let mut bodies = conn
    .prepare("SELECT body_hash, flags FROM tokn_archive_bodies WHERE request_id = ?1 AND slot = ?2")
    .map_err(invalid_data)?;
  for (ordinal, (body, &object)) in snapshot.capture.bodies.iter().zip(canonical).enumerate() {
    progress(0)?;
    let expected = index[object];
    let (hash, flags): ([u8; 32], u8) = bodies
      .query_row((&body.request_id, body.slot), |row| Ok((row.get(0)?, row.get(1)?)))
      .map_err(invalid_data)?;
    if hash != expected.sha256 || flags != body.flags {
      return Err(invalid_data("indexed request archive body index differs"));
    }
    if ordinal == object {
      let actual = objects
        .query_row([sql_int(object)?], read_object)
        .map_err(invalid_data)?;
      let expected = ObjectIndex {
        offset: expected.offset + HEADER_LEN,
        ..expected
      };
      if actual != expected {
        return Err(invalid_data("indexed request archive object index differs"));
      }
    }
  }
  Ok(())
}

fn number<T: TryFrom<i64>>(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<T> {
  let value: i64 = row.get(column)?;
  T::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(column, value))
}

fn sql_int(value: impl TryInto<i64>) -> io::Result<i64> {
  value
    .try_into()
    .map_err(|_| invalid_data("archive index exceeds SQLite integer range"))
}

fn create_private_file(path: &Path) -> io::Result<File> {
  let mut options = OpenOptions::new();
  options.write(true).create_new(true);
  #[cfg(unix)]
  {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
  }
  options.open(path)
}

fn copy_exact(
  input: &mut impl Read,
  output: &mut impl Write,
  mut len: u64,
  progress: &mut impl FnMut(u64) -> io::Result<()>,
) -> io::Result<()> {
  let mut buffer = [0; 64 * 1024];
  while len != 0 {
    progress(0)?;
    let count = len.min(buffer.len() as u64) as usize;
    input.read_exact(&mut buffer[..count])?;
    output.write_all(&buffer[..count])?;
    progress(count as u64)?;
    len -= count as u64;
  }
  Ok(())
}

struct ProgressIo<'a, T, F> {
  inner: T,
  progress: &'a mut F,
  error: Option<io::Error>,
}

impl<'a, T, F: FnMut(u64) -> io::Result<()>> ProgressIo<'a, T, F> {
  fn new(inner: T, progress: &'a mut F) -> Self {
    Self {
      inner,
      progress,
      error: None,
    }
  }

  fn report(&mut self, bytes: u64) -> io::Result<()> {
    if let Err(error) = (self.progress)(bytes) {
      // Read/Write automatically retries Interrupted, so recover the original
      // kind after unwinding out of the codec.
      self.error = Some(error);
      return Err(io::Error::other("request archive progress interrupted"));
    }
    Ok(())
  }

  fn finish<V>(self, result: anyhow::Result<V>) -> io::Result<V> {
    match self.error {
      Some(error) => Err(error),
      None => result.map_err(codec_error),
    }
  }
}

impl<T: Read, F: FnMut(u64) -> io::Result<()>> Read for ProgressIo<'_, T, F> {
  fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
    self.report(0)?;
    let count = self.inner.read(bytes)?;
    self.report(count as u64)?;
    Ok(count)
  }
}

impl<T: Write, F: FnMut(u64) -> io::Result<()>> Write for ProgressIo<'_, T, F> {
  fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
    self.report(0)?;
    self.inner.write(bytes)
  }

  fn flush(&mut self) -> io::Result<()> {
    self.inner.flush()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::request_files::StableFile;
  use std::cell::Cell;
  use std::io::Cursor;

  fn fixture() -> anyhow::Result<(RequestPack, String)> {
    let source = tempfile::tempdir()?;
    let day = source.path().join("day.db");
    let conn = crate::requests::open_day_db(&day)?;
    conn.execute_batch(
      "INSERT INTO request_connection (request_id, ts) VALUES ('first', 1), ('second', 2);
       INSERT INTO request_metadata (request_id, session_id) VALUES ('first', 'session'), ('second', 'session');",
    )?;
    let base = "the same request prefix ".repeat(1000);
    let target = format!("{base}and the next turn");
    conn.execute(
      "INSERT INTO request_downstream (request_id, inbound_req_body) VALUES ('first', ?1), ('second', ?2)",
      (&base, &target),
    )?;
    conn.execute(
      "INSERT INTO request_upstream (request_id, outbound_req_body) VALUES ('second', ?1)",
      [&target],
    )?;
    drop(conn);
    let snapshot = RequestPack::capture(&StableFile::open(&day)?, &mut |_| Ok(()))?
      .ok_or_else(|| invalid_data("test database is not packable"))?;
    Ok((snapshot, target))
  }

  #[test]
  fn reads_metadata_without_scanning_body_objects_and_reconstructs_requested_deltas() -> anyhow::Result<()> {
    let (snapshot, target) = fixture()?;
    let mut bytes = Vec::new();
    write(&snapshot, &mut bytes, &mut |_| Ok(()))?;
    verify(&snapshot, &mut Cursor::new(&bytes), &mut |_| Ok(()))?;

    let first_read = Cell::new(u64::MAX);
    let reader = IndexedPackReader::open(
      ObservedReader {
        input: Cursor::new(&bytes),
        first_read: &first_read,
      },
      Duration::ZERO,
    )?;
    assert_eq!(first_read.get(), reader.metadata_offset);
    let distinct: i64 = reader
      .connection()
      .query_row("SELECT count(*) FROM tokn_archive_objects", [], |row| row.get(0))?;
    assert_eq!(distinct, 2);
    let depth: i64 = reader
      .connection()
      .query_row("SELECT max(depth) FROM tokn_archive_objects", [], |row| row.get(0))?;
    assert_eq!(depth, 1);
    for slot in [super::super::INBOUND_REQUEST_SLOT, super::super::OUTBOUND_REQUEST_SLOT] {
      let (body, flags) = reader.read_body("second", slot)?.expect("archived body");
      assert_eq!(body, target.as_bytes());
      assert_eq!(flags, super::super::BODY_IS_TEXT);
    }
    assert_eq!(first_read.get(), HEADER_LEN);
    assert!(reader.read_body("missing", 0)?.is_none());
    Ok(())
  }

  #[test]
  fn raw_pack_round_trips_through_a_zstandard_stream() -> anyhow::Result<()> {
    let (snapshot, target) = fixture()?;
    let mut encoder = zstd::stream::Encoder::new(Vec::new(), 0)?;
    write(&snapshot, &mut encoder, &mut |_| Ok(()))?;
    let compressed = encoder.finish()?;
    let payload = zstd::stream::decode_all(compressed.as_slice())?;
    let reader = IndexedPackReader::open(Cursor::new(payload), Duration::ZERO)?;
    let (bytes, _) = reader
      .read_body("second", super::super::INBOUND_REQUEST_SLOT)?
      .expect("archived body");
    assert_eq!(bytes, target.as_bytes());
    Ok(())
  }

  struct ObservedReader<'a> {
    input: Cursor<&'a Vec<u8>>,
    first_read: &'a Cell<u64>,
  }

  impl Read for ObservedReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
      let offset = self.input.position();
      let count = self.input.read(bytes)?;
      if count > 0 {
        self.first_read.set(self.first_read.get().min(offset));
      }
      Ok(count)
    }
  }

  impl Seek for ObservedReader<'_> {
    fn seek(&mut self, seek: SeekFrom) -> io::Result<u64> {
      self.input.seek(seek)
    }
  }

  #[test]
  fn rejects_tampered_object_index_and_invalid_trailer() -> anyhow::Result<()> {
    let (snapshot, _) = fixture()?;
    let mut bytes = Vec::new();
    write(&snapshot, &mut bytes, &mut |_| Ok(()))?;
    let offset = IndexedPackReader::open(Cursor::new(&bytes), Duration::ZERO)?.metadata_offset as usize;
    let trailer = bytes.len() - TRAILER_LEN as usize;

    let mut malformed = bytes.clone();
    malformed[trailer..trailer + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(IndexedPackReader::open(Cursor::new(&malformed), Duration::ZERO).is_err());

    let temp = tempfile::tempdir()?;
    let path = temp.path().join("metadata.db");
    std::fs::write(&path, &bytes[offset..trailer])?;
    let conn = Connection::open(&path)?;
    conn.execute(
      "UPDATE tokn_archive_objects SET logical_offset = ?1 WHERE depth = 1",
      [offset as i64],
    )?;
    drop(conn);
    let metadata = std::fs::read(&path)?;
    bytes.truncate(offset);
    bytes.extend_from_slice(&metadata);
    bytes.extend_from_slice(&(metadata.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&MAGIC);

    let reader = IndexedPackReader::open(Cursor::new(&bytes), Duration::ZERO)?;
    assert!(reader.read_body("second", 0).is_err());
    assert!(verify(&snapshot, &mut Cursor::new(&bytes), &mut |_| Ok(())).is_err());
    Ok(())
  }

  #[test]
  fn propagates_interrupted_progress_during_body_writes() -> anyhow::Result<()> {
    let (snapshot, _) = fixture()?;
    let mut calls = 0;
    let error = write(&snapshot, &mut Vec::new(), &mut |_| {
      calls += 1;
      if calls >= 2 {
        Err(io::Error::new(io::ErrorKind::Interrupted, "archive cancelled"))
      } else {
        Ok(())
      }
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    Ok(())
  }
}
