use super::{has_snapshot_schema, objects, read_u32, verify_magic, FIELDS, MAX_DEPTH, MAX_REQUEST_ID_BYTES};
use anyhow::{ensure, Result};
use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{params, Connection, OpenFlags};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::Path;

const DECODED: u8 = 1;
const TEXT: u8 = 2;

/// Restore a packed request day to a new SQLite file in a private directory.
/// Decoded bodies retain their original captured headers and carry a marker
/// so readers can interpret the stored bytes without decoding them twice.
pub fn restore(reader: &mut dyn Read, destination: &Path) -> io::Result<()> {
  restore_to(reader, destination, Bodies::All)
}

/// Restore request metadata and captured headers without replaying body objects.
pub fn restore_metadata(reader: &mut dyn Read, destination: &Path) -> io::Result<()> {
  restore_to(reader, destination, Bodies::Omit)
}

/// Restore the metadata snapshot and only the specified request's body slots.
pub fn restore_request(reader: &mut dyn Read, destination: &Path, request_id: &str) -> io::Result<()> {
  restore_to(reader, destination, Bodies::Request(request_id))
}

#[derive(Clone, Copy)]
enum Bodies<'a> {
  All,
  Omit,
  Request(&'a str),
}

fn restore_to(reader: &mut dyn Read, destination: &Path, bodies: Bodies<'_>) -> io::Result<()> {
  restore_pack(reader, destination, bodies).map_err(|error| {
    let kind = error
      .chain()
      .find_map(|cause| cause.downcast_ref::<io::Error>().map(io::Error::kind))
      .unwrap_or(io::ErrorKind::InvalidData);
    io::Error::new(kind, error)
  })
}

fn restore_pack(input: &mut dyn Read, destination: &Path, bodies: Bodies<'_>) -> Result<()> {
  verify_magic(input)?;

  let parent = destination
    .parent()
    .filter(|parent| !parent.as_os_str().is_empty())
    .unwrap_or_else(|| Path::new("."));
  let mut snapshot = tempfile::Builder::new().prefix(".tokn-request-").tempfile_in(parent)?;
  let snapshot_len = u64::from(read_u32(input)?);
  ensure!(snapshot_len > 0, "empty request metadata snapshot");
  let written = io::copy(&mut (&mut *input).take(snapshot_len), snapshot.as_file_mut())?;
  ensure!(written == snapshot_len, "truncated request metadata snapshot");
  snapshot.as_file_mut().flush()?;

  let count = usize::try_from(read_u32(input)?)?;
  ensure!(count <= objects::MAX_OBJECTS, "too many request body descriptors");

  let snapshot = snapshot.into_temp_path();
  let mut conn = Connection::open_with_flags(&snapshot, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
  conn.execute_batch("PRAGMA trusted_schema = OFF;")?;
  ensure!(
    has_snapshot_schema(&conn, "main")?,
    "invalid request metadata snapshot schema"
  );
  for (table, body, _) in FIELDS {
    let populated: bool = conn.query_row(
      &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE {body} IS NOT NULL)"),
      [],
      |row| row.get(0),
    )?;
    ensure!(!populated, "request metadata snapshot already contains a body");
  }

  match bodies {
    Bodies::Omit => {
      let remaining = io::copy(input, &mut io::sink())?;
      // At minimum, each descriptor has a four-byte length and two flag
      // bytes, and its corresponding object has a tag and four-byte field.
      ensure!(
        (count == 0 && remaining == 0) || (count > 0 && remaining >= 11 * count as u64),
        "invalid request archive body size"
      );
    }
    Bodies::All => restore_bodies(&mut conn, input, count, None)?,
    Bodies::Request(id) => restore_bodies(&mut conn, input, count, Some(id))?,
  }
  conn.close().map_err(|(_, error)| error)?;
  snapshot.persist_noclobber(destination).map_err(|error| error.error)?;
  Ok(())
}

fn restore_bodies(
  conn: &mut Connection,
  mut input: &mut dyn Read,
  count: usize,
  request_id: Option<&str>,
) -> Result<()> {
  let tx = conn.transaction()?;
  tx.execute_batch(
    "CREATE TABLE tokn_archive_decoded_bodies (
       request_id TEXT NOT NULL,
       slot INTEGER NOT NULL,
       PRIMARY KEY (request_id, slot)
     ) WITHOUT ROWID;",
  )?;
  {
    let mut updates = FIELDS
      .iter()
      .map(|(table, body, _)| {
        tx.prepare(&format!(
          "UPDATE {table} SET {body} = ?1 WHERE request_id = ?2 AND {body} IS NULL"
        ))
      })
      .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut mark_decoded = tx.prepare("INSERT INTO tokn_archive_decoded_bodies (request_id, slot) VALUES (?1, ?2)")?;
    let mut install = |id: &str, slot: usize, flags: u8, body: &[u8]| -> Result<()> {
      let value = ToSqlOutput::Borrowed(if flags & TEXT == 0 {
        ValueRef::Blob(body)
      } else {
        ValueRef::Text(body)
      });
      let updated = updates[slot].execute(params![value, id])?;
      ensure!(
        updated == 1,
        "request body descriptor has no unique empty slot in the metadata snapshot"
      );
      if flags & DECODED != 0 {
        mark_decoded.execute(params![id, slot as u8])?;
      }
      Ok(())
    };
    if let Some(id) = request_id {
      let descriptors = select_descriptors(input, count, id)?;
      let mut objects = objects::index_objects(input, count, MAX_DEPTH)?;
      for (slot, descriptor) in descriptors.into_iter().enumerate() {
        if let Some((index, flags)) = descriptor {
          install(id, slot, flags, objects.body(index)?)?;
        }
      }
    } else {
      let mut descriptors = BufReader::new(spool_descriptors(input, count)?);
      let mut id = Vec::new();
      objects::restore_objects(&mut input, count, MAX_DEPTH, |_, body| {
        let (slot, flags) = read_descriptor(&mut descriptors, &mut id)?;
        install(std::str::from_utf8(&id)?, usize::from(slot), flags, body)
      })?;
    }
  }
  ensure!(
    input.read(&mut [0; 1])? == 0,
    "unexpected trailing request archive data"
  );
  tx.commit()?;
  Ok(())
}

fn select_descriptors(input: &mut dyn Read, count: usize, target: &str) -> Result<[Option<(usize, u8)>; FIELDS.len()]> {
  let mut selected = [None; FIELDS.len()];
  let mut id = Vec::new();
  for index in 0..count {
    let (slot, flags) = read_descriptor(input, &mut id)?;
    if std::str::from_utf8(&id)? == target {
      let previous = selected[usize::from(slot)].replace((index, flags));
      ensure!(previous.is_none(), "duplicate request body descriptor");
    }
  }
  Ok(selected)
}

fn spool_descriptors(input: &mut dyn Read, count: usize) -> Result<File> {
  let mut spool = tempfile::tempfile()?;
  let mut request_id = Vec::new();
  {
    let mut output = BufWriter::new(&mut spool);
    for _ in 0..count {
      let (slot, flags) = read_descriptor(input, &mut request_id)?;
      output.write_all(&u32::try_from(request_id.len())?.to_le_bytes())?;
      output.write_all(&request_id)?;
      output.write_all(&[slot, flags])?;
    }
    output.flush()?;
  }
  spool.rewind()?;
  Ok(spool)
}

fn read_descriptor(input: &mut dyn Read, request_id: &mut Vec<u8>) -> Result<(u8, u8)> {
  let len = usize::try_from(read_u32(input)?)?;
  ensure!(len <= MAX_REQUEST_ID_BYTES, "request identifier exceeds size limit");
  request_id.resize(len, 0);
  input.read_exact(request_id)?;
  let mut descriptor = [0; 2];
  input.read_exact(&mut descriptor)?;
  let [slot, flags] = descriptor;
  ensure!(usize::from(slot) < FIELDS.len(), "invalid request body slot");
  ensure!(flags & !(DECODED | TEXT) == 0, "invalid request body flags");
  Ok((slot, flags))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::request_archive::objects::{InputBody, Sharing};
  use crate::request_archive::MAGIC;
  use crate::requests::open_day_db;
  use anyhow::Context;

  const HEADERS: &[u8] = br#"{"content-encoding":"gzip","content-type":"application/json"}"#;

  #[test]
  fn restores_exact_body_bytes_types_and_decoded_marker() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let template = dir.path().join("metadata.db");
    let conn = open_day_db(&template)?;
    conn.execute_batch(
      "INSERT INTO request_connection (request_id, ts)
       VALUES ('first', 1), ('second', 2), ('third', 3), ('reference', 4), ('text', 5);",
    )?;
    for id in ["first", "second", "third", "reference", "text"] {
      conn.execute(
        "INSERT INTO request_downstream (request_id, inbound_req_headers) VALUES (?1, ?2)",
        params![id, HEADERS],
      )?;
    }
    drop(conn);
    let metadata_bytes = std::fs::read(template)?;
    let first = br#"{"message":"A stable and substantial payload for a request archive restore fixture"}"#.repeat(16);
    let mut second = first.clone();
    second.extend_from_slice(b" second");
    let mut third = second.clone();
    third.extend_from_slice(b" third");
    let text = b"stored as text, even with invalid UTF-8 \xff";
    let descriptors = [
      ("first", 0),
      ("second", DECODED),
      ("third", DECODED),
      ("reference", 0),
      ("text", TEXT),
    ];
    let mut archive = pack_prefix(&metadata_bytes, &descriptors)?;
    let bodies = [
      InputBody {
        bytes: &first,
        base: None,
        session_bases: &[],
      },
      InputBody {
        bytes: &second,
        base: None,
        session_bases: &[0],
      },
      InputBody {
        bytes: &third,
        base: None,
        session_bases: &[1],
      },
      InputBody {
        bytes: &first,
        base: None,
        session_bases: &[],
      },
      InputBody {
        bytes: text,
        base: None,
        session_bases: &[],
      },
    ];
    let stats = objects::write_objects(&mut archive, &bodies, Sharing::Delta, MAX_DEPTH)?;
    assert_eq!(stats.deltas, 2);
    assert_eq!(stats.max_depth, 2);
    assert_eq!(stats.references, 1);
    let destination = dir.path().join("restored.db");
    restore(&mut archive.as_slice(), &destination)?;
    let conn = Connection::open_with_flags(&destination, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    for (index, (id, flags)) in descriptors.into_iter().enumerate() {
      let mut stmt =
        conn.prepare("SELECT inbound_req_headers, inbound_req_body FROM request_downstream WHERE request_id = ?1")?;
      let mut rows = stmt.query([id])?;
      let row = rows.next()?.context("restored request")?;
      assert_eq!(row.get_ref(0)?, ValueRef::Blob(HEADERS));
      let expected = if flags & TEXT == 0 {
        ValueRef::Blob(bodies[index].bytes)
      } else {
        ValueRef::Text(bodies[index].bytes)
      };
      assert_eq!(row.get_ref(1)?, expected);
      let decoded: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM tokn_archive_decoded_bodies WHERE request_id = ?1 AND slot = 0)",
        [id],
        |row| row.get(0),
      )?;
      assert_eq!(decoded, flags & DECODED != 0);
    }

    let metadata_only = dir.path().join("metadata-only.db");
    restore_metadata(&mut archive.as_slice(), &metadata_only)?;
    let snapshot = Connection::open_with_flags(&metadata_only, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let body_count: i64 = snapshot.query_row("SELECT COUNT(inbound_req_body) FROM request_downstream", [], |row| {
      row.get(0)
    })?;
    assert_eq!(body_count, 0);
    let headers: Vec<u8> = snapshot.query_row(
      "SELECT inbound_req_headers FROM request_downstream WHERE request_id = 'second'",
      [],
      |row| row.get(0),
    )?;
    assert_eq!(headers, HEADERS);

    let selected = dir.path().join("selected.db");
    restore_request(&mut archive.as_slice(), &selected, "third")?;
    let selected = Connection::open_with_flags(&selected, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let chosen: (String, Vec<u8>) = selected.query_row(
      "SELECT request_id, inbound_req_body FROM request_downstream WHERE inbound_req_body IS NOT NULL",
      [],
      |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(chosen, ("third".to_string(), third));
    let count: i64 = selected.query_row("SELECT COUNT(inbound_req_body) FROM request_downstream", [], |row| {
      row.get(0)
    })?;
    assert_eq!(count, 1);
    Ok(())
  }

  #[test]
  fn rejects_noncanonical_snapshot_and_trailing_bytes_without_publishing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let template = dir.path().join("metadata.db");
    let conn = open_day_db(&template)?;
    let mut extra = pack_prefix(&std::fs::read(&template)?, &[])?;
    extra.push(0);
    let destination = dir.path().join("restored.db");
    assert_eq!(
      restore(&mut extra.as_slice(), &destination).unwrap_err().kind(),
      io::ErrorKind::InvalidData
    );
    assert!(!destination.exists());

    conn.execute_batch(
      "CREATE TRIGGER unexpected AFTER UPDATE ON request_downstream BEGIN DELETE FROM request_connection; END;",
    )?;
    drop(conn);
    let untrusted = pack_prefix(&std::fs::read(&template)?, &[])?;
    assert_eq!(
      restore(&mut untrusted.as_slice(), &destination).unwrap_err().kind(),
      io::ErrorKind::InvalidData
    );
    assert!(!destination.exists());
    Ok(())
  }

  fn pack_prefix(metadata_bytes: &[u8], descriptors: &[(&str, u8)]) -> Result<Vec<u8>> {
    let mut bytes = Vec::from(MAGIC);
    bytes.extend_from_slice(&u32::try_from(metadata_bytes.len())?.to_le_bytes());
    bytes.extend_from_slice(metadata_bytes);
    bytes.extend_from_slice(&u32::try_from(descriptors.len())?.to_le_bytes());
    for (id, flags) in descriptors {
      bytes.extend_from_slice(&u32::try_from(id.len())?.to_le_bytes());
      bytes.extend_from_slice(id.as_bytes());
      bytes.extend_from_slice(&[0, *flags]);
    }
    Ok(bytes)
  }
}
