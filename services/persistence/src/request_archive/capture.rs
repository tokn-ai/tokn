use super::{invalid_data, FIELDS, MAX_REQUEST_ID_BYTES, SESSION_WINDOW};
use crate::request_files::StableFile;
use flate2::bufread::MultiGzDecoder;
use memmap2::Mmap;
use rusqlite::{types::ValueRef, Connection, ErrorCode, OpenFlags};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{self, BufWriter, Read, Write};
use std::ops::Range;
use std::path::Path;
use tempfile::NamedTempFile;
use url::Url;

use super::objects::{MAX_BODY_BYTES, MAX_OBJECTS};

const TABLES: [&str; 5] = [
  "schema_migrations",
  "request_connection",
  "request_metadata",
  "request_downstream",
  "request_upstream",
];
const SCHEMA_VERSION: i64 = 8;

pub(super) struct Capture {
  pub metadata_snapshot: NamedTempFile,
  pub bodies: Vec<Body>,
  candidate_ranges: Vec<Range<usize>>,
  candidate_indices: Vec<usize>,
  backing: Option<Mmap>,
}

impl Capture {
  pub fn candidates(&self, body_index: usize) -> &[usize] {
    &self.candidate_indices[self.candidate_ranges[body_index].clone()]
  }

  pub fn bytes(&self, body: &Body) -> &[u8] {
    &self.backing.as_deref().unwrap_or_default()[body.bytes.clone()]
  }
}

pub(super) struct Body {
  pub request_id: String,
  pub slot: u8,
  pub flags: u8,
  bytes: Range<usize>,
}

struct Spool {
  writer: BufWriter<fs::File>,
  len: usize,
}

impl Spool {
  fn new() -> io::Result<Self> {
    Ok(Self {
      writer: BufWriter::new(tempfile::tempfile()?),
      len: 0,
    })
  }

  fn append(&mut self, bytes: &[u8]) -> io::Result<Option<Range<usize>>> {
    let start = self.len;
    let Some(end) = start.checked_add(bytes.len()).filter(|&end| end <= isize::MAX as usize) else {
      return Ok(None);
    };
    self.writer.write_all(bytes)?;
    self.len = end;
    Ok(Some(start..end))
  }

  fn finish(mut self) -> io::Result<Option<Mmap>> {
    self.writer.flush()?;
    if self.len == 0 {
      return Ok(None);
    }
    // SAFETY: the unnamed temporary file is private. After flushing, this
    // consumes its only writer and no one can truncate or change the mapping.
    unsafe { Mmap::map(self.writer.get_ref()) }.map(Some)
  }
}

pub(super) fn load(
  source: &StableFile,
  progress: &mut impl FnMut(u64) -> io::Result<()>,
) -> io::Result<Option<Capture>> {
  progress(0)?;
  source.revalidate()?;
  let before = source.file().metadata()?;
  let path = source.path();
  check_sidecars(path)?;
  // The caller keeps the source stable and the checkpoint check rules out
  // pending writes. Immutable mode avoids creating sidecars or competing with
  // the prune guard.
  let mut uri = Url::from_file_path(path.canonicalize()?)
    .map_err(|()| io::Error::new(io::ErrorKind::InvalidInput, "invalid request database path"))?;
  uri.set_query(Some("mode=ro&immutable=1"));

  let metadata_snapshot = NamedTempFile::new()?;
  let mut snapshot_db = Connection::open(metadata_snapshot.path()).map_err(invalid_data)?;
  snapshot_db
    .execute_batch("PRAGMA trusted_schema = OFF;")
    .map_err(invalid_data)?;
  let main = snapshot_db.transaction().map_err(invalid_data)?;
  initialize_snapshot(&main).map_err(invalid_data)?;
  main.commit().map_err(invalid_data)?;
  match snapshot_db.execute("ATTACH DATABASE ?1 AS source", [uri.as_str()]) {
    Ok(_) => {}
    Err(error) if error.sqlite_error_code() == Some(ErrorCode::NotADatabase) => return Ok(None),
    Err(error) => return Err(invalid_data(error)),
  }
  source.revalidate()?;
  let tx = snapshot_db.transaction().map_err(invalid_data)?;
  if schema(&tx, "source").map_err(invalid_data)? != schema(&tx, "main").map_err(invalid_data)? {
    return Ok(None);
  }
  let version: Option<i64> = tx
    .query_row("SELECT max(version) FROM source.schema_migrations", [], |row| {
      row.get(0)
    })
    .map_err(invalid_data)?;
  if version != Some(SCHEMA_VERSION) {
    return Ok(None);
  }
  for name in ["application_id", "user_version"] {
    let value: i64 = tx
      .pragma_query_value(Some("source"), name, |row| row.get(0))
      .map_err(invalid_data)?;
    tx.pragma_update(Some("main"), name, value).map_err(invalid_data)?;
  }
  for table in TABLES {
    progress(0)?;
    copy_snapshot_table(&tx, table).map_err(invalid_data)?;
  }
  let mut spool = Spool::new()?;
  let Some(bodies) = read_bodies(&tx, &mut spool, progress)? else {
    return Ok(None);
  };
  let (candidate_ranges, candidate_indices) = find_session_candidates(&tx, &bodies, progress)?;
  progress(0)?;
  tx.commit().map_err(invalid_data)?;
  drop(snapshot_db);

  source.revalidate()?;
  check_sidecars(path)?;
  let after = source.file().metadata()?;
  if before.len() != after.len() || before.modified()? != after.modified()? {
    return Err(invalid_data("request database changed during archive capture"));
  }
  if u32::try_from(metadata_snapshot.as_file().metadata()?.len()).is_err() {
    return Ok(None);
  }

  Ok(Some(Capture {
    metadata_snapshot,
    bodies: bodies.into_iter().map(|(_, body)| body).collect(),
    candidate_ranges,
    candidate_indices,
    backing: spool.finish()?,
  }))
}

fn check_sidecars(path: &Path) -> io::Result<()> {
  for suffix in ["-journal", "-wal", "-shm"] {
    let mut sidecar = path.as_os_str().to_owned();
    sidecar.push(suffix);
    match fs::symlink_metadata(&sidecar) {
      Ok(info) if info.is_file() && info.len() == 0 => {}
      Ok(_) => {
        return Err(invalid_data(format!(
          "request database has an active SQLite {suffix} sidecar"
        )))
      }
      Err(error) if error.kind() == io::ErrorKind::NotFound => {}
      Err(error) => return Err(error),
    }
  }
  Ok(())
}

fn initialize_snapshot(conn: &Connection) -> rusqlite::Result<()> {
  crate::migrate::ensure_schema_table(conn)?;
  // The packed body fields are tied to the v8 request schema. The forward-only
  // migration history lets newer readers keep recognizing it.
  for migration in crate::requests::MIGRATIONS
    .iter()
    .take_while(|migration| i64::from(migration.version) <= SCHEMA_VERSION)
  {
    conn.execute_batch(migration.sql)?;
  }
  Ok(())
}

pub(super) fn has_snapshot_schema_with(
  conn: &Connection,
  database: &str,
  extra_schema: &str,
) -> rusqlite::Result<bool> {
  let expected = Connection::open_in_memory()?;
  initialize_snapshot(&expected)?;
  expected.execute_batch(extra_schema)?;
  Ok(schema(conn, database)? == schema(&expected, "main")?)
}

fn schema(conn: &Connection, db: &str) -> rusqlite::Result<Vec<String>> {
  let mut stmt = conn.prepare(&format!(
    "SELECT sql FROM {db}.sqlite_schema WHERE sql IS NOT NULL ORDER BY type, name"
  ))?;
  let definitions = stmt.query_map([], |row| {
    Ok(
      row
        .get::<_, String>(0)?
        .split_ascii_whitespace()
        .collect::<Vec<_>>()
        .join(" "),
    )
  })?;
  definitions.collect()
}

fn copy_snapshot_table(conn: &Connection, table: &str) -> rusqlite::Result<()> {
  let mut stmt = conn.prepare("SELECT name FROM pragma_table_info(?1, 'main') ORDER BY cid")?;
  let columns = stmt
    .query_map([table], |row| row.get::<_, String>(0))?
    .collect::<rusqlite::Result<Vec<_>>>()?;
  let select: Vec<_> = columns
    .iter()
    .map(|column| {
      if FIELDS.iter().any(|(owner, body, _)| *owner == table && *body == column) {
        "NULL"
      } else {
        column.as_str()
      }
    })
    .collect();
  conn.execute(
    &format!(
      "INSERT INTO main.{table}(rowid, {}) SELECT rowid, {} FROM source.{table} ORDER BY rowid",
      columns.join(", "),
      select.join(", ")
    ),
    [],
  )?;
  Ok(())
}

type OrderedBody = (Option<i64>, Body);

fn read_bodies(
  conn: &Connection,
  spool: &mut Spool,
  progress: &mut impl FnMut(u64) -> io::Result<()>,
) -> io::Result<Option<Vec<OrderedBody>>> {
  let mut bodies = Vec::new();
  for (slot, (table, body, headers)) in FIELDS.iter().enumerate() {
    progress(0)?;
    let mut stmt = conn
      .prepare(&format!(
        "SELECT c.rowid, t.request_id, t.{body}, t.{headers} FROM source.{table} t
         LEFT JOIN source.request_connection c ON c.request_id = t.request_id WHERE t.{body} IS NOT NULL"
      ))
      .map_err(invalid_data)?;
    let mut rows = stmt.query([]).map_err(invalid_data)?;
    while let Some(row) = rows.next().map_err(invalid_data)? {
      let (is_text, wire) = match row.get_ref(2).map_err(invalid_data)? {
        ValueRef::Text(bytes) => (true, bytes),
        ValueRef::Blob(bytes) => (false, bytes),
        _ => return Ok(None),
      };
      if bodies.len() >= MAX_OBJECTS || wire.len() > MAX_BODY_BYTES {
        return Ok(None);
      }
      let ValueRef::Text(request_id) = row.get_ref(1).map_err(invalid_data)? else {
        return Ok(None);
      };
      let Ok(request_id) = std::str::from_utf8(request_id) else {
        return Ok(None);
      };
      if request_id.len() > MAX_REQUEST_ID_BYTES {
        return Ok(None);
      }
      progress(wire.len() as u64)?;
      let decoded = row
        .get_ref(3)
        .map_err(invalid_data)?
        .as_bytes()
        .ok()
        .and_then(|headers| decode(headers, wire));
      let Some(bytes) = spool.append(decoded.as_deref().unwrap_or(wire))? else {
        return Ok(None);
      };
      let flags = u8::from(decoded.is_some()) | (u8::from(is_text) << 1);
      let rowid = row.get::<_, Option<i64>>(0).map_err(invalid_data)?;
      bodies.push((
        rowid,
        Body {
          request_id: request_id.to_string(),
          slot: slot as u8,
          flags,
          bytes,
        },
      ));
    }
  }
  bodies.sort_unstable_by(|(a, left), (b, right)| {
    (a.is_none(), a, &left.request_id, left.slot).cmp(&(b.is_none(), b, &right.request_id, right.slot))
  });
  Ok(Some(bodies))
}

type SessionCandidates = (Vec<Range<usize>>, Vec<usize>);

fn find_session_candidates(
  conn: &Connection,
  bodies: &[OrderedBody],
  progress: &mut impl FnMut(u64) -> io::Result<()>,
) -> io::Result<SessionCandidates> {
  let mut request_bodies = HashMap::<i64, [Option<usize>; FIELDS.len()]>::new();
  for (index, (rowid, body)) in bodies.iter().enumerate() {
    if let Some(rowid) = rowid {
      request_bodies.entry(*rowid).or_default()[usize::from(body.slot)] = Some(index);
    }
  }
  let mut statement = conn
    .prepare(
      "SELECT c.rowid, CAST(m.session_id AS BLOB)
       FROM source.request_connection c
       JOIN source.request_metadata m ON m.request_id = c.request_id
       WHERE typeof(m.session_id) = 'text' AND trim(m.session_id) <> ''
       ORDER BY m.session_id, c.rowid",
    )
    .map_err(invalid_data)?;
  let mut rows = statement.query([]).map_err(invalid_data)?;
  let empty_turn = [None; FIELDS.len()];
  let mut history = VecDeque::<[Option<usize>; FIELDS.len()]>::with_capacity(SESSION_WINDOW);
  let mut session_id = Vec::new();
  let mut ranges = vec![0..0; bodies.len()];
  let mut indices = Vec::new();
  while let Some(row) = rows.next().map_err(invalid_data)? {
    progress(0)?;
    let rowid: i64 = row.get(0).map_err(invalid_data)?;
    let id = row.get_ref(1).map_err(invalid_data)?.as_blob().map_err(invalid_data)?;
    if session_id != id {
      session_id.clear();
      session_id.extend_from_slice(id);
      history.clear();
    }
    let turn = request_bodies.get(&rowid).copied().unwrap_or(empty_turn);
    for (slot, index) in turn.into_iter().enumerate() {
      let Some(index) = index else { continue };
      let start = indices.len();
      indices.extend(history.iter().filter_map(|prior| prior[slot]));
      ranges[index] = start..indices.len();
    }
    // Requests without bodies still take a place in the turn window.
    if history.len() == SESSION_WINDOW {
      history.pop_back();
    }
    history.push_front(turn);
  }
  Ok((ranges, indices))
}

pub(super) fn verify_metadata_rows(expected: &Path, actual: &Connection) -> io::Result<()> {
  let expected = Connection::open_with_flags(expected, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(invalid_data)?;
  expected
    .execute_batch("PRAGMA trusted_schema = OFF;")
    .map_err(invalid_data)?;
  let integrity: String = actual
    .query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))
    .map_err(invalid_data)?;
  if integrity != "ok" {
    return Err(invalid_data(
      "archive metadata snapshot failed SQLite integrity validation",
    ));
  }
  for name in ["application_id", "user_version"] {
    let value = |conn: &Connection| conn.pragma_query_value(None, name, |row| row.get::<_, i64>(0));
    if value(&expected).map_err(invalid_data)? != value(actual).map_err(invalid_data)? {
      return Err(invalid_data("archive metadata snapshot database identifier differs"));
    }
  }
  for table in TABLES {
    if !table_matches(&expected, actual, table).map_err(invalid_data)? {
      return Err(invalid_data(format!("archive metadata snapshot {table} rows differ")));
    }
  }
  Ok(())
}

fn table_matches(expected: &Connection, actual: &Connection, table: &str) -> rusqlite::Result<bool> {
  let sql = format!("SELECT rowid, * FROM {table} NOT INDEXED ORDER BY rowid");
  let mut expected_stmt = expected.prepare(&sql)?;
  let mut actual_stmt = actual.prepare(&sql)?;
  let columns = expected_stmt.column_count();
  let mut expected_rows = expected_stmt.query([])?;
  let mut actual_rows = actual_stmt.query([])?;
  loop {
    let (left, right) = match (expected_rows.next()?, actual_rows.next()?) {
      (Some(left), Some(right)) => (left, right),
      (None, None) => return Ok(true),
      _ => return Ok(false),
    };
    for column in 0..columns {
      if left.get_ref(column)? != right.get_ref(column)? {
        return Ok(false);
      }
    }
  }
}

fn decode(headers: &[u8], wire: &[u8]) -> Option<Vec<u8>> {
  let map = serde_json::from_slice::<serde_json::Map<String, Value>>(headers).ok()?;
  let mut encoding = None;
  let mut length = None;
  for (name, value) in &map {
    let header = if name.eq_ignore_ascii_case("content-encoding") {
      &mut encoding
    } else if name.eq_ignore_ascii_case("content-length") {
      &mut length
    } else {
      continue;
    };
    if header.replace(value.as_str()?.trim()).is_some() {
      return None;
    }
  }
  if let Some(length) = length {
    if !length.bytes().all(|byte| byte.is_ascii_digit()) || length.parse::<usize>().ok()? != wire.len() {
      return None;
    }
  }
  if wire.is_empty() {
    return None;
  }
  let encoding = encoding?;
  if encoding.eq_ignore_ascii_case("gzip") {
    bounded_decode(MultiGzDecoder::new(wire))
  } else if encoding.eq_ignore_ascii_case("zstd") {
    let mut decoder = zstd::Decoder::with_buffer(wire).ok()?;
    decoder.window_log_max(26).ok()?;
    bounded_decode(decoder)
  } else {
    None
  }
}

fn bounded_decode(reader: impl Read) -> Option<Vec<u8>> {
  let mut bytes = Vec::new();
  reader.take(MAX_BODY_BYTES as u64 + 1).read_to_end(&mut bytes).ok()?;
  (bytes.len() <= MAX_BODY_BYTES).then_some(bytes)
}
