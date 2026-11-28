//! Body objects with whole-byte deduplication and bounded copy/literal delta chains.

use anyhow::{ensure, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;

const FULL: u8 = 0;
const REFERENCE: u8 = 1;
const DELTA: u8 = 2;
const COPY: u8 = 0;
const LITERAL: u8 = 1;

pub(super) const MAX_OBJECTS: usize = 1_000_000;
pub(super) const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_OPERATIONS: usize = 1_000_000;
const MAX_ANCHORS: usize = 262_144;
const ANCHOR_BYTES: usize = 16;
const ROLLING_BASE: u64 = 257;
const ANCHOR_FACTOR: u64 = ROLLING_BASE.wrapping_pow(ANCHOR_BYTES as u32 - 1);
const MISSING_OFFSET: u32 = u32::MAX;
const DELTA_HEADER_BYTES: usize = 13;
const COPY_BYTES: usize = 9;
const MAX_CACHED_BASES: usize = 128;
const MAX_CACHED_ANCHOR_BYTES: usize = 96 * 1024 * 1024;
// Allows for hash table control bytes and unused buckets as well as entries.
const ANCHOR_ENTRY_BYTES: usize = 32;

#[derive(Clone, Copy)]
pub(super) enum Sharing {
  #[cfg(test)]
  Full,
  #[cfg(test)]
  Dedup,
  Delta,
}

impl Sharing {
  fn dedup(self) -> bool {
    match self {
      #[cfg(test)]
      Self::Full => false,
      _ => true,
    }
  }
}

pub(super) struct InputBody<'a> {
  pub bytes: &'a [u8],
  pub base: Option<usize>,
  pub session_bases: &'a [usize],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ObjectIndex {
  pub offset: u64,
  pub encoded_len: u64,
  pub body_len: usize,
  pub sha256: [u8; 32],
  pub base: Option<usize>,
  pub depth: usize,
}

#[derive(Default)]
pub(super) struct ObjectStats {
  pub full: usize,
  pub references: usize,
  pub deltas: usize,
  pub session_deltas: usize,
  pub max_depth: usize,
}

#[derive(Clone, Copy)]
enum Form {
  Full,
  Reference(usize),
  Delta(usize),
}

impl Form {
  fn canonical_depth(self) -> Option<usize> {
    match self {
      Self::Full => Some(0),
      Self::Delta(depth) => Some(depth),
      Self::Reference(_) => None,
    }
  }
}

pub(super) fn write_objects(
  out: &mut impl Write,
  bodies: &[InputBody<'_>],
  sharing: Sharing,
  max_depth: usize,
) -> Result<ObjectStats> {
  write_objects_with_index(out, bodies, sharing, max_depth, |_, _| {})
}

/// Each index entry describes an object relative to the first encoded byte.
/// The second vector directs body slots to their canonical object ordinals.
pub(super) fn write_indexed_objects(
  out: &mut impl Write,
  bodies: &[InputBody<'_>],
  max_depth: usize,
) -> Result<(Vec<ObjectIndex>, Vec<usize>)> {
  let mut entries = Vec::new();
  let mut canonical = Vec::new();
  write_objects_with_index(out, bodies, Sharing::Delta, max_depth, |entry, ordinal| {
    entries.push(entry);
    canonical.push(ordinal);
  })?;
  Ok((entries, canonical))
}

fn write_objects_with_index(
  out: &mut impl Write,
  bodies: &[InputBody<'_>],
  sharing: Sharing,
  max_depth: usize,
  mut record: impl FnMut(ObjectIndex, usize),
) -> Result<ObjectStats> {
  ensure!(bodies.len() <= MAX_OBJECTS, "too many body objects");
  for (index, body) in bodies.iter().enumerate() {
    ensure!(body.bytes.len() <= MAX_BODY_BYTES, "body exceeds size limit");
    ensure!(
      body
        .base
        .into_iter()
        .chain(body.session_bases.iter().copied())
        .all(|base| base < index),
      "body's bases must precede it"
    );
  }

  let mut stats = ObjectStats::default();
  let mut forms = Vec::<Form>::with_capacity(bodies.len());
  let mut seen = HashMap::<[u8; 32], Vec<usize>>::new();
  let mut anchors = AnchorCache::default();
  let mut offset = 0;
  for (index, body) in bodies.iter().enumerate() {
    let mut sha256 = [0; 32];
    let canonical = if sharing.dedup() {
      sha256 = Sha256::digest(body.bytes).into();
      let matches = seen.entry(sha256).or_default();
      let duplicate = matches.iter().copied().find(|&prior| bodies[prior].bytes == body.bytes);
      if duplicate.is_none() {
        matches.push(index);
      }
      duplicate
    } else {
      None
    };
    let delta = if canonical.is_none() && matches!(sharing, Sharing::Delta) {
      best_delta(bodies, &forms, body, max_depth, &mut anchors)
    } else {
      None
    };
    let (form, base, encoded_len, depth) = if let Some(prior) = canonical {
      out.write_all(&[REFERENCE])?;
      write_uint(out, prior)?;
      stats.references += 1;
      let depth = forms[prior]
        .canonical_depth()
        .expect("deduplication indexes canonical objects");
      (Form::Reference(prior), None, 5, depth)
    } else if let Some(delta) = delta {
      write_delta(out, delta.base, body.bytes, &delta.edits)?;
      stats.deltas += 1;
      stats.session_deltas += usize::from(delta.is_session);
      stats.max_depth = stats.max_depth.max(delta.depth);
      (
        Form::Delta(delta.depth),
        Some(delta.base),
        delta.wire_len as u64,
        delta.depth,
      )
    } else {
      out.write_all(&[FULL])?;
      write_uint(out, body.bytes.len())?;
      out.write_all(body.bytes)?;
      stats.full += 1;
      (Form::Full, None, 5 + body.bytes.len() as u64, 0)
    };
    forms.push(form);
    record(
      ObjectIndex {
        offset,
        encoded_len,
        body_len: body.bytes.len(),
        sha256,
        base,
        depth,
      },
      canonical.unwrap_or(index),
    );
    offset += encoded_len;
  }
  Ok(stats)
}

struct Delta {
  base: usize,
  depth: usize,
  edits: Vec<Edit>,
  wire_len: usize,
  is_session: bool,
}

fn best_delta(
  bodies: &[InputBody<'_>],
  forms: &[Form],
  body: &InputBody<'_>,
  max_depth: usize,
  anchors: &mut AnchorCache,
) -> Option<Delta> {
  let limit = body_delta_limit(body.bytes.len())?;
  let candidates = body
    .base
    .into_iter()
    .map(|base| (base, false))
    .chain(body.session_bases.iter().map(|&base| (base, true)));
  let mut tried = Vec::new();
  let mut best: Option<Delta> = None;
  for (base, is_session) in candidates {
    let base = match forms[base] {
      Form::Reference(target) => target,
      _ => base,
    };
    let Some(base_depth) = forms[base].canonical_depth() else {
      continue;
    };
    let depth = base_depth + 1;
    let source = bodies[base].bytes;
    if depth > max_depth || source.len() < ANCHOR_BYTES || tried.contains(&base) {
      continue;
    }
    tried.push(base);
    // Equal wire lengths win only with a shallower chain. This preserves the
    // paired base and then the most recent turn on exact ties.
    let budget = best
      .as_ref()
      .map_or(limit, |best| best.wire_len - usize::from(depth >= best.depth));
    if budget < delta_wire_lower_bound(source.len(), body.bytes.len()) {
      continue;
    }
    if let Some((edits, wire_len)) = select_delta(anchors.get(base, source), source, body.bytes, limit, budget) {
      best = Some(Delta {
        base,
        depth,
        edits,
        wire_len,
        is_session,
      });
    }
  }
  best
}

/// Check each object against the original bytes. Earlier bodies can serve as
/// references only after their bytes have been verified in this stream.
pub(super) fn verify_objects(input: &mut impl Read, bodies: &[InputBody<'_>], max_depth: usize) -> Result<()> {
  ensure!(bodies.len() <= MAX_OBJECTS, "too many body objects");
  let mut forms = Vec::with_capacity(bodies.len());
  let mut buffer = [0; 16 * 1024];
  for (index, body) in bodies.iter().enumerate() {
    let (form, _) = verify_object(input, &bodies[..index], &forms, body.bytes, &mut buffer, max_depth)
      .with_context(|| format!("verify body object {index}"))?;
    forms.push(form);
  }
  Ok(())
}

/// Verify the objects against their source bodies and rebuild the indexes from
/// the encoded stream. Object offsets are relative to its first byte.
pub(super) fn verify_indexed_objects(
  input: &mut impl Read,
  bodies: &[InputBody<'_>],
  max_depth: usize,
) -> Result<(Vec<ObjectIndex>, Vec<usize>)> {
  ensure!(bodies.len() <= MAX_OBJECTS, "too many body objects");
  let mut forms = Vec::with_capacity(bodies.len());
  let mut entries = Vec::<ObjectIndex>::with_capacity(bodies.len());
  let mut canonical = Vec::with_capacity(bodies.len());
  let mut buffer = [0; 16 * 1024];
  let mut input = CountedRead { inner: input, bytes: 0 };
  let mut offset = 0;
  for (index, body) in bodies.iter().enumerate() {
    let (form, base) = verify_object(&mut input, &bodies[..index], &forms, body.bytes, &mut buffer, max_depth)
      .with_context(|| format!("verify body object {index}"))?;
    let (ordinal, sha256, base, depth) = match form {
      Form::Full => (index, Sha256::digest(body.bytes).into(), None, 0),
      Form::Delta(depth) => (index, Sha256::digest(body.bytes).into(), base, depth),
      Form::Reference(target) => (target, entries[target].sha256, None, entries[target].depth),
    };
    entries.push(ObjectIndex {
      offset,
      encoded_len: input.bytes - offset,
      body_len: body.bytes.len(),
      sha256,
      base,
      depth,
    });
    canonical.push(ordinal);
    forms.push(form);
    offset = input.bytes;
  }
  Ok((entries, canonical))
}

struct CountedRead<R> {
  inner: R,
  bytes: u64,
}

impl<R: Read> Read for CountedRead<R> {
  fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
    let count = self.inner.read(buffer)?;
    self.bytes += count as u64;
    Ok(count)
  }
}

fn verify_object(
  input: &mut impl Read,
  prior: &[InputBody<'_>],
  forms: &[Form],
  expected: &[u8],
  buffer: &mut [u8],
  max_depth: usize,
) -> Result<(Form, Option<usize>)> {
  let header = read_header(input, max_depth, |index| {
    forms.get(index).and_then(|form| form.canonical_depth())
  })?;
  match header {
    ObjectHeader::Full { len } => {
      ensure!(len == expected.len(), "body length differs");
      verify_bytes(input, expected, buffer)?;
      Ok((Form::Full, None))
    }
    ObjectHeader::Reference { target } => {
      ensure!(prior[target].bytes == expected, "referenced body differs");
      Ok((Form::Reference(target), None))
    }
    ObjectHeader::Delta { base, len, depth } => {
      ensure!(len == expected.len(), "body length differs");
      verify_delta(input, prior[base].bytes, expected, buffer)?;
      Ok((Form::Delta(depth), Some(base)))
    }
  }
}

enum ObjectHeader {
  Full { len: usize },
  Reference { target: usize },
  Delta { base: usize, len: usize, depth: usize },
}

fn read_header(
  input: &mut impl Read,
  max_depth: usize,
  prior_depth: impl Fn(usize) -> Option<usize>,
) -> Result<ObjectHeader> {
  match read_tag(input)? {
    FULL => Ok(ObjectHeader::Full {
      len: read_body_len(input)?,
    }),
    REFERENCE => {
      let target = read_uint(input)?;
      ensure!(
        prior_depth(target).is_some(),
        "body reference must point to an earlier canonical body"
      );
      Ok(ObjectHeader::Reference { target })
    }
    DELTA => {
      let base = read_uint(input)?;
      let depth = prior_depth(base).context("delta base must be an earlier canonical body")? + 1;
      ensure!(depth <= max_depth, "delta chain exceeds maximum depth");
      Ok(ObjectHeader::Delta {
        base,
        len: read_body_len(input)?,
        depth,
      })
    }
    tag => anyhow::bail!("unknown body object tag {tag}"),
  }
}

/// Rebuild each object in pack order. A private spool retains canonical bodies
/// for later copies and references while one buffer is reused for SQLite writes.
pub(super) fn restore_objects(
  input: &mut impl Read,
  count: usize,
  max_depth: usize,
  mut emit: impl FnMut(usize, &[u8]) -> Result<()>,
) -> Result<()> {
  ensure!(count <= MAX_OBJECTS, "too many body objects");
  let mut bodies = Vec::with_capacity(count);
  let mut spool = BodySpool::new()?;
  let mut buffer = Vec::new();
  for index in 0..count {
    let stored = restore_object(input, &bodies, &mut spool, &mut buffer, max_depth)
      .with_context(|| format!("restore body object {index}"))?;
    emit(index, &buffer)?;
    bodies.push(stored);
  }
  Ok(())
}

#[derive(Clone, Copy)]
struct StoredBody {
  offset: u64,
  len: usize,
  depth: usize,
}

struct BodySpool {
  file: File,
  end: u64,
}

impl BodySpool {
  fn new() -> Result<Self> {
    Ok(Self {
      file: tempfile::tempfile()?,
      end: 0,
    })
  }

  fn append(&mut self, bytes: &[u8], depth: usize) -> Result<StoredBody> {
    let offset = self.end;
    let end = offset
      .checked_add(bytes.len() as u64)
      .context("body spool offset overflow")?;
    self.file.seek(SeekFrom::Start(offset))?;
    self.file.write_all(bytes)?;
    self.end = end;
    Ok(StoredBody {
      offset,
      len: bytes.len(),
      depth,
    })
  }

  fn read_at(&mut self, offset: u64, output: &mut [u8]) -> Result<()> {
    self.file.seek(SeekFrom::Start(offset))?;
    self.file.read_exact(output)?;
    Ok(())
  }
}

fn restore_object(
  input: &mut impl Read,
  prior: &[Option<StoredBody>],
  spool: &mut BodySpool,
  buffer: &mut Vec<u8>,
  max_depth: usize,
) -> Result<Option<StoredBody>> {
  let header = read_header(input, max_depth, |index| prior.get(index)?.map(|body| body.depth))?;
  match header {
    ObjectHeader::Full { len } => {
      buffer.resize(len, 0);
      input.read_exact(buffer)?;
      Ok(Some(spool.append(buffer, 0)?))
    }
    ObjectHeader::Reference { target } => {
      let target = prior[target].expect("canonical target validated by the object header");
      buffer.resize(target.len, 0);
      spool.read_at(target.offset, buffer)?;
      Ok(None)
    }
    ObjectHeader::Delta { base, len, depth } => {
      let base = prior[base].expect("canonical base validated by the object header");
      buffer.resize(len, 0);
      read_delta(input, base.len, len, |input, operation| {
        match operation {
          DeltaOperation::Copy { source, target } => {
            spool.read_at(base.offset + source.start as u64, &mut buffer[target])?;
          }
          DeltaOperation::Literal { target } => input.read_exact(&mut buffer[target])?,
        }
        Ok(())
      })?;
      Ok(Some(spool.append(buffer, depth)?))
    }
  }
}

/// Index the encoded stream for selective restoration without expanding any
/// canonical body. The private spool stores one copy of the encoded objects.
pub(super) fn index_objects(input: &mut dyn Read, count: usize, max_depth: usize) -> Result<IndexedBodies> {
  ensure!(count <= MAX_OBJECTS, "too many body objects");
  let mut input = SpoolingInput {
    input,
    file: BufWriter::new(tempfile::tempfile()?),
    len: 0,
  };
  let mut entries = Vec::<IndexedBody>::with_capacity(count);
  let mut buffer = [0; 16 * 1024];
  for _ in 0..count {
    let header = read_header(&mut input, max_depth, |index| entries.get(index)?.canonical_depth())?;
    let offset = input.len;
    let entry = match header {
      ObjectHeader::Full { len } => {
        skip_exact(&mut input, len, &mut buffer)?;
        IndexedBody {
          offset,
          len,
          depth: 0,
          form: IndexedForm::Full,
        }
      }
      ObjectHeader::Reference { target } => IndexedBody {
        offset,
        len: entries[target].len,
        depth: entries[target].depth,
        form: IndexedForm::Reference(target),
      },
      ObjectHeader::Delta { base, len, depth } => {
        read_delta(&mut input, entries[base].len, len, |input, operation| {
          if let DeltaOperation::Literal { target } = operation {
            skip_exact(input, target.len(), &mut buffer)?;
          }
          Ok(())
        })?;
        IndexedBody {
          offset,
          len,
          depth,
          form: IndexedForm::Delta(base),
        }
      }
    };
    entries.push(entry);
  }
  let file = input.file.into_inner().map_err(|error| error.into_error())?;
  Ok(IndexedBodies {
    file: BufReader::new(file),
    entries,
    body: Vec::new(),
    next: Vec::new(),
  })
}

struct SpoolingInput<'a> {
  input: &'a mut dyn Read,
  file: BufWriter<File>,
  len: u64,
}

impl Read for SpoolingInput<'_> {
  fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
    let n = self.input.read(output)?;
    self.file.write_all(&output[..n])?;
    self.len = self
      .len
      .checked_add(n as u64)
      .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "object spool offset overflow"))?;
    Ok(n)
  }
}

fn skip_exact(input: &mut impl Read, mut len: usize, buffer: &mut [u8]) -> Result<()> {
  while len != 0 {
    let count = len.min(buffer.len());
    input.read_exact(&mut buffer[..count])?;
    len -= count;
  }
  Ok(())
}

#[derive(Clone, Copy)]
enum IndexedForm {
  Full,
  Reference(usize),
  Delta(usize),
}

#[derive(Clone, Copy)]
struct IndexedBody {
  offset: u64,
  len: usize,
  depth: usize,
  form: IndexedForm,
}

impl IndexedBody {
  fn canonical_depth(self) -> Option<usize> {
    match self.form {
      IndexedForm::Full | IndexedForm::Delta(_) => Some(self.depth),
      IndexedForm::Reference(_) => None,
    }
  }
}

pub(super) struct IndexedBodies {
  file: BufReader<File>,
  entries: Vec<IndexedBody>,
  body: Vec<u8>,
  next: Vec<u8>,
}

impl IndexedBodies {
  pub(super) fn body(&mut self, index: usize) -> Result<&[u8]> {
    let target = *self.entries.get(index).context("missing indexed body")?;
    let mut entry = match target.form {
      IndexedForm::Reference(base) => self.entries[base],
      _ => target,
    };
    let mut deltas = Vec::with_capacity(entry.depth);
    while let IndexedForm::Delta(base) = entry.form {
      deltas.push(entry);
      entry = self.entries[base];
    }
    self.file.seek(SeekFrom::Start(entry.offset))?;
    self.body.resize(entry.len, 0);
    self.file.read_exact(&mut self.body)?;
    for delta in deltas.into_iter().rev() {
      self.file.seek(SeekFrom::Start(delta.offset))?;
      self.next.resize(delta.len, 0);
      materialize_delta(&mut self.file, &self.body, &mut self.next)?;
      std::mem::swap(&mut self.body, &mut self.next);
    }
    Ok(&self.body)
  }
}

/// Reconstruct an object by reading only its canonical entries. The lookup
/// supplies absolute logical offsets, including any prefix of the body pack.
pub(super) fn read_indexed_body(
  input: &mut (impl Read + Seek),
  mut ordinal: usize,
  max_depth: usize,
  mut lookup: impl FnMut(usize) -> Result<ObjectIndex>,
) -> Result<Vec<u8>> {
  let mut chain = Vec::<ObjectIndex>::new();
  loop {
    ensure!(ordinal < MAX_OBJECTS, "indexed object ordinal exceeds limit");
    let entry = lookup(ordinal)?;
    ensure!(
      entry.depth <= max_depth && entry.depth <= ordinal,
      "invalid indexed object depth"
    );
    ensure!(entry.body_len <= MAX_BODY_BYTES, "indexed body exceeds size limit");
    ensure!(
      (5..=entry.body_len as u64 + 5).contains(&entry.encoded_len),
      "invalid indexed object length"
    );
    let end = entry
      .offset
      .checked_add(entry.encoded_len)
      .context("indexed object offset overflow")?;
    if let Some(child) = chain.last() {
      ensure!(entry.depth + 1 == child.depth, "inconsistent indexed object depth");
      ensure!(end <= child.offset, "indexed delta base overlaps or follows its child");
    }
    chain.push(entry);
    match entry.base {
      Some(base) => {
        ensure!(base < ordinal && entry.depth > 0, "invalid indexed delta base");
        ordinal = base;
      }
      None => {
        ensure!(entry.depth == 0, "full indexed object has a nonzero depth");
        break;
      }
    }
  }

  let mut body = Vec::new();
  let mut next = Vec::new();
  for entry in chain.into_iter().rev() {
    input.seek(SeekFrom::Start(entry.offset))?;
    let mut encoded = (&mut *input).take(entry.encoded_len);
    let prior = entry.base.zip(entry.depth.checked_sub(1));
    let header = read_header(&mut encoded, max_depth, |ordinal| {
      prior.filter(|&(base, _)| base == ordinal).map(|(_, depth)| depth)
    })?;
    match header {
      ObjectHeader::Full { len } if entry.base.is_none() && len == entry.body_len => {
        body.resize(len, 0);
        encoded.read_exact(&mut body)?;
      }
      ObjectHeader::Delta { len, .. } if entry.base.is_some() && len == entry.body_len => {
        next.resize(len, 0);
        materialize_delta(&mut encoded, &body, &mut next)?;
        std::mem::swap(&mut body, &mut next);
      }
      _ => anyhow::bail!("object header differs from its index"),
    }
    ensure!(encoded.limit() == 0, "encoded object length differs from its index");
    let digest: [u8; 32] = Sha256::digest(&body).into();
    ensure!(digest == entry.sha256, "body hash differs from its index");
  }
  Ok(body)
}

fn materialize_delta(input: &mut impl Read, base: &[u8], next: &mut [u8]) -> Result<()> {
  read_delta(input, base.len(), next.len(), |input, operation| {
    match operation {
      DeltaOperation::Copy { source, target } => next[target].copy_from_slice(&base[source]),
      DeltaOperation::Literal { target } => input.read_exact(&mut next[target])?,
    }
    Ok(())
  })
}

fn read_body_len(input: &mut impl Read) -> Result<usize> {
  let len = read_uint(input)?;
  ensure!(len <= MAX_BODY_BYTES, "body exceeds size limit");
  Ok(len)
}

fn verify_bytes(input: &mut impl Read, expected: &[u8], buffer: &mut [u8]) -> Result<()> {
  for chunk in expected.chunks(buffer.len()) {
    let actual = &mut buffer[..chunk.len()];
    input.read_exact(actual)?;
    ensure!(actual == chunk, "body bytes differ");
  }
  Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Edit {
  Copy { offset: usize, len: usize },
  Literal { offset: usize, len: usize },
}

impl Edit {
  fn wire_len(self) -> usize {
    match self {
      Self::Copy { .. } => COPY_BYTES,
      Self::Literal { len, .. } => 5 + len,
    }
  }
}

fn write_delta(out: &mut impl Write, base: usize, target: &[u8], edits: &[Edit]) -> Result<()> {
  out.write_all(&[DELTA])?;
  write_uint(out, base)?;
  write_uint(out, target.len())?;
  write_uint(out, edits.len())?;
  for edit in edits {
    match *edit {
      Edit::Copy { offset, len } => {
        out.write_all(&[COPY])?;
        write_uint(out, offset)?;
        write_uint(out, len)?;
      }
      Edit::Literal { offset, len } => {
        out.write_all(&[LITERAL])?;
        write_uint(out, len)?;
        out.write_all(&target[offset..offset + len])?;
      }
    }
  }
  Ok(())
}

fn verify_delta(input: &mut impl Read, base: &[u8], expected: &[u8], buffer: &mut [u8]) -> Result<()> {
  read_delta(input, base.len(), expected.len(), |input, operation| {
    match operation {
      DeltaOperation::Copy { source, target } => ensure!(base[source] == expected[target], "copied body bytes differ"),
      DeltaOperation::Literal { target } => verify_bytes(input, &expected[target], buffer)?,
    }
    Ok(())
  })
}

enum DeltaOperation {
  Copy { source: Range<usize>, target: Range<usize> },
  Literal { target: Range<usize> },
}

fn read_delta<R: Read>(
  input: &mut R,
  base_len: usize,
  target_len: usize,
  mut apply: impl FnMut(&mut R, DeltaOperation) -> Result<()>,
) -> Result<()> {
  let count = read_uint(input)?;
  ensure!(
    count > 0 && count <= MAX_OPERATIONS && count <= target_len,
    "invalid delta operation count"
  );
  let mut written = 0;
  for _ in 0..count {
    let end = match read_tag(input)? {
      COPY => {
        let start = read_uint(input)?;
        let len = read_uint(input)?;
        let source_end = start.checked_add(len).context("delta copy offset overflow")?;
        ensure!(source_end <= base_len, "delta copy falls outside base body");
        let end = delta_end(written, len, target_len)?;
        apply(
          input,
          DeltaOperation::Copy {
            source: start..source_end,
            target: written..end,
          },
        )?;
        end
      }
      LITERAL => {
        let len = read_uint(input)?;
        let end = delta_end(written, len, target_len)?;
        apply(input, DeltaOperation::Literal { target: written..end })?;
        end
      }
      tag => anyhow::bail!("unknown delta operation tag {tag}"),
    };
    written = end;
  }
  ensure!(written == target_len, "delta length differs from declaration");
  Ok(())
}

fn delta_end(start: usize, len: usize, expected: usize) -> Result<usize> {
  ensure!(len > 0, "empty delta operation");
  let end = start.checked_add(len).context("delta length overflow")?;
  ensure!(end <= expected, "delta exceeds declared output length");
  Ok(end)
}

type Anchors = HashMap<u64, [u32; 4], foldhash::fast::RandomState>;

struct CachedAnchors {
  anchors: Anchors,
  last_use: u64,
}

#[derive(Default)]
struct AnchorCache {
  entries: HashMap<usize, CachedAnchors>,
  bytes: usize,
  clock: u64,
}

impl AnchorCache {
  fn get(&mut self, body: usize, base: &[u8]) -> &Anchors {
    self.clock += 1;
    if !self.entries.contains_key(&body) {
      let anchors = base_anchors(base);
      let bytes = anchors.capacity() * ANCHOR_ENTRY_BYTES;
      while self.bytes + bytes > MAX_CACHED_ANCHOR_BYTES || self.entries.len() >= MAX_CACHED_BASES {
        let (&least_recent, _) = self
          .entries
          .iter()
          .min_by_key(|(_, cached)| cached.last_use)
          .expect("a single base anchor index fits in the cache");
        let removed = self.entries.remove(&least_recent).expect("cached base exists");
        self.bytes -= removed.anchors.capacity() * ANCHOR_ENTRY_BYTES;
      }
      self.bytes += bytes;
      self.entries.insert(
        body,
        CachedAnchors {
          anchors,
          last_use: self.clock,
        },
      );
    }
    let cached = self.entries.get_mut(&body).expect("base anchors were cached");
    cached.last_use = self.clock;
    &cached.anchors
  }
}

fn body_delta_limit(len: usize) -> Option<usize> {
  (len >= 64).then(|| len + 5 - (len / 20).max(32))
}

fn delta_wire_lower_bound(base_len: usize, target_len: usize) -> usize {
  // Each copy emits at most base_len bytes. Replacing a max-length copy
  // with literals costs more, so an ideal packing can only beat another
  // copy's overhead by storing part of the final run as a literal.
  let full_copies = target_len / base_len;
  let remainder = target_len % base_len;
  let last_run = if remainder == 0 {
    0
  } else {
    COPY_BYTES.min(5 + remainder)
  };
  DELTA_HEADER_BYTES + full_copies * COPY_BYTES + last_run
}

/// Index a bounded sample of base windows and scan every target window with
/// a rolling hash. This finds shared runs between rewritten SSE event
/// envelopes even when their byte offsets have shifted.
fn select_delta(
  anchors: &Anchors,
  base: &[u8],
  target: &[u8],
  limit: usize,
  incumbent_budget: usize,
) -> Option<(Vec<Edit>, usize)> {
  let mut edits = Vec::new();
  let mut wire_len = DELTA_HEADER_BYTES;
  let mut cursor = 0;
  let mut literal_start = 0;
  let mut hash = window_hash(&target[..ANCHOR_BYTES]);
  while cursor + ANCHOR_BYTES <= target.len() {
    if let Some((base_start, target_start, len)) = best_copy(anchors, base, target, hash, cursor, literal_start) {
      if target_start > literal_start {
        let literal = Edit::Literal {
          offset: literal_start,
          len: target_start - literal_start,
        };
        wire_len += literal.wire_len();
        edits.push(literal);
      }
      let copy = Edit::Copy {
        offset: base_start,
        len,
      };
      wire_len += copy.wire_len();
      edits.push(copy);
      if edits.len() >= MAX_OPERATIONS || wire_len >= limit || wire_len > incumbent_budget {
        return None;
      }
      cursor = target_start + len;
      literal_start = cursor;
      if let Some(bytes) = target.get(cursor..cursor + ANCHOR_BYTES) {
        hash = window_hash(bytes);
      }
    } else {
      if let Some(&next) = target.get(cursor + ANCHOR_BYTES) {
        hash = hash
          .wrapping_sub(u64::from(target[cursor]).wrapping_mul(ANCHOR_FACTOR))
          .wrapping_mul(ROLLING_BASE)
          .wrapping_add(u64::from(next));
      }
      cursor += 1;
    }
  }
  if target.len() > literal_start {
    let literal = Edit::Literal {
      offset: literal_start,
      len: target.len() - literal_start,
    };
    wire_len += literal.wire_len();
    edits.push(literal);
  }
  (wire_len <= limit.min(incumbent_budget)).then_some((edits, wire_len))
}

fn base_anchors(base: &[u8]) -> Anchors {
  let windows = base.len() - ANCHOR_BYTES + 1;
  let stride = windows.div_ceil(MAX_ANCHORS).max(4);
  let mut anchors = Anchors::with_capacity_and_hasher(windows.div_ceil(stride), Default::default());
  for offset in (0..windows).step_by(stride) {
    let hash = window_hash(&base[offset..offset + ANCHOR_BYTES]);
    let offsets = anchors.entry(hash).or_insert([MISSING_OFFSET; 4]);
    let slot = offsets.iter().position(|&value| value == MISSING_OFFSET).unwrap_or(3);
    offsets[slot] = offset as u32;
  }
  anchors
}

fn window_hash(bytes: &[u8]) -> u64 {
  bytes.iter().fold(0u64, |hash, byte| {
    hash.wrapping_mul(ROLLING_BASE).wrapping_add(u64::from(*byte))
  })
}

fn best_copy(
  anchors: &Anchors,
  base: &[u8],
  target: &[u8],
  hash: u64,
  cursor: usize,
  literal_start: usize,
) -> Option<(usize, usize, usize)> {
  let mut best = None;
  let target_end = cursor + ANCHOR_BYTES;
  let target_anchor = &target[cursor..target_end];
  for &base_offset in anchors.get(&hash)? {
    if base_offset == MISSING_OFFSET {
      break;
    }
    let offset = base_offset as usize;
    let base_end = offset + ANCHOR_BYTES;
    if &base[offset..base_end] != target_anchor {
      continue;
    }
    let overlap =
      offset.min(cursor - literal_start) + ANCHOR_BYTES + (base.len() - base_end).min(target.len() - target_end);
    if best.is_some_and(|(_, _, previous_len)| overlap <= previous_len) {
      continue;
    }
    let back = matching_suffix(&base[..offset], &target[literal_start..cursor]);
    let forward = matching_prefix(&base[base_end..], &target[target_end..]);
    let len = back + ANCHOR_BYTES + forward;
    if best.is_none_or(|(_, _, previous_len)| len > previous_len) {
      best = Some((offset - back, cursor - back, len));
    }
  }
  best
}

const MATCH_BLOCK_BYTES: usize = 64;

fn matching_prefix(left: &[u8], right: &[u8]) -> usize {
  let mut matched = 0;
  let left_blocks = left.as_chunks::<MATCH_BLOCK_BYTES>().0;
  let right_blocks = right.as_chunks::<MATCH_BLOCK_BYTES>().0;
  for (left, right) in left_blocks.iter().zip(right_blocks) {
    if left != right {
      break;
    }
    matched += MATCH_BLOCK_BYTES;
  }
  matched
    + left[matched..]
      .iter()
      .zip(&right[matched..])
      .take_while(|(left, right)| left == right)
      .count()
}

fn matching_suffix(left: &[u8], right: &[u8]) -> usize {
  let mut matched = 0;
  let left_blocks = left.as_rchunks::<MATCH_BLOCK_BYTES>().1;
  let right_blocks = right.as_rchunks::<MATCH_BLOCK_BYTES>().1;
  for (left, right) in left_blocks.iter().rev().zip(right_blocks.iter().rev()) {
    if left != right {
      break;
    }
    matched += MATCH_BLOCK_BYTES;
  }
  matched
    + left[..left.len() - matched]
      .iter()
      .rev()
      .zip(right[..right.len() - matched].iter().rev())
      .take_while(|(left, right)| left == right)
      .count()
}

fn write_uint(out: &mut impl Write, value: usize) -> Result<()> {
  out.write_all(&u32::try_from(value)?.to_le_bytes())?;
  Ok(())
}

fn read_uint(input: &mut impl Read) -> Result<usize> {
  let mut bytes = [0; 4];
  input.read_exact(&mut bytes)?;
  Ok(usize::try_from(u32::from_le_bytes(bytes))?)
}

fn read_tag(input: &mut impl Read) -> Result<u8> {
  let mut byte = [0];
  input.read_exact(&mut byte)?;
  Ok(byte[0])
}

#[cfg(test)]
mod tests {
  use super::*;

  fn translated_streams() -> (Vec<u8>, Vec<u8>) {
    let mut original = String::from("event: upstream.response.created\n\n");
    let mut converted = String::from("data: {\"type\":\"new.stream\"}\n\n");
    for index in 0..6 {
      let text =
        format!("chunk {index}: an internal passage shared between both event formats, including offset shifts.");
      original.push_str(&format!(
        "event: upstream.delta\ndata: {{\"sequence\":{index},\"delta\":\"{text}\"}}\n\n"
      ));
      converted.push_str(&format!(
        "data: {{\"kind\":\"content.delta\",\"content\":\"{text}\",\"index\":{index}}}\n\n"
      ));
    }
    original.push_str("event: upstream.done\n\n");
    converted.push_str("data: [DONE]\n\n");
    (original.into_bytes(), converted.into_bytes())
  }

  #[test]
  fn all_sharing_modes_round_trip_and_find_disjoint_translated_runs() {
    let (base, converted) = translated_streams();
    let limit = body_delta_limit(converted.len()).unwrap();
    let copies = select_delta(&base_anchors(&base), &base, &converted, limit, limit)
      .unwrap()
      .0
      .iter()
      .filter(|edit| matches!(edit, Edit::Copy { .. }))
      .count();
    assert!(copies >= 6, "expected the distinct shared SSE contents to be copied");
    let bodies = [
      InputBody {
        bytes: &base,
        base: None,
        session_bases: &[],
      },
      InputBody {
        bytes: &base,
        base: None,
        session_bases: &[],
      },
      InputBody {
        bytes: &converted,
        base: Some(1),
        session_bases: &[],
      },
      InputBody {
        bytes: &converted,
        base: Some(2),
        session_bases: &[],
      },
      InputBody {
        bytes: b"new body",
        base: Some(3),
        session_bases: &[],
      },
      InputBody {
        bytes: b"",
        base: None,
        session_bases: &[],
      },
    ];
    for (sharing, counts) in [
      (Sharing::Full, (6, 0, 0)),
      (Sharing::Dedup, (4, 2, 0)),
      (Sharing::Delta, (3, 2, 1)),
    ] {
      let mut archive = Vec::new();
      let stats = write_objects(&mut archive, &bodies, sharing, 1).unwrap();
      assert_eq!((stats.full, stats.references, stats.deltas), counts);
      let mut encoded = &archive[..];
      verify_objects(&mut encoded, &bodies, 1).unwrap();
      assert!(encoded.is_empty());
    }
  }

  #[test]
  fn incumbent_budget_preserves_equal_deltas_for_shallower_candidates() {
    let (base, converted) = translated_streams();
    let anchors = base_anchors(&base);
    let limit = body_delta_limit(converted.len()).unwrap();
    let baseline = select_delta(&anchors, &base, &converted, limit, limit).unwrap();
    assert_eq!(
      select_delta(&anchors, &base, &converted, limit, baseline.1),
      Some(baseline.clone())
    );
    assert!(select_delta(&anchors, &base, &converted, limit, baseline.1 - 1).is_none());
  }

  #[test]
  fn block_matching_extends_only_the_identical_bytes() {
    let left: Vec<_> = (0..=255).collect();
    for offset in [0, 1, 63, 64, 65, 200, 255] {
      let mut right = left.clone();
      right[offset] ^= 1;
      assert_eq!(matching_prefix(&left, &right), offset);
      assert_eq!(matching_suffix(&left, &right), left.len() - offset - 1);
    }
    assert_eq!(matching_prefix(&left, &left[..100]), 100);
    assert_eq!(matching_suffix(&left, &left[100..]), 156);
  }

  #[test]
  fn session_base_wins_when_smaller_and_dedup_still_takes_precedence() {
    let body: Vec<_> = (0..128).collect();
    let mut paired = body.clone();
    paired[64..].fill(b'p');
    let mut previous = body.clone();
    previous[127] = b'v';
    let mut following = body.clone();
    following[127] = b'f';
    let bodies = [
      InputBody {
        bytes: &paired,
        base: None,
        session_bases: &[],
      },
      InputBody {
        bytes: &previous,
        base: None,
        session_bases: &[],
      },
      InputBody {
        bytes: &body,
        base: Some(0),
        session_bases: &[1],
      },
      InputBody {
        bytes: &body,
        base: None,
        session_bases: &[1],
      },
      InputBody {
        bytes: &following,
        base: None,
        session_bases: &[3],
      },
    ];
    let mut archive = Vec::new();
    let stats = write_objects(&mut archive, &bodies, Sharing::Delta, 1).unwrap();
    assert_eq!(
      (stats.full, stats.references, stats.deltas, stats.session_deltas),
      (3, 1, 1, 1)
    );
    verify_objects(&mut &archive[..], &bodies, 1).unwrap();

    archive.clear();
    let stats = write_objects(&mut archive, &bodies, Sharing::Delta, 2).unwrap();
    assert_eq!(
      (stats.full, stats.references, stats.session_deltas, stats.max_depth),
      (2, 1, 2, 2)
    );
    verify_objects(&mut &archive[..], &bodies, 2).unwrap();
    assert_rejected(
      &archive,
      &[&paired, &previous, &body, &body, &following],
      "exceeds maximum depth",
    );
  }

  #[test]
  fn paired_base_wins_a_tie() {
    let body: Vec<_> = (0..128).collect();
    let mut paired = body.clone();
    paired[127] = b'p';
    let mut previous = body.clone();
    previous[127] = b'v';
    let bodies = [
      InputBody {
        bytes: &paired,
        base: None,
        session_bases: &[],
      },
      InputBody {
        bytes: &previous,
        base: None,
        session_bases: &[],
      },
      InputBody {
        bytes: &body,
        base: Some(0),
        session_bases: &[1],
      },
    ];
    let mut archive = Vec::new();
    let stats = write_objects(&mut archive, &bodies, Sharing::Delta, 1).unwrap();
    assert_eq!((stats.deltas, stats.session_deltas), (1, 0));
    verify_objects(&mut &archive[..], &bodies, 1).unwrap();
  }

  #[test]
  fn shallower_base_wins_an_equal_size_delta() {
    let first: Vec<_> = (0..128).collect();
    let mut second = first.clone();
    second[127] = b's';
    let mut third = first.clone();
    third[127] = b't';
    let bodies = [
      InputBody {
        bytes: &first,
        base: None,
        session_bases: &[],
      },
      InputBody {
        bytes: &second,
        base: Some(0),
        session_bases: &[],
      },
      InputBody {
        bytes: &third,
        base: Some(1),
        session_bases: &[0],
      },
    ];
    let mut archive = Vec::new();
    let stats = write_objects(&mut archive, &bodies, Sharing::Delta, 2).unwrap();
    assert_eq!((stats.deltas, stats.session_deltas, stats.max_depth), (2, 1, 1));
    verify_objects(&mut &archive[..], &bodies, 1).unwrap();
  }

  #[test]
  fn falls_back_to_an_older_base_when_the_latest_one_is_at_max_depth() {
    let first = (0..128).cycle().take(512).collect::<Vec<u8>>();
    let mut second = first.clone();
    second.extend([b'b'; 128]);
    let mut third = second.clone();
    third.extend([b'c'; 128]);
    let mut fourth = third.clone();
    fourth.extend([b'd'; 128]);
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
        session_bases: &[1, 0],
      },
      InputBody {
        bytes: &fourth,
        base: None,
        session_bases: &[2, 1, 0],
      },
    ];
    for max_depth in [2, 3] {
      let mut archive = Vec::new();
      let stats = write_objects(&mut archive, &bodies, Sharing::Delta, max_depth).unwrap();
      assert_eq!((stats.full, stats.session_deltas, stats.max_depth), (1, 3, max_depth));
      verify_objects(&mut &archive[..], &bodies, max_depth).unwrap();
    }
  }

  #[test]
  fn indexed_restore_seeks_only_to_the_selected_canonical_chain() -> Result<()> {
    let first = (0..128).cycle().take(512).collect::<Vec<u8>>();
    let mut second = first.clone();
    second.extend([b'b'; 128]);
    let mut third = second.clone();
    third.extend([b'c'; 128]);
    let inputs = [
      (first.as_slice(), None),
      (b"unrelated body".as_slice(), None),
      (second.as_slice(), Some(0)),
      (second.as_slice(), None),
      (third.as_slice(), Some(3)),
    ]
    .map(|(bytes, base)| InputBody {
      bytes,
      base,
      session_bases: &[],
    });
    let prefix = b"body stream prefix";
    let mut pack = prefix.to_vec();
    let (mut entries, canonical) = write_indexed_objects(&mut pack, &inputs, 2)?;
    assert_eq!(canonical, [0, 1, 2, 2, 4]);
    assert_eq!((entries[4].base, entries[4].depth), (Some(2), 2));

    let mut legacy = prefix.to_vec();
    write_objects(&mut legacy, &inputs, Sharing::Delta, 2)?;
    assert_eq!(legacy, pack);
    let prefix_len = prefix.len() as u64;
    let mut input = io::Cursor::new(&pack);
    input.seek(SeekFrom::Start(prefix_len))?;
    let (verified_entries, verified_canonical) = verify_indexed_objects(&mut input, &inputs, 2)?;
    assert_eq!(verified_entries, entries);
    assert_eq!(verified_canonical, canonical);
    for entry in &mut entries {
      entry.offset += prefix_len;
    }

    let mut input = ObservedReader {
      cursor: io::Cursor::new(&pack),
      positions: Vec::new(),
    };
    let mut lookups = Vec::new();
    let restored = read_indexed_body(&mut input, canonical[4], 2, |ordinal| {
      lookups.push(ordinal);
      Ok(entries[ordinal])
    })?;
    assert_eq!(restored, third);
    assert_eq!(lookups, [4, 2, 0]);
    assert_eq!(
      input.positions,
      [entries[0].offset, entries[2].offset, entries[4].offset]
    );
    Ok(())
  }

  struct ObservedReader<'a> {
    cursor: io::Cursor<&'a Vec<u8>>,
    positions: Vec<u64>,
  }

  impl Read for ObservedReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
      self.cursor.read(output)
    }
  }

  impl Seek for ObservedReader<'_> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
      let offset = self.cursor.seek(position)?;
      self.positions.push(offset);
      Ok(offset)
    }
  }

  #[test]
  fn indexed_restore_rejects_invalid_locations_and_body_hashes() -> Result<()> {
    let body = InputBody {
      bytes: b"body bytes",
      base: None,
      session_bases: &[],
    };
    let mut pack = Vec::new();
    let (entries, _) = write_indexed_objects(&mut pack, &[body], 1)?;
    let entry = entries[0];
    let invalid = [
      ObjectIndex {
        offset: u64::MAX,
        ..entry
      },
      ObjectIndex {
        encoded_len: entry.encoded_len - 1,
        ..entry
      },
      ObjectIndex {
        base: Some(0),
        depth: 1,
        ..entry
      },
      ObjectIndex {
        sha256: [0; 32],
        ..entry
      },
    ];
    for entry in invalid {
      assert!(read_indexed_body(&mut io::Cursor::new(&pack), 0, 1, |_| Ok(entry)).is_err());
    }
    Ok(())
  }

  fn full(out: &mut Vec<u8>, bytes: &[u8]) {
    write_objects(
      out,
      &[InputBody {
        bytes,
        base: None,
        session_bases: &[],
      }],
      Sharing::Full,
      1,
    )
    .unwrap();
  }

  fn reference(out: &mut Vec<u8>, index: usize) {
    out.push(REFERENCE);
    write_uint(out, index).unwrap();
  }

  fn assert_rejected(mut bytes: &[u8], bodies: &[&[u8]], message: &str) {
    let expected: Vec<_> = bodies
      .iter()
      .map(|bytes| InputBody {
        bytes,
        base: None,
        session_bases: &[],
      })
      .collect();
    let error = verify_objects(&mut bytes, &expected, 1).unwrap_err();
    assert!(format!("{error:#}").contains(message), "{error:#}");
  }

  #[test]
  fn rejects_forward_references_and_indirect_delta_bases() {
    let mut forward = Vec::new();
    reference(&mut forward, 0);
    assert_rejected(&forward, &[b"source"], "earlier canonical body");

    let mut bytes = Vec::new();
    full(&mut bytes, b"source");
    reference(&mut bytes, 0);

    let mut reference_chain = bytes.clone();
    reference(&mut reference_chain, 1);
    assert_rejected(
      &reference_chain,
      &[b"source", b"source", b"source"],
      "earlier canonical body",
    );

    let edits = [Edit::Copy { offset: 0, len: 1 }];
    write_delta(&mut bytes, 1, b"s", &edits).unwrap();
    assert_rejected(&bytes, &[b"source", b"source", b"s"], "earlier canonical body");

    let mut delta_chain = Vec::new();
    full(&mut delta_chain, b"source");
    write_delta(&mut delta_chain, 0, b"s", &edits).unwrap();
    write_delta(&mut delta_chain, 1, b"s", &edits).unwrap();
    assert_rejected(&delta_chain, &[b"source", b"s", b"s"], "exceeds maximum depth");
  }

  #[test]
  fn rejects_malformed_and_out_of_bounds_delta_operations() {
    for (len, edits, error) in [
      (2_u32, vec![Edit::Copy { offset: 5, len: 2 }], "copy falls outside base"),
      (1, vec![Edit::Copy { offset: 0, len: 2 }], "exceeds declared output"),
      (2, vec![Edit::Copy { offset: 0, len: 1 }], "length differs"),
      (1, vec![Edit::Copy { offset: 0, len: 0 }], "empty delta operation"),
      (1, vec![], "invalid delta operation count"),
      (1, vec![Edit::Literal { offset: 0, len: 2 }], "exceeds declared output"),
      (1, vec![Edit::Copy { offset: 1, len: 1 }], "copied body bytes differ"),
    ] {
      let mut bytes = Vec::new();
      full(&mut bytes, b"source");
      let declared_length = bytes.len() + 5;
      write_delta(&mut bytes, 0, b"ab", &edits).unwrap();
      bytes[declared_length..declared_length + 4].copy_from_slice(&len.to_le_bytes());
      assert_rejected(&bytes, &[b"source", &b"so"[..len as usize]], error);
    }
    let mut bytes = Vec::new();
    full(&mut bytes, b"source");
    write_delta(&mut bytes, 0, b"target", &[Edit::Literal { offset: 0, len: 6 }]).unwrap();
    bytes.pop();
    assert_rejected(&bytes, &[b"source", b"target"], "failed to fill whole buffer");
  }

  #[test]
  fn rejects_well_formed_objects_with_incorrect_contents() {
    let mut bytes = Vec::new();
    full(&mut bytes, b"source");
    assert_rejected(&bytes, &[b"target"], "body bytes differ");
    reference(&mut bytes, 0);
    assert_rejected(&bytes, &[b"source", b"target"], "referenced body differs");
  }

  #[test]
  fn rejects_invalid_object_declarations_before_reading_body_data() {
    let mut bytes = vec![FULL];
    write_uint(&mut bytes, MAX_BODY_BYTES + 1).unwrap();
    assert_rejected(&bytes, &[b""], "body exceeds size limit");
    assert_rejected(&[255], &[b""], "unknown body object tag");
    let bad = [InputBody {
      bytes: b"body",
      base: Some(0),
      session_bases: &[],
    }];
    assert!(write_objects(&mut Vec::new(), &bad, Sharing::Delta, 1).is_err());
    let bad = [InputBody {
      bytes: b"body",
      base: None,
      session_bases: &[0],
    }];
    assert!(write_objects(&mut Vec::new(), &bad, Sharing::Delta, 1).is_err());
  }
}
