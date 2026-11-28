//! Read and verify archive contents through the caller's open file handles.

use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use tokn_persistence::indexed_archive::{IndexedArchiveFormat, IndexedArchiveReader};
use tokn_persistence::request_archive::{
  indexed::{self, PackWriteProgress},
  RequestPack,
};
use tokn_persistence::request_files::{ArchiveFormat, StableFile};

use crate::{encode_archive, hash_reader, ArchiveContents};

#[derive(Clone, Copy)]
pub(super) struct PayloadStats {
  pub bytes: u64,
  pub sqlite_sha256: Option<[u8; 32]>,
}

impl PayloadStats {
  fn new(format: ArchiveContents, bytes: u64, digest: [u8; 32]) -> Self {
    Self {
      bytes,
      sqlite_sha256: matches!(format, ArchiveContents::Snapshot(_)).then_some(digest),
    }
  }

  fn pack(bytes: u64) -> Self {
    Self {
      bytes,
      sqlite_sha256: None,
    }
  }
}

/// An open archive and its decoded stats, recorded as soon as they are available.
pub(super) struct ArchiveInput<'a> {
  pub file: &'a mut File,
  pub format: ArchiveContents,
  pub payload: &'a mut Option<PayloadStats>,
}

pub(super) struct ConvertedArchive {
  pub target: PayloadStats,
  pub source_capture: Option<(RequestPack, IndexedArchiveFormat)>,
}

/// Report the current operation and byte increments within that operation.
pub(super) enum ContentProgress {
  Stage(&'static str, Option<u64>),
  Bytes(u64),
}

pub(super) fn verify_equivalent(
  left: ArchiveInput<'_>,
  right: ArchiveInput<'_>,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<bool> {
  match (left.format, right.format) {
    (ArchiveContents::Snapshot(_), ArchiveContents::Snapshot(_))
    | (ArchiveContents::IndexedPack(_), ArchiveContents::IndexedPack(_)) => {
      let (left_digest, left_bytes) = hash_content(left.file, left.format, "Compare A", progress)?;
      *left.payload = Some(PayloadStats::new(left.format, left_bytes, left_digest));
      let (right_digest, right_bytes) = hash_content(right.file, right.format, "Compare B", progress)?;
      *right.payload = Some(PayloadStats::new(right.format, right_bytes, right_digest));
      Ok(left_digest == right_digest)
    }
    (ArchiveContents::Snapshot(snapshot_format), ArchiveContents::IndexedPack(indexed_format)) => {
      verify_snapshot_against_pack(left, snapshot_format, right, indexed_format, progress)
    }
    (ArchiveContents::IndexedPack(indexed_format), ArchiveContents::Snapshot(snapshot_format)) => {
      verify_snapshot_against_pack(right, snapshot_format, left, indexed_format, progress)
    }
  }
}

/// Measure the decoded source and target payloads without compressing the target.
pub(super) fn measure_conversion(
  source: ArchiveInput<'_>,
  target_format: ArchiveContents,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<u64> {
  check_conversion(source.format, target_format)?;
  let target_bytes = match (source.format, target_format) {
    (ArchiveContents::Snapshot(format), ArchiveContents::Snapshot(_)) => {
      let input = open_snapshot_for_stage(source.file, format, "Read DB", progress)?;
      let payload = copy_sqlite(input, io::sink())?;
      *source.payload = Some(payload);
      payload.bytes
    }
    (ArchiveContents::IndexedPack(format), ArchiveContents::IndexedPack(_)) => {
      let input = open_indexed_for_stage(source.file, format, "Read pack", progress)?;
      let bytes = copy_with_progress(input, io::sink(), progress)?;
      *source.payload = Some(PayloadStats::pack(bytes));
      bytes
    }
    (ArchiveContents::Snapshot(format), ArchiveContents::IndexedPack(_)) => {
      let capture = capture_snapshot(source.file, format, source.payload, progress)?;
      write_pack(&capture, io::sink(), progress)?
    }
    (ArchiveContents::IndexedPack(_), ArchiveContents::Snapshot(_)) => return Err(indexed_snapshot_unsupported()),
  };
  Ok(target_bytes)
}

/// Encode a conversion to a fresh output stream.
pub(super) fn convert(
  source: ArchiveInput<'_>,
  output: impl Write,
  target_format: ArchiveContents,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<ConvertedArchive> {
  check_conversion(source.format, target_format)?;
  let (target, source_capture) = match (source.format, target_format) {
    (ArchiveContents::Snapshot(source_format), ArchiveContents::Snapshot(target_format)) => {
      encode_archive(BufWriter::new(output), target_format, |encoder| {
        let input = open_snapshot_for_stage(source.file, source_format, "Compress DB", progress)?;
        *source.payload = Some(copy_sqlite(input, encoder)?);
        progress(ContentProgress::Stage("Finish", None));
        Ok(())
      })?;
      (
        source.payload.expect("archive encoder copied the SQLite snapshot"),
        None,
      )
    }
    (ArchiveContents::IndexedPack(source_format), ArchiveContents::IndexedPack(target_format)) => {
      let input = open_indexed_for_stage(source.file, source_format, "Compress pack", progress)?;
      let mut encoder = target_format.writer(BufWriter::new(output))?;
      let bytes = copy_with_progress(input, &mut encoder, progress)?;
      progress(ContentProgress::Stage("Finish", None));
      encoder.finish()?.flush()?;
      let payload = PayloadStats::pack(bytes);
      *source.payload = Some(payload);
      (payload, None)
    }
    (ArchiveContents::Snapshot(source_format), ArchiveContents::IndexedPack(target_format)) => {
      let capture = capture_snapshot(source.file, source_format, source.payload, progress)?;
      let mut encoder = target_format.writer(BufWriter::new(output))?;
      let target_bytes = write_pack(&capture, &mut encoder, progress)?;
      progress(ContentProgress::Stage("Finish", None));
      encoder.finish()?.flush()?;
      (PayloadStats::pack(target_bytes), Some((capture, target_format)))
    }
    (ArchiveContents::IndexedPack(_), ArchiveContents::Snapshot(_)) => return Err(indexed_snapshot_unsupported()),
  };
  Ok(ConvertedArchive { target, source_capture })
}

fn write_pack(
  capture: &RequestPack,
  output: impl Write,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<u64> {
  progress(ContentProgress::Stage("Build pack", Some(capture.body_bytes())));
  let mut writer = TrackedIo::new(output, |_: &[u8]| {});
  indexed::write_with_body_progress(capture, &mut writer, &mut |_| Ok(()), |event| match event {
    PackWriteProgress::BodyBytes(bytes) => report_bytes(progress, bytes),
    PackWriteProgress::Indexing => progress(ContentProgress::Stage("Index pack", None)),
  })?;
  Ok(writer.bytes)
}

pub(super) fn open_snapshot(source: &mut File, format: ArchiveFormat) -> io::Result<Box<dyn Read + '_>> {
  rewind_snapshot(source, format)?;
  open_snapshot_decoder(source, format)
}

fn open_snapshot_for_stage<'a>(
  source: &'a mut File,
  format: ArchiveFormat,
  label: &'static str,
  progress: &'a mut impl FnMut(ContentProgress),
) -> io::Result<Box<dyn Read + 'a>> {
  let compressed_bytes = rewind_snapshot(source, format)?;
  progress(ContentProgress::Stage(label, Some(compressed_bytes)));
  let tracked = TrackedIo::new(source, |bytes: &[u8]| report_bytes(progress, bytes.len() as u64));
  open_snapshot_decoder(tracked, format)
}

fn rewind_snapshot(source: &mut File, format: ArchiveFormat) -> io::Result<u64> {
  source.rewind()?;
  let bytes = source.metadata()?.len();
  // XZ streams and padding are both aligned to four bytes. The decoder
  // validates internal padding but can accept unaligned trailing padding.
  if cfg!(feature = "lzma") && format == ArchiveFormat::Xz && !bytes.is_multiple_of(4) {
    return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid XZ archive length"));
  }
  Ok(bytes)
}

fn open_snapshot_decoder<'a>(source: impl Read + 'a, format: ArchiveFormat) -> io::Result<Box<dyn Read + 'a>> {
  match format {
    ArchiveFormat::Zstd => Ok(Box::new(zstd::Decoder::new(source)?)),
    #[cfg(feature = "lzma")]
    ArchiveFormat::Xz => Ok(Box::new(lzma_rust2::XzReader::new(BufReader::new(source), true))),
    #[cfg(not(feature = "lzma"))]
    ArchiveFormat::Xz => Err(io::Error::new(
      io::ErrorKind::Unsupported,
      "xz archive decompression requires the lzma feature",
    )),
  }
}

fn open_indexed_for_stage<'a>(
  source: &'a mut File,
  format: IndexedArchiveFormat,
  label: &'static str,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<IndexedArchiveReader<BufReader<&'a mut File>>> {
  source.rewind()?;
  let mut input = format.open(BufReader::new(source))?;
  let bytes = input.seek(SeekFrom::End(0))?;
  progress(ContentProgress::Stage(label, Some(bytes)));
  indexed::validate(&mut input)?;
  input.rewind()?;
  Ok(input)
}

fn verify_snapshot_against_pack(
  snapshot: ArchiveInput<'_>,
  snapshot_format: ArchiveFormat,
  pack: ArchiveInput<'_>,
  pack_format: IndexedArchiveFormat,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<bool> {
  let capture = capture_snapshot(snapshot.file, snapshot_format, snapshot.payload, progress)?;
  let (equivalent, payload) = verify_captured_pack(&capture, pack.file, pack_format, progress)?;
  *pack.payload = Some(payload);
  Ok(equivalent)
}

pub(super) fn verify_captured_pack(
  capture: &RequestPack,
  pack: &mut File,
  pack_format: IndexedArchiveFormat,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<(bool, PayloadStats)> {
  let mut input = open_indexed_for_stage(pack, pack_format, "Read pack", progress)?;
  let bytes = copy_with_progress(&mut input, io::sink(), progress)?;
  progress(ContentProgress::Stage("Verify pack", Some(bytes)));
  let equivalent = match indexed::verify(capture, &mut input, &mut |bytes| {
    report_bytes(progress, bytes);
    Ok(())
  }) {
    Ok(()) => true,
    Err(error) if error.kind() == io::ErrorKind::InvalidData => false,
    Err(error) => return Err(error),
  };
  Ok((equivalent, PayloadStats::pack(bytes)))
}

fn capture_snapshot(
  source: &mut File,
  format: ArchiveFormat,
  source_payload: &mut Option<PayloadStats>,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<RequestPack> {
  let mut snapshot = tempfile::NamedTempFile::new()?;
  let payload = {
    let mut output = BufWriter::new(snapshot.as_file_mut());
    let input = open_snapshot_for_stage(source, format, "Unpack DB", progress)?;
    let payload = copy_sqlite(input, &mut output)?;
    output.flush()?;
    payload
  };
  // Keep the decoded stats even if the database schema cannot be packed.
  *source_payload = Some(payload);
  progress(ContentProgress::Stage("Capture DB", None));
  let source = StableFile::open(snapshot.path())?;
  RequestPack::capture(&source, &mut |bytes| {
    report_bytes(progress, bytes);
    Ok(())
  })?
  .ok_or_else(|| {
    io::Error::new(
      io::ErrorKind::Unsupported,
      "request snapshot schema or contents cannot be represented in an indexed pack",
    )
  })
}

fn hash_content(
  source: &mut File,
  format: ArchiveContents,
  label: &'static str,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<([u8; 32], u64)> {
  let mut bytes = 0;
  let digest = match format {
    ArchiveContents::Snapshot(format) => {
      let input = open_snapshot_for_stage(source, format, label, progress)?;
      hash_reader(input, None, &mut |read| bytes += read)?
    }
    ArchiveContents::IndexedPack(format) => {
      let input = open_indexed_for_stage(source, format, label, progress)?;
      hash_reader(input, None, &mut |read| {
        bytes += read;
        report_bytes(progress, read);
      })?
    }
  };
  Ok((digest, bytes))
}

fn copy_with_progress(
  input: impl Read,
  mut output: impl Write,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<u64> {
  let mut reader = TrackedIo::new(input, |bytes: &[u8]| report_bytes(progress, bytes.len() as u64));
  io::copy(&mut reader, &mut output)
}

fn copy_sqlite(input: impl Read, mut output: impl Write) -> io::Result<PayloadStats> {
  let mut digest = Sha256::new();
  let mut reader = TrackedIo::new(input, |bytes: &[u8]| digest.update(bytes));
  let bytes = io::copy(&mut reader, &mut output)?;
  Ok(PayloadStats {
    bytes,
    sqlite_sha256: Some(digest.finalize().into()),
  })
}

fn report_bytes(progress: &mut impl FnMut(ContentProgress), bytes: u64) {
  if bytes != 0 {
    progress(ContentProgress::Bytes(bytes));
  }
}

struct TrackedIo<T, F> {
  inner: T,
  on_bytes: F,
  bytes: u64,
}

impl<T, F: FnMut(&[u8])> TrackedIo<T, F> {
  fn new(inner: T, on_bytes: F) -> Self {
    Self {
      inner,
      on_bytes,
      bytes: 0,
    }
  }

  fn record(&mut self, bytes: &[u8]) {
    self.bytes += bytes.len() as u64;
    (self.on_bytes)(bytes);
  }
}

impl<R: Read, F: FnMut(&[u8])> Read for TrackedIo<R, F> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let bytes = self.inner.read(buf)?;
    self.record(&buf[..bytes]);
    Ok(bytes)
  }
}

impl<W: Write, F: FnMut(&[u8])> Write for TrackedIo<W, F> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    let bytes = self.inner.write(buf)?;
    self.record(&buf[..bytes]);
    Ok(bytes)
  }

  fn flush(&mut self) -> io::Result<()> {
    self.inner.flush()
  }
}

fn check_conversion(source: ArchiveContents, target: ArchiveContents) -> io::Result<()> {
  #[cfg(not(feature = "lzma"))]
  if matches!(target, ArchiveContents::Snapshot(ArchiveFormat::Xz)) {
    return Err(io::Error::new(
      io::ErrorKind::Unsupported,
      "xz archive compression requires the lzma feature",
    ));
  }
  if matches!(
    (source, target),
    (ArchiveContents::IndexedPack(_), ArchiveContents::Snapshot(_))
  ) {
    return Err(indexed_snapshot_unsupported());
  }
  Ok(())
}

fn indexed_snapshot_unsupported() -> io::Error {
  io::Error::new(
    io::ErrorKind::Unsupported,
    "an indexed pack cannot reconstruct the original SQLite snapshot and wire bodies",
  )
}

#[cfg(test)]
mod tests {
  use super::*;

  fn input<'a>(file: &'a mut File, format: ArchiveContents, payload: &'a mut Option<PayloadStats>) -> ArchiveInput<'a> {
    ArchiveInput { file, format, payload }
  }

  #[test]
  fn snapshot_comparison_checks_every_zstd_frame() -> io::Result<()> {
    let format = ArchiveContents::Snapshot(ArchiveFormat::Zstd);
    let mut frames = tempfile::tempfile()?;
    encode_archive(&mut frames, ArchiveFormat::Zstd, |output| output.write_all(b"one"))?;
    encode_archive(&mut frames, ArchiveFormat::Zstd, |output| output.write_all(b" two"))?;
    let compressed_bytes = frames.metadata()?.len();
    let mut source_payload = None;
    let measured = measure_conversion(input(&mut frames, format, &mut source_payload), format, &mut |_| {})?;
    assert_eq!((source_payload.unwrap().bytes, measured), (7, 7));
    let mut converted = tempfile::tempfile()?;
    let mut converted_bytes = 0;
    let result = convert(
      input(&mut frames, format, &mut source_payload),
      &mut converted,
      format,
      &mut |progress| {
        if let ContentProgress::Bytes(bytes) = progress {
          converted_bytes += bytes;
        }
      },
    )?;
    assert_eq!(
      (source_payload.unwrap().bytes, result.target.bytes, converted_bytes),
      (7, 7, compressed_bytes)
    );
    let mut left = None;
    let mut right = None;
    assert!(verify_equivalent(
      input(&mut frames, format, &mut left),
      input(&mut converted, format, &mut right),
      &mut |_| {},
    )?);
    assert_eq!((left.unwrap().bytes, right.unwrap().bytes), (7, 7));
    let digest: [u8; 32] = Sha256::digest(b"one two").into();
    assert_eq!(source_payload.unwrap().sqlite_sha256, Some(digest));
    assert_eq!(result.target.sqlite_sha256, Some(digest));
    assert_eq!(left.unwrap().sqlite_sha256, Some(digest));
    assert_eq!(right.unwrap().sqlite_sha256, Some(digest));

    frames.seek(io::SeekFrom::End(0))?;
    frames.write_all(b"trailing invalid data")?;
    assert!(verify_equivalent(
      input(&mut frames, format, &mut left),
      input(&mut converted, format, &mut right),
      &mut |_| {},
    )
    .is_err());
    Ok(())
  }

  #[test]
  fn conversion_sizes_include_all_indexed_pack_bytes() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("requests.db");
    drop(tokn_persistence::requests::open_day_db(&db).map_err(io::Error::other)?);
    let db = File::open(db)?;
    let snapshot_bytes = db.metadata()?.len();
    let mut snapshot = tempfile::tempfile()?;
    zstd::stream::copy_encode(db, &mut snapshot, 0)?;

    let snapshot_format = ArchiveContents::Snapshot(ArchiveFormat::Zstd);
    let pack_format = ArchiveContents::IndexedPack(IndexedArchiveFormat::Zstd);
    let mut pack = tempfile::tempfile()?;
    let mut source_payload = None;
    let converted = convert(
      input(&mut snapshot, snapshot_format, &mut source_payload),
      &mut pack,
      pack_format,
      &mut |_| {},
    )?;
    pack.rewind()?;
    let pack_bytes = io::copy(&mut zstd::Decoder::new(&mut pack)?, &mut io::sink())?;
    assert_eq!(
      (source_payload.unwrap().bytes, converted.target.bytes),
      (snapshot_bytes, pack_bytes)
    );
    let mut left = None;
    let mut right = None;
    assert!(verify_equivalent(
      input(&mut pack, pack_format, &mut left),
      input(&mut snapshot, snapshot_format, &mut right),
      &mut |_| {},
    )?);
    assert_eq!(
      (left.unwrap().bytes, right.unwrap().bytes),
      (pack_bytes, snapshot_bytes)
    );
    assert_eq!(left.unwrap().sqlite_sha256, None);
    assert_eq!(right.unwrap().sqlite_sha256, source_payload.unwrap().sqlite_sha256);
    Ok(())
  }

  #[cfg(feature = "lzma")]
  #[test]
  fn xz_snapshots_require_valid_padding_and_every_stream() -> io::Result<()> {
    let mut archive = tempfile::tempfile()?;
    encode_archive(&mut archive, ArchiveFormat::Xz, |output| output.write_all(b"one"))?;
    archive.write_all(&[0; 4])?;
    encode_archive(&mut archive, ArchiveFormat::Xz, |output| output.write_all(b" two"))?;
    archive.write_all(&[0; 4])?;
    let mut decoded = Vec::new();
    open_snapshot(&mut archive, ArchiveFormat::Xz)?.read_to_end(&mut decoded)?;
    assert_eq!(decoded, b"one two");

    archive.write_all(&[0])?;
    assert_eq!(
      open_snapshot(&mut archive, ArchiveFormat::Xz).err().unwrap().kind(),
      io::ErrorKind::InvalidData
    );
    archive.seek(io::SeekFrom::End(-5))?;
    archive.write_all(b"junk")?;
    archive.set_len(archive.metadata()?.len() - 1)?;
    assert!(open_snapshot(&mut archive, ArchiveFormat::Xz)?
      .read_to_end(&mut Vec::new())
      .is_err());
    Ok(())
  }
}
