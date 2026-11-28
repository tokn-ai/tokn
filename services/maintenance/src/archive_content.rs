//! Read and verify archive contents through the caller's open file handles.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use tokn_persistence::indexed_archive::{IndexedArchiveFormat, IndexedArchiveReader};
use tokn_persistence::request_archive::{indexed, RequestPack};
use tokn_persistence::request_files::{ArchiveFormat, StableFile};

use crate::{encode_archive, hash_reader, ArchiveContents};

/// Decoded sizes of the two inputs, in argument order.
pub(super) struct ContentComparison {
  pub equivalent: bool,
  pub left_bytes: u64,
  pub right_bytes: u64,
}

/// Sizes of the complete uncompressed payloads before and after conversion.
pub(super) struct ConversionSizes {
  pub source_bytes: u64,
  pub target_bytes: u64,
}

/// Report the current operation and byte increments within that operation.
pub(super) enum ContentProgress {
  Stage(&'static str),
  Bytes(u64),
}

pub(super) fn verify_equivalent(
  left: &mut File,
  left_format: ArchiveContents,
  right: &mut File,
  right_format: ArchiveContents,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<ContentComparison> {
  match (left_format, right_format) {
    (ArchiveContents::Snapshot(_), ArchiveContents::Snapshot(_))
    | (ArchiveContents::IndexedPack(_), ArchiveContents::IndexedPack(_)) => {
      progress(ContentProgress::Stage("Hashing first archive"));
      let (left_digest, left_bytes) = hash_content(left, left_format, progress)?;
      progress(ContentProgress::Stage("Hashing second archive"));
      let (right_digest, right_bytes) = hash_content(right, right_format, progress)?;
      Ok(ContentComparison {
        equivalent: left_digest == right_digest,
        left_bytes,
        right_bytes,
      })
    }
    (ArchiveContents::Snapshot(snapshot_format), ArchiveContents::IndexedPack(indexed_format)) => {
      verify_snapshot_against_pack(left, snapshot_format, right, indexed_format, progress)
    }
    (ArchiveContents::IndexedPack(indexed_format), ArchiveContents::Snapshot(snapshot_format)) => {
      let mut comparison = verify_snapshot_against_pack(right, snapshot_format, left, indexed_format, progress)?;
      std::mem::swap(&mut comparison.left_bytes, &mut comparison.right_bytes);
      Ok(comparison)
    }
  }
}

/// Measure the decoded source and target payloads without compressing the target.
pub(super) fn measure_conversion(
  source: &mut File,
  source_format: ArchiveContents,
  target_format: ArchiveContents,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<ConversionSizes> {
  check_conversion(source_format, target_format)?;
  let (source_bytes, target_bytes) = match (source_format, target_format) {
    (ArchiveContents::Snapshot(format), ArchiveContents::Snapshot(_)) => {
      progress(ContentProgress::Stage("Measuring SQLite snapshot"));
      let bytes = copy_with_progress(open_snapshot(source, format)?, io::sink(), progress)?;
      (bytes, bytes)
    }
    (ArchiveContents::IndexedPack(format), ArchiveContents::IndexedPack(_)) => {
      progress(ContentProgress::Stage("Measuring indexed pack"));
      let bytes = copy_with_progress(open_indexed(source, format)?, io::sink(), progress)?;
      (bytes, bytes)
    }
    (ArchiveContents::Snapshot(format), ArchiveContents::IndexedPack(_)) => {
      let (capture, source_bytes) = capture_snapshot(source, format, progress)?;
      (source_bytes, write_pack(&capture, io::sink(), progress)?)
    }
    (ArchiveContents::IndexedPack(_), ArchiveContents::Snapshot(_)) => return Err(indexed_snapshot_unsupported()),
  };
  Ok(ConversionSizes {
    source_bytes,
    target_bytes,
  })
}

/// Encode a conversion to a fresh output stream.
pub(super) fn convert(
  source: &mut File,
  source_format: ArchiveContents,
  output: impl Write,
  target_format: ArchiveContents,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<ConversionSizes> {
  check_conversion(source_format, target_format)?;
  let (source_bytes, target_bytes) = match (source_format, target_format) {
    (ArchiveContents::Snapshot(source_format), ArchiveContents::Snapshot(target_format)) => {
      progress(ContentProgress::Stage("Recompressing snapshot"));
      let input = open_snapshot(source, source_format)?;
      let mut bytes = 0;
      encode_archive(BufWriter::new(output), target_format, |encoder| {
        bytes = copy_with_progress(input, encoder, progress)?;
        progress(ContentProgress::Stage("Finishing archive"));
        Ok(())
      })?;
      (bytes, bytes)
    }
    (ArchiveContents::IndexedPack(source_format), ArchiveContents::IndexedPack(target_format)) => {
      progress(ContentProgress::Stage("Recompressing indexed pack"));
      let input = open_indexed(source, source_format)?;
      let mut encoder = target_format.writer(BufWriter::new(output))?;
      let bytes = copy_with_progress(input, &mut encoder, progress)?;
      progress(ContentProgress::Stage("Finishing archive"));
      encoder.finish()?.flush()?;
      (bytes, bytes)
    }
    (ArchiveContents::Snapshot(source_format), ArchiveContents::IndexedPack(target_format)) => {
      let (capture, source_bytes) = capture_snapshot(source, source_format, progress)?;
      let mut encoder = target_format.writer(BufWriter::new(output))?;
      let target_bytes = write_pack(&capture, &mut encoder, progress)?;
      progress(ContentProgress::Stage("Finishing archive"));
      encoder.finish()?.flush()?;
      (source_bytes, target_bytes)
    }
    (ArchiveContents::IndexedPack(_), ArchiveContents::Snapshot(_)) => return Err(indexed_snapshot_unsupported()),
  };
  Ok(ConversionSizes {
    source_bytes,
    target_bytes,
  })
}

fn write_pack(
  capture: &RequestPack,
  output: impl Write,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<u64> {
  progress(ContentProgress::Stage("Building indexed pack"));
  let mut writer = TrackedIo::new(output, progress);
  indexed::write(capture, &mut writer, &mut |_| Ok(()))?;
  Ok(writer.bytes)
}

pub(super) fn open_snapshot(source: &mut File, format: ArchiveFormat) -> io::Result<Box<dyn Read + '_>> {
  source.rewind()?;
  match format {
    ArchiveFormat::Zstd => Ok(Box::new(zstd::Decoder::new(source)?)),
    #[cfg(feature = "lzma")]
    ArchiveFormat::Xz => {
      // XZ streams and their padding are both aligned to four bytes. The
      // concatenated-stream decoder checks internal padding, but accepts
      // trailing padding of any length when it encounters EOF.
      if !source.metadata()?.len().is_multiple_of(4) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid XZ archive length"));
      }
      Ok(Box::new(lzma_rust2::XzReader::new(BufReader::new(source), true)))
    }
    #[cfg(not(feature = "lzma"))]
    ArchiveFormat::Xz => Err(io::Error::new(
      io::ErrorKind::Unsupported,
      "xz archive decompression requires the lzma feature",
    )),
  }
}

fn open_indexed(
  source: &mut File,
  format: IndexedArchiveFormat,
) -> io::Result<IndexedArchiveReader<BufReader<&mut File>>> {
  source.rewind()?;
  let mut input = format.open(BufReader::new(source))?;
  indexed::validate(&mut input)?;
  input.rewind()?;
  Ok(input)
}

fn verify_snapshot_against_pack(
  snapshot: &mut File,
  snapshot_format: ArchiveFormat,
  pack: &mut File,
  pack_format: IndexedArchiveFormat,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<ContentComparison> {
  let (capture, left_bytes) = capture_snapshot(snapshot, snapshot_format, progress)?;
  progress(ContentProgress::Stage("Validating indexed pack"));
  let mut input = open_indexed(pack, pack_format)?;
  let right_bytes = copy_with_progress(&mut input, io::sink(), progress)?;
  progress(ContentProgress::Stage("Verifying indexed pack against snapshot"));
  let equivalent = match indexed::verify(&capture, &mut input, &mut |bytes| {
    report_bytes(progress, bytes);
    Ok(())
  }) {
    Ok(()) => true,
    Err(error) if error.kind() == io::ErrorKind::InvalidData => false,
    Err(error) => return Err(error),
  };
  Ok(ContentComparison {
    equivalent,
    left_bytes,
    right_bytes,
  })
}

fn capture_snapshot(
  source: &mut File,
  format: ArchiveFormat,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<(RequestPack, u64)> {
  progress(ContentProgress::Stage("Restoring SQLite snapshot"));
  let mut snapshot = tempfile::NamedTempFile::new()?;
  let bytes = {
    let mut output = BufWriter::new(snapshot.as_file_mut());
    let bytes = copy_with_progress(open_snapshot(source, format)?, &mut output, progress)?;
    output.flush()?;
    bytes
  };
  progress(ContentProgress::Stage("Capturing request data"));
  let source = StableFile::open(snapshot.path())?;
  let capture = RequestPack::capture(&source, &mut |bytes| {
    report_bytes(progress, bytes);
    Ok(())
  })?
  .ok_or_else(|| {
    io::Error::new(
      io::ErrorKind::Unsupported,
      "request snapshot schema or contents cannot be represented in an indexed pack",
    )
  })?;
  Ok((capture, bytes))
}

fn hash_content(
  source: &mut File,
  format: ArchiveContents,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<([u8; 32], u64)> {
  let mut bytes = 0;
  let mut count = |read| {
    bytes += read;
    report_bytes(progress, read);
  };
  let digest = match format {
    ArchiveContents::Snapshot(format) => hash_reader(open_snapshot(source, format)?, None, &mut count)?,
    ArchiveContents::IndexedPack(format) => hash_reader(open_indexed(source, format)?, None, &mut count)?,
  };
  Ok((digest, bytes))
}

fn copy_with_progress(
  input: impl Read,
  mut output: impl Write,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<u64> {
  io::copy(&mut TrackedIo::new(input, progress), &mut output)
}

fn report_bytes(progress: &mut impl FnMut(ContentProgress), bytes: u64) {
  if bytes != 0 {
    progress(ContentProgress::Bytes(bytes));
  }
}

struct TrackedIo<T, F> {
  inner: T,
  progress: F,
  bytes: u64,
}

impl<T, F: FnMut(ContentProgress)> TrackedIo<T, F> {
  fn new(inner: T, progress: F) -> Self {
    Self {
      inner,
      progress,
      bytes: 0,
    }
  }

  fn record(&mut self, bytes: usize) {
    self.bytes += bytes as u64;
    report_bytes(&mut self.progress, bytes as u64);
  }
}

impl<R: Read, F: FnMut(ContentProgress)> Read for TrackedIo<R, F> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let bytes = self.inner.read(buf)?;
    self.record(bytes);
    Ok(bytes)
  }
}

impl<W: Write, F: FnMut(ContentProgress)> Write for TrackedIo<W, F> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    let bytes = self.inner.write(buf)?;
    self.record(bytes);
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

  #[test]
  fn snapshot_comparison_checks_every_zstd_frame() -> io::Result<()> {
    let format = ArchiveContents::Snapshot(ArchiveFormat::Zstd);
    let mut frames = tempfile::tempfile()?;
    encode_archive(&mut frames, ArchiveFormat::Zstd, |output| output.write_all(b"one"))?;
    encode_archive(&mut frames, ArchiveFormat::Zstd, |output| output.write_all(b" two"))?;
    let measured = measure_conversion(&mut frames, format, format, &mut |_| {})?;
    assert_eq!((measured.source_bytes, measured.target_bytes), (7, 7));
    let mut converted = tempfile::tempfile()?;
    let mut converted_bytes = 0;
    let sizes = convert(&mut frames, format, &mut converted, format, &mut |progress| {
      if let ContentProgress::Bytes(bytes) = progress {
        converted_bytes += bytes;
      }
    })?;
    assert_eq!((sizes.source_bytes, sizes.target_bytes, converted_bytes), (7, 7, 7));
    let compared = verify_equivalent(&mut frames, format, &mut converted, format, &mut |_| {})?;
    assert!(compared.equivalent);
    assert_eq!((compared.left_bytes, compared.right_bytes), (7, 7));

    frames.seek(io::SeekFrom::End(0))?;
    frames.write_all(b"trailing invalid data")?;
    assert!(verify_equivalent(&mut frames, format, &mut converted, format, &mut |_| {}).is_err());
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
    let converted = convert(&mut snapshot, snapshot_format, &mut pack, pack_format, &mut |_| {})?;
    pack.rewind()?;
    let pack_bytes = io::copy(&mut zstd::Decoder::new(&mut pack)?, &mut io::sink())?;
    assert_eq!(
      (converted.source_bytes, converted.target_bytes),
      (snapshot_bytes, pack_bytes)
    );
    let compared = verify_equivalent(&mut pack, pack_format, &mut snapshot, snapshot_format, &mut |_| {})?;
    assert!(compared.equivalent);
    assert_eq!(
      (compared.left_bytes, compared.right_bytes),
      (pack_bytes, snapshot_bytes)
    );
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
