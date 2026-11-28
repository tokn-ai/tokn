//! Read and verify archive contents through the caller's open file handles.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use tokn_persistence::indexed_archive::{IndexedArchiveFormat, IndexedArchiveReader};
use tokn_persistence::request_archive::{indexed, RequestPack};
use tokn_persistence::request_files::{ArchiveFormat, StableFile};

use crate::{encode_archive, hash_reader, ArchiveContents};

pub(super) fn verify_equivalent(
  left: &mut File,
  left_format: ArchiveContents,
  right: &mut File,
  right_format: ArchiveContents,
) -> io::Result<bool> {
  match (left_format, right_format) {
    (ArchiveContents::Snapshot(left_format), ArchiveContents::Snapshot(right_format)) => {
      let left_digest = hash_reader(open_snapshot(left, left_format)?, None, &mut |_| {})?;
      let right_digest = hash_reader(open_snapshot(right, right_format)?, None, &mut |_| {})?;
      Ok(left_digest == right_digest)
    }
    (ArchiveContents::IndexedPack(left_format), ArchiveContents::IndexedPack(right_format)) => {
      let left_digest = hash_reader(open_indexed(left, left_format)?, None, &mut |_| {})?;
      let right_digest = hash_reader(open_indexed(right, right_format)?, None, &mut |_| {})?;
      Ok(left_digest == right_digest)
    }
    (ArchiveContents::Snapshot(snapshot_format), ArchiveContents::IndexedPack(indexed_format)) => {
      verify_snapshot_against_pack(left, snapshot_format, right, indexed_format)
    }
    (ArchiveContents::IndexedPack(indexed_format), ArchiveContents::Snapshot(snapshot_format)) => {
      verify_snapshot_against_pack(right, snapshot_format, left, indexed_format)
    }
  }
}

/// Check a source and the requested conversion without writing an archive.
pub(super) fn validate(
  source: &mut File,
  source_format: ArchiveContents,
  target_format: ArchiveContents,
) -> io::Result<()> {
  check_conversion(source_format, target_format)?;
  match (source_format, target_format) {
    (ArchiveContents::Snapshot(format), ArchiveContents::IndexedPack(_)) => {
      capture_snapshot(source, format)?;
    }
    (ArchiveContents::Snapshot(format), ArchiveContents::Snapshot(_)) => {
      io::copy(&mut open_snapshot(source, format)?, &mut io::sink())?;
    }
    (ArchiveContents::IndexedPack(format), _) => {
      io::copy(&mut open_indexed(source, format)?, &mut io::sink())?;
    }
  }
  Ok(())
}

/// Write a conversion to a private staging file for subsequent verification.
pub(super) fn convert(
  source: &mut File,
  source_format: ArchiveContents,
  output: &mut File,
  target_format: ArchiveContents,
) -> io::Result<()> {
  check_conversion(source_format, target_format)?;
  output.rewind()?;
  output.set_len(0)?;
  match (source_format, target_format) {
    (ArchiveContents::Snapshot(source_format), ArchiveContents::Snapshot(target_format)) => {
      let mut input = open_snapshot(source, source_format)?;
      encode_archive(BufWriter::new(output), target_format, |encoder| {
        io::copy(&mut input, encoder).map(|_| ())
      })
    }
    (ArchiveContents::IndexedPack(source_format), ArchiveContents::IndexedPack(target_format)) => {
      let mut input = open_indexed(source, source_format)?;
      let mut encoder = target_format.writer(BufWriter::new(output))?;
      io::copy(&mut input, &mut encoder)?;
      encoder.finish()?.flush()
    }
    (ArchiveContents::Snapshot(source_format), ArchiveContents::IndexedPack(target_format)) => {
      let capture = capture_snapshot(source, source_format)?;
      let mut encoder = target_format.writer(BufWriter::new(output))?;
      indexed::write(&capture, &mut encoder, &mut |_| Ok(()))?;
      encoder.finish()?.flush()
    }
    (ArchiveContents::IndexedPack(_), ArchiveContents::Snapshot(_)) => Err(indexed_snapshot_unsupported()),
  }
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
) -> io::Result<bool> {
  let capture = capture_snapshot(snapshot, snapshot_format)?;
  let mut input = open_indexed(pack, pack_format)?;
  io::copy(&mut input, &mut io::sink())?;
  match indexed::verify(&capture, &mut input, &mut |_| Ok(())) {
    Ok(()) => Ok(true),
    Err(error) if error.kind() == io::ErrorKind::InvalidData => Ok(false),
    Err(error) => Err(error),
  }
}

fn capture_snapshot(source: &mut File, format: ArchiveFormat) -> io::Result<RequestPack> {
  let mut snapshot = tempfile::NamedTempFile::new()?;
  {
    let mut output = BufWriter::new(snapshot.as_file_mut());
    io::copy(&mut open_snapshot(source, format)?, &mut output)?;
    output.flush()?;
  }
  let source = StableFile::open(snapshot.path())?;
  RequestPack::capture(&source, &mut |_| Ok(()))?.ok_or_else(|| {
    io::Error::new(
      io::ErrorKind::Unsupported,
      "request snapshot schema or contents cannot be represented in an indexed pack",
    )
  })
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
    let mut converted = tempfile::tempfile()?;
    convert(&mut frames, format, &mut converted, format)?;
    assert!(verify_equivalent(&mut frames, format, &mut converted, format)?);

    frames.seek(io::SeekFrom::End(0))?;
    frames.write_all(b"trailing invalid data")?;
    assert!(verify_equivalent(&mut frames, format, &mut converted, format).is_err());
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
