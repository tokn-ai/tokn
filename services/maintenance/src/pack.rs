use std::io::{self, BufReader, BufWriter, Read, Seek};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use tokn_persistence::archive_xz::{BlockReader, BlockWriter};
use tokn_persistence::request_archive::{indexed, RequestPack, MAGIC};
use tokn_persistence::request_files::StableFile;

use crate::{check_cancelled, sync_archive_directory};

const IO_CHUNK: usize = 64 * 1024;

pub(super) fn is_pack(file: &mut StableFile) -> io::Result<bool> {
  file.file_mut().rewind()?;
  let mut reader = lzma_rust2::XzReader::new(BufReader::new(file.file_mut()), false);
  let mut prefix = [0; MAGIC.len()];
  match reader.read_exact(&mut prefix) {
    Ok(()) => Ok(prefix == MAGIC),
    Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
    Err(error) => Err(error),
  }
}

pub(super) fn publish_indexed(
  snapshot: &RequestPack,
  archive: &Path,
  progress: &mut impl FnMut(u64) -> io::Result<()>,
  validate_source: impl FnOnce() -> io::Result<()>,
) -> io::Result<u64> {
  let parent = archive
    .parent()
    .filter(|path| !path.as_os_str().is_empty())
    .unwrap_or_else(|| Path::new("."));
  let mut staging = tempfile::Builder::new().prefix(".tokn-archive-").tempfile_in(parent)?;
  let mut encoder = BlockWriter::new(BufWriter::new(staging.as_file_mut()));
  indexed::write(snapshot, &mut encoder, progress)?;
  encoder.finish()?;
  staging.as_file().sync_all()?;
  {
    let mut input = BlockReader::open_xz(BufReader::new(staging.reopen()?))?;
    indexed::verify(snapshot, &mut input, progress)?;
  }
  validate_source()?;
  progress(0)?;
  let bytes = staging.as_file().metadata()?.len();
  staging.persist_noclobber(archive).map_err(|error| error.error)?;
  sync_archive_directory(parent)?;
  Ok(bytes)
}

pub(super) fn verify_indexed(
  file: &mut StableFile,
  pack: &RequestPack,
  cancelled: Option<&AtomicBool>,
  progress: &mut impl FnMut(u64),
) -> io::Result<()> {
  check_cancelled(cancelled)?;
  let mut input = BlockReader::open_xz(BufReader::new(file.file_mut()))?;
  let result = indexed::verify(pack, &mut input, &mut |bytes| {
    check_cancelled(cancelled)?;
    if bytes > 0 {
      progress(bytes);
    }
    Ok(())
  });
  check_cancelled(cancelled)?;
  result?;
  file.revalidate()
}

pub(super) fn verify(
  file: &mut StableFile,
  pack: &RequestPack,
  cancelled: Option<&AtomicBool>,
  progress: &mut impl FnMut(u64),
) -> io::Result<()> {
  file.file_mut().rewind()?;
  let result = pack.verify(&mut CheckedReader {
    inner: lzma_rust2::XzReader::new(BufReader::new(file.file_mut()), false),
    cancelled,
    progress,
  });
  check_cancelled(cancelled)?;
  result?;
  file.revalidate()
}

fn check_io_cancelled(cancelled: Option<&AtomicBool>) -> io::Result<()> {
  // Standard Read/Write helpers retry Interrupted. Translate at the codec
  // boundary so a cancelled job can unwind through any of those helpers.
  check_cancelled(cancelled).map_err(io::Error::other)
}

struct CheckedReader<'a, R, F> {
  inner: R,
  cancelled: Option<&'a AtomicBool>,
  progress: &'a mut F,
}

impl<R: Read, F: FnMut(u64)> Read for CheckedReader<'_, R, F> {
  fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
    check_io_cancelled(self.cancelled)?;
    let limit = bytes.len().min(IO_CHUNK);
    let count = self.inner.read(&mut bytes[..limit])?;
    if count > 0 {
      (self.progress)(count as u64);
    }
    Ok(count)
  }
}
