use std::io::{self, BufReader, BufWriter};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use tokn_persistence::indexed_archive::IndexedArchiveFormat;
use tokn_persistence::request_archive::{indexed, RequestPack};
use tokn_persistence::request_files::StableFile;

use crate::{check_cancelled, sync_archive_directory};

pub(super) fn publish_indexed(
  format: IndexedArchiveFormat,
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
  let mut encoder = format.writer(BufWriter::new(staging.as_file_mut()))?;
  indexed::write(snapshot, &mut encoder, progress)?;
  encoder.finish()?;
  staging.as_file().sync_all()?;
  {
    let mut input = format.open(BufReader::new(staging.reopen()?))?;
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
  format: IndexedArchiveFormat,
  file: &mut StableFile,
  pack: &RequestPack,
  cancelled: Option<&AtomicBool>,
  progress: &mut impl FnMut(u64),
) -> io::Result<()> {
  check_cancelled(cancelled)?;
  let mut input = format.open(BufReader::new(file.file_mut()))?;
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
