use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use time::Date;

use super::{archive_content, hash_file, parse_day, ArchiveContents, StableFile};
use archive_content::ContentProgress;

pub(super) struct ArchiveDay {
  pub db: PathBuf,
  pub has_db: bool,
  pub archives: Vec<PathBuf>,
}

pub(super) fn archive_days(dir: &Path) -> io::Result<BTreeMap<Date, ArchiveDay>> {
  // Recognize XZ filenames even in a build without the decoder. An archive
  // that cannot be verified must still count when choosing a source to keep.
  let suffixes = [".pack.zst", ".pack.xz", ".db.zstd", ".db.xz"];
  let mut days = BTreeMap::new();
  for entry in fs::read_dir(dir)? {
    let entry = entry?;
    let name = entry.file_name();
    let Some(name) = name.to_str() else { continue };
    let (day, is_db) = if let Some(day) = name.strip_suffix(".db") {
      if !entry.file_type()?.is_file() {
        continue;
      }
      (day, true)
    } else if let Some(day) = suffixes.iter().find_map(|suffix| name.strip_suffix(suffix)) {
      (day, false)
    } else {
      continue;
    };
    let Some(date) = parse_day(day).filter(|date| date.to_string() == day) else {
      continue;
    };
    let archive_day = days.entry(date).or_insert_with(|| ArchiveDay {
      db: dir.join(format!("{day}.db")),
      has_db: false,
      archives: Vec::new(),
    });
    if is_db {
      archive_day.has_db = true;
    } else {
      archive_day.archives.push(entry.path());
    }
  }
  for day in days.values_mut() {
    day.archives.sort_unstable();
  }
  Ok(days)
}

pub(super) struct CheckedArchive {
  pub file: StableFile,
  pub format: ArchiveContents,
  pub digest: [u8; 32],
  pub compressed_bytes: u64,
  pub uncompressed_bytes: Option<u64>,
}

pub(super) struct ArchiveSet {
  pub archives: Vec<CheckedArchive>,
}

#[derive(Debug)]
pub(super) enum ArchiveSetError {
  Conflict { first: PathBuf, other: PathBuf },
  Io(io::Error),
}

impl From<io::Error> for ArchiveSetError {
  fn from(error: io::Error) -> Self {
    Self::Io(error)
  }
}

impl ArchiveSet {
  pub fn verify(paths: &[PathBuf]) -> Result<Self, ArchiveSetError> {
    let mut set = Self::open(paths, &mut |_| {})?;
    set.verify_contents(&mut |_| {})?;
    Ok(set)
  }

  pub fn open(paths: &[PathBuf], progress: &mut impl FnMut(ContentProgress)) -> Result<Self, ArchiveSetError> {
    let mut archives = Vec::with_capacity(paths.len());
    for path in paths {
      let format = ArchiveContents::from_path(path).map_err(|error| file_error(path, error))?;
      let mut file = StableFile::open(path)?;
      let mut compressed_bytes = 0;
      progress(ContentProgress::Stage("Hash archive input"));
      let digest = archive_digest(&mut file, &mut |bytes| {
        compressed_bytes += bytes;
        progress(ContentProgress::Bytes(bytes));
      })?;
      archives.push(CheckedArchive {
        file,
        format,
        digest,
        compressed_bytes,
        uncompressed_bytes: None,
      });
    }
    Ok(Self { archives })
  }

  pub fn verify_contents(&mut self, progress: &mut impl FnMut(ContentProgress)) -> Result<(), ArchiveSetError> {
    for index in 1..self.archives.len() {
      let (earlier, rest) = self.archives.split_at_mut(index);
      let other = &mut rest[0];
      for first in earlier {
        let comparison = archive_content::verify_equivalent(
          first.file.file_mut(),
          first.format,
          other.file.file_mut(),
          other.format,
          progress,
        )
        .map_err(|error| {
          io::Error::new(
            error.kind(),
            format!(
              "could not compare {} and {}: {error}",
              first.file.path().display(),
              other.file.path().display()
            ),
          )
        })?;
        first.uncompressed_bytes = Some(comparison.left_bytes);
        other.uncompressed_bytes = Some(comparison.right_bytes);
        if !comparison.equivalent {
          return Err(ArchiveSetError::Conflict {
            first: first.file.path().to_path_buf(),
            other: other.file.path().to_path_buf(),
          });
        }
      }
    }
    if self.archives.len() > 1 {
      self.recheck_with_progress(progress)?;
    }
    Ok(())
  }

  pub fn recheck(&mut self) -> io::Result<()> {
    self.recheck_with_progress(&mut |_| {})
  }

  pub fn recheck_with_progress(&mut self, progress: &mut impl FnMut(ContentProgress)) -> io::Result<()> {
    progress(ContentProgress::Stage("Recheck archive inputs"));
    for archive in &mut self.archives {
      if archive_digest(&mut archive.file, &mut |bytes| progress(ContentProgress::Bytes(bytes)))? != archive.digest {
        return Err(io::Error::new(
          io::ErrorKind::InvalidData,
          format!("{} changed during verification", archive.file.path().display()),
        ));
      }
    }
    Ok(())
  }
}

fn archive_digest(file: &mut StableFile, progress: &mut impl FnMut(u64)) -> io::Result<[u8; 32]> {
  hash_file(file, None, progress).map_err(|error| file_error(file.path(), error))
}

fn file_error(path: &Path, error: io::Error) -> io::Error {
  io::Error::new(error.kind(), format!("{}: {error}", path.display()))
}
