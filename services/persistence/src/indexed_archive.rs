//! Seekable compression formats for indexed request packs.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(feature = "lzma")]
use crate::archive_xz;
use crate::archive_zstd;

/// Compression for an independently readable indexed request archive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexedArchiveFormat {
  Zstd,
  #[cfg(feature = "lzma")]
  Xz,
}

impl IndexedArchiveFormat {
  /// Supported formats in discovery order.
  pub const ALL: &'static [Self] = &[
    Self::Zstd,
    #[cfg(feature = "lzma")]
    Self::Xz,
  ];

  pub const fn extension(self) -> &'static str {
    match self {
      Self::Zstd => "pack.zst",
      #[cfg(feature = "lzma")]
      Self::Xz => "pack.xz",
    }
  }

  /// The published archive path for a live request database.
  pub fn path(self, db: &Path) -> PathBuf {
    db.with_extension(self.extension())
  }

  /// Identify an archive from its filename suffix.
  pub fn from_path(path: &Path) -> Option<Self> {
    Self::identify_path(path).map(|(format, _)| format)
  }

  /// Get the filename prefix before a supported indexed archive suffix.
  pub fn day_from_path(path: &Path) -> Option<&str> {
    Self::identify_path(path).map(|(_, day)| day)
  }

  fn identify_path(path: &Path) -> Option<(Self, &str)> {
    let name = path.file_name()?.to_str()?;
    Self::ALL.iter().find_map(|&format| {
      let day = name.strip_suffix(format.extension())?.strip_suffix('.')?;
      Some((format, day))
    })
  }

  /// Open the archive as a stream of uncompressed bytes with seek support.
  pub fn open<R: Read + Seek>(self, input: R) -> io::Result<IndexedArchiveReader<R>> {
    match self {
      Self::Zstd => archive_zstd::BlockReader::open_zstd(input).map(IndexedArchiveReader::Zstd),
      #[cfg(feature = "lzma")]
      Self::Xz => archive_xz::BlockReader::open_xz(input).map(IndexedArchiveReader::Xz),
    }
  }

  /// Start writing an archive at byte zero of a fresh output.
  pub fn writer<W: Write>(self, output: W) -> io::Result<IndexedArchiveWriter<W>> {
    match self {
      Self::Zstd => archive_zstd::BlockWriter::new(output).map(IndexedArchiveWriter::Zstd),
      #[cfg(feature = "lzma")]
      Self::Xz => Ok(IndexedArchiveWriter::Xz(archive_xz::BlockWriter::new(output))),
    }
  }
}

/// Read and seek in the uncompressed byte stream of an indexed archive.
pub enum IndexedArchiveReader<R> {
  Zstd(archive_zstd::BlockReader<R>),
  #[cfg(feature = "lzma")]
  Xz(archive_xz::BlockReader<R>),
}

impl<R: Read + Seek> Read for IndexedArchiveReader<R> {
  fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
    match self {
      Self::Zstd(reader) => reader.read(buffer),
      #[cfg(feature = "lzma")]
      Self::Xz(reader) => reader.read(buffer),
    }
  }
}

impl<R: Read + Seek> Seek for IndexedArchiveReader<R> {
  fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
    match self {
      Self::Zstd(reader) => reader.seek(position),
      #[cfg(feature = "lzma")]
      Self::Xz(reader) => reader.seek(position),
    }
  }
}

/// Write the uncompressed bytes of an indexed archive.
pub enum IndexedArchiveWriter<W> {
  Zstd(archive_zstd::BlockWriter<W>),
  #[cfg(feature = "lzma")]
  Xz(archive_xz::BlockWriter<W>),
}

impl<W: Write> IndexedArchiveWriter<W> {
  /// Finalize the archive and return the underlying output.
  pub fn finish(self) -> io::Result<W> {
    match self {
      Self::Zstd(writer) => writer.finish(),
      #[cfg(feature = "lzma")]
      Self::Xz(writer) => writer.finish(),
    }
  }
}

impl<W: Write> Write for IndexedArchiveWriter<W> {
  fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
    match self {
      Self::Zstd(writer) => writer.write(buffer),
      #[cfg(feature = "lzma")]
      Self::Xz(writer) => writer.write(buffer),
    }
  }

  fn flush(&mut self) -> io::Result<()> {
    match self {
      Self::Zstd(writer) => writer.flush(),
      #[cfg(feature = "lzma")]
      Self::Xz(writer) => writer.flush(),
    }
  }
}
