//! Seekable byte streams stored as independently compressed Zstandard frames.

use std::io::{self, Read, Seek, SeekFrom, Write};
use zstd::bulk::{Compressor, Decompressor};
use zstd::zstd_safe::{self, CParameter, DParameter};

const FRAME_SIZE: usize = 64 * 1024 * 1024;
const COMPRESSION_LEVEL: i32 = 19;
const DATA_MAGIC: u32 = 0xFD2F_B528;
const SKIPPABLE_MAGIC: u32 = 0x184D_2A5E;
const SEEKABLE_MAGIC: u32 = 0x8F92_EAB1;
const TABLE_HEADER_SIZE: usize = 8;
const TABLE_ENTRY_SIZE: usize = 8;
const TABLE_FOOTER_SIZE: usize = 9;
// Magic, frame header, one block header, and a content checksum.
const MIN_COMPRESSED_SIZE: usize = 4 + 2 + 3 + 4;
const MAX_FRAME_COUNT: usize = (u32::MAX as usize - TABLE_FOOTER_SIZE) / TABLE_ENTRY_SIZE;

/// Write independent Zstandard frames and a standard seek table to fresh output.
pub struct BlockWriter<W> {
  output: W,
  compressor: Compressor<'static>,
  pending: Vec<u8>,
  compressed: Vec<u8>,
  entries: Vec<(u32, u32)>,
  frame_size: usize,
  failed: bool,
}

impl<W: Write> BlockWriter<W> {
  pub fn new(output: W) -> io::Result<Self> {
    Self::with_frame_size(output, FRAME_SIZE)
  }

  fn with_frame_size(output: W, frame_size: usize) -> io::Result<Self> {
    let mut compressor = Compressor::new(COMPRESSION_LEVEL)?;
    compressor.set_parameter(CParameter::ChecksumFlag(true))?;
    compressor.set_parameter(CParameter::ContentSizeFlag(true))?;
    Ok(Self {
      output,
      compressor,
      pending: Vec::new(),
      compressed: Vec::new(),
      entries: Vec::new(),
      frame_size,
      failed: false,
    })
  }

  pub fn finish(mut self) -> io::Result<W> {
    self.check_active()?;
    if !self.pending.is_empty() || self.entries.is_empty() {
      self.finish_frame()?;
    }

    // The frames already contain checksums. Use only the size fields in the
    // seek table, which lives in a standard Zstandard skippable frame.
    let count = self.entries.len() as u32;
    self.output.write_all(&SKIPPABLE_MAGIC.to_le_bytes())?;
    self.output.write_all(&table_size(count)?.to_le_bytes())?;
    for (compressed_len, logical_len) in self.entries {
      self.output.write_all(&compressed_len.to_le_bytes())?;
      self.output.write_all(&logical_len.to_le_bytes())?;
    }
    self.output.write_all(&count.to_le_bytes())?;
    self.output.write_all(&[0])?;
    self.output.write_all(&SEEKABLE_MAGIC.to_le_bytes())?;
    self.output.flush()?;
    Ok(self.output)
  }

  fn check_active(&self) -> io::Result<()> {
    if self.failed {
      return Err(io::Error::other("Zstandard archive writer has already failed"));
    }
    Ok(())
  }

  fn finish_frame(&mut self) -> io::Result<()> {
    if self.entries.len() >= MAX_FRAME_COUNT {
      return Err(io::Error::other("Zstandard seek table exceeds its size limit"));
    }
    self.entries.try_reserve(1).map_err(io::Error::other)?;
    self.compressed.clear();
    self
      .compressed
      .try_reserve_exact(zstd_safe::compress_bound(self.pending.len()))
      .map_err(io::Error::other)?;
    let compressed_len = self
      .compressor
      .compress_to_buffer(&self.pending, &mut self.compressed)?;

    // When the output fails, part of the frame may already be present. Let the
    // caller discard it without allowing any retries or a seek table footer.
    self.failed = true;
    self.output.write_all(&self.compressed)?;
    self.failed = false;
    self.entries.push((compressed_len as u32, self.pending.len() as u32));
    self.pending.clear();
    Ok(())
  }
}

impl<W: Write> Write for BlockWriter<W> {
  fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
    self.check_active()?;
    if bytes.is_empty() {
      return Ok(0);
    }
    if self.pending.len() == self.frame_size {
      self.finish_frame()?;
    }
    let count = bytes.len().min(self.frame_size - self.pending.len());
    self.pending.try_reserve(count).map_err(io::Error::other)?;
    self.pending.extend_from_slice(&bytes[..count]);
    Ok(count)
  }

  fn flush(&mut self) -> io::Result<()> {
    self.check_active()?;
    if !self.pending.is_empty() {
      self.finish_frame()?;
    }
    let result = self.output.flush();
    self.failed = result.is_err();
    result
  }
}

#[derive(Clone, Copy)]
struct Frame {
  compressed_offset: u64,
  logical_offset: u64,
  compressed_len: usize,
  logical_len: usize,
}

/// Seek by logical (uncompressed) offset. At most one checked frame is cached.
pub struct BlockReader<R> {
  input: R,
  frames: Vec<Frame>,
  logical_len: u64,
  position: u64,
  cached_frame: Option<usize>,
  compressed: Vec<u8>,
  decoded: Vec<u8>,
  decompressor: Decompressor<'static>,
}

impl<R: Read + Seek> BlockReader<R> {
  /// Open the standard seek table without decoding any nonempty frames. The
  /// supported layout includes checksums in the data frames themselves.
  pub fn open_zstd(mut input: R) -> io::Result<Self> {
    let (frames, logical_len) = read_seek_table(&mut input)?;
    let mut decompressor = Decompressor::new()?;
    decompressor.set_parameter(DParameter::WindowLogMax(FRAME_SIZE.ilog2()))?;
    let mut reader = Self {
      input,
      frames,
      logical_len,
      position: 0,
      cached_frame: None,
      compressed: Vec::new(),
      decoded: Vec::new(),
      decompressor,
    };
    // A zero-length archive reaches EOF without visiting a data frame.
    if reader.logical_len == 0 && !reader.frames.is_empty() {
      reader.load_frame(0)?;
    }
    Ok(reader)
  }

  fn load_frame(&mut self, index: usize) -> io::Result<()> {
    if self.cached_frame == Some(index) {
      return Ok(());
    }
    self.cached_frame = None;
    let frame = self.frames[index];
    self.input.seek(SeekFrom::Start(frame.compressed_offset))?;
    resize_buffer(&mut self.compressed, frame.compressed_len)?;
    self.input.read_exact(&mut self.compressed)?;
    verify_frame_header(&self.compressed, frame.logical_len)?;
    resize_buffer(&mut self.decoded, frame.logical_len)?;
    let decoded_len = self
      .decompressor
      .decompress_to_buffer(&self.compressed, self.decoded.as_mut_slice())?;
    if decoded_len != frame.logical_len {
      return Err(invalid_data(
        "Zstandard decoded length differs from its seek table entry",
      ));
    }
    self.cached_frame = Some(index);
    Ok(())
  }
}

impl<R: Read + Seek> Read for BlockReader<R> {
  fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
    if bytes.is_empty() || self.position >= self.logical_len {
      return Ok(0);
    }
    let index = self
      .frames
      .partition_point(|frame| frame.logical_offset <= self.position)
      - 1;
    self.load_frame(index)?;
    let start = (self.position - self.frames[index].logical_offset) as usize;
    let count = bytes.len().min(self.decoded.len() - start);
    bytes[..count].copy_from_slice(&self.decoded[start..start + count]);
    self.position += count as u64;
    Ok(count)
  }
}

impl<R: Read + Seek> Seek for BlockReader<R> {
  fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
    self.position = match position {
      SeekFrom::Start(offset) => Some(offset),
      SeekFrom::End(offset) => self.logical_len.checked_add_signed(offset),
      SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
    }
    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid seekable Zstandard offset"))?;
    Ok(self.position)
  }
}

fn read_seek_table(input: &mut (impl Read + Seek)) -> io::Result<(Vec<Frame>, u64)> {
  let file_len = input.seek(SeekFrom::End(0))?;
  let footer_at = file_len
    .checked_sub(TABLE_FOOTER_SIZE as u64)
    .ok_or_else(|| invalid_data("truncated Zstandard seek table"))?;
  input.seek(SeekFrom::Start(footer_at))?;
  let mut footer = [0; TABLE_FOOTER_SIZE];
  input.read_exact(&mut footer)?;
  if little_u32(&footer[5..]) != SEEKABLE_MAGIC || footer[4] & 0xfc != 0 {
    return Err(invalid_data("invalid Zstandard seek table descriptor or footer"));
  }
  let count = little_u32(&footer[..4]);
  let table_size = table_size(count)?;
  let table_offset = file_len
    .checked_sub(TABLE_HEADER_SIZE as u64 + u64::from(table_size))
    .ok_or_else(|| invalid_data("Zstandard seek table exceeds the file length"))?;
  if u64::from(count) > table_offset / MIN_COMPRESSED_SIZE as u64 {
    return Err(invalid_data("Zstandard seek table declares too many frames"));
  }

  input.seek(SeekFrom::Start(table_offset))?;
  let mut header = [0; TABLE_HEADER_SIZE];
  input.read_exact(&mut header)?;
  if little_u32(&header[..4]) != SKIPPABLE_MAGIC || little_u32(&header[4..]) != table_size {
    return Err(invalid_data("invalid Zstandard seek table header"));
  }

  let mut frames = Vec::new();
  let mut compressed_offset = 0u64;
  let mut logical_len = 0u64;
  let mut entry = [0; TABLE_ENTRY_SIZE];
  for _ in 0..count {
    input.read_exact(&mut entry)?;
    let compressed_len = little_u32(&entry[..4]) as usize;
    let frame_len = little_u32(&entry[4..]) as usize;
    if frame_len > FRAME_SIZE
      || (frame_len == 0 && count != 1)
      || !(MIN_COMPRESSED_SIZE..=zstd_safe::compress_bound(frame_len)).contains(&compressed_len)
    {
      return Err(invalid_data("invalid seekable Zstandard frame size"));
    }
    let next_offset = compressed_offset
      .checked_add(compressed_len as u64)
      .filter(|&offset| offset <= table_offset)
      .ok_or_else(|| invalid_data("Zstandard frame points outside the data stream"))?;
    frames.try_reserve(1).map_err(io::Error::other)?;
    frames.push(Frame {
      compressed_offset,
      logical_offset: logical_len,
      compressed_len,
      logical_len: frame_len,
    });
    logical_len = logical_len
      .checked_add(frame_len as u64)
      .ok_or_else(|| invalid_data("Zstandard logical archive size overflows"))?;
    compressed_offset = next_offset;
  }
  if compressed_offset != table_offset {
    return Err(invalid_data("Zstandard frame sizes do not match the seek table"));
  }
  Ok((frames, logical_len))
}

fn verify_frame_header(compressed: &[u8], logical_len: usize) -> io::Result<()> {
  let descriptor = compressed[4];
  if little_u32(&compressed[..4]) != DATA_MAGIC || descriptor & 4 == 0 {
    return Err(invalid_data("expected a Zstandard data frame with a checksum"));
  }
  // A single-segment frame has a window equal to its content size. Otherwise
  // the frame's window descriptor encodes an exponent and a mantissa.
  if descriptor & 0x20 == 0 {
    let window = compressed[5];
    let base = 1u64 << (10 + (window >> 3));
    if base + (base / 8) * u64::from(window & 7) > FRAME_SIZE as u64 {
      return Err(invalid_data("Zstandard frame window exceeds the size limit"));
    }
  }
  if zstd_safe::get_frame_content_size(compressed).ok() != Some(Some(logical_len as u64)) {
    return Err(invalid_data(
      "Zstandard frame content size differs from its seek table entry",
    ));
  }
  let compressed_len = zstd_safe::find_frame_compressed_size(compressed)
    .map_err(|error| invalid_data(zstd_safe::get_error_name(error)))?;
  if compressed_len != compressed.len() {
    return Err(invalid_data("Zstandard frame length differs from its seek table entry"));
  }
  Ok(())
}

fn table_size(count: u32) -> io::Result<u32> {
  count
    .checked_mul(TABLE_ENTRY_SIZE as u32)
    .and_then(|entries| entries.checked_add(TABLE_FOOTER_SIZE as u32))
    .ok_or_else(|| invalid_data("Zstandard seek table exceeds its size limit"))
}

fn resize_buffer(buffer: &mut Vec<u8>, len: usize) -> io::Result<()> {
  buffer.clear();
  buffer.try_reserve_exact(len).map_err(io::Error::other)?;
  buffer.resize(len, 0);
  Ok(())
}

fn little_u32(bytes: &[u8]) -> u32 {
  u32::from_le_bytes(bytes.try_into().expect("four-byte Zstandard archive field"))
}

fn invalid_data(message: &'static str) -> io::Error {
  io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Cursor;

  fn encode(bytes: &[u8]) -> io::Result<Vec<u8>> {
    let mut writer = BlockWriter::with_frame_size(Vec::new(), 8)?;
    writer.write_all(bytes)?;
    writer.finish()
  }

  #[test]
  fn seeks_across_frames_and_decodes_as_standard_zstandard() -> io::Result<()> {
    let input = b"abcdefghijklmnopqrstuvwxyz";
    let compressed = encode(input)?;
    assert_eq!(zstd::stream::decode_all(compressed.as_slice())?, input);

    let mut reader = BlockReader::open_zstd(Cursor::new(compressed))?;
    assert_eq!(reader.frames.len(), 4);
    reader.seek(SeekFrom::Start(7))?;
    let mut bytes = [0; 10];
    reader.read_exact(&mut bytes)?;
    assert_eq!(&bytes, &input[7..17]);
    assert_eq!(reader.seek(SeekFrom::Current(-4))?, 13);
    assert_eq!(reader.seek(SeekFrom::End(-3))?, 23);
    let mut tail = Vec::new();
    reader.read_to_end(&mut tail)?;
    assert_eq!(&tail, &input[23..]);
    assert!(reader.seek(SeekFrom::End(-27)).is_err());
    Ok(())
  }

  #[test]
  fn checks_the_requested_frame_before_returning_bytes() -> io::Result<()> {
    let mut compressed = encode(b"first---second!")?;
    let first_len = BlockReader::open_zstd(Cursor::new(&compressed))?.frames[0].compressed_len;
    compressed[first_len - 1] ^= 1;

    let mut reader = BlockReader::open_zstd(Cursor::new(compressed))?;
    reader.seek(SeekFrom::Start(8))?;
    let mut bytes = [0; 7];
    reader.read_exact(&mut bytes)?;
    assert_eq!(&bytes, b"second!");
    reader.rewind()?;
    assert!(reader.read_exact(&mut bytes).is_err());
    assert_eq!(reader.stream_position()?, 0);
    Ok(())
  }

  #[test]
  fn rejects_malformed_seek_tables_and_oversized_windows() -> io::Result<()> {
    let mut compressed = encode(b"first---second!")?;
    let footer_at = compressed.len() - TABLE_FOOTER_SIZE;
    let table_at = footer_at - 2 * TABLE_ENTRY_SIZE - TABLE_HEADER_SIZE;
    for offset in [table_at, table_at + TABLE_HEADER_SIZE, footer_at + 5, footer_at + 4] {
      let mut corrupted = compressed.clone();
      corrupted[offset] ^= 0x80;
      assert!(BlockReader::open_zstd(Cursor::new(corrupted)).is_err());
    }
    compressed[footer_at..footer_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(BlockReader::open_zstd(Cursor::new(compressed)).is_err());

    let mut oversized_window = [0; MIN_COMPRESSED_SIZE];
    oversized_window[..4].copy_from_slice(&DATA_MAGIC.to_le_bytes());
    oversized_window[4] = 4;
    oversized_window[5] = ((FRAME_SIZE.ilog2() + 1 - 10) << 3) as u8;
    let error = verify_frame_header(&oversized_window, 8).unwrap_err();
    assert_eq!(error.to_string(), "Zstandard frame window exceeds the size limit");
    Ok(())
  }

  #[test]
  fn rejects_a_seek_entry_covering_two_frames() -> io::Result<()> {
    let mut compressed = encode(b"first---second!")?;
    compressed.truncate(compressed.len() - TABLE_HEADER_SIZE - 2 * TABLE_ENTRY_SIZE - TABLE_FOOTER_SIZE);
    let data_len = compressed.len() as u32;
    compressed.write_all(&SKIPPABLE_MAGIC.to_le_bytes())?;
    compressed.write_all(&table_size(1)?.to_le_bytes())?;
    compressed.write_all(&data_len.to_le_bytes())?;
    compressed.write_all(&8u32.to_le_bytes())?;
    compressed.write_all(&1u32.to_le_bytes())?;
    compressed.write_all(&[0])?;
    compressed.write_all(&SEEKABLE_MAGIC.to_le_bytes())?;

    let mut reader = BlockReader::open_zstd(Cursor::new(compressed))?;
    let error = reader.read_exact(&mut [0; 8]).unwrap_err();
    assert_eq!(
      error.to_string(),
      "Zstandard frame length differs from its seek table entry"
    );
    Ok(())
  }

  #[test]
  fn checks_empty_frame_and_allows_frames_to_be_flushed() -> io::Result<()> {
    let mut compressed = BlockWriter::new(Vec::new())?.finish()?;
    assert!(zstd::stream::decode_all(compressed.as_slice())?.is_empty());
    let mut reader = BlockReader::open_zstd(Cursor::new(&compressed))?;
    assert_eq!(reader.seek(SeekFrom::End(0))?, 0);
    let empty_len = reader.frames[0].compressed_len;
    compressed[empty_len - 1] ^= 1;
    assert!(BlockReader::open_zstd(Cursor::new(compressed)).is_err());

    let mut writer = BlockWriter::new(Vec::new())?;
    writer.write_all(b"first---")?;
    writer.flush()?;
    writer.write_all(b"second!")?;
    let compressed = writer.finish()?;
    let mut reader = BlockReader::open_zstd(Cursor::new(compressed))?;
    assert_eq!(reader.frames.len(), 2);
    let mut decoded = Vec::new();
    reader.read_to_end(&mut decoded)?;
    assert_eq!(&decoded, b"first---second!");
    Ok(())
  }

  #[test]
  fn cannot_resume_after_a_partial_output_failure() -> io::Result<()> {
    struct FailAfter(usize);

    impl Write for FailAfter {
      fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0 == 0 {
          return Err(io::Error::other("simulated archive output failure"));
        }
        let count = bytes.len().min(self.0);
        self.0 -= count;
        Ok(count)
      }

      fn flush(&mut self) -> io::Result<()> {
        Ok(())
      }
    }

    let mut writer = BlockWriter::new(FailAfter(5))?;
    writer.write_all(b"partial frame")?;
    assert!(writer.flush().is_err());
    assert!(writer.write(b"later payload").is_err());
    assert!(writer.finish().is_err());
    Ok(())
  }
}
