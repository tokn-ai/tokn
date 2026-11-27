//! Seekable byte streams stored as independently compressed XZ streams.

use flate2::Crc;
use lzma_rust2::{XzOptions, XzReader, XzWriter};
use std::io::{self, Read, Seek, SeekFrom, Write};

const BLOCK_SIZE: usize = 64 * 1024 * 1024;
const XZ_HEADER_SIZE: usize = 12;
const XZ_FOOTER_SIZE: usize = 12;
const XZ_CRC32_SIZE: usize = 4;
const XZ_CRC64_SIZE: u64 = 8;
const XZ_CRC64_FLAGS: [u8; 2] = [0, 4];
const MAX_XZ_BLOCK_HEADER_SIZE: usize = 1024;
// A one-block Index has two size integers of at most nine bytes each.
const MAX_XZ_INDEX_SIZE: usize = 24;
const MAX_DICTIONARY_PROPERTY: u8 = 28; // The XZ code for a 64 MiB dictionary.

#[derive(Clone, Copy, Debug)]
struct Block {
  compressed_offset: u64,
  compressed_len: u64,
  logical_offset: u64,
  logical_len: u64,
}

/// Write one XZ stream per block to a fresh output starting at byte zero.
pub struct BlockWriter<W> {
  output: W,
  pending: Vec<u8>,
  block_size: usize,
  wrote_stream: bool,
  failed: bool,
}

impl<W: Write> BlockWriter<W> {
  pub fn new(output: W) -> Self {
    Self::with_block_size(output, BLOCK_SIZE)
  }

  fn with_block_size(output: W, block_size: usize) -> Self {
    Self {
      output,
      pending: Vec::new(),
      block_size,
      wrote_stream: false,
      failed: false,
    }
  }

  pub fn finish(mut self) -> io::Result<W> {
    if !self.wrote_stream && self.pending.is_empty() {
      self.finish_stream()?;
    }
    self.flush()?;
    Ok(self.output)
  }

  fn finish_stream(&mut self) -> io::Result<()> {
    if self.failed {
      return Err(io::Error::other("body pack writer has already failed"));
    }
    // A failed encoder may already have written part of the XZ stream. The
    // caller can discard the output, but it cannot be finished or retried.
    self.failed = true;
    let mut encoder = XzWriter::new(&mut self.output, XzOptions::with_preset(6))?;
    encoder.write_all(&self.pending)?;
    encoder.finish()?;
    self.failed = false;
    self.wrote_stream = true;
    self.pending.clear();
    Ok(())
  }
}

impl<W: Write> Write for BlockWriter<W> {
  fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
    if bytes.is_empty() {
      return Ok(0);
    }
    if self.pending.len() == self.block_size {
      self.finish_stream()?;
    }
    let count = bytes.len().min(self.block_size - self.pending.len());
    self.pending.try_reserve(count).map_err(io::Error::other)?;
    self.pending.extend_from_slice(&bytes[..count]);
    Ok(count)
  }

  fn flush(&mut self) -> io::Result<()> {
    if !self.pending.is_empty() {
      self.finish_stream()?;
    }
    self.output.flush()
  }
}

/// Seek by logical (uncompressed) offset. At most one checked block is cached.
pub struct BlockReader<R> {
  input: R,
  blocks: Vec<Block>,
  logical_len: u64,
  position: u64,
  cached_block: Option<usize>,
  decoded: Vec<u8>,
}

impl<R: Read + Seek> BlockReader<R> {
  /// Open concatenated XZ streams with at most one LZMA2 block each. Scan
  /// standard stream footers and indexes to find the blocks without decoding
  /// their payloads.
  pub fn open_xz(mut input: R) -> io::Result<Self> {
    let mut end = input.seek(SeekFrom::End(0))?;
    if end == 0 || !end.is_multiple_of(4) {
      return Err(invalid_data("invalid body pack XZ stream length"));
    }

    let mut blocks = Vec::new();
    while end != 0 {
      end = skip_stream_padding(&mut input, end)?;
      let (start, logical_len) = read_stream_index(&mut input, end)?;
      if logical_len != 0 {
        blocks.try_reserve(1).map_err(io::Error::other)?;
        blocks.push(Block {
          compressed_offset: start,
          compressed_len: end - start,
          logical_offset: 0,
          logical_len,
        });
      }
      end = start;
    }

    blocks.reverse();
    let mut logical_len = 0u64;
    for block in &mut blocks {
      block.logical_offset = logical_len;
      logical_len = logical_len
        .checked_add(block.logical_len)
        .ok_or_else(|| invalid_data("body pack logical length overflows"))?;
    }

    Ok(Self {
      input,
      blocks,
      logical_len,
      position: 0,
      cached_block: None,
      decoded: Vec::new(),
    })
  }

  fn load_block(&mut self, index: usize) -> io::Result<()> {
    if self.cached_block == Some(index) {
      return Ok(());
    }

    self.cached_block = None;
    let block = self.blocks[index];
    let len = block.logical_len as usize;
    self.decoded.clear();
    self.decoded.try_reserve_exact(len).map_err(io::Error::other)?;
    self.decoded.resize(len, 0);

    self.input.seek(SeekFrom::Start(block.compressed_offset))?;
    let mut decoder = XzReader::new(self.input.by_ref().take(block.compressed_len), false);
    decoder.read_exact(&mut self.decoded)?;
    // Continue through the XZ checksum and footer before exposing any bytes.
    if decoder.read(&mut [0])? != 0 || decoder.into_inner().limit() != 0 {
      return Err(invalid_data("body pack block length differs from its index"));
    }
    self.cached_block = Some(index);
    Ok(())
  }
}

impl<R: Read + Seek> Read for BlockReader<R> {
  fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
    if bytes.is_empty() || self.position >= self.logical_len {
      return Ok(0);
    }
    let index = self
      .blocks
      .partition_point(|block| block.logical_offset <= self.position)
      - 1;
    self.load_block(index)?;
    let start = (self.position - self.blocks[index].logical_offset) as usize;
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
    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid body pack seek offset"))?;
    Ok(self.position)
  }
}

fn skip_stream_padding(input: &mut (impl Read + Seek), mut end: u64) -> io::Result<u64> {
  let mut word = [0; 4];
  while let Some(offset) = end.checked_sub(word.len() as u64) {
    read_at(input, offset, &mut word)?;
    if word != [0; 4] {
      return Ok(end);
    }
    end = offset;
  }
  Err(invalid_data("body pack XZ stream is missing before padding"))
}

fn read_stream_index(input: &mut (impl Read + Seek), end: u64) -> io::Result<(u64, u64)> {
  let footer_offset = end
    .checked_sub(XZ_FOOTER_SIZE as u64)
    .ok_or_else(|| invalid_data("truncated body pack XZ footer"))?;
  let mut footer = [0; XZ_FOOTER_SIZE];
  read_at(input, footer_offset, &mut footer)?;
  if &footer[10..] != b"YZ" || footer[8..10] != XZ_CRC64_FLAGS || !crc32_matches(&footer[4..10], &footer[..4]) {
    return Err(invalid_data("invalid body pack XZ footer"));
  }

  let backward_size = u32::from_le_bytes(footer[4..8].try_into().expect("4-byte footer field"));
  let index_len = (u64::from(backward_size) + 1) * 4;
  if !(8..=MAX_XZ_INDEX_SIZE as u64).contains(&index_len) {
    return Err(invalid_data("invalid body pack XZ index length"));
  }
  let index_offset = footer_offset
    .checked_sub(index_len)
    .ok_or_else(|| invalid_data("truncated body pack XZ index"))?;
  let mut index = [0; MAX_XZ_INDEX_SIZE];
  let index = &mut index[..index_len as usize];
  read_at(input, index_offset, index)?;
  let (unpadded_len, logical_len) = parse_index(index)?;
  let padded_len = unpadded_len.next_multiple_of(4);
  let start = index_offset
    .checked_sub(padded_len + XZ_HEADER_SIZE as u64)
    .ok_or_else(|| invalid_data("body pack XZ index exceeds its stream length"))?;

  let mut header = [0; XZ_HEADER_SIZE];
  read_at(input, start, &mut header)?;
  if &header[..6] != b"\xFD7zXZ\0" || header[6..8] != XZ_CRC64_FLAGS || !crc32_matches(&header[6..8], &header[8..]) {
    return Err(invalid_data("invalid body pack XZ stream header"));
  }
  if unpadded_len != 0 {
    verify_block_header(input, start + XZ_HEADER_SIZE as u64, unpadded_len, logical_len)?;
  }
  Ok((start, logical_len))
}

fn parse_index(index: &[u8]) -> io::Result<(u64, u64)> {
  let (fields, checksum) = index.split_at(index.len() - XZ_CRC32_SIZE);
  if fields[0] != 0 || !crc32_matches(fields, checksum) {
    return Err(invalid_data("invalid body pack XZ index checksum or indicator"));
  }
  let mut fields = &fields[1..];
  let lengths = match read_xz_integer(&mut fields)? {
    0 => (0, 0),
    1 => {
      let unpadded_len = read_xz_integer(&mut fields)?;
      let logical_len = read_xz_integer(&mut fields)?;
      if unpadded_len == 0 || !(1..=BLOCK_SIZE as u64).contains(&logical_len) {
        return Err(invalid_data("invalid body pack XZ block length"));
      }
      (unpadded_len, logical_len)
    }
    _ => return Err(invalid_data("expected at most one block per body pack XZ stream")),
  };
  if fields.len() > 3 || fields.iter().any(|&byte| byte != 0) {
    return Err(invalid_data("invalid body pack XZ index padding"));
  }
  Ok(lengths)
}

fn verify_block_header(
  input: &mut (impl Read + Seek),
  offset: u64,
  unpadded_len: u64,
  logical_len: u64,
) -> io::Result<()> {
  let mut header = [0; MAX_XZ_BLOCK_HEADER_SIZE];
  read_at(input, offset, &mut header[..1])?;
  let header_len = (usize::from(header[0]) + 1) * 4;
  let compressed_len = unpadded_len
    .checked_sub(header_len as u64 + XZ_CRC64_SIZE)
    .filter(|&len| len != 0 && header_len >= 8)
    .ok_or_else(|| invalid_data("invalid body pack XZ block header length"))?;
  read_at(input, offset + 1, &mut header[1..header_len])?;
  let (fields, checksum) = header[..header_len].split_at(header_len - XZ_CRC32_SIZE);
  if !crc32_matches(fields, checksum) {
    return Err(invalid_data("invalid body pack XZ block header checksum"));
  }

  let flags = fields[1];
  let mut fields = &fields[2..];
  if flags & 0x3F != 0
    || (flags & 0x40 != 0 && read_xz_integer(&mut fields)? != compressed_len)
    || (flags & 0x80 != 0 && read_xz_integer(&mut fields)? != logical_len)
    || read_xz_integer(&mut fields)? != 0x21 // LZMA2 filter identifier.
    || read_xz_integer(&mut fields)? != 1
  // Length of the dictionary property.
  {
    return Err(invalid_data("invalid body pack XZ block flags or filter"));
  }
  match fields.split_first() {
    Some((&property, padding)) if property <= MAX_DICTIONARY_PROPERTY && padding.iter().all(|&byte| byte == 0) => {}
    _ => return Err(invalid_data("invalid body pack XZ block dictionary or header padding")),
  }

  let padding_len = (unpadded_len.next_multiple_of(4) - unpadded_len) as usize;
  let mut padding = [0; 3];
  if padding_len != 0 {
    read_at(
      input,
      offset + header_len as u64 + compressed_len,
      &mut padding[..padding_len],
    )?;
    if padding != [0; 3] {
      return Err(invalid_data("invalid body pack XZ block padding"));
    }
  }
  Ok(())
}

fn read_xz_integer(input: &mut &[u8]) -> io::Result<u64> {
  let mut value = 0;
  for shift in (0..63).step_by(7) {
    let (&byte, remaining) = input
      .split_first()
      .ok_or_else(|| invalid_data("truncated body pack XZ integer"))?;
    *input = remaining;
    if shift != 0 && byte == 0 {
      return Err(invalid_data("noncanonical body pack XZ integer"));
    }
    value |= u64::from(byte & 0x7F) << shift;
    if byte & 0x80 == 0 {
      return Ok(value);
    }
  }
  Err(invalid_data("body pack XZ integer exceeds its size limit"))
}

fn read_at(input: &mut (impl Read + Seek), position: u64, bytes: &mut [u8]) -> io::Result<()> {
  input.seek(SeekFrom::Start(position))?;
  input.read_exact(bytes)
}

fn crc32_matches(bytes: &[u8], checksum: &[u8]) -> bool {
  let mut crc = Crc::new();
  crc.update(bytes);
  crc.sum().to_le_bytes() == checksum
}

fn invalid_data(message: &'static str) -> io::Error {
  io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Cursor;

  fn encode_blocks(input: &[u8]) -> io::Result<Vec<u8>> {
    let mut writer = BlockWriter::with_block_size(Vec::new(), 8);
    writer.write_all(input)?;
    writer.finish()
  }

  #[test]
  fn reaches_overlapping_ranges_and_seeks_from_end() -> io::Result<()> {
    let input = b"abcdefghijklmnopqrstuvwxyz";
    let compressed = encode_blocks(input)?;
    let mut reader = BlockReader::open_xz(Cursor::new(&compressed))?;
    assert_eq!(reader.blocks.len(), 4);
    assert_eq!(reader.blocks[1].logical_offset, 8);
    assert_eq!(reader.blocks[3].logical_len, 2);
    reader.seek(SeekFrom::Start(7))?;
    let mut cross_block = [0; 10];
    reader.read_exact(&mut cross_block)?;
    assert_eq!(&cross_block, &input[7..17]);
    assert_eq!(reader.seek(SeekFrom::Current(-4))?, 13);
    assert_eq!(reader.seek(SeekFrom::End(-3))?, 23);
    let mut tail = Vec::new();
    reader.read_to_end(&mut tail)?;
    assert_eq!(&tail, &input[23..]);
    assert!(reader.seek(SeekFrom::End(-27)).is_err());
    Ok(())
  }

  #[test]
  fn seeking_skips_corrupt_blocks_until_they_are_read() -> io::Result<()> {
    let mut compressed = encode_blocks(b"first---second!")?;
    let first = BlockReader::open_xz(Cursor::new(&compressed))?.blocks[0];
    let footer_offset = first.compressed_len as usize - XZ_FOOTER_SIZE;
    let backward_size = u32::from_le_bytes(compressed[footer_offset + 4..footer_offset + 8].try_into().unwrap());
    let index_offset = footer_offset - (backward_size as usize + 1) * 4;
    // Damage the first block's CRC64 without touching either stream index.
    compressed[index_offset - 1] ^= 1;
    let mut reader = BlockReader::open_xz(Cursor::new(compressed))?;
    reader.seek(SeekFrom::Start(8))?;
    let mut output = [0; 7];
    reader.read_exact(&mut output)?;
    assert_eq!(&output, b"second!");
    reader.rewind()?;
    assert!(reader.read_exact(&mut output).is_err());
    Ok(())
  }

  #[test]
  fn standard_xz_reader_decodes_multiple_streams_and_padding() -> io::Result<()> {
    let mut compressed = encode_blocks(b"first---")?;
    compressed.extend_from_slice(&[0; 8]);
    compressed.extend_from_slice(&BlockWriter::new(Vec::new()).finish()?);
    compressed.extend_from_slice(&encode_blocks(b"second!")?);
    compressed.extend_from_slice(&[0; 4]);

    let mut standard = Vec::new();
    XzReader::new(Cursor::new(&compressed), true).read_to_end(&mut standard)?;
    assert_eq!(&standard, b"first---second!");
    let mut seekable = Vec::new();
    BlockReader::open_xz(Cursor::new(&compressed))?.read_to_end(&mut seekable)?;
    assert_eq!(seekable, standard);
    Ok(())
  }

  #[test]
  fn rejects_corrupt_headers_indexes_footers_and_invalid_padding() -> io::Result<()> {
    let compressed = encode_blocks(b"first---second!")?;
    let footer_offset = compressed.len() - XZ_FOOTER_SIZE;
    for offset in [
      0,
      XZ_HEADER_SIZE + 4,
      footer_offset - 1,
      footer_offset,
      compressed.len() - 1,
    ] {
      let mut corrupted = compressed.clone();
      corrupted[offset] ^= 1;
      assert!(BlockReader::open_xz(Cursor::new(corrupted)).is_err());
    }
    let mut leading_padding = vec![0; 4];
    leading_padding.extend_from_slice(&compressed);
    assert!(BlockReader::open_xz(Cursor::new(leading_padding)).is_err());

    let mut trailing_garbage = compressed;
    trailing_garbage.extend_from_slice(&[1; 4]);
    assert!(BlockReader::open_xz(Cursor::new(&trailing_garbage)).is_err());
    trailing_garbage.truncate(trailing_garbage.len() - 4);
    trailing_garbage.push(0);
    assert!(BlockReader::open_xz(Cursor::new(trailing_garbage)).is_err());
    Ok(())
  }
}
