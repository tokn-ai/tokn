use alloc::{string::String, vec::Vec};
use core::{fmt, mem, str};

/// A failure while framing a bounded UTF-8 server-sent event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
  /// The current record exceeded its configured byte budget.
  EventTooLarge { limit: usize },
  /// A complete SSE line contains invalid UTF-8.
  InvalidUtf8,
}

impl fmt::Display for DecodeError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::EventTooLarge { limit } => write!(f, "SSE record exceeds its {limit}-byte limit"),
      Self::InvalidUtf8 => f.write_str("SSE record contains invalid UTF-8"),
    }
  }
}

impl core::error::Error for DecodeError {}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Record {
  pub event: Option<String>,
  pub data: String,
}

/// Frames one event at a time without retaining or queuing earlier events.
///
/// The budget includes every field, comment, ignored field, and line ending
/// in a record, including its terminating blank line. CRLF counts as one line
/// ending, like CR or LF. The limit is checked before storing another byte.
/// Records without a `data` field are discarded. An unterminated record is
/// never dispatched: the caller must handle EOF according to its protocol.
pub(super) struct Decoder {
  max_event_bytes: usize,
  record_bytes: usize,
  line: Vec<u8>,
  data: String,
  event: Option<String>,
  data_seen: bool,
  first_line: bool,
  skip_lf: bool,
}

impl Decoder {
  pub fn new(max_event_bytes: usize) -> Self {
    Self {
      max_event_bytes,
      record_bytes: 0,
      line: Vec::new(),
      data: String::new(),
      event: None,
      data_seen: false,
      first_line: true,
      skip_lf: false,
    }
  }

  pub fn push(&mut self, byte: u8) -> Result<Option<Record>, DecodeError> {
    if self.skip_lf {
      self.skip_lf = false;
      if byte == b'\n' {
        return Ok(None);
      }
    }
    if self.record_bytes == self.max_event_bytes {
      return Err(DecodeError::EventTooLarge {
        limit: self.max_event_bytes,
      });
    }
    self.record_bytes += 1;
    if matches!(byte, b'\r' | b'\n') {
      self.skip_lf = byte == b'\r';
      self.finish_line()
    } else {
      self.line.push(byte);
      Ok(None)
    }
  }

  fn finish_line(&mut self) -> Result<Option<Record>, DecodeError> {
    let mut line = str::from_utf8(&self.line).map_err(|_| DecodeError::InvalidUtf8)?;
    if self.first_line {
      self.first_line = false;
      line = line.strip_prefix('\u{feff}').unwrap_or(line);
    }
    if line.is_empty() {
      self.line.clear();
      self.record_bytes = 0;
      let event = self.event.take();
      if !mem::take(&mut self.data_seen) {
        return Ok(None);
      }
      // Each `data` field contributes a newline, including empty fields.
      // SSE removes only the final newline when dispatching the record.
      self.data.pop();
      return Ok(Some(Record {
        event,
        data: mem::take(&mut self.data),
      }));
    }
    if !line.starts_with(':') {
      let (field, value) = line.split_once(':').unwrap_or((line, ""));
      let value = value.strip_prefix(' ').unwrap_or(value);
      match field {
        "data" => {
          self.data_seen = true;
          self.data.push_str(value);
          self.data.push('\n');
        }
        "event" => {
          self.event = (!value.is_empty()).then(|| String::from(value));
        }
        _ => {}
      }
    }
    self.line.clear();
    Ok(None)
  }
}

#[cfg(test)]
mod tests {
  use super::{DecodeError, Decoder, Record};
  use alloc::{string::String, vec, vec::Vec};

  fn records(decoder: &mut Decoder, input: &[u8]) -> Result<Vec<Record>, DecodeError> {
    let mut result = Vec::new();
    for &byte in input {
      if let Some(record) = decoder.push(byte)? {
        result.push(record);
      }
    }
    Ok(result)
  }

  fn record(event: Option<&str>, data: &str) -> Record {
    Record {
      event: event.map(String::from),
      data: data.into(),
    }
  }

  #[test]
  fn accepts_each_line_ending_and_splits_crlf_across_calls() {
    for ending in ["\n", "\r", "\r\n"] {
      let input = alloc::format!("event: delta{ending}data: first{ending}{ending}data: second{ending}{ending}");
      let mut decoder = Decoder::new(128);
      assert_eq!(
        records(&mut decoder, input.as_bytes()).unwrap(),
        vec![record(Some("delta"), "first"), record(None, "second")]
      );
    }
    let mut decoder = Decoder::new(128);
    assert!(records(&mut decoder, b"data: first\r").unwrap().is_empty());
    assert_eq!(records(&mut decoder, b"\n\r").unwrap(), vec![record(None, "first")]);
    assert_eq!(
      records(&mut decoder, b"\ndata: second\r\n\r\n").unwrap(),
      vec![record(None, "second")]
    );
  }

  #[test]
  fn joins_data_fields_and_ignores_comments_ids_and_retry_fields() {
    let mut decoder = Decoder::new(512);
    assert_eq!(
      records(
        &mut decoder,
        b": keep alive\nid: event-1\nretry: 1000\nunknown: value\nevent: old\nevent: delta\ndata: first\ndata:  second\ndata\n\n",
      )
      .unwrap(),
      vec![record(Some("delta"), "first\n second\n")]
    );
  }

  #[test]
  fn discards_records_without_data_and_resets_the_event_name() {
    let mut decoder = Decoder::new(128);
    assert_eq!(
      records(
        &mut decoder,
        b"event: discarded\n: comment\n\n\ndata:\n\nevent: old\nevent:\ndata: payload\n\n",
      )
      .unwrap(),
      vec![record(None, ""), record(None, "payload")]
    );
  }

  #[test]
  fn handles_fragmented_unicode_and_only_strips_the_initial_bom() {
    let mut decoder = Decoder::new(128);
    let input = "\u{feff}data: héllo 🌍\n\ndata: \u{feff}kept\n\n";
    assert_eq!(
      records(&mut decoder, input.as_bytes()).unwrap(),
      vec![record(None, "héllo 🌍"), record(None, "\u{feff}kept")]
    );
    // A later BOM is part of the field name, so this is an unknown field.
    assert!(records(&mut decoder, "\u{feff}data: ignored\n\n".as_bytes())
      .unwrap()
      .is_empty());
  }

  #[test]
  fn bom_before_an_initial_blank_line_is_not_a_field() {
    let mut decoder = Decoder::new(128);
    assert_eq!(
      records(&mut decoder, "\u{feff}\n\ndata: hello\n\n".as_bytes()).unwrap(),
      vec![record(None, "hello")]
    );
  }

  #[test]
  fn rejects_invalid_utf8_in_data_and_ignored_lines() {
    for input in [b"data: \xff\n".as_slice(), b": \xff\n", b"ignored: \xff\n"] {
      let mut decoder = Decoder::new(128);
      assert_eq!(records(&mut decoder, input), Err(DecodeError::InvalidUtf8));
    }
    let mut decoder = Decoder::new(128);
    assert!(records(&mut decoder, b"data: \xe2").unwrap().is_empty());
    assert_eq!(decoder.push(b'\n'), Err(DecodeError::InvalidUtf8));
  }

  #[test]
  fn never_dispatches_an_unterminated_record() {
    let mut decoder = Decoder::new(128);
    assert!(records(&mut decoder, b"data: incomplete\n").unwrap().is_empty());
    assert_eq!(decoder.push(b'\n').unwrap(), Some(record(None, "incomplete")));
    assert!(records(&mut decoder, b"data: also incomplete").unwrap().is_empty());
  }

  #[test]
  fn enforces_exact_record_limits_and_resets_after_each_record() {
    let mut decoder = Decoder::new(9);
    assert_eq!(
      records(&mut decoder, b"data: x\n\ndata: y\r\n\r\n").unwrap(),
      vec![record(None, "x"), record(None, "y")]
    );
    let mut decoder = Decoder::new(8);
    assert_eq!(
      records(&mut decoder, b"data: x\n\n"),
      Err(DecodeError::EventTooLarge { limit: 8 })
    );
    let mut decoder = Decoder::new(0);
    assert_eq!(decoder.push(b'\n'), Err(DecodeError::EventTooLarge { limit: 0 }));
  }

  #[test]
  fn counts_ignored_fields_and_all_lines_toward_the_limit_before_storing() {
    for input in [b": long comment".as_slice(), b"unknown field", b"data:x\ndata:y"] {
      let mut decoder = Decoder::new(8);
      assert_eq!(
        records(&mut decoder, input),
        Err(DecodeError::EventTooLarge { limit: 8 })
      );
      assert_eq!(decoder.record_bytes, 8);
      assert!(decoder.line.len() <= 8);
      assert!(decoder.data.len() <= 8);
    }
    let mut decoder = Decoder::new(8);
    assert!(records(&mut decoder, b": ping\n\n: pong\n\n").unwrap().is_empty());
  }
}
