use crate::viewer::days::is_valid_request_day;

/// An opaque position in the newest-first request history ordering.
///
/// Cursors include the day because SQLite row identities are scoped to a day
/// database. Callers should persist and replay the encoded value rather than
/// constructing one themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestCursor {
  day: String,
  ts: i64,
  row_id: i64,
}

impl RequestCursor {
  const VERSION: &'static str = "v2";

  pub fn decode(value: &str) -> std::result::Result<Self, InvalidRequestCursor> {
    let mut parts = value.split('.');
    let version = parts.next().ok_or(InvalidRequestCursor)?;
    let day = parts.next().ok_or(InvalidRequestCursor)?;
    let ts = parts
      .next()
      .ok_or(InvalidRequestCursor)?
      .parse::<i64>()
      .map_err(|_| InvalidRequestCursor)?;
    let row_id = parts
      .next()
      .ok_or(InvalidRequestCursor)?
      .parse::<i64>()
      .map_err(|_| InvalidRequestCursor)?;
    if parts.next().is_some() || version != Self::VERSION || !is_valid_request_day(day) {
      return Err(InvalidRequestCursor);
    }
    Ok(Self {
      day: day.to_string(),
      ts,
      row_id,
    })
  }

  pub fn day(&self) -> &str {
    &self.day
  }

  pub(super) fn timestamp(&self) -> i64 {
    self.ts
  }

  pub(super) fn row_id(&self) -> i64 {
    self.row_id
  }

  pub(super) fn from_position(day: &str, ts: i64, row_id: i64) -> Self {
    Self {
      day: day.to_string(),
      ts,
      row_id,
    }
  }

  pub(super) fn encode(&self) -> String {
    format!("{}.{}.{}.{}", Self::VERSION, self.day, self.ts, self.row_id)
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRequestCursor;

impl std::fmt::Display for InvalidRequestCursor {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.write_str("invalid request cursor")
  }
}

impl std::error::Error for InvalidRequestCursor {}
