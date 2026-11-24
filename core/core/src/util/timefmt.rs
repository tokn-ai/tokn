//! Compact relative-time rendering for CLI output.
//!
//! All inputs are unix epoch seconds (`i64`). We render against
//! `time::OffsetDateTime::now_utc()`. Past instants render as `"5m ago"`,
//! future ones as `"in 3h 21m"`. Granularity collapses gracefully so the
//! result fits in a single column.

use time::OffsetDateTime;

/// Render an absolute unix-seconds timestamp relative to "now".
pub fn relative_from_now(unix_seconds: i64) -> String {
  let now = OffsetDateTime::now_utc().unix_timestamp();
  relative_delta(unix_seconds - now)
}

/// Render a unix-millis timestamp relative to "now".
pub fn relative_from_now_ms(unix_ms: i64) -> String {
  relative_from_now(unix_ms / 1000)
}

/// Render a signed delta in seconds. Positive = future, negative = past.
pub fn relative_delta(delta_secs: i64) -> String {
  let (suffix, prefix, abs) = if delta_secs >= 0 {
    ("", "in ", delta_secs)
  } else {
    (" ago", "", -delta_secs)
  };

  let s = if abs < 5 {
    return if delta_secs >= 0 {
      "now".into()
    } else {
      "just now".into()
    };
  } else if abs < 60 {
    format!("{abs}s")
  } else if abs < 3600 {
    let m = abs / 60;
    let s = abs % 60;
    if s == 0 || m >= 10 {
      format!("{m}m")
    } else {
      format!("{m}m {s}s")
    }
  } else if abs < 86_400 {
    let h = abs / 3600;
    let m = (abs % 3600) / 60;
    if m == 0 {
      format!("{h}h")
    } else {
      format!("{h}h {m}m")
    }
  } else {
    let d = abs / 86_400;
    let h = (abs % 86_400) / 3600;
    if h == 0 {
      format!("{d}d")
    } else {
      format!("{d}d {h}h")
    }
  };
  format!("{prefix}{s}{suffix}")
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn seconds_past_and_future() {
    assert_eq!(relative_delta(0), "now");
    assert_eq!(relative_delta(-1), "just now");
    assert_eq!(relative_delta(30), "in 30s");
    assert_eq!(relative_delta(-30), "30s ago");
  }

  #[test]
  fn minutes_and_hours() {
    assert_eq!(relative_delta(60), "in 1m");
    assert_eq!(relative_delta(125), "in 2m 5s");
    assert_eq!(relative_delta(11 * 60 + 5), "in 11m"); // collapses seconds at >=10m
    assert_eq!(relative_delta(3 * 3600 + 21 * 60), "in 3h 21m");
    assert_eq!(relative_delta(-(2 * 3600)), "2h ago");
  }

  #[test]
  fn days() {
    assert_eq!(relative_delta(2 * 86_400), "in 2d");
    assert_eq!(relative_delta(2 * 86_400 + 5 * 3600), "in 2d 5h");
    assert_eq!(relative_delta(-(17 * 86_400 + 4 * 3600)), "17d 4h ago");
  }
}
