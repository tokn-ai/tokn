use crate::v2::CompileError;
use alloc::string::{String, ToString};

pub(super) fn canonical_path_prefix(raw: &str, location: &str) -> Result<String, CompileError> {
  if raw.is_empty() || !raw.starts_with('/') {
    return Err(invalid_value(
      location.to_string(),
      "path prefixes must be non-empty and start with `/`",
    ));
  }
  if raw == "/" {
    return Err(invalid_value(
      location.to_string(),
      "`/` matches every path; omit this dimension (and use the listener default action if no constraints remain)",
    ));
  }
  if !raw.is_ascii() {
    return Err(invalid_value(
      location.to_string(),
      "path prefixes must be ASCII URI paths; percent-encode non-ASCII bytes",
    ));
  }

  let bytes = raw.as_bytes();
  let mut canonical = String::with_capacity(raw.len());
  let mut index = 0;
  while index < bytes.len() {
    let byte = bytes[index];
    if byte == b'%' {
      let Some(encoded) = bytes.get(index + 1..index + 3) else {
        return Err(invalid_value(
          location.to_string(),
          "percent escapes in path prefixes must contain exactly two hexadecimal digits",
        ));
      };
      if !encoded.iter().all(u8::is_ascii_hexdigit) {
        return Err(invalid_value(
          location.to_string(),
          "percent escapes in path prefixes must contain exactly two hexadecimal digits",
        ));
      }
      canonical.push('%');
      canonical.push(char::from(encoded[0].to_ascii_uppercase()));
      canonical.push(char::from(encoded[1].to_ascii_uppercase()));
      index += 3;
      continue;
    }
    if byte != b'/' && !is_rfc3986_pchar(byte) {
      return Err(invalid_value(
        location.to_string(),
        "path prefixes may only contain RFC 3986 path characters and percent escapes",
      ));
    }
    canonical.push(char::from(byte));
    index += 1;
  }

  if canonical.split('/').any(is_dot_segment) {
    return Err(invalid_value(
      location.to_string(),
      "path prefixes must not contain literal or percent-encoded `.` or `..` segments",
    ));
  }
  Ok(canonical)
}

fn is_rfc3986_pchar(byte: u8) -> bool {
  byte.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@".contains(&byte)
}

fn is_dot_segment(segment: &str) -> bool {
  let bytes = segment.as_bytes();
  let mut dots = 0;
  let mut index = 0;
  while index < bytes.len() {
    if bytes[index] == b'.' {
      dots += 1;
      index += 1;
    } else if bytes.get(index..index + 3) == Some(b"%2E") {
      dots += 1;
      index += 3;
    } else {
      return false;
    }
  }
  matches!(dots, 1 | 2)
}

fn invalid_value(location: String, message: impl Into<String>) -> CompileError {
  CompileError::InvalidValue {
    location,
    message: message.into(),
  }
}
