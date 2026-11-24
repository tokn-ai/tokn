use base64::Engine;
use rusqlite::types::ValueRef;
use serde_json::Value;

const JSON_COLUMNS: &[&str] = &[
  "ctx_json",
  "params_json",
  "usage_json",
  "inbound_req_headers",
  "inbound_req_body",
  "inbound_resp_headers",
  "inbound_resp_body",
  "outbound_req_headers",
  "outbound_req_body",
  "outbound_resp_headers",
  "outbound_resp_body",
];

pub(super) fn serialize_i64_as_string<S>(value: &i64, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
  S: serde::Serializer,
{
  serializer.serialize_str(&value.to_string())
}

pub(super) fn sqlite_status(value: Option<i64>) -> Option<u16> {
  value.and_then(|value| u16::try_from(value).ok())
}

pub(super) fn sqlite_value_to_json(value: ValueRef<'_>, name: &str) -> Value {
  match value {
    ValueRef::Null => Value::Null,
    ValueRef::Integer(value) => Value::from(value),
    ValueRef::Real(value) => serde_json::Number::from_f64(value)
      .map(Value::Number)
      .unwrap_or(Value::Null),
    ValueRef::Text(value) => decode_bytes(value, name),
    ValueRef::Blob(value) => decode_bytes(value, name),
  }
}

fn decode_bytes(value: &[u8], name: &str) -> Value {
  match std::str::from_utf8(value) {
    Ok(value) if JSON_COLUMNS.contains(&name) => {
      serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()))
    }
    Ok(value) => Value::String(value.to_string()),
    Err(_) => base64_json(value),
  }
}

fn base64_json(value: &[u8]) -> Value {
  serde_json::json!({
    "encoding": "base64",
    "data": base64::engine::general_purpose::STANDARD.encode(value),
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn decodes_json_blobs_only_for_json_columns() {
    let json = br#"{"route":"default"}"#;

    assert_eq!(
      sqlite_value_to_json(ValueRef::Blob(json), "ctx_json"),
      serde_json::json!({"route": "default"})
    );
    assert_eq!(
      sqlite_value_to_json(ValueRef::Blob(json), "non_json_column"),
      Value::String("{\"route\":\"default\"}".to_string())
    );
    assert_eq!(
      sqlite_value_to_json(ValueRef::Blob(b"plain value"), "ctx_json"),
      Value::String("plain value".to_string())
    );
  }
}
