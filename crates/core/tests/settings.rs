use tokn_core::settings::{Datetime, Table, Value};

#[test]
fn credential_settings_preserve_toml_value_kinds() {
  let datetime: Datetime = "2026-10-21T13:45:07Z".parse().unwrap();
  let settings = Table::from_iter([
    ("name".into(), Value::String("in-memory".into())),
    ("limit".into(), Value::Integer(i64::MAX)),
    ("ratio".into(), Value::Float(0.75)),
    ("enabled".into(), Value::Boolean(true)),
    ("expires".into(), Value::Datetime(datetime)),
    ("models".into(), Value::Array(vec![Value::String("model-a".into())])),
    (
      "nested".into(),
      Value::Table(Table::from_iter([("count".into(), Value::Integer(-1))])),
    ),
  ]);
  let encoded = serde_json::to_vec(&settings).unwrap();
  let decoded: Table = serde_json::from_slice(&encoded).unwrap();
  assert_eq!(decoded, settings);
  assert_eq!(decoded["limit"].as_integer(), Some(i64::MAX));
  assert_eq!(decoded["expires"].as_datetime(), Some(&datetime));
  assert_eq!(decoded["nested"].as_table().unwrap()["count"].as_integer(), Some(-1));
}

#[test]
fn settings_reject_json_values_that_toml_cannot_represent() {
  assert!(serde_json::from_str::<Value>("null").is_err());
  assert!(serde_json::from_str::<Value>("18446744073709551615").is_err());
}

#[cfg(feature = "std")]
#[test]
fn standard_settings_retain_upstream_type_identity() {
  fn table(value: Table) -> toml::Table {
    value
  }
  fn value(value: Value) -> toml::Value {
    value
  }
  assert!(table(Table::new()).is_empty());
  assert_eq!(value(Value::Boolean(true)), toml::Value::Boolean(true));
}
