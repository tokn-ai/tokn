use alloc::{
  collections::BTreeMap,
  string::{String, ToString},
  vec::Vec,
};
use core::fmt;
use serde::{
  de::{self, MapAccess, SeqAccess, Visitor},
  Deserialize, Deserializer, Serialize, Serializer,
};
use toml_datetime::{de::VisitMap, Datetime};

pub type Table = BTreeMap<String, Value>;

/// An owned TOML-compatible provider setting without atomic ownership.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
  String(String),
  Integer(i64),
  Float(f64),
  Boolean(bool),
  Datetime(Datetime),
  Array(Vec<Value>),
  Table(Table),
}

impl Value {
  pub fn as_str(&self) -> Option<&str> {
    if let Self::String(value) = self {
      Some(value)
    } else {
      None
    }
  }
  pub fn as_integer(&self) -> Option<i64> {
    if let Self::Integer(value) = self {
      Some(*value)
    } else {
      None
    }
  }
  pub fn as_float(&self) -> Option<f64> {
    if let Self::Float(value) = self {
      Some(*value)
    } else {
      None
    }
  }
  pub fn as_bool(&self) -> Option<bool> {
    if let Self::Boolean(value) = self {
      Some(*value)
    } else {
      None
    }
  }
  pub fn as_datetime(&self) -> Option<&Datetime> {
    if let Self::Datetime(value) = self {
      Some(value)
    } else {
      None
    }
  }
  pub fn as_array(&self) -> Option<&Vec<Value>> {
    if let Self::Array(value) = self {
      Some(value)
    } else {
      None
    }
  }
  pub fn as_array_mut(&mut self) -> Option<&mut Vec<Value>> {
    if let Self::Array(value) = self {
      Some(value)
    } else {
      None
    }
  }
  pub fn as_table(&self) -> Option<&Table> {
    if let Self::Table(value) = self {
      Some(value)
    } else {
      None
    }
  }
  pub fn as_table_mut(&mut self) -> Option<&mut Table> {
    if let Self::Table(value) = self {
      Some(value)
    } else {
      None
    }
  }
  pub fn get(&self, key: &str) -> Option<&Value> {
    self.as_table()?.get(key)
  }
}

impl From<String> for Value {
  fn from(value: String) -> Self {
    Self::String(value)
  }
}
impl From<&str> for Value {
  fn from(value: &str) -> Self {
    Self::String(value.to_string())
  }
}
impl From<i64> for Value {
  fn from(value: i64) -> Self {
    Self::Integer(value)
  }
}
impl From<f64> for Value {
  fn from(value: f64) -> Self {
    Self::Float(value)
  }
}
impl From<bool> for Value {
  fn from(value: bool) -> Self {
    Self::Boolean(value)
  }
}
impl From<Datetime> for Value {
  fn from(value: Datetime) -> Self {
    Self::Datetime(value)
  }
}
impl From<Vec<Value>> for Value {
  fn from(value: Vec<Value>) -> Self {
    Self::Array(value)
  }
}
impl From<Table> for Value {
  fn from(value: Table) -> Self {
    Self::Table(value)
  }
}

impl Serialize for Value {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    match self {
      Self::String(value) => value.serialize(serializer),
      Self::Integer(value) => value.serialize(serializer),
      Self::Float(value) => value.serialize(serializer),
      Self::Boolean(value) => value.serialize(serializer),
      Self::Datetime(value) => value.serialize(serializer),
      Self::Array(value) => value.serialize(serializer),
      Self::Table(value) => value.serialize(serializer),
    }
  }
}

impl<'de> Deserialize<'de> for Value {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    deserializer.deserialize_any(ValueVisitor)
  }
}

struct ValueVisitor;
impl<'de> Visitor<'de> for ValueVisitor {
  type Value = Value;
  fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("a TOML-compatible setting")
  }
  fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
    Ok(Value::Boolean(value))
  }
  fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
    Ok(Value::Integer(value))
  }
  fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
    i64::try_from(value)
      .map(Value::Integer)
      .map_err(|_| E::custom("integer is outside the TOML signed 64-bit range"))
  }
  fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
    Ok(Value::Float(value))
  }
  fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
    Ok(Value::String(value.to_string()))
  }
  fn visit_string<E: de::Error>(self, value: String) -> Result<Value, E> {
    Ok(Value::String(value))
  }
  fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
    let mut values = Vec::new();
    while let Some(value) = access.next_element()? {
      values.push(value);
    }
    Ok(Value::Array(values))
  }
  fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
    let mut values = Table::new();
    match VisitMap::next_key_seed(&mut access)? {
      Some(VisitMap::Datetime(value)) => return Ok(Value::Datetime(value)),
      Some(VisitMap::Key(key)) => {
        values.insert(key.into_owned(), access.next_value()?);
      }
      None => return Ok(Value::Table(values)),
    }
    while let Some((key, value)) = access.next_entry()? {
      values.insert(key, value);
    }
    Ok(Value::Table(values))
  }
}
