use crate::v2::{
  CompileError, OutboundPlan, PersistencePlan, RawOutbound, RawPersistence, RawRequestLimits, RawService,
  RequestLimitsPlan, ServiceSettings,
};
use std::collections::BTreeSet;

pub(super) fn compile_service(raw: &RawService) -> Result<ServiceSettings, CompileError> {
  Ok(ServiceSettings::new(
    (&raw.logging).into(),
    compile_outbound(&raw.outbound)?,
    compile_request_limits(&raw.request_limits)?,
    compile_persistence(&raw.persistence)?,
    super::super::ModelRefreshPlan::compile(&raw.models)?,
  ))
}

fn compile_outbound(raw: &RawOutbound) -> Result<OutboundPlan, CompileError> {
  if raw.proxy_url.is_some() && raw.use_system_proxy {
    return Err(invalid_value(
      "service.outbound",
      "proxy_url and use_system_proxy cannot be enabled together",
    ));
  }

  let proxy_url = raw.proxy_url.as_deref().map(compile_proxy_url).transpose()?;
  let no_proxy = normalize_no_proxy(&raw.no_proxy);
  if proxy_url.is_none() && !no_proxy.is_empty() {
    return Err(invalid_value(
      "service.outbound.no_proxy",
      "no_proxy requires an explicit proxy_url",
    ));
  }

  Ok(OutboundPlan::new(
    proxy_url,
    no_proxy.into_boxed_slice(),
    raw.use_system_proxy,
  ))
}

fn compile_proxy_url(value: &str) -> Result<String, CompileError> {
  if value.trim() != value {
    return Err(invalid_value(
      "service.outbound.proxy_url",
      "proxy URL must not have surrounding whitespace",
    ));
  }
  let parsed = reqwest::Url::parse(value)
    .map_err(|source| invalid_value("service.outbound.proxy_url", format!("invalid proxy URL: {source}")))?;
  if !matches!(parsed.scheme(), "http" | "https" | "socks5" | "socks5h") {
    return Err(invalid_value(
      "service.outbound.proxy_url",
      format!("unsupported proxy URL scheme `{}`", parsed.scheme()),
    ));
  }
  if parsed.host_str().is_none()
    || parsed.cannot_be_a_base()
    || !matches!(parsed.path(), "" | "/")
    || parsed.query().is_some()
    || parsed.fragment().is_some()
    || parsed.port() == Some(0)
  {
    return Err(invalid_value(
      "service.outbound.proxy_url",
      "proxy URL must contain only a scheme, authority, and optional credentials",
    ));
  }
  Ok(parsed.to_string())
}

fn normalize_no_proxy(values: &[String]) -> Vec<String> {
  let mut seen = BTreeSet::new();
  values
    .iter()
    .map(|value| value.trim())
    .filter(|value| !value.is_empty())
    .filter(|value| seen.insert((*value).to_string()))
    .map(str::to_string)
    .collect()
}

fn compile_request_limits(raw: &RawRequestLimits) -> Result<RequestLimitsPlan, CompileError> {
  let max_wire_bytes = compile_limit("service.request_limits.max_wire_bytes", raw.max_wire_bytes)?;
  let max_decoded_bytes = compile_limit("service.request_limits.max_decoded_bytes", raw.max_decoded_bytes)?;
  Ok(RequestLimitsPlan::new(max_wire_bytes, max_decoded_bytes))
}

fn compile_limit(location: &'static str, value: Option<u64>) -> Result<usize, CompileError> {
  let Some(value) = value else {
    return Ok(usize::MAX);
  };
  if value == 0 {
    return Err(invalid_value(location, "must be greater than zero"));
  }
  compile_usize(location, value)
}

fn compile_persistence(raw: &RawPersistence) -> Result<PersistencePlan, CompileError> {
  let body_max_bytes = compile_usize("service.persistence.body_max_bytes", raw.body_max_bytes)?;
  let write_queue_capacity =
    compile_usize("service.persistence.write_queue_capacity", raw.write_queue_capacity)?.max(256);
  let archive_after_days = compile_days("service.persistence.archive_after_days", raw.archive_after_days)?;
  let prune_after_days = compile_days("service.persistence.prune_after_days", raw.prune_after_days)?;
  if prune_after_days <= archive_after_days {
    return Err(invalid_value(
      "service.persistence.prune_after_days",
      "must be greater than service.persistence.archive_after_days",
    ));
  }
  Ok(PersistencePlan::new(
    raw.enabled,
    raw.usage_db_path.clone(),
    raw.sessions_db_path.clone(),
    raw.requests_dir.clone(),
    raw.record_sessions,
    raw.record_request_bodies,
    body_max_bytes,
    write_queue_capacity,
    raw.archive_extension.clone(),
    archive_after_days,
    prune_after_days,
  ))
}

fn compile_days(location: &'static str, value: u64) -> Result<i64, CompileError> {
  if value == 0 {
    return Err(invalid_value(location, "must be greater than zero"));
  }
  let value = i64::try_from(value).map_err(|_| invalid_value(location, "is too large"))?;
  value
    .checked_mul(86_400)
    .ok_or_else(|| invalid_value(location, "is too large for a time duration"))?;
  Ok(value)
}

fn compile_usize(location: &'static str, value: u64) -> Result<usize, CompileError> {
  usize::try_from(value).map_err(|_| invalid_value(location, "does not fit this platform's address space"))
}

fn invalid_value(location: impl Into<String>, message: impl Into<String>) -> CompileError {
  CompileError::InvalidValue {
    location: location.into(),
    message: message.into(),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn omitted_request_limits_are_unbounded_and_remain_absent_in_toml() {
    let raw = RawRequestLimits::default();
    assert_eq!(raw.max_wire_bytes, None);
    assert_eq!(raw.max_decoded_bytes, None);
    let limits = compile_request_limits(&raw).unwrap();
    assert_eq!(limits.max_wire_bytes(), usize::MAX);
    assert_eq!(limits.max_decoded_bytes(), usize::MAX);
    assert_eq!(limits, RequestLimitsPlan::default());
    let serialized = toml::to_string(&raw).unwrap();
    assert!(!serialized.contains("max_wire_bytes"));
    assert!(!serialized.contains("max_decoded_bytes"));
    assert_eq!(toml::from_str::<RawRequestLimits>(&serialized).unwrap(), raw);
    assert_eq!(RawPersistence::default().body_max_bytes, 10 * 1024 * 1024);
  }

  #[test]
  fn explicit_request_limits_are_independent_and_must_be_positive() {
    for field in ["max_wire_bytes", "max_decoded_bytes"] {
      let raw: RawRequestLimits = toml::from_str(&format!("{field} = 4096")).unwrap();
      let limits = compile_request_limits(&raw).unwrap();
      let expected_wire = if field == "max_wire_bytes" { 4096 } else { usize::MAX };
      let expected_decoded = if field == "max_decoded_bytes" { 4096 } else { usize::MAX };
      assert_eq!(limits.max_wire_bytes(), expected_wire);
      assert_eq!(limits.max_decoded_bytes(), expected_decoded);
      let serialized = toml::to_string(&raw).unwrap();
      assert!(serialized.contains(&format!("{field} = 4096")));
      assert_eq!(toml::from_str::<RawRequestLimits>(&serialized).unwrap(), raw);

      let raw: RawRequestLimits = toml::from_str(&format!("{field} = 0")).unwrap();
      assert!(matches!(
        compile_request_limits(&raw),
        Err(CompileError::InvalidValue { location, .. })
          if location == format!("service.request_limits.{field}")
      ));
    }
  }

  #[cfg(target_pointer_width = "32")]
  #[test]
  fn service_sizes_reject_platform_usize_overflow() {
    assert!(matches!(
      compile_usize("service.persistence.body_max_bytes", u64::MAX),
      Err(CompileError::InvalidValue { location, .. })
        if location == "service.persistence.body_max_bytes"
    ));
    for field in [
      "service.request_limits.max_wire_bytes",
      "service.request_limits.max_decoded_bytes",
    ] {
      assert_eq!(compile_limit(field, None).unwrap(), usize::MAX);
      assert!(matches!(
        compile_limit(field, Some(u64::MAX)),
        Err(CompileError::InvalidValue { location, .. }) if location == field
      ));
    }
  }
}
