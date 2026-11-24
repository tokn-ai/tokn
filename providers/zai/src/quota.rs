//! Z.ai / Zhipu coding-plan quota lookup.
//!
//! Hits the (community-reverse-engineered) monitor endpoint
//! `GET /api/monitor/usage/quota/limit` with header
//! `Authorization: <api_key>` (note: NO `Bearer ` prefix — confirmed against
//! the official z.ai dashboard XHR).
//!
//! Endpoint host depends on the operator-chosen provider id:
//!   - `zai-coding-plan` / `zai`         -> https://api.z.ai
//!   - `zhipuai-coding-plan` / `zhipuai` -> https://open.bigmodel.cn
//!
//! Response is undocumented; we tolerate missing/extra fields. A failure here
//! is informational only — the caller should render `"quota unavailable"`
//! rather than aborting the whole `account list`.

use crate::provider::{error, Result};
use crate::util::redact::token_fingerprint;
use serde::Deserialize;
use snafu::ResultExt;
use std::time::Duration;
use tracing::{debug, instrument};

const PATH: &str = "/api/monitor/usage/quota/limit";

/// Resolve the monitor-API host for a given provider id. Defaults to the
/// public z.ai host so unknown aliases still produce a meaningful attempt.
pub fn host_for(provider_id: &str) -> &'static str {
  match provider_id {
    "zhipuai" | "zhipuai-coding-plan" => "https://open.bigmodel.cn",
    _ => "https://api.z.ai",
  }
}

#[derive(Debug, Clone)]
pub struct ZaiQuota {
  pub level: Option<String>,
  pub five_hour: Option<TokenBucket>,
  pub weekly: Option<TokenBucket>,
  pub mcp_monthly: Option<McpBucket>,
}

#[derive(Debug, Clone)]
pub struct TokenBucket {
  pub percent_used: f64,
  pub total: Option<u64>,
  /// Reset time as unix epoch milliseconds, when present.
  pub next_reset_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct McpBucket {
  pub percent_used: f64,
  pub used: u64,
  pub total: u64,
  pub next_reset_ms: Option<i64>,
}

#[derive(Deserialize)]
struct Envelope {
  #[serde(default)]
  data: Option<RawData>,
  // Some deployments unwrap; we also try parsing as RawData directly below.
}

#[derive(Deserialize, Default)]
struct RawData {
  #[serde(default)]
  level: Option<String>,
  #[serde(default)]
  limits: Vec<RawLimit>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawLimit {
  Token {
    #[serde(rename = "type")]
    kind: String, // expected "TOKENS_LIMIT"
    unit: u32,
    number: u32,
    #[serde(default)]
    percentage: Option<f64>,
    #[serde(default)]
    total: Option<u64>,
    #[serde(default, rename = "nextResetTime")]
    next_reset_time: Option<i64>,
  },
  Time {
    #[serde(rename = "type")]
    kind: String, // expected "TIME_LIMIT"
    #[serde(default)]
    percentage: Option<f64>,
    #[serde(default, rename = "currentValue")]
    current_value: Option<u64>,
    /// Wire field is `usage`; semantically it is the cap.
    #[serde(default)]
    usage: Option<u64>,
    #[serde(default, rename = "nextResetTime")]
    next_reset_time: Option<i64>,
  },
  /// Tolerate unknown / future shapes without failing the whole parse.
  #[allow(dead_code)]
  Unknown(serde_json::Value),
}

/// Fetch and classify the quota response. Returns an error if the request
/// itself failed; a successful 200 with no recognisable buckets yields a
/// `ZaiQuota` with all fields `None`.
#[instrument(
  name = "zai_quota",
  skip_all,
  fields(
    provider = %provider_id,
    key_fp = %token_fingerprint(api_key),
    status = tracing::field::Empty,
  ),
)]
pub async fn fetch(http: &reqwest::Client, provider_id: &str, api_key: &str) -> Result<ZaiQuota> {
  let url = format!("{}{PATH}", host_for(provider_id));
  debug!(%url, "GET zai quota");
  let resp = http
    .get(&url)
    .timeout(Duration::from_secs(5))
    .header("Authorization", api_key) // raw token, NO `Bearer`
    .header("Accept-Language", "en-US,en")
    .header("Content-Type", "application/json")
    .send()
    .await
    .context(error::HttpSnafu { what: "zai quota" })?;

  let status = resp.status();
  tracing::Span::current().record("status", status.as_u16());
  let body = resp.text().await.unwrap_or_default();
  if !status.is_success() {
    return error::HttpStatusSnafu {
      what: "zai quota",
      status,
      body,
    }
    .fail();
  }

  // Most deployments wrap as `{ data: {...} }`; some return bare data.
  let raw: RawData = match serde_json::from_str::<Envelope>(&body) {
    Ok(env) => env.data.unwrap_or_default(),
    Err(_) => serde_json::from_str::<RawData>(&body).context(error::JsonSnafu {
      what: "zai quota",
      body: body.clone(),
    })?,
  };

  Ok(classify(raw))
}

fn classify(raw: RawData) -> ZaiQuota {
  let mut q = ZaiQuota {
    level: raw.level,
    five_hour: None,
    weekly: None,
    mcp_monthly: None,
  };

  for lim in raw.limits {
    match lim {
      RawLimit::Token {
        kind,
        unit,
        number,
        percentage,
        total,
        next_reset_time,
      } if kind == "TOKENS_LIMIT" => {
        let bucket = TokenBucket {
          percent_used: percentage.unwrap_or(0.0),
          total,
          next_reset_ms: next_reset_time,
        };
        match (unit, number) {
          // unit=3 (Hour), number=5 -> rolling 5-hour window
          (3, 5) => q.five_hour = Some(bucket),
          // unit=6 (Week), number=1 -> weekly window
          (6, 1) => q.weekly = Some(bucket),
          _ => {}
        }
      }
      RawLimit::Time {
        kind,
        percentage,
        current_value,
        usage,
        next_reset_time,
      } if kind == "TIME_LIMIT" => {
        q.mcp_monthly = Some(McpBucket {
          percent_used: percentage.unwrap_or(0.0),
          used: current_value.unwrap_or(0),
          total: usage.unwrap_or(0),
          next_reset_ms: next_reset_time,
        });
      }
      _ => {}
    }
  }

  q
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn host_routing() {
    assert_eq!(host_for("zai-coding-plan"), "https://api.z.ai");
    assert_eq!(host_for("zai"), "https://api.z.ai");
    assert_eq!(host_for("zhipuai"), "https://open.bigmodel.cn");
    assert_eq!(host_for("zhipuai-coding-plan"), "https://open.bigmodel.cn");
    // Unknown providers fall back to z.ai rather than panicking.
    assert_eq!(host_for("totally-new-alias"), "https://api.z.ai");
  }

  #[test]
  fn classify_full_payload() {
    let body = r#"{
          "data": {
            "level": "PRO",
            "limits": [
              {"type":"TOKENS_LIMIT","unit":3,"number":5,"percentage":18.5,"total":6000000,"nextResetTime":1735000000000},
              {"type":"TOKENS_LIMIT","unit":6,"number":1,"percentage":47.2,"total":80000000,"nextResetTime":1735500000000},
              {"type":"TIME_LIMIT","percentage":4.0,"currentValue":12,"usage":300,"nextResetTime":1736000000000}
            ]
          }
        }"#;
    let env: Envelope = serde_json::from_str(body).unwrap();
    let q = classify(env.data.unwrap());
    assert_eq!(q.level.as_deref(), Some("PRO"));
    let h = q.five_hour.unwrap();
    assert!((h.percent_used - 18.5).abs() < 1e-9);
    assert_eq!(h.total, Some(6_000_000));
    let w = q.weekly.unwrap();
    assert_eq!(w.total, Some(80_000_000));
    let m = q.mcp_monthly.unwrap();
    assert_eq!(m.used, 12);
    assert_eq!(m.total, 300);
  }

  #[test]
  fn unknown_buckets_are_ignored() {
    let body = r#"{"data":{"limits":[
          {"type":"TOKENS_LIMIT","unit":99,"number":42,"percentage":1.0},
          {"type":"NEW_FUTURE_KIND","foo":"bar"}
        ]}}"#;
    let env: Envelope = serde_json::from_str(body).unwrap();
    let q = classify(env.data.unwrap());
    assert!(q.five_hour.is_none() && q.weekly.is_none() && q.mcp_monthly.is_none());
  }
}
