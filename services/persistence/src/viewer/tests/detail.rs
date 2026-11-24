use rusqlite::params;

use crate::requests::open_day_db;

use super::super::{get_request, get_request_payload, RequestPayloadField};
use super::support::{tempdir, write_request};

const PAYLOAD_FIELDS: &[&str] = &[
  "inbound_req_headers",
  "inbound_req_body",
  "inbound_resp_headers",
  "inbound_resp_body",
  "outbound_req_headers",
  "outbound_req_body",
  "outbound_resp_headers",
  "outbound_resp_body",
];

#[test]
fn request_overview_omits_payloads_and_payloads_are_loaded_separately() {
  let dir = tempdir();
  write_request(
    &dir,
    "2026-07-14",
    "request-detail",
    1_784_444_800_000,
    Some("session-1"),
    Some("openai"),
  );
  let conn = open_day_db(&dir.join("2026-07-14.db")).unwrap();
  conn
    .execute(
      "INSERT INTO request_upstream (request_id, outbound_resp_body) VALUES (?1, ?2)",
      params!["request-detail", &[0xff_u8, 0x00]],
    )
    .unwrap();

  let detail = get_request(&dir, "2026-07-14", "request-detail", None)
    .unwrap()
    .unwrap();
  assert_eq!(detail.request["ctx_json"], serde_json::json!({"route": "default"}));
  assert_eq!(detail.request["params_json"], serde_json::json!({"stream": false}));
  for field in PAYLOAD_FIELDS {
    assert!(!detail.request.contains_key(*field));
  }

  let inbound_body = get_request_payload(
    &dir,
    "2026-07-14",
    "request-detail",
    None,
    RequestPayloadField::InboundReqBody,
  )
  .unwrap()
  .unwrap();
  assert_eq!(inbound_body.field, "inbound_req_body");
  assert_eq!(inbound_body.value, serde_json::json!({"input": "hello"}));

  let binary_body = get_request_payload(
    &dir,
    "2026-07-14",
    "request-detail",
    None,
    RequestPayloadField::OutboundRespBody,
  )
  .unwrap()
  .unwrap();
  assert_eq!(
    binary_body.value,
    serde_json::json!({"encoding": "base64", "data": "/wA="})
  );
  assert!("endpoint".parse::<RequestPayloadField>().is_err());
  assert!(get_request(&dir, "2026-07-14", "missing", None).unwrap().is_none());
  assert!(get_request(&dir, "../../outside", "request-detail", None)
    .unwrap()
    .is_none());
}
