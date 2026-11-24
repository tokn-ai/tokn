use rusqlite::params;
use serde_json::json;

use crate::requests::open_day_db;

use super::super::{get_request_llm_message, get_request_llm_summary, get_request_llm_tool_definition};
use super::support::{tempdir, write_request};

#[test]
fn indexes_all_messages_and_tool_definitions_with_lazy_details() {
  let dir = tempdir();
  write_request(
    &dir,
    "2026-07-14",
    "request-llm-summary",
    1_784_444_800_000,
    Some("session-1"),
    Some("openai"),
  );
  let input = (0..8)
    .map(|index| json!({"role": "user", "content": [{"type": "input_text", "text": format!("message {index}")}]}))
    .chain([
      json!({
        "type": "additional_tools",
        "role": "developer",
        "tools": [{"type": "function", "name": "browser", "description": "Open a page", "parameters": {"type": "object"}}]
      }),
      json!({"type": "function_call", "name": "lookup", "call_id": "call-1", "arguments": "{\"id\":1}"}),
      json!({"type": "function_call_output", "call_id": "call-1", "output": "result"}),
      json!({"type": "reasoning", "encrypted_content": "opaque"}),
      json!({"type": "compaction", "encrypted_content": "summary"}),
    ])
    .collect::<Vec<_>>();
  let request_body = serde_json::to_vec(&json!({
    "input": input,
    "tools": [
      {"type": "function", "function": {"name": "lookup", "description": "Find a record", "parameters": {"type": "object"}}},
      {"type": "custom", "name": "shell"}
    ]
  }))
  .unwrap();
  let compressed = zstd::stream::encode_all(request_body.as_slice(), 0).unwrap();
  let response = [
    "event: response.output_item.done",
    r#"data: {"type":"response.output_item.done","item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"finished"}]}}"#,
    "",
    "event: response.output_item.done",
    r#"data: {"type":"response.output_item.done","item":{"type":"custom_tool_call","name":"shell","input":"echo ok","status":"completed"}}"#,
    "",
  ]
  .join("\n");
  let conn = open_day_db(&dir.join("2026-07-14.db")).unwrap();
  conn
    .execute(
      "UPDATE request_downstream
       SET inbound_req_headers = ?2, inbound_req_body = ?3, inbound_resp_body = ?4
       WHERE request_id = ?1",
      params![
        "request-llm-summary",
        br#"{"content-encoding":"zstd","content-type":"application/json"}"#,
        compressed,
        response.as_bytes()
      ],
    )
    .unwrap();

  let summary = get_request_llm_summary(&dir, "2026-07-14", "request-llm-summary", None)
    .unwrap()
    .unwrap();
  assert_eq!(summary.messages.len(), 14);
  assert_eq!(summary.messages.first().unwrap().index, 0);
  assert_eq!(summary.messages.first().unwrap().preview.as_deref(), Some("message 0"));
  assert_eq!(summary.messages[8].role, "assistant");
  assert_eq!(summary.messages[8].kind, "function_call");
  assert_eq!(summary.messages[8].name.as_deref(), Some("lookup"));
  assert_eq!(summary.messages[8].call_id.as_deref(), Some("call-1"));
  assert_eq!(summary.messages[8].preview.as_deref(), Some("{\"id\":1}"));
  assert_eq!(summary.messages[9].role, "tool");
  assert_eq!(summary.messages[9].name.as_deref(), Some("lookup"));
  assert_eq!(summary.messages[9].preview.as_deref(), Some("result"));
  assert_eq!(summary.messages[10].kind, "reasoning");
  assert_eq!(summary.messages[11].kind, "compaction");
  assert_eq!(summary.messages[12].preview.as_deref(), Some("finished"));
  assert_eq!(summary.messages.last().unwrap().name.as_deref(), Some("shell"));
  assert_eq!(summary.messages.last().unwrap().kind, "custom_tool_call");
  assert_eq!(summary.messages.last().unwrap().phase, "output");
  assert_eq!(summary.messages.last().unwrap().index, 13);
  assert_eq!(summary.tool_definitions.len(), 3);
  assert_eq!(summary.tool_definitions[0].name, "lookup");
  assert_eq!(
    summary.tool_definitions[0].description.as_deref(),
    Some("Find a record")
  );
  assert!(summary.tool_definitions[0].schema_bytes > 0);
  assert_eq!(summary.tool_definitions[1].name, "shell");
  assert_eq!(summary.tool_definitions[2].name, "browser");
  assert!(summary.warning.is_none());

  let message = get_request_llm_message(&dir, "2026-07-14", "request-llm-summary", None, 9)
    .unwrap()
    .unwrap();
  assert_eq!(message.index, 9);
  assert_eq!(message.value["type"], "function_call_output");
  assert_eq!(message.value["output"], "result");

  let definition = get_request_llm_tool_definition(&dir, "2026-07-14", "request-llm-summary", None, 0)
    .unwrap()
    .unwrap();
  assert_eq!(definition.index, 0);
  assert_eq!(definition.value["function"]["name"], "lookup");
  assert!(
    get_request_llm_message(&dir, "2026-07-14", "request-llm-summary", None, 14)
      .unwrap()
      .is_none()
  );
}

#[test]
fn returns_a_warning_instead_of_failing_for_unsupported_content_encoding() {
  let dir = tempdir();
  write_request(
    &dir,
    "2026-07-14",
    "request-unsupported-encoding",
    1_784_444_800_000,
    None,
    Some("openai"),
  );
  let conn = open_day_db(&dir.join("2026-07-14.db")).unwrap();
  conn
    .execute(
      "UPDATE request_downstream
       SET inbound_req_headers = ?2, inbound_req_body = ?3
       WHERE request_id = ?1",
      params![
        "request-unsupported-encoding",
        br#"{"content-encoding":"br"}"#,
        b"opaque"
      ],
    )
    .unwrap();

  let summary = get_request_llm_summary(&dir, "2026-07-14", "request-unsupported-encoding", None)
    .unwrap()
    .unwrap();
  assert!(summary.messages.is_empty());
  assert!(summary.warning.unwrap().contains("unsupported content encoding br"));
}
