use flate2::read::GzDecoder;
use rusqlite::types::ValueRef;
use rusqlite::{params_from_iter, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use super::detail::{quote_identifier, request_lookup_condition};
use crate::viewer::database::open_readonly;
use crate::viewer::days::request_day_files;
use crate::viewer::schema::RequestSchema;
use crate::Result;

const MAX_DECODED_BODY_BYTES: u64 = 8 * 1024 * 1024;
const MAX_PREVIEW_CHARS: usize = 280;
const MAX_DESCRIPTION_CHARS: usize = 240;
const MAX_LABEL_CHARS: usize = 120;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LlmMessageSummary {
  pub index: usize,
  pub role: String,
  pub phase: String,
  pub kind: String,
  pub name: Option<String>,
  pub call_id: Option<String>,
  pub preview: Option<String>,
  pub truncated: bool,
  pub content_bytes: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LlmToolDefinitionSummary {
  pub index: usize,
  pub name: String,
  pub kind: String,
  pub description: Option<String>,
  pub truncated: bool,
  pub schema_bytes: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LlmRequestContentSummary {
  pub messages: Vec<LlmMessageSummary>,
  pub tool_definitions: Vec<LlmToolDefinitionSummary>,
  pub warning: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LlmItemDetail {
  pub index: usize,
  pub value: Value,
}

struct CollectedMessage {
  summary: LlmMessageSummary,
  value: Value,
}

struct MessageMetadata<'a> {
  role: &'a str,
  phase: &'a str,
  kind: &'a str,
  name: Option<&'a str>,
  call_id: Option<&'a str>,
}

struct CollectedToolDefinition {
  summary: LlmToolDefinitionSummary,
  value: Value,
}

#[derive(Default)]
struct CollectedContent {
  messages: Vec<CollectedMessage>,
  tool_definitions: Vec<CollectedToolDefinition>,
  tool_names_by_call_id: HashMap<String, String>,
  warnings: Vec<String>,
}

impl CollectedContent {
  fn warning(&mut self, context: &str, error: impl std::fmt::Display) {
    self.warnings.push(format!("{context}: {error}"));
  }

  fn into_summary(self) -> LlmRequestContentSummary {
    LlmRequestContentSummary {
      messages: self.messages.into_iter().map(|message| message.summary).collect(),
      tool_definitions: self
        .tool_definitions
        .into_iter()
        .map(|definition| definition.summary)
        .collect(),
      warning: (!self.warnings.is_empty()).then(|| self.warnings.join("; ")),
    }
  }
}

/// Return the complete semantic index for an LLM request. The response is
/// limited to short previews and metadata; complete message and definition
/// values are available through the item detail queries.
pub fn get_request_llm_summary(
  requests_dir: &Path,
  day: &str,
  request_id: &str,
  row_id: Option<i64>,
) -> Result<Option<LlmRequestContentSummary>> {
  Ok(load_request_content(requests_dir, day, request_id, row_id)?.map(CollectedContent::into_summary))
}

pub fn get_request_llm_message(
  requests_dir: &Path,
  day: &str,
  request_id: &str,
  row_id: Option<i64>,
  message_index: usize,
) -> Result<Option<LlmItemDetail>> {
  let Some(content) = load_request_content(requests_dir, day, request_id, row_id)? else {
    return Ok(None);
  };
  Ok(
    content
      .messages
      .into_iter()
      .nth(message_index)
      .map(|message| LlmItemDetail {
        index: message_index,
        value: message.value,
      }),
  )
}

pub fn get_request_llm_tool_definition(
  requests_dir: &Path,
  day: &str,
  request_id: &str,
  row_id: Option<i64>,
  tool_index: usize,
) -> Result<Option<LlmItemDetail>> {
  let Some(content) = load_request_content(requests_dir, day, request_id, row_id)? else {
    return Ok(None);
  };
  Ok(
    content
      .tool_definitions
      .into_iter()
      .nth(tool_index)
      .map(|definition| LlmItemDetail {
        index: tool_index,
        value: definition.value,
      }),
  )
}

fn load_request_content(
  requests_dir: &Path,
  day: &str,
  request_id: &str,
  row_id: Option<i64>,
) -> Result<Option<CollectedContent>> {
  let Some(day_file) = request_day_files(requests_dir)?
    .into_iter()
    .find(|file| file.day == day)
  else {
    return Ok(None);
  };
  let Some(conn) = open_readonly(&day_file.path)? else {
    return Ok(None);
  };

  let schema = RequestSchema::read(&conn)?;
  let request_condition = request_lookup_condition(schema, row_id.is_some());
  let fields = [
    "inbound_req_headers",
    "inbound_req_body",
    "outbound_req_headers",
    "outbound_req_body",
    "inbound_resp_body",
    "outbound_resp_body",
  ];
  let projection = fields.map(quote_identifier).join(", ");
  let mut values = vec![rusqlite::types::Value::Text(request_id.to_string())];
  if let Some(row_id) = row_id {
    values.push(rusqlite::types::Value::Integer(row_id));
  }
  let mut statement = conn.prepare(&format!(
    "SELECT {projection} FROM requests WHERE {request_condition} LIMIT 1"
  ))?;
  let row = statement
    .query_row(params_from_iter(values.iter()), |row| {
      Ok([
        sql_bytes(row.get_ref(0)?),
        sql_bytes(row.get_ref(1)?),
        sql_bytes(row.get_ref(2)?),
        sql_bytes(row.get_ref(3)?),
        sql_bytes(row.get_ref(4)?),
        sql_bytes(row.get_ref(5)?),
      ])
    })
    .optional()?;
  let Some([inbound_headers, inbound_request, outbound_headers, outbound_request, inbound_response, outbound_response]) =
    row
  else {
    return Ok(None);
  };

  let (request_headers, request_body) = if !inbound_request.is_empty() {
    (&inbound_headers, &inbound_request)
  } else {
    (&outbound_headers, &outbound_request)
  };
  let response_body = if !inbound_response.is_empty() {
    &inbound_response
  } else {
    &outbound_response
  };

  let mut content = CollectedContent::default();
  if !request_body.is_empty() {
    match decode_json_request(request_headers, request_body) {
      Ok(request) => collect_request(&request, &mut content),
      Err(error) => content.warning("request summary unavailable", error),
    }
  }
  if !response_body.is_empty() {
    collect_response(response_body, &mut content);
  }
  Ok(Some(content))
}

fn sql_bytes(value: ValueRef<'_>) -> Vec<u8> {
  match value {
    ValueRef::Text(value) | ValueRef::Blob(value) => value.to_vec(),
    _ => Vec::new(),
  }
}

fn decode_json_request(headers: &[u8], body: &[u8]) -> std::result::Result<Value, String> {
  let headers = serde_json::from_slice::<Value>(headers).unwrap_or(Value::Null);
  let encoding = header_str(&headers, "content-encoding").unwrap_or("identity");
  let decoded = match encoding.trim().to_ascii_lowercase().as_str() {
    "" | "identity" => bounded_read(body)?,
    "gzip" => bounded_read(GzDecoder::new(body))?,
    "zstd" => {
      let decoder = zstd::stream::read::Decoder::new(body).map_err(|error| error.to_string())?;
      bounded_read(decoder)?
    }
    other => return Err(format!("unsupported content encoding {other}")),
  };
  serde_json::from_slice(&decoded).map_err(|error| error.to_string())
}

fn bounded_read(reader: impl Read) -> std::result::Result<Vec<u8>, String> {
  let mut output = Vec::new();
  reader
    .take(MAX_DECODED_BODY_BYTES + 1)
    .read_to_end(&mut output)
    .map_err(|error| error.to_string())?;
  if output.len() as u64 > MAX_DECODED_BODY_BYTES {
    return Err(format!("decoded body exceeds {MAX_DECODED_BODY_BYTES} bytes"));
  }
  Ok(output)
}

fn header_str<'a>(headers: &'a Value, name: &str) -> Option<&'a str> {
  headers
    .as_object()?
    .iter()
    .find(|(key, _)| key.eq_ignore_ascii_case(name))
    .and_then(|(_, value)| value.as_str())
}

fn collect_request(request: &Value, content: &mut CollectedContent) {
  collect_tool_definitions(request.get("tools"), content);
  if let Some(input) = request.get("input").and_then(Value::as_array) {
    collect_items(input, "input", content);
  }
  if let Some(messages) = request.get("messages").and_then(Value::as_array) {
    collect_items(messages, "input", content);
  }
  if let Some(system) = request.get("system") {
    push_message(
      "system",
      "input",
      "message",
      system,
      &json!({"role": "system", "content": system}),
      content,
    );
  }
}

fn collect_response(body: &[u8], content: &mut CollectedContent) {
  if body.len() as u64 > MAX_DECODED_BODY_BYTES {
    content.warning("response summary unavailable", "body exceeds summary limit");
    return;
  }
  if let Ok(response) = serde_json::from_slice::<Value>(body) {
    collect_response_value(&response, content);
    return;
  }

  let Ok(text) = std::str::from_utf8(body) else {
    content.warning("response summary unavailable", "body is not UTF-8 JSON or SSE");
    return;
  };
  let mut found_event = false;
  for line in text.lines().filter_map(|line| line.strip_prefix("data: ")) {
    if line == "[DONE]" {
      continue;
    }
    let Ok(event) = serde_json::from_str::<Value>(line) else {
      continue;
    };
    found_event = true;
    collect_response_event(&event, content);
  }
  if !found_event {
    content.warning("response summary unavailable", "body is not JSON or recognized SSE");
  }
}

fn collect_response_value(response: &Value, content: &mut CollectedContent) {
  if let Some(output) = response.get("output").and_then(Value::as_array) {
    collect_items(output, "output", content);
  }
  if let Some(parts) = response.get("content").and_then(Value::as_array) {
    let value = json!({"role": "assistant", "content": parts});
    push_message(
      "assistant",
      "output",
      "message",
      &Value::Array(parts.clone()),
      &value,
      content,
    );
  }
  if let Some(choices) = response.get("choices").and_then(Value::as_array) {
    for message in choices.iter().filter_map(|choice| choice.get("message")) {
      collect_item(message, "output", content);
    }
  }
}

fn collect_response_event(event: &Value, content: &mut CollectedContent) {
  match event.get("type").and_then(Value::as_str) {
    Some("response.output_item.done") => {
      if let Some(item) = event.get("item") {
        collect_item(item, "output", content);
      }
    }
    Some("message_stop") => {
      if let Some(message) = event.get("message") {
        collect_item(message, "output", content);
      }
    }
    _ => {}
  }
}

fn collect_items(items: &[Value], phase: &str, content: &mut CollectedContent) {
  for item in items {
    collect_item(item, phase, content);
  }
}

fn collect_item(item: &Value, phase: &str, content: &mut CollectedContent) {
  let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
  if item_type == "additional_tools" {
    collect_tool_definitions(item.get("tools"), content);
    return;
  }

  let role = item.get("role").and_then(Value::as_str).or(match item_type {
    "compaction" => Some("compaction"),
    "custom_tool_call" | "function_call" | "tool_call" | "reasoning" => Some("assistant"),
    "custom_tool_call_output" | "function_call_output" | "tool_result" => Some("tool"),
    "message" => Some("assistant"),
    _ => None,
  });
  if let Some(role) = role {
    let kind = if item_type.is_empty() { "message" } else { item_type };
    let call_id = item
      .get("call_id")
      .or_else(|| item.get("tool_call_id"))
      .and_then(Value::as_str);
    let explicit_name = item
      .get("name")
      .or_else(|| item.pointer("/function/name"))
      .and_then(Value::as_str);
    if is_tool_call_type(item_type) {
      if let (Some(call_id), Some(name)) = (call_id, explicit_name) {
        content
          .tool_names_by_call_id
          .insert(call_id.to_string(), name.to_string());
      }
    }
    let name = explicit_name
      .map(str::to_owned)
      .or_else(|| call_id.and_then(|call_id| content.tool_names_by_call_id.get(call_id).cloned()));
    push_message_with_metadata(
      MessageMetadata {
        role,
        phase,
        kind,
        name: name.as_deref(),
        call_id,
      },
      item.get("content").unwrap_or(item),
      item,
      content,
    );
  }
}

fn push_message(
  role: &str,
  phase: &str,
  kind: &str,
  preview_value: &Value,
  value: &Value,
  content: &mut CollectedContent,
) {
  push_message_with_metadata(
    MessageMetadata {
      role,
      phase,
      kind,
      name: None,
      call_id: None,
    },
    preview_value,
    value,
    content,
  );
}

fn push_message_with_metadata(
  metadata: MessageMetadata<'_>,
  preview_value: &Value,
  value: &Value,
  content: &mut CollectedContent,
) {
  let preview = extract_text(preview_value);
  let (preview, truncated) = preview
    .as_deref()
    .map(|preview| truncate_text(preview, MAX_PREVIEW_CHARS))
    .map_or((None, false), |(preview, truncated)| (Some(preview), truncated));
  let index = content.messages.len();
  content.messages.push(CollectedMessage {
    summary: LlmMessageSummary {
      index,
      role: truncate_label(metadata.role),
      phase: metadata.phase.to_string(),
      kind: truncate_label(metadata.kind),
      name: metadata.name.map(truncate_label),
      call_id: metadata.call_id.map(truncate_label),
      preview,
      truncated,
      content_bytes: serialized_len(value),
    },
    value: value.clone(),
  });
}

fn collect_tool_definitions(tools: Option<&Value>, content: &mut CollectedContent) {
  let Some(tools) = tools.and_then(Value::as_array) else {
    return;
  };
  for tool in tools {
    let kind = tool.get("type").and_then(Value::as_str).unwrap_or("tool");
    let name = tool
      .get("name")
      .or_else(|| tool.pointer("/function/name"))
      .and_then(Value::as_str)
      .unwrap_or(kind);
    let description = tool
      .get("description")
      .or_else(|| tool.pointer("/function/description"))
      .and_then(Value::as_str);
    let (description, truncated) = description
      .map(|description| truncate_text(description, MAX_DESCRIPTION_CHARS))
      .map_or((None, false), |(description, truncated)| (Some(description), truncated));
    let schema = tool
      .get("parameters")
      .or_else(|| tool.get("input_schema"))
      .or_else(|| tool.pointer("/function/parameters"));
    let index = content.tool_definitions.len();
    content.tool_definitions.push(CollectedToolDefinition {
      summary: LlmToolDefinitionSummary {
        index,
        name: truncate_label(name),
        kind: truncate_label(kind),
        description,
        truncated,
        schema_bytes: schema.map(serialized_len).unwrap_or(0),
      },
      value: tool.clone(),
    });
  }
}

fn serialized_len(value: &Value) -> usize {
  value.as_str().map(str::len).unwrap_or_else(|| value.to_string().len())
}

fn is_tool_call_type(item_type: &str) -> bool {
  matches!(item_type, "custom_tool_call" | "function_call" | "tool_call")
}

fn extract_text(value: &Value) -> Option<String> {
  match value {
    Value::String(text) => Some(text.clone()),
    Value::Array(parts) => {
      let texts = parts.iter().filter_map(extract_text).collect::<Vec<_>>();
      (!texts.is_empty()).then(|| texts.join("\n"))
    }
    Value::Object(object) => [
      "text",
      "input_text",
      "output_text",
      "content",
      "output",
      "arguments",
      "input",
      "summary",
    ]
    .into_iter()
    .find_map(|field| object.get(field).and_then(extract_text)),
    _ => None,
  }
}

fn truncate_text(value: &str, limit: usize) -> (String, bool) {
  let value = value.trim();
  if value.chars().count() <= limit {
    return (value.to_string(), false);
  }
  let mut output = value.chars().take(limit).collect::<String>();
  output.push('…');
  (output, true)
}

fn truncate_label(value: &str) -> String {
  truncate_text(value, MAX_LABEL_CHARS).0
}
