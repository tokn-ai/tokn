use crate::error::{ConvertError, Result};
use crate::ir::*;
use crate::provider::Endpoint;
use alloc::collections::BTreeMap;
use alloc::{boxed::Box, string::String, string::ToString, vec::Vec};
use serde_json::{json, Map, Value};

const REQUEST_KEYS: &[&str] = &[
  "model",
  "messages",
  "tools",
  "tool_choice",
  "temperature",
  "top_p",
  "max_tokens",
  "max_completion_tokens",
  "stop",
  "n",
  "seed",
  "stream",
  "reasoning",
  "thinking",
];

pub fn request_from_value(v: &Value) -> Result<IrRequest> {
  let obj = v
    .as_object()
    .ok_or_else(|| ConvertError::bad_shape("body", "expected object"))?;
  let model = obj
    .get("model")
    .and_then(Value::as_str)
    .unwrap_or("unknown")
    .to_string();
  let mut system = Vec::new();
  let mut messages = Vec::new();
  for msg in obj
    .get("messages")
    .and_then(Value::as_array)
    .ok_or(ConvertError::MissingField { field: "messages" })?
  {
    let role = msg.get("role").and_then(Value::as_str).unwrap_or("user");
    let mut ir = message_from_value(msg, role)?;
    if ir.role == Role::System {
      let text = text_from_parts(&ir.content);
      if !text.is_empty() {
        system.push(text);
      }
    } else {
      if let Some(tool_calls) = msg.get("tool_calls").and_then(Value::as_array) {
        for call in tool_calls {
          ir.content.push(ContentPart::ToolCall {
            call: tool_call_from_chat(call),
          });
        }
      }
      if let Some(reasoning) = msg.get("reasoning_content").and_then(Value::as_str) {
        ir.content.push(ContentPart::Reasoning {
          text: reasoning.to_string(),
        });
      }
      messages.push(ir);
    }
  }
  Ok(IrRequest {
    model,
    system: (!system.is_empty()).then(|| system.join("\n\n")),
    messages,
    tools: crate::tools::normalise_tools(
      obj
        .get("tools")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]),
    ),
    tool_choice: obj.get("tool_choice").map(crate::tools::normalise_tool_choice),
    sampling: Sampling {
      temperature: obj.get("temperature").and_then(Value::as_f64),
      top_p: obj.get("top_p").and_then(Value::as_f64),
      max_output_tokens: obj
        .get("max_completion_tokens")
        .or_else(|| obj.get("max_tokens"))
        .and_then(Value::as_u64),
      stop: obj.get("stop").cloned(),
      n: obj.get("n").and_then(Value::as_u64),
      seed: obj.get("seed").and_then(Value::as_i64),
    },
    reasoning: obj.get("reasoning").or_else(|| obj.get("thinking")).cloned(),
    stream: obj.get("stream").and_then(Value::as_bool).unwrap_or(false),
    extras: extras_from_object(obj, REQUEST_KEYS),
  })
}

pub fn request_to_value(req: &IrRequest) -> Result<Value> {
  let mut out = Map::new();
  out.insert("model".into(), Value::String(req.model.clone()));
  let mut messages = Vec::new();
  if let Some(system) = &req.system {
    messages.push(json!({ "role": "system", "content": system }));
  }
  for msg in &req.messages {
    messages.extend(message_to_chat(msg)?);
  }
  out.insert("messages".into(), Value::Array(messages));
  if !req.tools.is_empty() {
    // Chat-completions only understands function tools. Drop provider-specific
    // entries (e.g. `{type:"web_search"}`) instead of forwarding them and
    // letting the upstream reject the whole request.
    let tools: Vec<Value> = req
      .tools
      .iter()
      .filter(|t| t.get("function").and_then(Value::as_object).is_some())
      .cloned()
      .collect();
    if !tools.is_empty() {
      out.insert("tools".into(), Value::Array(tools));
    }
  }
  insert_opt(
    &mut out,
    "tool_choice",
    req.tool_choice.as_ref().map(crate::tools::tool_choice_to_chat),
  );
  insert_opt_f64(&mut out, "temperature", req.sampling.temperature);
  insert_opt_f64(&mut out, "top_p", req.sampling.top_p);
  insert_opt_u64(&mut out, "max_tokens", req.sampling.max_output_tokens);
  insert_opt(&mut out, "stop", req.sampling.stop.clone());
  insert_opt_u64(&mut out, "n", req.sampling.n);
  if let Some(seed) = req.sampling.seed {
    out.insert("seed".into(), Value::Number(seed.into()));
  }
  if req.stream {
    out.insert("stream".into(), Value::Bool(true));
  }
  insert_opt(&mut out, "reasoning", req.reasoning.clone());
  for (k, v) in &req.extras {
    out.entry(k.clone()).or_insert_with(|| v.clone());
  }
  Ok(Value::Object(out))
}

pub fn response_from_value(v: &Value) -> Result<IrResponse> {
  let choice = v
    .get("choices")
    .and_then(Value::as_array)
    .and_then(|a| a.first())
    .ok_or(ConvertError::MissingField { field: "choices" })?;
  let msg = choice
    .get("message")
    .unwrap_or(choice.get("delta").unwrap_or(&Value::Null));
  let role = msg.get("role").and_then(Value::as_str).map(Role::from_wire);
  let mut content = content_from_chat(msg.get("content"))?;
  let mut extras = BTreeMap::new();
  if let Some(audio) = msg.get("audio").filter(|audio| !audio.is_null()) {
    content.push(ContentPart::Media {
      content: Box::new(crate::media::audio_from_chat(audio)?),
    });
    extras.insert("__chat_audio".into(), audio.clone());
  }
  if let Some(reasoning) = msg.get("reasoning_content").and_then(Value::as_str) {
    content.push(ContentPart::Reasoning {
      text: reasoning.to_string(),
    });
  }
  let tool_calls = msg
    .get("tool_calls")
    .and_then(Value::as_array)
    .map(|calls| calls.iter().map(tool_call_from_chat).collect())
    .unwrap_or_default();
  Ok(IrResponse {
    id: v.get("id").and_then(Value::as_str).map(str::to_string),
    model: v.get("model").and_then(Value::as_str).map(str::to_string),
    role,
    content,
    tool_calls,
    usage: usage_from_openai(v),
    finish_reason: choice.get("finish_reason").and_then(Value::as_str).map(str::to_string),
    extras,
  })
}

pub fn response_to_value(resp: &IrResponse) -> Result<Value> {
  let mut msg = Map::new();
  msg.insert(
    "role".into(),
    Value::String(resp.role.as_ref().unwrap_or(&Role::Assistant).as_str().to_string()),
  );
  msg.insert("content".into(), Value::String(text_from_parts(&resp.content)));
  for part in &resp.content {
    match part {
      ContentPart::Media { content } if matches!(content.as_ref(), tokn_core::content::Content::Audio(_)) => {
        let tokn_core::content::Content::Audio(audio) = content.as_ref() else {
          unreachable!()
        };
        let native = resp.extras.get("__chat_audio").cloned().unwrap_or_else(
          || json!({"data":audio.resource.encode_base64(), "transcript":audio.transcript.as_ref().map(|t| &t.text)}),
        );
        msg.insert("audio".into(), native);
      }
      ContentPart::Media { .. } | ContentPart::Native { .. } | ContentPart::Raw { .. } => {
        return Err(crate::media::unsupported(
          "nontext output has no portable Chat assistant representation",
        ))
      }
      _ => {}
    }
  }
  if let Some(reasoning) = reasoning_from_parts(&resp.content) {
    msg.insert("reasoning_content".into(), Value::String(reasoning));
  }
  if !resp.tool_calls.is_empty() {
    msg.insert(
      "tool_calls".into(),
      Value::Array(
        resp
          .tool_calls
          .iter()
          .enumerate()
          .map(|(i, c)| tool_call_to_chat(i, c))
          .collect(),
      ),
    );
  }
  let mut out = json!({
    "id": resp.id.clone().unwrap_or_else(|| "chatcmpl-converted".into()),
    "object": "chat.completion",
    "model": resp.model.clone().unwrap_or_default(),
    "choices": [{
      "index": 0,
      "message": Value::Object(msg),
      "finish_reason": resp.finish_reason.clone().unwrap_or_else(|| "stop".into()),
    }],
  });
  if let Some(usage) = &resp.usage {
    out["usage"] = usage_to_chat(usage);
  }
  Ok(out)
}

pub fn delta_from_chat_chunk(v: &Value) -> Vec<IrDelta> {
  let mut out = Vec::new();
  let Some(choice) = v.get("choices").and_then(Value::as_array).and_then(|a| a.first()) else {
    if let Some(usage) = usage_from_openai(v) {
      out.push(IrDelta::Usage(usage));
    }
    return out;
  };
  if let Some(delta) = choice.get("delta") {
    if let Some(text) = delta.get("content").and_then(Value::as_str) {
      out.push(IrDelta::Text(text.to_string()));
    }
    if let Some(text) = delta.get("reasoning_content").and_then(Value::as_str) {
      out.push(IrDelta::Reasoning(text.to_string()));
    }
    if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
      for call in calls {
        let index = call.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
        out.push(IrDelta::ToolCall {
          index,
          id: call.get("id").and_then(Value::as_str).map(str::to_string),
          name: call
            .get("function")
            .and_then(|f| f.get("name"))
            .and_then(Value::as_str)
            .map(str::to_string),
          arguments_delta: call
            .get("function")
            .and_then(|f| f.get("arguments"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        });
      }
    }
  }
  if let Some(reason) = choice.get("finish_reason") {
    if !reason.is_null() {
      out.push(IrDelta::Finish(reason.as_str().map(str::to_string)));
    }
  }
  if let Some(usage) = usage_from_openai(v) {
    out.push(IrDelta::Usage(usage));
  }
  out
}

pub fn chunk_from_deltas(resp_id: &str, model: &str, deltas: &[IrDelta], finish: bool) -> Result<Vec<Value>> {
  let mut values = Vec::new();
  for delta in deltas {
    match delta {
      IrDelta::Audio {
        id,
        data,
        transcript,
        expires_at,
        ..
      } => {
        if !data.is_empty() {
          tokn_core::content::decode_base64(data).map_err(|error| ConvertError::sse(error.to_string()))?;
        }
        let mut audio = Map::new();
        if let Some(id) = id {
          audio.insert("id".into(), json!(id));
        }
        if !data.is_empty() {
          audio.insert("data".into(), json!(data));
        }
        if let Some(transcript) = transcript {
          audio.insert("transcript".into(), json!(transcript));
        }
        if let Some(expires_at) = expires_at {
          audio.insert("expires_at".into(), json!(expires_at));
        }
        values.push(json!({"id":resp_id,"object":"chat.completion.chunk","model":model,"choices":[{"index":0,"delta":{"audio":audio},"finish_reason":null}]}));
      }
      IrDelta::Media { .. } | IrDelta::ImagePreview { .. } => {
        return Err(crate::media::unsupported(
          "Chat streaming cannot emit this media content",
        ))
      }
      IrDelta::Text(text) => values.push(json!({
        "id": resp_id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{ "index": 0, "delta": { "content": text }, "finish_reason": null }]
      })),
      IrDelta::Reasoning(text) => values.push(json!({
        "id": resp_id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{ "index": 0, "delta": { "reasoning_content": text }, "finish_reason": null }]
      })),
      IrDelta::ToolCall {
        index,
        id,
        name,
        arguments_delta,
      } => values.push(json!({
        "id": resp_id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{
          "index": 0,
          "delta": { "tool_calls": [{
            "index": index,
            "id": id,
            "type": "function",
            "function": { "name": name, "arguments": arguments_delta }
          }]},
          "finish_reason": null
        }]
      })),
      IrDelta::Usage(usage) => values.push(json!({
        "id": resp_id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [],
        "usage": usage_to_chat(usage),
      })),
      IrDelta::Finish(reason) => values.push(json!({
        "id": resp_id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{ "index": 0, "delta": {}, "finish_reason": reason.clone().unwrap_or_else(|| "stop".into()) }]
      })),
    }
  }
  if finish {
    values.push(json!({
      "id": resp_id,
      "object": "chat.completion.chunk",
      "model": model,
      "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }]
    }));
  }
  Ok(values)
}

fn message_from_value(msg: &Value, role: &str) -> Result<IrMessage> {
  let tool_call_id = msg.get("tool_call_id").and_then(Value::as_str).map(str::to_string);
  let content = if role == "tool" {
    if let Some(parts) = crate::media::tool_parts(Endpoint::ChatCompletions, &msg["content"])? {
      vec![ContentPart::ToolResultParts {
        id: tool_call_id.clone(),
        content: parts,
      }]
    } else {
      vec![ContentPart::ToolResult {
        id: tool_call_id.clone(),
        content: msg.get("content").cloned().unwrap_or(Value::Null),
      }]
    }
  } else {
    content_from_chat(msg.get("content"))?
  };
  Ok(IrMessage {
    role: Role::from_wire(role),
    content,
    tool_call_id,
    name: msg.get("name").and_then(Value::as_str).map(str::to_string),
    raw: None,
  })
}

fn content_from_chat(content: Option<&Value>) -> Result<Vec<ContentPart>> {
  Ok(match content {
    Some(Value::String(s)) => vec![ContentPart::Text { text: s.clone() }],
    Some(Value::Array(parts)) => parts.iter().map(part_from_chat).collect::<Result<_>>()?,
    Some(Value::Null) | None => Vec::new(),
    Some(_) => return Err(ConvertError::bad_shape("content", "expected text or content parts")),
  })
}

fn part_from_chat(v: &Value) -> Result<ContentPart> {
  crate::media::parse_part(Endpoint::ChatCompletions, v)
}

fn message_to_chat(msg: &IrMessage) -> Result<Vec<Value>> {
  let mut messages = Vec::new();
  for part in &msg.content {
    let (id, content) = match part {
      ContentPart::ToolResult { id, content } => (id, Value::String(tool_result_text(content))),
      ContentPart::ToolResultParts { id, content } => {
        (id, crate::media::render_tool_parts(Endpoint::ChatCompletions, content)?)
      }
      _ => continue,
    };
    messages.push(json!({"role":"tool", "tool_call_id":id.as_ref().or(msg.tool_call_id.as_ref()).cloned().unwrap_or_default(), "content":content}));
  }
  let remaining: Vec<_> = msg
    .content
    .iter()
    .filter(|part| {
      !matches!(
        part,
        ContentPart::ToolResult { .. } | ContentPart::ToolResultParts { .. }
      )
    })
    .collect();
  if remaining.is_empty() && !messages.is_empty() {
    return Ok(messages);
  }

  let mut out = Map::new();
  let role = if messages.is_empty() {
    msg.role.as_str()
  } else {
    // Messages-style tool results share a user turn with any follow-up
    // content. Chat Completions requires the results to be separate `tool`
    // messages, followed by the remaining content in a `user` message.
    Role::User.as_str()
  };
  out.insert("role".into(), Value::String(role.to_string()));
  if role == Role::Tool.as_str() {
    if let Some(id) = &msg.tool_call_id {
      out.insert("tool_call_id".into(), Value::String(id.clone()));
    }
  }
  if let Some(name) = &msg.name {
    out.insert("name".into(), Value::String(name.clone()));
  }
  out.insert("content".into(), content_to_chat(&remaining, role)?);
  let tool_calls: Vec<_> = remaining
    .iter()
    .filter_map(|p| match p {
      ContentPart::ToolCall { call } => Some(call),
      _ => None,
    })
    .enumerate()
    .map(|(i, call)| tool_call_to_chat(i, call))
    .collect();
  if !tool_calls.is_empty() {
    out.insert("tool_calls".into(), Value::Array(tool_calls));
  }
  let reasoning = remaining
    .iter()
    .filter_map(|part| match part {
      ContentPart::Reasoning { text } => Some(text.as_str()),
      _ => None,
    })
    .collect::<Vec<_>>()
    .join("");
  if !reasoning.is_empty() {
    out.insert("reasoning_content".into(), Value::String(reasoning));
  }
  messages.push(Value::Object(out));
  Ok(messages)
}

fn content_to_chat(parts: &[&ContentPart], role: &str) -> Result<Value> {
  if parts.iter().any(|part| {
    matches!(
      part,
      ContentPart::Raw { .. } | ContentPart::Native { .. } | ContentPart::Media { .. }
    )
  }) {
    let mut rendered = Vec::new();
    for part in parts {
      match part {
        ContentPart::Text { text } => rendered.push(json!({"type":"text", "text":text})),
        ContentPart::Media { content } => rendered.extend(crate::media::content_to_parts(
          Endpoint::ChatCompletions,
          content,
          role,
        )?),
        ContentPart::Native { endpoint, value } => {
          rendered.push(crate::media::native_part(Endpoint::ChatCompletions, endpoint, value)?)
        }
        ContentPart::Raw { value } => {
          rendered.extend(crate::media::raw_to_parts(Endpoint::ChatCompletions, value, role)?)
        }
        _ => {}
      }
    }
    return Ok(Value::Array(rendered));
  }

  Ok(Value::String(
    parts
      .iter()
      .filter_map(|part| match part {
        ContentPart::Text { text } => Some(text.as_str()),
        _ => None,
      })
      .collect::<Vec<_>>()
      .join(""),
  ))
}

fn tool_result_text(content: &Value) -> String {
  content
    .as_str()
    .map(str::to_string)
    .unwrap_or_else(|| serde_json::to_string(content).unwrap_or_default())
}

fn tool_call_from_chat(v: &Value) -> ToolCall {
  let args = v
    .get("function")
    .and_then(|f| f.get("arguments"))
    .cloned()
    .unwrap_or(Value::Null);
  ToolCall {
    id: v.get("id").and_then(Value::as_str).map(str::to_string),
    name: v
      .get("function")
      .and_then(|f| f.get("name"))
      .and_then(Value::as_str)
      .unwrap_or_default()
      .to_string(),
    arguments: parse_args(args),
  }
}

fn tool_call_to_chat(index: usize, call: &ToolCall) -> Value {
  json!({
    "index": index,
    "id": call.id.clone().unwrap_or_else(|| format!("call_{index}")),
    "type": "function",
    "function": {
      "name": call.name,
      "arguments": args_to_string(&call.arguments),
    }
  })
}

fn parse_args(args: Value) -> Value {
  if let Some(s) = args.as_str() {
    serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string()))
  } else {
    args
  }
}

pub(crate) fn args_to_string(args: &Value) -> String {
  args
    .as_str()
    .map(str::to_string)
    .unwrap_or_else(|| serde_json::to_string(args).unwrap_or_else(|_| "{}".into()))
}

fn insert_opt(out: &mut Map<String, Value>, key: &str, value: Option<Value>) {
  if let Some(value) = value {
    out.insert(key.into(), value);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn request_to_value_preserves_all_tool_results_and_sibling_content() {
    let request = IrRequest {
      model: "test-model".into(),
      system: None,
      messages: vec![IrMessage {
        role: Role::User,
        content: vec![
          ContentPart::ToolResult {
            id: Some("call_1".into()),
            content: json!("first result"),
          },
          ContentPart::ToolResult {
            id: Some("call_2".into()),
            content: json!({ "value": 2 }),
          },
          ContentPart::Text {
            text: "Now compare them.".into(),
          },
          ContentPart::Raw {
            value: json!({
              "type": "image_url",
              "image_url": { "url": "https://example.invalid/chart.png" }
            }),
          },
        ],
        tool_call_id: None,
        name: None,
        raw: None,
      }],
      tools: Vec::new(),
      tool_choice: None,
      sampling: Sampling::default(),
      reasoning: None,
      stream: false,
      extras: BTreeMap::new(),
    };

    let body = request_to_value(&request).expect("request should render");
    let messages = body["messages"].as_array().expect("messages");

    assert_eq!(messages.len(), 3);
    assert_eq!(
      messages[0],
      json!({
        "role": "tool",
        "tool_call_id": "call_1",
        "content": "first result",
      })
    );
    assert_eq!(
      messages[1],
      json!({
        "role": "tool",
        "tool_call_id": "call_2",
        "content": "{\"value\":2}",
      })
    );
    assert_eq!(messages[2]["role"], "user");
    assert_eq!(
      messages[2]["content"],
      json!([
        { "type": "text", "text": "Now compare them." },
        {
          "type": "image_url",
          "image_url": { "url": "https://example.invalid/chart.png" }
        }
      ])
    );
  }
}
