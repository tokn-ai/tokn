//! Opt-in, offline checks against a local sessions database. No history content
//! or identifiers are printed or copied into fixtures; the database is read-only.
//!
//! Set TOKN_TEST_HISTORY_DB and run this test with --ignored --nocapture.

use std::{
  collections::{BTreeMap, BTreeSet},
  io::{BufRead, BufReader},
  time::Duration,
};

use rusqlite::{types::Value as SqlValue, Connection, OpenFlags};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use tokn_convert::{convert_request, media, provider::Endpoint};
use tokn_core::content::{Content, ImageContent, ImageDetail};

const ENDPOINTS: [Endpoint; 3] = [Endpoint::Responses, Endpoint::ChatCompletions, Endpoint::Messages];
const TABLES: [(&str, &str); 2] = [("message_tree", "message_parts"), ("node_messages", "node_parts")];

#[derive(Default)]
struct Report {
  counts: BTreeMap<String, u64>,
  failures: BTreeMap<&'static str, u64>,
}

impl Report {
  fn count(&mut self, label: impl Into<String>) {
    *self.counts.entry(label.into()).or_default() += 1;
  }

  fn check(&mut self, passed: bool, label: &'static str) {
    if !passed {
      *self.failures.entry(label).or_default() += 1;
    }
  }
}

// Deliberately discard error details: parser errors can contain private text,
// SQLite errors can contain paths, and equality assertions can print images.
fn checked<T, E>(result: Result<T, E>, message: &'static str) -> T {
  match result {
    Ok(value) => value,
    Err(_) => panic!("{message}"),
  }
}

fn endpoint(part: &Value) -> Option<Endpoint> {
  match part["type"].as_str()? {
    "input_image" => Some(Endpoint::Responses),
    "image_url" => Some(Endpoint::ChatCompletions),
    "image" => Some(Endpoint::Messages),
    _ => None,
  }
}

fn request(endpoint: Endpoint, role: &str, parts: &[Value]) -> Value {
  let message = json!({"role":role,"content":parts});
  if endpoint == Endpoint::Responses {
    json!({"model":"history-test","input":[message]})
  } else {
    json!({"model":"history-test","messages":[message]})
  }
}

fn request_parts(endpoint: Endpoint, request: &Value) -> Option<&Vec<Value>> {
  request[if endpoint == Endpoint::Responses {
    "input"
  } else {
    "messages"
  }][0]["content"]
    .as_array()
}

fn typed_roundtrip<T: DeserializeOwned + Serialize>(part: &Value) -> bool {
  serde_json::from_value::<T>(part.clone())
    .ok()
    .and_then(|value| serde_json::to_value(value).ok())
    .is_some_and(|value| value == *part)
}

fn typed_image_roundtrip(endpoint: Endpoint, part: &Value) -> bool {
  match endpoint {
    Endpoint::Responses => typed_roundtrip::<tokn_endpoint_responses::InputContentPart>(part),
    Endpoint::ChatCompletions => typed_roundtrip::<tokn_endpoint_chat_completions::ContentPart>(part),
    Endpoint::Messages => typed_roundtrip::<tokn_endpoint_messages::ContentBlock>(part),
  }
}

fn normalize_default_detail(content: &mut Content) {
  if let Content::Image(image) = content {
    if image.detail == Some(ImageDetail::Auto) {
      image.detail = None;
    }
  }
}

fn signature(endpoint: Endpoint, parts: &[Value]) -> Option<Vec<Content>> {
  let mut result = Vec::new();
  for part in parts {
    if let Some(mut content) = media::content_from_part(endpoint, part).ok()? {
      normalize_default_detail(&mut content);
      result.push(content);
    }
  }
  Some(result)
}

fn unsupported_image(image: &ImageContent, to: Endpoint) -> bool {
  (to == Endpoint::Messages && image.detail.is_some_and(|detail| detail != ImageDetail::Auto))
    || (to == Endpoint::ChatCompletions && image.detail == Some(ImageDetail::Original))
    || image
      .resource
      .media_type
      .as_deref()
      .is_some_and(|mime| !matches!(mime, "image/png" | "image/jpeg" | "image/webp" | "image/gif"))
}

fn native_history_rejection(error: &tokn_convert::error::ConvertError) -> bool {
  matches!(error, tokn_convert::error::ConvertError::UnsupportedFeature { message }
    if matches!(message.as_str(),
      "native content has no verified representation in the target endpoint"
        | "tool result contains a native block that cannot be portably converted")
      || message.contains(" content cannot be translated from "))
}

fn has_nonimage_native_history(from: Endpoint, body: &Value) -> bool {
  let request = match from {
    Endpoint::Responses => tokn_convert::value::responses::request_from_value(body),
    Endpoint::ChatCompletions => tokn_convert::value::chat::request_from_value(body),
    Endpoint::Messages => tokn_convert::value::messages::request_from_value(body),
  };
  request.is_ok_and(|request| {
    request.messages.iter().any(|message| {
      message.content.iter().any(|content| {
        matches!(content, tokn_convert::ir::ContentPart::Native { value, .. }
          if endpoint(value).is_none() && value["type"] != "image_generation_call")
      })
    })
  })
}

fn check_image(report: &mut Report, part: &Value) {
  report.count("image_parts");
  let Some(from) = endpoint(part) else {
    report.check(false, "image_endpoint");
    return;
  };
  report.check(typed_image_roundtrip(from, part), "typed_image_roundtrip");
  let source = request(from, "user", std::slice::from_ref(part));
  report.check(
    convert_request(from, from, &source).is_ok_and(|value| value == source),
    "same_endpoint_passthrough",
  );
  let content = match media::content_from_part(from, part) {
    Ok(Some(Content::Image(image))) => Content::Image(image),
    Ok(None) => {
      report.count("native_image_references");
      return;
    }
    _ => {
      report.check(false, "decode_image");
      return;
    }
  };
  report.count("decoded_images");
  report.check(
    serde_json::to_vec(&content)
      .ok()
      .and_then(|bytes| serde_json::from_slice::<Content>(&bytes).ok())
      .is_some_and(|roundtrip| roundtrip == content),
    "portable_content_roundtrip",
  );
  let Content::Image(image) = &content else {
    unreachable!()
  };
  if let Some(bytes) = image.resource.bytes() {
    report.count("inline_images");
    *report.counts.entry("decoded_bytes".into()).or_default() += bytes.len() as u64;
    let max = report.counts.entry("largest_image_bytes".into()).or_default();
    *max = (*max).max(bytes.len() as u64);
  } else {
    report.count("remote_images_without_fetching");
  }
  let text_kind = if from == Endpoint::Responses {
    "input_text"
  } else {
    "text"
  };
  let parts = vec![
    json!({"type":text_kind,"text":"before"}),
    part.clone(),
    json!({"type":text_kind,"text":"after"}),
  ];
  let source = request(from, "user", &parts);
  let expected = signature(from, &parts);
  report.check(expected.is_some(), "decode_image_signature");
  for to in ENDPOINTS.into_iter().filter(|to| *to != from) {
    let converted = convert_request(from, to, &source);
    if unsupported_image(image, to) {
      report.check(converted.is_err(), "unsupported_image_was_accepted");
      report.count(format!("expected_image_rejection_{}", to.as_str()));
      continue;
    }
    let Ok(converted) = converted else {
      report.check(false, "image_conversion");
      continue;
    };
    report.count(format!("image_conversion_{}", to.as_str()));
    report.check(
      request_parts(to, &converted).and_then(|parts| signature(to, parts)) == expected,
      "image_bytes_mime_detail_order",
    );
    report.check(
      request_parts(to, &converted)
        .and_then(|parts| parts.get(1))
        .is_some_and(|image| typed_image_roundtrip(to, image)),
      "converted_typed_image_roundtrip",
    );
    report.check(
      convert_request(to, from, &converted)
        .ok()
        .and_then(|value| request_parts(from, &value).and_then(|parts| signature(from, parts)))
        == expected,
      "image_reverse_conversion",
    );
  }
}

fn check_message(report: &mut Report, role: &str, stored: Vec<(String, Vec<u8>)>) {
  let from = stored.iter().find_map(|(kind, _)| endpoint(&json!({"type":kind})));
  let Some(from) = from else {
    report.check(false, "message_endpoint");
    return;
  };
  let parts: Vec<Value> = stored
    .into_iter()
    .map(|(kind, bytes)| {
      if kind == "text" {
        let text = checked(String::from_utf8(bytes), "stored message text is not UTF-8");
        let kind = if from == Endpoint::Responses {
          "input_text"
        } else {
          "text"
        };
        json!({"type":kind,"text":text})
      } else {
        checked(serde_json::from_slice(&bytes), "stored message part is not JSON")
      }
    })
    .collect();
  let source = request(from, role, &parts);
  report.count("image_bearing_messages");
  report.check(
    convert_request(from, from, &source).is_ok_and(|value| value == source),
    "message_same_endpoint_passthrough",
  );
  let expected = signature(from, &parts);
  report.check(expected.is_some(), "decode_message_content");
  for to in ENDPOINTS.into_iter().filter(|to| *to != from) {
    let converted = convert_request(from, to, &source);
    let image_restriction = parts.iter().any(|part| {
      matches!(media::content_from_part(from, part), Ok(Some(Content::Image(image))) if unsupported_image(&image, to))
    }) || matches!(role, "assistant" | "system" | "developer")
      || (role == "tool" && to == Endpoint::ChatCompletions);
    if image_restriction {
      report.check(converted.is_err(), "unsupported_message_image_was_accepted");
      report.count("expected_message_image_rejection");
      continue;
    }
    match converted {
      Ok(value) => {
        report.count(format!("message_conversion_{}", to.as_str()));
        report.check(
          request_parts(to, &value).and_then(|parts| signature(to, parts)) == expected,
          "message_image_text_order",
        );
      }
      Err(error) => {
        if native_history_rejection(&error)
          && parts
            .iter()
            .any(|part| endpoint(part).is_none() && matches!(media::content_from_part(from, part), Ok(None)))
        {
          report.count("native_nonimage_message_rejection");
        } else {
          report.check(false, "portable_message_conversion");
        }
      }
    }
  }
}

#[test]
#[ignore = "requires an explicitly selected local history database"]
fn persisted_history_images_convert_without_losing_content() {
  let path = checked(
    std::env::var("TOKN_TEST_HISTORY_DB"),
    "set TOKN_TEST_HISTORY_DB to a sessions database",
  );
  let db = checked(
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX),
    "could not open history database read-only",
  );
  checked(
    db.busy_timeout(Duration::from_secs(10)),
    "could not configure database read timeout",
  );
  checked(
    db.execute_batch("PRAGMA query_only=ON"),
    "could not enable query-only mode",
  );
  let mut report = Report::default();
  let mut hashes = Vec::new();
  let upper_rowid: i64 = checked(
    db.query_row("SELECT COALESCE(MAX(rowid), 0) FROM part_blobs", [], |row| row.get(0)),
    "could not determine history read boundary",
  );
  let mut cursor = 0_i64;
  loop {
    // Finish the SQLite statement before decoding or hashing bytes. In DELETE
    // journal mode a live cursor would otherwise block the application's writer.
    let batch = {
      let mut query = checked(
        db.prepare("SELECT rowid, hash, content FROM part_blobs WHERE rowid>?1 AND rowid<=?2 AND part_type IN ('input_image', 'image_url', 'image') ORDER BY rowid LIMIT 32"),
        "could not prepare history image query",
      );
      let rows = checked(
        query.query_map([cursor, upper_rowid], |row| {
          Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Vec<u8>>(2)?,
          ))
        }),
        "could not query history images",
      );
      checked(
        rows.collect::<Result<Vec<_>, _>>(),
        "could not read history image batch",
      )
    };
    if batch.is_empty() {
      break;
    }
    for (rowid, hash, bytes) in batch {
      cursor = rowid;
      let part = checked(serde_json::from_slice(&bytes), "history image is not JSON");
      check_image(&mut report, &part);
      hashes.push((hash, bytes.len()));
    }
  }
  report.check(!hashes.is_empty(), "no_images_found");
  println!("history image corpus counts: {:?}", report.counts);

  let message_limit = std::env::var("TOKN_TEST_HISTORY_MESSAGE_LIMIT")
    .map(|value| checked(value.parse::<usize>(), "history message limit must be an integer"))
    .unwrap_or(128);
  report
    .counts
    .insert("message_sample_limit".into(), message_limit as u64);
  let mut tables = Vec::new();
  for (messages, parts) in TABLES {
    let exists: bool = checked(
      db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [messages],
        |row| row.get(0),
      ),
      "could not inspect history schema",
    );
    if exists {
      tables.push((messages, parts, BTreeSet::new()));
    }
  }
  // Cover the largest stored images first, then spread the sample across the
  // corpus instead of replaying many prefixes that all contain its first image.
  let mut order: Vec<usize> = (0..hashes.len()).collect();
  order.sort_by_key(|index| std::cmp::Reverse(hashes[*index].1));
  order.truncate(8);
  let step = hashes.len().div_ceil(message_limit.max(1)).max(1);
  for offset in 0..step {
    order.extend((offset..hashes.len()).step_by(step));
  }
  let mut visited_hashes = BTreeSet::new();
  let mut sampled = 0;
  'sample: for index in order {
    if sampled == message_limit {
      break;
    }
    if !visited_hashes.insert(index) {
      continue;
    }
    let hash = &hashes[index].0;
    for (messages, parts, seen) in &mut tables {
      let candidates = {
        let mut lookup = checked(
          db.prepare(&format!(
            "SELECT m.id, m.role FROM {messages} m JOIN {parts} p ON p.message_id=m.id WHERE p.part_hash=?1 LIMIT 4"
          )),
          "could not prepare image-bearing message lookup",
        );
        let rows = checked(
          lookup.query_map([hash], |row| Ok((row.get::<_, SqlValue>(0)?, row.get::<_, String>(1)?))),
          "could not find image-bearing messages",
        );
        checked(
          rows.collect::<Result<Vec<_>, _>>(),
          "could not read image-bearing message rows",
        )
      };
      for (id, role) in candidates {
        let key = match &id {
          SqlValue::Blob(value) => value.clone(),
          SqlValue::Text(value) => value.as_bytes().to_vec(),
          _ => panic!("unsupported stored message key type"),
        };
        if !seen.insert(key) {
          continue;
        }
        let content = {
          let mut query = checked(
            db.prepare(&format!("SELECT b.part_type, b.content FROM {parts} p JOIN part_blobs b ON b.hash=p.part_hash WHERE p.message_id=?1 ORDER BY p.part_index")),
            "could not prepare message content query",
          );
          let rows = checked(
            query.query_map([id], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))),
            "could not read stored message content",
          );
          checked(rows.collect::<Result<Vec<_>, _>>(), "invalid stored message content")
        };
        check_message(&mut report, &role, content);
        sampled += 1;
        if sampled == message_limit {
          break 'sample;
        }
        break;
      }
    }
  }
  for (messages, _, seen) in tables {
    report
      .counts
      .insert(format!("sampled_{messages}_image_messages"), seen.len() as u64);
  }
  report.check(message_limit == 0 || sampled > 0, "no_history_messages_sampled");
  println!("history image counts: {:?}", report.counts);
  println!("history image failures: {:?}", report.failures);
  assert!(
    report.failures.is_empty(),
    "history image replay checks failed; see aggregate categories"
  );
}

fn collect_images(value: &Value, result: &mut Vec<Value>) {
  if endpoint(value).is_some() {
    result.push(value.clone());
    return;
  }
  match value {
    Value::Array(values) => values.iter().for_each(|value| collect_images(value, result)),
    Value::Object(values) => values.values().for_each(|value| collect_images(value, result)),
    _ => {}
  }
}

fn request_images(endpoint: Endpoint, body: &Value) -> Vec<Value> {
  let mut result = Vec::new();
  let items = &body[if endpoint == Endpoint::Responses {
    "input"
  } else {
    "messages"
  }];
  collect_images(items, &mut result);
  result
}

#[test]
#[ignore = "requires explicitly selected original request bodies in local JSONL"]
fn recorded_requests_preserve_image_payloads_and_order() {
  // Every line is {"endpoint":"responses"|"chat_completions"|"messages", "body":{...}}.
  // The selected file is local input only; it must never become a tracked fixture.
  let path = checked(
    std::env::var("TOKN_TEST_IMAGE_REQUESTS_JSONL"),
    "set TOKN_TEST_IMAGE_REQUESTS_JSONL to a local request sample",
  );
  let file = checked(std::fs::File::open(path), "could not open local request sample");
  let mut report = Report::default();
  for line in BufReader::new(file).lines() {
    let line = checked(line, "could not read local request sample");
    let record: Value = checked(serde_json::from_str(&line), "local request sample is not JSON");
    let from = match record["endpoint"].as_str() {
      Some("responses") => Endpoint::Responses,
      Some("chat_completions" | "chat-completions") => Endpoint::ChatCompletions,
      Some("messages") => Endpoint::Messages,
      _ => panic!("unsupported local request sample endpoint"),
    };
    let body = &record["body"];
    let images = request_images(from, body);
    report.check(!images.is_empty(), "recorded_request_has_no_images");
    report.count("recorded_requests");
    report.check(
      convert_request(from, from, body).is_ok_and(|value| value == *body),
      "recorded_request_same_endpoint_passthrough",
    );
    for image in &images {
      check_image(&mut report, image);
    }
    let expected = signature(from, &images);
    report.check(expected.is_some(), "decode_recorded_request_images");
    let native_nonimage_history = has_nonimage_native_history(from, body);
    for to in ENDPOINTS.into_iter().filter(|to| *to != from) {
      match convert_request(from, to, body) {
        Ok(converted) => {
          report.count(format!("recorded_request_conversion_{}", to.as_str()));
          report.check(
            signature(to, &request_images(to, &converted)) == expected,
            "recorded_request_image_payloads_and_order",
          );
          report.check(
            convert_request(to, from, &converted)
              .ok()
              .and_then(|value| signature(from, &request_images(from, &value)))
              == expected,
            "recorded_request_reverse_image_conversion",
          );
        }
        Err(error) if native_nonimage_history && native_history_rejection(&error) => {
          // Each actual image was independently verified above. Keep rejection
          // of unrelated provider-native history distinct from image failures.
          report.count("recorded_request_native_history_rejection");
        }
        Err(_) => report.check(false, "recorded_request_conversion"),
      }
    }
  }
  report.check(
    report.counts.contains_key("recorded_requests"),
    "no_recorded_requests_found",
  );
  println!("recorded request counts: {:?}", report.counts);
  println!("recorded request failures: {:?}", report.failures);
  assert!(
    report.failures.is_empty(),
    "recorded request image replay checks failed; see aggregate categories"
  );
}
