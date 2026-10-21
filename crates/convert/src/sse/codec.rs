use super::event::SseEvent;
use alloc::string::String;
use tokn_core::types::Bytes;

pub fn encode_sse(event: Option<&str>, data: &str) -> Bytes {
  let mut out = String::new();
  if let Some(event) = event {
    out.push_str("event: ");
    out.push_str(event);
    out.push('\n');
  }
  if data.is_empty() {
    out.push_str("data:\n\n");
    return Bytes::from(out);
  }
  for line in data.lines() {
    out.push_str("data: ");
    out.push_str(line);
    out.push('\n');
  }
  out.push('\n');
  Bytes::from(out)
}

pub fn encode_done() -> Bytes {
  Bytes::from_static(b"data: [DONE]\n\n")
}

pub fn encode_event(event: &SseEvent) -> Bytes {
  if event.is_done() {
    encode_done()
  } else {
    encode_sse(event.event.as_deref(), &event.data)
  }
}
