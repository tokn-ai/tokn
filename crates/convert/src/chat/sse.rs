use crate::error::Result;
use crate::ir::IrDelta;
use alloc::vec::Vec;
use tokn_endpoint_chat_completions::ChatEvent;

pub fn delta_from_endpoint_event(event: &ChatEvent) -> Result<Vec<IrDelta>> {
  match event {
    ChatEvent::Done => Ok(Vec::new()),
    ChatEvent::Chunk(chunk) => {
      let value = serde_json::to_value(chunk)?;
      Ok(crate::value::chat::delta_from_chat_chunk(&value))
    }
  }
}

pub fn events_from_deltas(resp_id: &str, model: &str, deltas: &[IrDelta], finish: bool) -> Result<Vec<ChatEvent>> {
  crate::value::chat::chunk_from_deltas(resp_id, model, deltas, finish)
    .into_iter()
    .map(|value| serde_json::from_value(value).map_err(Into::into))
    .collect()
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn endpoint_event_matches_value_deltas() {
    let value = json!({
      "id": "chatcmpl_1",
      "model": "gpt-4.1",
      "choices": [{"index": 0, "delta": {"content": "hi"}, "finish_reason": null}]
    });
    let event: ChatEvent = serde_json::from_value(value.clone()).unwrap();

    let from_value = crate::value::chat::delta_from_chat_chunk(&value);
    let from_endpoint = delta_from_endpoint_event(&event).unwrap();

    assert_eq!(format!("{from_endpoint:?}"), format!("{from_value:?}"));
  }
}
