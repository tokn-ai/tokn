use crate::error::Result;
use crate::ir::IrDelta;
use alloc::vec::Vec;
use tokn_endpoint_messages::MessagesEvent;

pub fn delta_from_endpoint_event(event: &MessagesEvent) -> Result<Vec<IrDelta>> {
  let value = serde_json::to_value(event)?;
  Ok(crate::value::messages::delta_from_messages_event(&value))
}

/// Format delta fragments, or a complete stream when `finish` is true.
/// Retain `tokn_convert::sse::messages_emit::MessagesEmitter` for live streams.
pub fn events_from_deltas(resp_id: &str, model: &str, deltas: &[IrDelta], finish: bool) -> Result<Vec<MessagesEvent>> {
  crate::value::messages::events_from_deltas(resp_id, model, deltas, finish)
    .into_iter()
    .map(|(_, value)| serde_json::from_value(value).map_err(Into::into))
    .collect()
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn endpoint_event_matches_value_deltas() {
    let value = json!({
      "type": "content_block_delta",
      "index": 0,
      "delta": {"type": "text_delta", "text": "hi"}
    });
    let event: MessagesEvent = serde_json::from_value(value.clone()).unwrap();

    let from_value = crate::value::messages::delta_from_messages_event(&value);
    let from_endpoint = delta_from_endpoint_event(&event).unwrap();

    assert_eq!(format!("{from_endpoint:?}"), format!("{from_value:?}"));
  }

  #[test]
  fn complete_tool_stream_includes_its_block_lifecycle() {
    let events = events_from_deltas(
      "msg_1",
      "model",
      &[
        IrDelta::ToolCall {
          index: 7,
          id: Some("call_1".into()),
          name: Some("lookup".into()),
          arguments_delta: "{}".into(),
        },
        IrDelta::Finish(Some("tool_calls".into())),
      ],
      true,
    )
    .unwrap();
    assert_eq!(
      events.iter().map(MessagesEvent::kind).collect::<Vec<_>>(),
      [
        "message_start",
        "content_block_start",
        "content_block_delta",
        "content_block_stop",
        "message_delta",
        "message_stop",
      ]
    );
    assert!(matches!(&events[1], MessagesEvent::ContentBlockStart {
      index: 0, content_block: tokn_endpoint_messages::content::ContentBlock::ToolUse { id, name, .. }, ..
    } if id == "call_1" && name == "lookup"));
  }
}
