use serde_json::{json, Value};
use tokn_convert::{
  convert_request, error::ConvertError, media, provider::Endpoint, responses::request::request_from_endpoint,
  value::responses::request_from_value,
};
use tokn_endpoint_responses::ResponsesRequest;

const TARGETS: [Endpoint; 2] = [Endpoint::ChatCompletions, Endpoint::Messages];
const PAYLOAD: &str = "payload-must-not-appear-in-conversion-errors";

fn request(items: Vec<Value>) -> Value {
  json!({"model":"test-model","input":items})
}

fn native_items() -> Vec<Value> {
  vec![
    json!({"type":"custom_tool_call","call_id":"call-1","name":"edit","input":PAYLOAD}),
    json!({"type":"custom_tool_call_output","call_id":"call-1","output":PAYLOAD}),
    json!({"type":"compaction","encrypted_content":PAYLOAD}),
    json!({"type":"additional_tools","tools":[{"name":"edit","description":PAYLOAD}]}),
    json!({"type":"agent_message","message":PAYLOAD}),
    json!({"type":"item_reference","id":PAYLOAD}),
    json!({"type":"future_item","payload":PAYLOAD}),
  ]
}

fn assert_unsupported(error: ConvertError, kind: &str) {
  assert!(!error.to_string().contains(PAYLOAD));
  let ConvertError::UnsupportedFeature { message } = error else {
    panic!("expected an unsupported item error");
  };
  assert_eq!(
    message,
    format!("Responses input item type `{kind}` has no cross-endpoint representation")
  );
}

#[test]
fn native_input_items_fail_cross_endpoint_and_remain_exact_on_passthrough() {
  for mut item in native_items() {
    for role in [None, Some("user"), Some("assistant"), Some("system"), Some("developer")] {
      if let Some(role) = role {
        // An unknown item is still unknown when it resembles a message. In
        // particular, system/developer extraction must not swallow its tag.
        item["role"] = json!(role);
        item["content"] = json!(PAYLOAD);
      }
      let body = request(vec![
        json!({"role":"user","content":"before"}),
        item.clone(),
        json!({"type":"message","role":"user","content":"after"}),
      ]);
      let kind = item["type"].as_str().unwrap();
      assert_unsupported(request_from_value(&body).unwrap_err(), kind);
      for to in TARGETS {
        assert_unsupported(convert_request(Endpoint::Responses, to, &body).unwrap_err(), kind);
      }
      assert_eq!(
        convert_request(Endpoint::Responses, Endpoint::Responses, &body).unwrap(),
        body
      );
    }
  }
}

#[test]
fn typed_requests_do_not_hide_unknown_or_malformed_item_discriminators() {
  for item in native_items() {
    let body = request(vec![item.clone()]);
    let typed: ResponsesRequest = serde_json::from_value(body).unwrap();
    assert_unsupported(
      request_from_endpoint(&typed).unwrap_err(),
      item["type"].as_str().unwrap(),
    );
  }
  for kind in [Value::Null, json!(false), json!(7), json!([]), json!({}), json!("")] {
    let body = request(vec![json!({"type":kind,"role":"user","content":PAYLOAD})]);
    let typed: ResponsesRequest = serde_json::from_value(body.clone()).unwrap();
    assert_eq!(serde_json::to_value(&typed).unwrap()["input"], body["input"]);
    assert!(matches!(
      request_from_endpoint(&typed),
      Err(ConvertError::BadShape { .. })
    ));
  }
}

#[test]
fn malformed_items_cannot_be_reinterpreted_as_empty_or_user_messages() {
  let mut items = vec![
    Value::Null,
    json!(false),
    json!(7),
    json!([]),
    json!(PAYLOAD),
    json!({}),
    json!({"content":PAYLOAD}),
    json!({"type":"message","content":PAYLOAD}),
    json!({"type":"message","role":null,"content":PAYLOAD}),
    json!({"role":"","content":PAYLOAD}),
    json!({"role":"future_role","content":PAYLOAD}),
    json!({"role":"tool","content":PAYLOAD}),
    json!({"role":"user"}),
    json!({"role":"user","content":null}),
    json!({"role":"user","content":{"text":PAYLOAD}}),
  ];
  for kind in [Value::Null, json!(false), json!(7), json!([]), json!({}), json!("")] {
    items.push(json!({"type":kind,"role":"user","content":PAYLOAD}));
  }
  for item in items {
    let body = request(vec![item]);
    for to in TARGETS {
      let error = convert_request(Endpoint::Responses, to, &body).unwrap_err();
      assert!(matches!(&error, ConvertError::BadShape { .. }));
      assert!(!error.to_string().contains(PAYLOAD));
    }
    assert_eq!(
      convert_request(Endpoint::Responses, Endpoint::Responses, &body).unwrap(),
      body
    );
  }
}

#[test]
fn supported_messages_reasoning_and_tools_still_preserve_their_meaning() {
  let image = json!({"type":"input_image","image_url":"data:image/png;base64,aW1n"});
  let body = request(vec![
    json!({"type":"message","role":"system","content":"system rules"}),
    json!({"role":"developer","content":"developer rules"}),
    json!({"type":"message","role":"user","content":[
      {"type":"input_text","text":"before"},image,{"type":"input_text","text":"after"}
    ]}),
    json!({"type":"reasoning","content":[{"type":"reasoning_text","text":"check the image"}]}),
    json!({"type":"function_call","call_id":"call-1","name":"inspect","arguments":"{\"area\":\"title\"}"}),
    json!({"type":"function_call_output","call_id":"call-1","output":"title found"}),
    json!({"role":"assistant","content":"finished"}),
  ]);
  let source = request_from_value(&body).unwrap();
  assert_eq!(source.system.as_deref(), Some("system rules\n\ndeveloper rules"));
  let source_image = media::content_from_part(Endpoint::Responses, &image).unwrap();
  for to in TARGETS {
    let converted = convert_request(Endpoint::Responses, to, &body).unwrap();
    let messages = converted["messages"].as_array().unwrap();
    let user = messages.iter().find(|message| message["role"] == "user").unwrap();
    let parts = user["content"].as_array().unwrap();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0]["text"], "before");
    assert_eq!(parts[2]["text"], "after");
    assert_eq!(media::content_from_part(to, &parts[1]).unwrap(), source_image);

    let roundtrip = convert_request(to, Endpoint::Responses, &converted).unwrap();
    assert_eq!(roundtrip["instructions"], "system rules\n\ndeveloper rules");
    let items = roundtrip["input"].as_array().unwrap();
    let call = items.iter().find(|item| item["type"] == "function_call").unwrap();
    assert_eq!(call["call_id"], "call-1");
    assert_eq!(call["name"], "inspect");
    assert_eq!(
      serde_json::from_str::<Value>(call["arguments"].as_str().unwrap()).unwrap(),
      json!({"area":"title"})
    );
    let result = items
      .iter()
      .find(|item| item["type"] == "function_call_output")
      .unwrap();
    assert_eq!(result["call_id"], "call-1");
    assert_eq!(result["output"], "title found");
    assert!(items.iter().any(|item| item["role"] == "assistant"
      && item["content"]
        .as_array()
        .is_some_and(|parts| parts.iter().any(|part| part["text"] == "finished"))));

    match to {
      Endpoint::ChatCompletions => assert!(messages
        .iter()
        .any(|item| item["reasoning_content"] == "check the image")),
      Endpoint::Messages => assert!(messages
        .iter()
        .filter_map(|item| item["content"].as_array())
        .flatten()
        .any(|part| part["type"] == "thinking" && part["thinking"] == "check the image")),
      Endpoint::Responses => unreachable!(),
    }
  }
}

#[test]
fn plain_text_and_empty_valid_messages_remain_supported() {
  for body in [
    json!({"model":"test-model","input":"hello"}),
    request(vec![json!({"role":"user","content":""})]),
    request(vec![json!({"type":"message","role":"user","content":[]})]),
  ] {
    let value_ir = request_from_value(&body).unwrap();
    let typed: ResponsesRequest = serde_json::from_value(body.clone()).unwrap();
    let typed_ir = request_from_endpoint(&typed).unwrap();
    assert_eq!(
      serde_json::to_value(value_ir).unwrap(),
      serde_json::to_value(typed_ir).unwrap()
    );
    for to in TARGETS {
      assert!(convert_request(Endpoint::Responses, to, &body).is_ok());
    }
  }
}
