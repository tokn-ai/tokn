use super::*;
use crate::driver::Session as _;
use core::convert::Infallible;
use serde_json::json;

struct NoNetwork;
impl Transport for NoNetwork {
  type Error = Infallible;
  async fn send(&mut self, _: Request) -> Result<ResponseHead, Infallible> {
    panic!("media routing must finish before network I/O")
  }
  async fn read(&mut self, _: &mut [u8]) -> Result<usize, Infallible> {
    panic!("media routing must finish before network I/O")
  }
}
struct Time;
impl Clock for Time {
  fn now(&self) -> Duration {
    Duration::ZERO
  }
  fn unix_seconds(&self) -> u64 {
    100
  }
  async fn sleep(&mut self, _: Duration) {
    panic!("media routing must not sleep")
  }
}
struct Silent;
impl Events for Silent {
  fn enabled(&self) -> bool {
    false
  }
  fn emit(&self, _: tokn_core::request_event::RequestEvent) {
    unreachable!()
  }
}

fn state() -> Shared<ExecutionState> {
  let config = serde_json::from_value(json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":"upstream"}},
    "providers":{"upstream":{"driver":"openai","base_url":"https://backend.example/v1"}}
  }))
  .unwrap();
  let rules = tokn_config::v2::compile_execution(&config).unwrap();
  let accounts = ["first", "second"]
    .into_iter()
    .map(|id| {
      serde_json::from_value(json!({
        "id":id, "provider":"upstream", "api_key":"fixture-token"
      }))
      .unwrap()
    })
    .collect();
  let state = ExecutionState::new(rules, accounts).unwrap();
  for account in ["first", "second"] {
    state.set_binding_models(
      &ProviderId::new("upstream").unwrap(),
      account,
      BTreeMap::from([(
        SmolStr::new("model"),
        ModelCapabilities {
          endpoints: Some(vec![Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages]),
          ..Default::default()
        },
      )]),
    );
  }
  Shared::new(state)
}

fn image_input() -> RequestInput {
  RequestInput::new(
    Endpoint::Responses,
    json!({"model":"model","input":[{"role":"user","content":[
      {"type":"input_image","image_url":"https://example.invalid/image.png"}
    ]}]}),
    HeaderMap::new(),
    "media-route".into(),
  )
}

fn asset(state: &ExecutionState) -> ProviderAssetReference {
  ProviderAssetReference {
    id: "file-fixture".into(),
    namespace: "openai.files".into(),
    scope: state.bindings[1]
      .continuation_origin(Endpoint::Responses, "model")
      .asset_scope(),
    expires_at: Some(200),
  }
}

#[tokio::test]
async fn skips_known_incompatible_accounts_but_keeps_unknown_capabilities() {
  let state = state();
  state.bindings[0].models.with(|models| {
    models.get_mut("model").unwrap().input_modalities = Some(vec![MediaKind::Text]);
  });
  let mut session = Session::new(state.clone(), image_input(), NoNetwork, Time, Silent);
  let extracted = session.extract().await.unwrap();
  let resolved = session.resolve(&extracted).await.unwrap();
  assert!(matches!(resolved.target, Target::Account { binding: 1, .. }));

  state.bindings[1].models.with(|models| {
    models.get_mut("model").unwrap().input_modalities = Some(Vec::new());
  });
  let mut session = Session::new(state, image_input(), NoNetwork, Time, Silent);
  let extracted = session.extract().await.unwrap();
  assert!(matches!(
    session.resolve(&extracted).await,
    Err(Error {
      kind: ErrorKind::NoAccount { .. },
      ..
    })
  ));
}

#[tokio::test]
async fn skips_unrepresentable_endpoints_before_selecting_a_fallback() {
  let mut image = image_input();
  image.body["input"][0]["content"][0]["detail"] = json!("high");
  let document = RequestInput::new(
    Endpoint::Messages,
    json!({"model":"model","messages":[{"role":"user","content":[
      {"type":"document","title":"Source title","source":{"type":"text","media_type":"text/plain","data":"Reference text"}}
    ]}]}),
    HeaderMap::new(),
    "document-route".into(),
  );
  for (input, incompatible, compatible) in [
    (image, Endpoint::Messages, Endpoint::Responses),
    (document, Endpoint::Responses, Endpoint::Messages),
  ] {
    let state = state();
    for (binding, endpoint) in state.bindings.iter().zip([incompatible, compatible]) {
      binding.models.with(|models| {
        models.get_mut("model").unwrap().endpoints = Some(vec![endpoint]);
      });
    }
    let mut session = Session::new(state, input, NoNetwork, Time, Silent);
    let extracted = session.extract().await.unwrap();
    let resolved = session.resolve(&extracted).await.unwrap();
    assert!(matches!(resolved.target, Target::Account { binding: 1, .. }));
    assert_eq!(resolved.endpoint, compatible);
  }
}

#[tokio::test]
async fn provider_assets_pin_the_account_and_reject_wrong_backend_or_expiry() {
  let state = state();
  let mut input = image_input();
  input.media_assets.push(asset(&state));
  let mut session = Session::new(state.clone(), input, NoNetwork, Time, Silent);
  let extracted = session.extract().await.unwrap();
  let resolved = session.resolve(&extracted).await.unwrap();
  assert!(matches!(resolved.target, Target::Account { binding: 1, .. }));

  let mut input = image_input();
  let mut wrong = asset(&state);
  wrong.scope.backend = tokn_core::util::digest::sha256_hex(b"different backend");
  input.media_assets.push(wrong);
  let mut session = Session::new(state.clone(), input, NoNetwork, Time, Silent);
  let extracted = session.extract().await.unwrap();
  assert!(session.resolve(&extracted).await.is_err());

  let mut input = image_input();
  let mut expired = asset(&state);
  expired.expires_at = Some(100);
  input.media_assets.push(expired);
  let mut session = Session::new(state, input, NoNetwork, Time, Silent);
  let extracted = session.extract().await.unwrap();
  assert!(session
    .resolve(&extracted)
    .await
    .err()
    .unwrap()
    .message
    .contains("expired"));
}

#[tokio::test]
async fn output_requirements_are_checked_before_network_io() {
  let state = state();
  for binding in &state.bindings {
    binding.models.with(|models| {
      models.get_mut("model").unwrap().output_modalities = Some(vec![MediaKind::Text]);
    });
  }
  let input = RequestInput::new(
    Endpoint::ChatCompletions,
    json!({"model":"model",
      "messages":[{"role":"user","content":"speak"}], "modalities":["audio"],
      "audio":{"voice":"alloy","format":"wav"}
    }),
    HeaderMap::new(),
    "audio-route".into(),
  );
  let mut session = Session::new(state, input, NoNetwork, Time, Silent);
  let extracted = session.extract().await.unwrap();
  assert!(session.resolve(&extracted).await.is_err());
}

#[tokio::test]
async fn checkpoint_preflight_uses_its_own_limit_before_sending() {
  for streaming in [false, true] {
    let mut input = image_input();
    input.body["stream"] = json!(streaming);
    input.continuation = Some(ContinuationRequest::default());
    let session = Session::new(state(), input, NoNetwork, Time, Silent).with_limits(Limits {
      max_checkpoint_bytes: 16,
      ..Limits::default()
    });
    let error = session.run().await.err().unwrap();
    assert_eq!(error.stage, "convert_request");
    assert!(error.message.contains("checkpoint byte limit"));
  }

  let mut input = image_input();
  input.continuation = Some(ContinuationRequest::default());
  let mut session = Session::new(state(), input, NoNetwork, Time, Silent).with_limits(Limits {
    max_response_bytes: 1,
    max_checkpoint_bytes: 4096,
    ..Limits::default()
  });
  let extracted = session.extract().await.unwrap();
  let resolved = session.resolve(&extracted).await.unwrap();
  let headers = session.build_headers(&extracted, &resolved).await.unwrap();
  assert!(session.convert_request(&extracted, &resolved, &headers).await.is_ok());
}
