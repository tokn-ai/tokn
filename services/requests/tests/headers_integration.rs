mod support;

use smol_str::SmolStr;
use std::sync::Arc;
use support::*;
use tokn_accounts::AccountHandle;
use tokn_core::provider::{Endpoint, HeaderPatchCtx, ProviderRequestKind};
use tokn_core::AgentId;
use tokn_mock_server::{MockAuthConfig, MockLlmConfig, MockLlmServer};
use tokn_requests::event::{EventPayload, StageEvent};
use tokn_requests::stage_traits::{BuildHeadersStage, ExtractStage, Resolved, ResolvedRoute};
use tokn_requests::stages::{
  AccountSelector, DefaultBuildHeaders, DefaultConvertRequest, DefaultConvertResponse, DefaultExtract, DefaultSend,
  PoolResolve, SelectorOutcome,
};
use tokn_requests::{PipelineError, PipelineRunner, Profile};

const CODEX_CLI_OPENAI_SEND_HEADERS_YAML: &str = include_str!("fixtures/agent_id_headers/codex-cli_openai_send.yaml");
const OPENCODE_OPENAI_SEND_HEADERS_YAML: &str = include_str!("fixtures/agent_id_headers/opencode_openai_send.yaml");
const CLAUDE_CODE_OPENAI_SEND_HEADERS_YAML: &str =
  include_str!("fixtures/agent_id_headers/claude-code_openai_send.yaml");
const CLINE_OPENAI_SEND_HEADERS_YAML: &str = include_str!("fixtures/agent_id_headers/cline_openai_send.yaml");
const COPILOT_CLI_OPENAI_SEND_HEADERS_YAML: &str =
  include_str!("fixtures/agent_id_headers/copilot-cli_openai_send.yaml");

const HEADERS_INPUT_CODEX_CLI_YAML: &str = include_str!("fixtures/headers/input/codex-cli.yaml");
const HEADERS_INPUT_OPENCODE_YAML: &str = include_str!("fixtures/headers/input/opencode.yaml");
const HEADERS_OUTPUT_CODEX_RESPONSES_CODEX_CLI_YAML: &str =
  include_str!("fixtures/headers/output/codex_responses_codex-cli.yaml");
const HEADERS_OUTPUT_CODEX_RESPONSES_OPENCODE_YAML: &str =
  include_str!("fixtures/headers/output/codex_responses_opencode.yaml");
const HEADERS_OUTPUT_COPILOT_RESPONSES_CODEX_CLI_YAML: &str =
  include_str!("fixtures/headers/output/copilot_responses_codex-cli.yaml");

#[tokio::test]
async fn send_compaction_policy_updates_only_codex_compaction_headers_and_body_digests() {
  use bytes::Bytes;
  use tokn_requests::stage_traits::{ConvertRequestStage, SendStage};
  use tokn_requests::stages::{PassthroughBuildHeaders, PassthroughConvertRequest};

  let stale_hint = "model=stale-model;tier=default;region=test;tier=flex;sticky=on";
  for (provider_id, compaction, tier, inbound_hint) in [
    ("codex", true, "auto", Some(stale_hint)),
    ("codex", true, "priority", Some(stale_hint)),
    ("codex", true, "priority", None),
    ("codex", false, "priority", Some(stale_hint)),
    ("openai", true, "priority", Some(stale_hint)),
  ] {
    let prioritizes = provider_id == "codex" && compaction;
    let (handle, seen_headers) = recording_handle(provider_id, "acct-test", ok_response(200, r#"{"output":[]}"#));
    let body = serde_json::json!({
      "model": "gpt-6.1-sol", "stream": false,
      "input": if compaction { serde_json::json!([{"type": "compaction_trigger"}]) } else { serde_json::json!([]) },
      "service_tier": tier
    });
    let decoded = Bytes::from(serde_json::to_vec_pretty(&body).unwrap());
    let original_wire_body = decoded.clone();
    let mut headers = tokn_headers::HeaderMap::new();
    for name in ["content-md5", "digest", "content-digest", "repr-digest"] {
      headers.insert(name, "inbound-digest");
    }
    if let Some(hint) = inbound_hint {
      headers.insert("x-codex-routing-hint", hint);
    }
    let ctx = tokn_requests::PipelineCtx::new(
      "req-compaction-digests",
      Endpoint::Responses.into(),
      Arc::new(tokn_requests::EventBus::new(16)),
    );
    let extracted = DefaultExtract
      .extract(
        &ctx,
        tokn_requests::RawInbound {
          request_endpoint: Endpoint::Responses.into(),
          headers,
          raw_body: decoded.clone(),
          decoded_body: decoded,
          body_json: body,
          request_id: None,
        },
      )
      .await
      .unwrap();
    let resolved = Resolved {
      agent_id: None,
      model: extracted.model.clone(),
      upstream_model: extracted.model.clone(),
      route: ResolvedRoute::operation(Endpoint::Responses, Endpoint::Responses),
      account_id: "acct-test".into(),
      provider_id: provider_id.into(),
      account_handle: handle,
    };
    let headers = PassthroughBuildHeaders::router_auth()
      .build_headers(&ctx, &extracted, &resolved)
      .await
      .unwrap();
    let converted = PassthroughConvertRequest
      .convert_request(&ctx, &extracted, &resolved)
      .await
      .unwrap();
    DefaultSend::new(reqwest::Client::new())
      .send(&ctx, &extracted, &resolved, &headers, &converted)
      .await
      .unwrap();

    let captured = seen_headers.lock().unwrap().clone().unwrap();
    for name in ["content-md5", "digest", "content-digest", "repr-digest"] {
      assert_eq!(
        captured.get(name).map(|value| value.as_str()),
        if prioritizes && tier != "priority" {
          None
        } else {
          Some("inbound-digest")
        }
      );
    }
    if prioritizes {
      let directives: Vec<_> = captured
        .get("x-codex-routing-hint")
        .expect("Codex compaction should have a routing hint")
        .as_str()
        .split(';')
        .map(str::trim)
        .collect();
      assert_eq!(
        directives
          .iter()
          .copied()
          .filter(|directive| directive.starts_with("model="))
          .collect::<Vec<_>>(),
        ["model=gpt-6.1-sol"]
      );
      assert_eq!(
        directives
          .iter()
          .copied()
          .filter(|directive| directive.starts_with("tier="))
          .collect::<Vec<_>>(),
        ["tier=priority"]
      );
      let unrelated = directives
        .iter()
        .copied()
        .filter(|directive| !directive.starts_with("model=") && !directive.starts_with("tier="))
        .collect::<Vec<_>>();
      assert_eq!(
        unrelated,
        if inbound_hint.is_some() {
          vec!["region=test", "sticky=on"]
        } else {
          vec![]
        }
      );
    } else {
      assert_eq!(
        captured.get("x-codex-routing-hint").map(|value| value.as_str()),
        inbound_hint
      );
    }
    let body: serde_json::Value = serde_json::from_slice(&converted.upstream_wire_body).unwrap();
    assert_eq!(body["service_tier"], "priority");
    if !prioritizes || tier == "priority" {
      assert_eq!(converted.upstream_wire_body, original_wire_body);
    }
  }
}

struct AgentHeaderCase {
  name: &'static str,
  agent_id: AgentId,
  provider_id: &'static str,
  fixture_yaml: &'static str,
}

struct HeaderScenario {
  name: &'static str,
  provider_id: &'static str,
  agent_id: AgentId,
  endpoint: Endpoint,
  model: &'static str,
  input_yaml: &'static str,
  output_yaml: &'static str,
}

struct HeaderScenarioSelector {
  provider_id: &'static str,
  endpoint: Endpoint,
  model: &'static str,
  handle: Arc<AccountHandle>,
}

#[async_trait::async_trait]
impl AccountSelector for HeaderScenarioSelector {
  async fn select(
    &self,
    _ctx: &tokn_requests::pipeline::ctx::PipelineCtx,
    _ex: &tokn_requests::stage_traits::Extracted,
  ) -> Result<SelectorOutcome, PipelineError> {
    Ok(SelectorOutcome::Selected {
      account_id: SmolStr::new(self.handle.config.load().id.clone()),
      provider_id: SmolStr::new(self.provider_id),
      upstream_endpoint: Some(self.endpoint),
      upstream_model: SmolStr::new(self.model),
      account_handle: self.handle.clone(),
    })
  }
}

#[tokio::test]
async fn full_pipeline_agent_id_shapes_headers_seen_by_send() {
  let cases = [
    AgentHeaderCase {
      name: "opencode_openai",
      agent_id: AgentId::Opencode,
      provider_id: "openai",
      fixture_yaml: OPENCODE_OPENAI_SEND_HEADERS_YAML,
    },
    AgentHeaderCase {
      name: "codex_cli_openai",
      agent_id: AgentId::CodexCli,
      provider_id: "openai",
      fixture_yaml: CODEX_CLI_OPENAI_SEND_HEADERS_YAML,
    },
    AgentHeaderCase {
      name: "claude_code_openai",
      agent_id: AgentId::ClaudeCode,
      provider_id: "openai",
      fixture_yaml: CLAUDE_CODE_OPENAI_SEND_HEADERS_YAML,
    },
    AgentHeaderCase {
      name: "cline_openai",
      agent_id: AgentId::Cline,
      provider_id: "openai",
      fixture_yaml: CLINE_OPENAI_SEND_HEADERS_YAML,
    },
    AgentHeaderCase {
      name: "copilot_cli_openai",
      agent_id: AgentId::CopilotCli,
      provider_id: "openai",
      fixture_yaml: COPILOT_CLI_OPENAI_SEND_HEADERS_YAML,
    },
  ];

  for case in cases {
    let (bus, _log) = capture_bus();
    let (handle, seen_client_headers) = recording_handle(
      case.provider_id,
      "acct-1",
      ok_response(
        200,
        r#"{"id":"resp-agent-id","choices":[{"message":{"role":"assistant","content":"hi"}}]}"#,
      ),
    );
    let selector = Arc::new(HeaderScenarioSelector {
      provider_id: case.provider_id,
      endpoint: Endpoint::ChatCompletions,
      model: "glm-4",
      handle,
    });

    let profile = Arc::new(Profile::full(
      "smoke-agent-id-headers",
      Arc::new(FixedAgentExtract {
        agent_id: case.agent_id.clone(),
      }),
      Arc::new(PoolResolve::new(selector)),
      Arc::new(DefaultBuildHeaders::with_provider_defaults()),
      Arc::new(DefaultConvertRequest),
      Arc::new(DefaultSend::new(reqwest::Client::new())),
      Arc::new(DefaultConvertResponse::new()),
    ));
    let runner = PipelineRunner::new(profile, bus);

    runner
      .run(raw_chat("glm-4"))
      .await
      .unwrap_or_else(|err| panic!("{}: pipeline should succeed: {err}", case.name));

    let seen = seen_client_headers
      .lock()
      .unwrap()
      .clone()
      .unwrap_or_else(|| panic!("{}: provider should observe client headers", case.name));
    assert_headers_match_fixture(&seen, case.fixture_yaml, case.name);
  }
}

#[tokio::test]
async fn provider_headers_patch_from_fixtures() {
  let scenarios = [
    HeaderScenario {
      name: "codex_responses_opencode",
      provider_id: "codex",
      agent_id: AgentId::Opencode,
      endpoint: Endpoint::Responses,
      model: "gpt-5-codex",
      input_yaml: HEADERS_INPUT_OPENCODE_YAML,
      output_yaml: HEADERS_OUTPUT_CODEX_RESPONSES_OPENCODE_YAML,
    },
    HeaderScenario {
      name: "codex_responses_codex-cli",
      provider_id: "codex",
      agent_id: AgentId::CodexCli,
      endpoint: Endpoint::Responses,
      model: "gpt-5-codex",
      input_yaml: HEADERS_INPUT_CODEX_CLI_YAML,
      output_yaml: HEADERS_OUTPUT_CODEX_RESPONSES_CODEX_CLI_YAML,
    },
    HeaderScenario {
      name: "copilot_responses_codex-cli",
      provider_id: "github-copilot",
      agent_id: AgentId::CodexCli,
      endpoint: Endpoint::Responses,
      model: "gpt-5",
      input_yaml: HEADERS_INPUT_OPENCODE_YAML,
      output_yaml: HEADERS_OUTPUT_COPILOT_RESPONSES_CODEX_CLI_YAML,
    },
  ];

  for scenario in scenarios {
    let provider = provider_fixture(scenario.provider_id);
    let ctx = tokn_requests::PipelineCtx::new(
      format!("req-{}-headers", scenario.name),
      scenario.endpoint.into(),
      Arc::new(tokn_requests::EventBus::new(64)),
    );
    let mut extracted = DefaultExtract
      .extract(
        &ctx,
        raw_responses(scenario.model, headers_from_fixture(scenario.input_yaml), false),
      )
      .await
      .unwrap_or_else(|err| panic!("{}: extract should succeed: {err}", scenario.name));
    extracted.agent_id = Some(scenario.agent_id.clone());
    let resolved = Resolved {
      agent_id: Some(scenario.agent_id.clone()),
      model: extracted.model.clone(),
      upstream_model: SmolStr::new(scenario.model),
      route: ResolvedRoute::operation(scenario.endpoint, scenario.endpoint),
      account_id: SmolStr::new(provider.handle.config.load().id.clone()),
      provider_id: SmolStr::new(scenario.provider_id),
      account_handle: provider.handle.clone(),
    };
    let built = DefaultBuildHeaders::with_provider_defaults()
      .build_headers(&ctx, &extracted, &resolved)
      .await
      .unwrap_or_else(|err| panic!("{}: build_headers should succeed: {err}", scenario.name));
    let mut headers = built.headers.clone();
    resolved
      .account_handle
      .provider
      .patch_headers(
        &mut headers,
        &HeaderPatchCtx {
          request_kind: ProviderRequestKind::Operation(scenario.endpoint),
          body: extracted.body_json.as_ref(),
          bearer_token: provider.bearer_token,
          content_encoding: extracted.content_encoding.map(|encoding| encoding.as_str()),
          stream: extracted.stream,
          initiator: extracted.initiator.as_deref().unwrap_or("user"),
          inbound_headers: &extracted.headers,
          vars: &built.vars,
          agent_id: &built.agent_id,
        },
      )
      .unwrap_or_else(|err| panic!("{}: patch_headers should succeed: {err}", scenario.name));

    assert_headers_match_fixture(&headers, scenario.output_yaml, scenario.name);
  }
}

#[tokio::test]
async fn full_pipeline_codex_headers_are_captured_after_build_and_patch() {
  let server = MockLlmServer::start(MockLlmConfig::default().with_auth(MockAuthConfig::bearer(["atk-codex"]))).await;
  let (bus, log) = capture_bus();
  let selector = Arc::new(HeaderScenarioSelector {
    provider_id: "codex",
    endpoint: Endpoint::Responses,
    model: "gpt-5-codex",
    handle: codex_handle(server.base_url()),
  });

  let profile = Arc::new(Profile::full(
    "smoke-codex-headers",
    Arc::new(FixedAgentExtract {
      agent_id: AgentId::CodexCli,
    }),
    Arc::new(PoolResolve::new(selector)),
    Arc::new(DefaultBuildHeaders::with_provider_defaults()),
    Arc::new(DefaultConvertRequest),
    Arc::new(DefaultSend::new(reqwest::Client::new())),
    Arc::new(DefaultConvertResponse::new()),
  ));
  let runner = PipelineRunner::new(profile, bus);

  let converted = runner
    .run(raw_responses(
      "gpt-5-codex",
      headers_from_fixture(HEADERS_INPUT_CODEX_CLI_YAML),
      false,
    ))
    .await
    .expect("codex responses pipeline must succeed");

  assert_eq!(converted.status, 200);
  let events = drain_until_completed(&log).await;
  let built_headers = events
    .iter()
    .find_map(|event| match &event.payload {
      EventPayload::Stage(StageEvent::BuildHeaders(headers)) => Some(headers.headers.clone()),
      _ => None,
    })
    .expect("BuildHeaders event should be emitted before Send");
  assert_eq!(
    built_headers.get("session_id").map(|value| value.as_str()),
    Some("019e271b-4023-7081-be3e-7a69d97138a2"),
    "BuildHeaders should carry session correlation before provider auth patching"
  );
  assert_eq!(
    built_headers.get("OpenAI-Beta").map(|value| value.as_str()),
    Some("responses=v1"),
    "BuildHeaders should include the Codex overlay beta before provider normalization"
  );

  let captured = server
    .last_request()
    .expect("mock server should capture the upstream request");
  assert_eq!(captured.path, "/responses");
  for HeaderFixtureEntry { name, value } in load_header_fixture(HEADERS_OUTPUT_CODEX_RESPONSES_CODEX_CLI_YAML) {
    if name.eq_ignore_ascii_case("x-request-id") {
      continue;
    }
    assert_eq!(
      captured.header(&name),
      Some(value.as_str()),
      "captured Codex output header mismatch for {name}"
    );
  }
  assert_eq!(
    captured.header("x-request-id"),
    Some("req-headers"),
    "captured Codex request id should come from this pipeline run"
  );
}

#[tokio::test]
async fn managed_codex_compaction_routing_hint_matches_rewritten_wire_model() {
  use bytes::Bytes;
  use tokn_core::request_event::RecordEvent;

  let server = MockLlmServer::start(MockLlmConfig::default().with_auth(MockAuthConfig::bearer(["atk-codex"]))).await;
  let (bus, log) = capture_bus();
  let selector = Arc::new(HeaderScenarioSelector {
    provider_id: "codex",
    endpoint: Endpoint::Responses,
    model: "gpt-6.1-sol",
    handle: codex_handle(server.base_url()),
  });
  let profile = Arc::new(Profile::full(
    "managed-codex-compaction-routing-hint",
    Arc::new(FixedAgentExtract {
      agent_id: AgentId::CodexCli,
    }),
    Arc::new(PoolResolve::new(selector)),
    Arc::new(DefaultBuildHeaders::with_provider_defaults()),
    Arc::new(DefaultConvertRequest),
    Arc::new(DefaultSend::new(reqwest::Client::new())),
    Arc::new(DefaultConvertResponse::new()),
  ));
  let runner = PipelineRunner::new(profile, bus);
  let mut headers = headers_from_fixture(HEADERS_INPUT_CODEX_CLI_YAML);
  headers.insert("x-codex-routing-hint", "model=stale-model;tier=default;region=test");
  headers.append("x-codex-routing-hint", "tier=flex;sticky=on");
  let body = serde_json::json!({
    "model": "client-model-alias",
    "input": [{"type": "compaction_trigger"}],
    "service_tier": "auto",
    "stream": false,
  });
  let decoded = Bytes::from(serde_json::to_vec(&body).unwrap());
  let response = runner
    .run(tokn_requests::RawInbound {
      request_endpoint: Endpoint::Responses.into(),
      headers,
      raw_body: decoded.clone(),
      decoded_body: decoded,
      body_json: body,
      request_id: Some("req-managed-compaction-routing-hint".into()),
    })
    .await
    .expect("managed Codex compaction pipeline should succeed");
  assert_eq!(response.status, 200);

  let captured = server
    .last_request()
    .expect("mock server should capture the managed Codex request");
  let wire_body: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
  assert_eq!(wire_body["model"], "gpt-6.1-sol");
  assert_eq!(wire_body["service_tier"], "priority");
  let wire_hint = captured
    .header("x-codex-routing-hint")
    .expect("routing hint should survive Codex header normalization");
  let mut directives: Vec<_> = wire_hint.split(';').map(str::trim).collect();
  directives.sort_unstable();
  assert_eq!(
    directives,
    ["model=gpt-6.1-sol", "region=test", "sticky=on", "tier=priority"]
  );

  let events = drain_until_completed(&log).await;
  let (recorded_headers, recorded_body) = events
    .iter()
    .find_map(|event| match &event.payload {
      EventPayload::Record(RecordEvent::UpstreamReq { headers, body, .. }) => Some((headers, body)),
      _ => None,
    })
    .expect("managed Codex request should emit a wire-accurate UpstreamReq record");
  assert_eq!(
    recorded_headers.get("x-codex-routing-hint").map(|value| value.as_str()),
    Some(wire_hint)
  );
  assert_eq!(recorded_body, &captured.body);
}
