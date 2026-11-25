//! Desktop-only inspector queries. All reads use the existing read-only viewer APIs.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokn_persistence::viewer::{
  get_request_llm_message, get_request_llm_summary, get_request_llm_tool_definition, get_request_payload,
  RequestCursor, RequestPayloadField,
};
use tokn_persistence::{
  get_request, get_session_from_db, get_session_node_from_db, get_session_usage, is_valid_request_day,
  list_latest_requests, list_request_days, list_request_url_paths, list_requests, list_sessions_from_db, DbPaths,
  RequestListOptions,
};

fn deserialize_row_id<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<i64>, D::Error> {
  Option::<String>::deserialize(deserializer)?
    .map(|value| value.parse().map_err(serde::de::Error::custom))
    .transpose()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestsQuery {
  day: Option<String>,
  limit: Option<usize>,
  cursor: Option<String>,
  session_id: Option<String>,
  provider_id: Option<String>,
  url_path: Option<String>,
  status: Option<u16>,
  #[serde(default)]
  errors_only: bool,
  query: Option<String>,
}

impl From<RequestsQuery> for RequestListOptions {
  fn from(query: RequestsQuery) -> Self {
    Self {
      day: query.day,
      limit: query.limit,
      cursor: None,
      session_id: query.session_id,
      provider_id: query.provider_id,
      url_path: query.url_path,
      status: query.status,
      errors_only: query.errors_only,
      query: query.query,
    }
  }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitQuery {
  limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestPageQuery {
  limit: Option<usize>,
  cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestDayQuery {
  day: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestDetailQuery {
  day: String,
  request_id: String,
  #[serde(default, deserialize_with = "deserialize_row_id")]
  row_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestPayloadQuery {
  day: String,
  request_id: String,
  #[serde(default, deserialize_with = "deserialize_row_id")]
  row_id: Option<i64>,
  field: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestLlmItemQuery {
  day: String,
  request_id: String,
  #[serde(default, deserialize_with = "deserialize_row_id")]
  row_id: Option<i64>,
  index: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDetailQuery {
  session_id: String,
  limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionNodeDetailQuery {
  session_id: String,
  node_id: String,
}

#[derive(Debug, Serialize)]
pub struct ViewerInfo {
  requests_dir: String,
  sessions_db: String,
  usage_db: String,
}

#[derive(Debug, Serialize)]
pub struct ApiError {
  status: u16,
  message: String,
}

impl ApiError {
  fn internal(error: impl std::fmt::Display) -> Self {
    Self {
      status: 500,
      message: error.to_string(),
    }
  }

  fn not_found(kind: &str) -> Self {
    Self {
      status: 404,
      message: format!("{kind} not found"),
    }
  }

  fn bad_request(message: &str) -> Self {
    Self {
      status: 400,
      message: message.to_string(),
    }
  }

  fn unavailable(kind: &str) -> Self {
    Self {
      status: 503,
      message: format!("{kind} unavailable"),
    }
  }

  fn unavailable_message(message: impl Into<String>) -> Self {
    Self {
      status: 503,
      message: message.into(),
    }
  }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InspectQuery {
  Info,
  RequestDays,
  RequestUrlPaths(RequestDayQuery),
  Requests(RequestsQuery),
  LatestRequests(RequestPageQuery),
  Request(RequestDetailQuery),
  RequestPayload(RequestPayloadQuery),
  RequestLlmSummary(RequestDetailQuery),
  RequestLlmMessage(RequestLlmItemQuery),
  RequestLlmToolDefinition(RequestLlmItemQuery),
  Sessions(LimitQuery),
  Session(SessionDetailQuery),
  SessionUsage(SessionDetailQuery),
  SessionNode(SessionNodeDetailQuery),
}

#[tauri::command]
pub async fn inspect_query(query: InspectQuery) -> Result<serde_json::Value, ApiError> {
  let paths = tokio::task::spawn_blocking(|| {
    crate::config::load()?
      .persistence()
      .resolve_paths()
      .map_err(anyhow::Error::from)
  })
  .await
  .map_err(ApiError::internal)?
  .map_err(ApiError::internal)?;
  dispatch(paths, query).await
}

async fn dispatch(paths: DbPaths, query: InspectQuery) -> Result<serde_json::Value, ApiError> {
  match query {
    InspectQuery::Info => info(paths).await,
    InspectQuery::RequestDays => request_days(paths.requests_dir).await,
    InspectQuery::RequestUrlPaths(query) => request_url_paths(paths.requests_dir, query).await,
    InspectQuery::Requests(query) => requests(paths.requests_dir, query).await,
    InspectQuery::LatestRequests(query) => latest_requests(paths.requests_dir, query).await,
    InspectQuery::Request(query) => request_detail(paths.requests_dir, query).await,
    InspectQuery::RequestPayload(query) => request_payload(paths.requests_dir, query).await,
    InspectQuery::RequestLlmSummary(query) => request_llm_summary(paths.requests_dir, query).await,
    InspectQuery::RequestLlmMessage(query) => request_llm_message(paths.requests_dir, query).await,
    InspectQuery::RequestLlmToolDefinition(query) => request_llm_tool_definition(paths.requests_dir, query).await,
    InspectQuery::Sessions(query) => sessions(paths.sessions_db, query).await,
    InspectQuery::Session(query) => session_detail(paths.sessions_db, query).await,
    InspectQuery::SessionUsage(query) => session_usage(paths.usage_db, query).await,
    InspectQuery::SessionNode(query) => session_node_detail(paths.sessions_db, query).await,
  }
}

async fn info(paths: DbPaths) -> Result<serde_json::Value, ApiError> {
  json_response(ViewerInfo {
    requests_dir: paths.requests_dir.display().to_string(),
    sessions_db: paths.sessions_db.display().to_string(),
    usage_db: paths.usage_db.display().to_string(),
  })
}

fn json_response<T: Serialize>(body: T) -> Result<serde_json::Value, ApiError> {
  serde_json::to_value(body).map_err(ApiError::internal)
}
async fn request_days(requests_dir: PathBuf) -> Result<serde_json::Value, ApiError> {
  let days = tokio::task::spawn_blocking(move || list_request_days(&requests_dir))
    .await
    .map_err(ApiError::internal)?
    .map_err(ApiError::internal)?;
  json_response(days)
}

async fn request_url_paths(requests_dir: PathBuf, query: RequestDayQuery) -> Result<serde_json::Value, ApiError> {
  validate_request_day(&query.day)?;
  let day = query.day;
  let paths = tokio::task::spawn_blocking(move || list_request_url_paths(&requests_dir, &day))
    .await
    .map_err(ApiError::internal)?
    .map_err(|_| ApiError::unavailable("request day"))?;
  json_response(paths)
}

async fn requests(requests_dir: PathBuf, query: RequestsQuery) -> Result<serde_json::Value, ApiError> {
  if let Some(day) = query.day.as_deref() {
    validate_request_day(day)?;
  }
  let cursor = parse_request_cursor(query.cursor.as_deref())?;
  if let (Some(day), Some(cursor)) = (query.day.as_deref(), cursor.as_ref()) {
    if cursor.day() != day {
      return Err(ApiError::bad_request("cursor does not belong to the selected day"));
    }
  }
  let selected_day = query.day.is_some();
  let mut options: RequestListOptions = query.into();
  options.cursor = cursor;
  let requests = tokio::task::spawn_blocking(move || list_requests(&requests_dir, &options))
    .await
    .map_err(ApiError::internal)?;
  let requests = requests.map_err(|error| {
    if selected_day {
      ApiError::unavailable("request day")
    } else {
      ApiError::internal(error)
    }
  })?;
  json_response(requests)
}

async fn latest_requests(requests_dir: PathBuf, query: RequestPageQuery) -> Result<serde_json::Value, ApiError> {
  let limit = query.limit;
  let cursor = parse_request_cursor(query.cursor.as_deref())?;
  let requests = tokio::task::spawn_blocking(move || list_latest_requests(&requests_dir, limit, cursor))
    .await
    .map_err(ApiError::internal)?
    .map_err(ApiError::internal)?;
  json_response(requests)
}

fn parse_request_cursor(cursor: Option<&str>) -> Result<Option<RequestCursor>, ApiError> {
  cursor
    .map(RequestCursor::decode)
    .transpose()
    .map_err(|_| ApiError::bad_request("cursor is malformed or unsupported"))
}

async fn request_detail(requests_dir: PathBuf, query: RequestDetailQuery) -> Result<serde_json::Value, ApiError> {
  validate_request_day(&query.day)?;
  let day = query.day;
  let request_id = query.request_id;
  let row_id = query.row_id;
  let request = tokio::task::spawn_blocking(move || get_request(&requests_dir, &day, &request_id, row_id))
    .await
    .map_err(ApiError::internal)?
    .map_err(|_| ApiError::unavailable("request day"))?;
  let request = request.ok_or_else(|| ApiError::not_found("request"))?;
  json_response(request)
}

async fn request_payload(requests_dir: PathBuf, query: RequestPayloadQuery) -> Result<serde_json::Value, ApiError> {
  validate_request_day(&query.day)?;
  let field = query
    .field
    .parse::<RequestPayloadField>()
    .map_err(|_| ApiError::bad_request("field is not a supported request payload field"))?;
  let day = query.day;
  let request_id = query.request_id;
  let row_id = query.row_id;
  let payload =
    tokio::task::spawn_blocking(move || get_request_payload(&requests_dir, &day, &request_id, row_id, field))
      .await
      .map_err(ApiError::internal)?
      .map_err(|_| ApiError::unavailable("request day"))?;
  let payload = payload.ok_or_else(|| ApiError::not_found("request payload"))?;
  json_response(payload)
}

async fn request_llm_summary(requests_dir: PathBuf, query: RequestDetailQuery) -> Result<serde_json::Value, ApiError> {
  validate_request_day(&query.day)?;
  let day = query.day;
  let request_id = query.request_id;
  let row_id = query.row_id;
  let summary = tokio::task::spawn_blocking(move || get_request_llm_summary(&requests_dir, &day, &request_id, row_id))
    .await
    .map_err(ApiError::internal)?
    .map_err(|_| ApiError::unavailable("request day"))?;
  let summary = summary.ok_or_else(|| ApiError::not_found("request LLM summary"))?;
  json_response(summary)
}

async fn request_llm_message(requests_dir: PathBuf, query: RequestLlmItemQuery) -> Result<serde_json::Value, ApiError> {
  validate_request_day(&query.day)?;
  let day = query.day;
  let request_id = query.request_id;
  let row_id = query.row_id;
  let index = query.index;
  let message =
    tokio::task::spawn_blocking(move || get_request_llm_message(&requests_dir, &day, &request_id, row_id, index))
      .await
      .map_err(ApiError::internal)?
      .map_err(|_| ApiError::unavailable("request day"))?;
  let message = message.ok_or_else(|| ApiError::not_found("request LLM message"))?;
  json_response(message)
}

async fn request_llm_tool_definition(
  requests_dir: PathBuf,
  query: RequestLlmItemQuery,
) -> Result<serde_json::Value, ApiError> {
  validate_request_day(&query.day)?;
  let day = query.day;
  let request_id = query.request_id;
  let row_id = query.row_id;
  let index = query.index;
  let definition = tokio::task::spawn_blocking(move || {
    get_request_llm_tool_definition(&requests_dir, &day, &request_id, row_id, index)
  })
  .await
  .map_err(ApiError::internal)?
  .map_err(|_| ApiError::unavailable("request day"))?;
  let definition = definition.ok_or_else(|| ApiError::not_found("request LLM tool definition"))?;
  json_response(definition)
}

fn validate_request_day(day: &str) -> Result<(), ApiError> {
  if is_valid_request_day(day) {
    Ok(())
  } else {
    Err(ApiError::bad_request("day must be a UTC date in YYYY-MM-DD format"))
  }
}

async fn sessions(sessions_db: PathBuf, query: LimitQuery) -> Result<serde_json::Value, ApiError> {
  let limit = query.limit;
  let sessions = tokio::task::spawn_blocking(move || list_sessions_from_db(&sessions_db, limit))
    .await
    .map_err(ApiError::internal)?
    .map_err(session_database_error)?;
  json_response(sessions)
}

async fn session_detail(sessions_db: PathBuf, query: SessionDetailQuery) -> Result<serde_json::Value, ApiError> {
  let session_id = query.session_id;
  let limit = query.limit;
  let session = tokio::task::spawn_blocking(move || get_session_from_db(&sessions_db, &session_id, limit))
    .await
    .map_err(ApiError::internal)?
    .map_err(session_database_error)?;
  let session = session.ok_or_else(|| ApiError::not_found("session"))?;
  json_response(session)
}

async fn session_node_detail(
  sessions_db: PathBuf,
  query: SessionNodeDetailQuery,
) -> Result<serde_json::Value, ApiError> {
  let session_id = query.session_id;
  let node_id = query.node_id;
  let node = tokio::task::spawn_blocking(move || get_session_node_from_db(&sessions_db, &session_id, &node_id))
    .await
    .map_err(ApiError::internal)?
    .map_err(session_database_error)?;
  let node = node.ok_or_else(|| ApiError::not_found("session node"))?;
  json_response(node)
}

async fn session_usage(usage_db: PathBuf, query: SessionDetailQuery) -> Result<serde_json::Value, ApiError> {
  let session_id = query.session_id;
  let usage = tokio::task::spawn_blocking(move || get_session_usage(&usage_db, &session_id))
    .await
    .map_err(ApiError::internal)?
    .map_err(usage_database_error)?;
  json_response(usage)
}

fn session_database_error(error: tokn_persistence::Error) -> ApiError {
  match error {
    tokn_persistence::Error::UnsupportedSessionSchema { .. } => ApiError::unavailable_message(
      "sessions database requires migration; migrate the selected database before opening the sessions view",
    ),
    _ => ApiError::unavailable("session database"),
  }
}

fn usage_database_error(error: tokn_persistence::Error) -> ApiError {
  match error {
    tokn_persistence::Error::UnsupportedUsageSchema { .. } => ApiError::unavailable_message(
      "usage database requires migration; migrate the selected database before opening session usage",
    ),
    _ => ApiError::unavailable("usage database"),
  }
}

#[cfg(test)]
mod tests;
