use std::collections::HashMap;
use std::path::Path;

use super::super::days::request_day_files;
use super::super::effective_limit;
use super::super::requests::{list_day_requests_best_effort, RequestListOptions, RequestSummary};
use super::{SessionDetail, SessionSummary};
use crate::Result;

/// Return the most recently active sessions inferred from request history.
pub fn list_sessions(requests_dir: &Path, limit: Option<usize>) -> Result<Vec<SessionSummary>> {
  let mut sessions = collect_sessions(requests_dir)?;
  sessions.sort_by(|left, right| {
    right
      .last_ts
      .cmp(&left.last_ts)
      .then_with(|| left.session_id.cmp(&right.session_id))
  });
  sessions.truncate(effective_limit(limit));
  Ok(sessions)
}

/// Return a chronological, bounded timeline inferred from request history.
pub fn get_session(requests_dir: &Path, session_id: &str, limit: Option<usize>) -> Result<Option<SessionDetail>> {
  let options = RequestListOptions {
    session_id: Some(session_id.to_string()),
    ..RequestListOptions::default()
  };
  let mut session = None;
  let mut requests = Vec::new();

  for day_file in request_day_files(requests_dir)? {
    for request in list_day_requests_best_effort(&day_file, &options, None) {
      if request.session_id.as_deref() != Some(session_id) {
        continue;
      }
      if let Some(summary) = session.as_mut() {
        update_session_summary(summary, &request);
      } else {
        session = Some(new_session_summary(session_id, &request));
      }
      requests.push(request);
    }
  }

  let Some(session) = session else {
    return Ok(None);
  };

  requests.sort_by(|left, right| {
    left
      .ts
      .cmp(&right.ts)
      .then_with(|| left.day.cmp(&right.day))
      .then_with(|| left.request_id.cmp(&right.request_id))
  });
  let limit = effective_limit(limit);
  if requests.len() > limit {
    requests.drain(..requests.len() - limit);
  }

  Ok(Some(SessionDetail { session, requests }))
}

fn collect_sessions(requests_dir: &Path) -> Result<Vec<SessionSummary>> {
  let mut sessions = HashMap::<String, SessionSummary>::new();

  for day_file in request_day_files(requests_dir)? {
    for request in list_day_requests_best_effort(&day_file, &RequestListOptions::default(), None) {
      let Some(session_id) = request.session_id.as_deref() else {
        continue;
      };
      match sessions.get_mut(session_id) {
        Some(summary) => {
          summary.first_ts = summary.first_ts.min(request.ts);
          summary.request_count += 1;
          if (request.ts, request.day.as_str(), request.request_id.as_str())
            > (
              summary.last_ts,
              summary.last_request_day.as_str(),
              summary.last_request_id.as_str(),
            )
          {
            summary.last_ts = request.ts;
            summary.last_request_day = request.day;
            summary.last_request_id = request.request_id;
            summary.endpoint = request.endpoint;
            summary.status = request.status;
            summary.account_id = request.account_id;
            summary.provider_id = request.provider_id;
            summary.model = request.model;
          }
        }
        None => {
          sessions.insert(session_id.to_string(), new_session_summary(session_id, &request));
        }
      }
    }
  }

  Ok(sessions.into_values().collect())
}

fn new_session_summary(session_id: &str, request: &RequestSummary) -> SessionSummary {
  SessionSummary {
    session_id: session_id.to_string(),
    source: None,
    first_ts: request.ts,
    last_ts: request.ts,
    request_count: 1,
    last_request_day: request.day.clone(),
    last_request_id: request.request_id.clone(),
    endpoint: request.endpoint.clone(),
    status: request.status,
    account_id: request.account_id.clone(),
    provider_id: request.provider_id.clone(),
    model: request.model.clone(),
  }
}

fn update_session_summary(summary: &mut SessionSummary, request: &RequestSummary) {
  summary.first_ts = summary.first_ts.min(request.ts);
  summary.request_count += 1;
  if (request.ts, request.day.as_str(), request.request_id.as_str())
    > (
      summary.last_ts,
      summary.last_request_day.as_str(),
      summary.last_request_id.as_str(),
    )
  {
    summary.last_ts = request.ts;
    summary.last_request_day = request.day.clone();
    summary.last_request_id = request.request_id.clone();
    summary.endpoint = request.endpoint.clone();
    summary.status = request.status;
    summary.account_id = request.account_id.clone();
    summary.provider_id = request.provider_id.clone();
    summary.model = request.model.clone();
  }
}
