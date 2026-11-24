use crate::route::MockRoute;

#[derive(Clone, Debug)]
pub struct MockLlmConfig {
  pub auth: Option<MockAuthConfig>,
  pub routes: Vec<MockRoute>,
  pub required_headers: Vec<HeaderExpectation>,
  pub forbidden_headers: Vec<String>,
}

impl Default for MockLlmConfig {
  fn default() -> Self {
    Self {
      auth: None,
      routes: vec![
        MockRoute::models(["mock-model"]),
        MockRoute::chat_completions(),
        MockRoute::responses(),
        MockRoute::messages(),
      ],
      required_headers: Vec::new(),
      forbidden_headers: Vec::new(),
    }
  }
}

impl MockLlmConfig {
  pub fn with_auth(mut self, auth: MockAuthConfig) -> Self {
    self.auth = Some(auth);
    self
  }

  pub fn with_route(mut self, route: MockRoute) -> Self {
    self.routes.push(route);
    self
  }

  pub fn require_header(mut self, header: HeaderExpectation) -> Self {
    self.required_headers.push(header);
    self
  }

  pub fn forbid_header(mut self, name: impl Into<String>) -> Self {
    self.forbidden_headers.push(name.into());
    self
  }
}

#[derive(Clone, Debug)]
pub struct MockAuthConfig {
  pub header_name: String,
  pub accepted_values: Vec<String>,
}

impl MockAuthConfig {
  pub fn bearer<I, S>(accepted_tokens: I) -> Self
  where
    I: IntoIterator<Item = S>,
    S: Into<String>,
  {
    Self {
      header_name: "authorization".into(),
      accepted_values: accepted_tokens
        .into_iter()
        .map(|token| format!("Bearer {}", token.into()))
        .collect(),
    }
  }

  pub fn header<I, S>(name: impl Into<String>, accepted_values: I) -> Self
  where
    I: IntoIterator<Item = S>,
    S: Into<String>,
  {
    Self {
      header_name: name.into(),
      accepted_values: accepted_values.into_iter().map(Into::into).collect(),
    }
  }
}

#[derive(Clone, Debug)]
pub struct HeaderExpectation {
  pub name: String,
  pub value: Option<String>,
}

impl HeaderExpectation {
  pub fn present(name: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      value: None,
    }
  }

  pub fn equals(name: impl Into<String>, value: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      value: Some(value.into()),
    }
  }
}
