use super::*;
use crate::schema::{ConfigSchema, SchemaMarkerError};
use std::path::Path;
use tokn_policy::GatewayPlan;

/// Decode a version 2 document without compiling references.
///
/// Migration code uses this boundary to validate emitted syntax before the
/// semantic compiler links it into a runtime plan.
pub fn decode(contents: &str, source: &Path) -> Result<RawConfig> {
  let document: toml::Value = toml::from_str(contents).map_err(|source_error| Error::Parse {
    path: source.to_path_buf(),
    source: source_error,
  })?;
  match crate::schema::detect_toml(&document) {
    Ok(ConfigSchema::V2) => {}
    Ok(ConfigSchema::Legacy) => {
      return Err(Error::MissingSchemaVersion {
        path: source.to_path_buf(),
      });
    }
    Err(SchemaMarkerError::NonInteger) => {
      return Err(Error::InvalidSchemaVersion {
        path: source.to_path_buf(),
      });
    }
    Err(SchemaMarkerError::Unsupported(found)) => {
      return Err(Error::UnsupportedSchemaVersion {
        path: source.to_path_buf(),
        found,
      });
    }
  }

  document.try_into().map_err(|source_error| Error::Parse {
    path: source.to_path_buf(),
    source: source_error,
  })
}

/// Read and strictly decode a version 2 document.
///
/// Unlike the legacy loader, a missing file is always an error.
pub fn load_raw(path: &Path) -> Result<RawConfig> {
  let contents = std::fs::read_to_string(path).map_err(|source| Error::Read {
    path: path.to_path_buf(),
    source,
  })?;
  decode(&contents, path)
}

/// Compile a decoded version 2 document into its routing graph.
///
/// This is the semantic boundary: identifiers are canonicalized, references
/// are resolved, matcher precedence is preserved, and invalid combinations
/// within the document are rejected. A separate runtime linker resolves
/// provider catalogue defaults and runtime/plugin-owned names; it must also
/// succeed before any listener starts.
pub fn compile(raw: &RawConfig, source: &Path) -> Result<GatewayPlan> {
  compile_config(raw, source).map(|config| config.into_parts().0)
}

/// Compile both the routing graph and process-wide service settings.
pub fn compile_config(raw: &RawConfig, source: &Path) -> Result<CompiledConfig> {
  validate_schema_version(raw, source)?;

  compile::compile_config(raw, source).map_err(|source_error| Error::Compile {
    path: source.to_path_buf(),
    source: Box::new(source_error),
  })
}

/// Compile execution policy and service settings without compiling listeners.
///
/// This accepts both listener-free SDK documents and gateway documents. Gateway
/// admission settings are outside this boundary; all execution references and
/// service settings still receive the same validation as a gateway config.
pub fn compile_execution_config(raw: &RawConfig, source: &Path) -> Result<CompiledExecutionConfig> {
  validate_schema_version(raw, source)?;
  compile::compile_execution_config(raw).map_err(|source_error| Error::Compile {
    path: source.to_path_buf(),
    source: Box::new(source_error),
  })
}

fn validate_schema_version(raw: &RawConfig, source: &Path) -> Result<()> {
  if raw.schema_version != SCHEMA_VERSION {
    return Err(Error::UnsupportedSchemaVersion {
      path: source.to_path_buf(),
      found: i64::from(raw.schema_version),
    });
  }

  Ok(())
}

/// Strictly decode and semantically compile a version 2 document.
pub fn parse(contents: &str, source: &Path) -> Result<GatewayPlan> {
  parse_config(contents, source).map(|config| config.into_parts().0)
}

/// Strictly decode and compile the routing graph and service settings.
pub fn parse_config(contents: &str, source: &Path) -> Result<CompiledConfig> {
  let raw = decode(contents, source)?;
  compile_config(&raw, source)
}

/// Read, strictly decode, and semantically compile a version 2 document.
///
/// Unlike the legacy loader, a missing file is always an error.
pub fn load(path: &Path) -> Result<GatewayPlan> {
  load_config(path).map(|config| config.into_parts().0)
}

/// Read and compile the routing graph and process-wide service settings.
pub fn load_config(path: &Path) -> Result<CompiledConfig> {
  let raw = load_raw(path)?;
  compile_config(&raw, path)
}

/// Strictly load v2 execution policy and service settings for an in-process client.
pub fn load_execution_config(path: &Path) -> Result<CompiledExecutionConfig> {
  let raw = load_raw(path)?;
  compile_execution_config(&raw, path)
}

#[cfg(test)]
mod tests {
  use super::*;

  const EMPTY_V2: &str = "schema_version = 2\n";

  #[test]
  fn version_errors_are_distinct() {
    let path = Path::new("config.toml");

    assert!(matches!(decode("", path), Err(Error::MissingSchemaVersion { .. })));
    assert!(matches!(
      decode("schema_version = \"2\"", path),
      Err(Error::InvalidSchemaVersion { .. })
    ));
    assert!(matches!(
      decode("schema_version = 1", path),
      Err(Error::UnsupportedSchemaVersion { found: 1, .. })
    ));
    assert_eq!(decode(EMPTY_V2, path).unwrap().schema_version, SCHEMA_VERSION);
  }

  #[test]
  fn malformed_toml_is_a_parse_error() {
    let error = decode("schema_version = [", Path::new("broken.toml")).unwrap_err();
    assert!(matches!(error, Error::Parse { .. }));
  }

  #[test]
  fn missing_file_is_not_replaced_with_defaults() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing.toml");
    let error = load_raw(&path).unwrap_err();
    assert!(matches!(error, Error::Read { source, .. } if source.kind() == std::io::ErrorKind::NotFound));
  }

  #[test]
  fn execution_config_uses_shared_policy_and_service_validation_without_listeners() {
    let path = Path::new("config.toml");
    let raw = decode(
      "schema_version = 2\n[defaults]\n[service.outbound]\nproxy_url = 'http://localhost:8080'\n",
      path,
    )
    .unwrap();
    let compiled = compile_execution_config(&raw, path).unwrap();
    assert_eq!(compiled.execution(), &super::super::compile_execution(&raw).unwrap());
    assert_eq!(
      compiled.service().outbound().proxy_url(),
      Some("http://localhost:8080/")
    );
    assert!(matches!(compile_config(&raw, path), Err(Error::Compile { .. })));

    let mut invalid = raw;
    invalid.service.outbound.use_system_proxy = true;
    assert!(matches!(
      compile_execution_config(&invalid, path),
      Err(Error::Compile { source, .. })
        if matches!(*source, CompileError::InvalidValue { ref location, .. } if location == "service.outbound")
    ));
  }
}
