use anyhow::{anyhow, Context, Result};
use jsonc_parser::cst::{CstObject, CstRootNode};
use jsonc_parser::ParseOptions;
use serde_json::Value;
use std::path::Path;

pub(crate) fn read_jsonc(path: &Path) -> Result<Value> {
  let raw = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
  parse_jsonc(&raw, path)
}

pub(crate) fn parse_jsonc(raw: &str, path: &Path) -> Result<Value> {
  let root = parse_cst(raw, path)?;
  let value = root
    .to_serde_value()
    .ok_or_else(|| anyhow!("{} must contain a JSON object", path.display()))?;
  if !value.is_object() {
    return Err(anyhow!("{} must contain a JSON object", path.display()));
  }
  Ok(value)
}

pub(crate) fn parse_cst(raw: &str, path: &Path) -> Result<CstRootNode> {
  CstRootNode::parse(raw, &opencode_parse_options()).with_context(|| format!("parsing {}", path.display()))
}

pub(crate) fn set_property(object: &CstObject, name: &str, value: impl Into<jsonc_parser::cst::CstInputValue>) {
  let value = value.into();
  match object.get(name) {
    Some(property) => property.set_value(value),
    None => {
      object.append(name, value);
    }
  }
}

fn opencode_parse_options() -> ParseOptions {
  ParseOptions {
    allow_comments: true,
    allow_loose_object_property_names: false,
    allow_trailing_commas: true,
    allow_missing_commas: false,
    allow_single_quoted_strings: false,
    allow_hexadecimal_numbers: false,
    allow_unary_plus_numbers: false,
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use jsonc_parser::json;

  #[test]
  fn parses_comments_and_trailing_commas() {
    let raw = r#"
{
  // line comment
  "url": "https://example.com//not-a-comment",
  "items": [
    "a",
  ],
}
"#;

    let parsed = parse_jsonc(raw, Path::new("opencode.jsonc")).unwrap();

    assert_eq!(parsed["url"], "https://example.com//not-a-comment");
    assert_eq!(parsed["items"][0], "a");
  }

  #[test]
  fn rejects_unterminated_block_comments() {
    assert!(parse_jsonc(r#"{"model":"openai/gpt-5"} /* unfinished"#, Path::new("opencode.jsonc")).is_err());
  }

  #[test]
  fn rejects_non_object_roots() {
    for raw in ["[]", "null", r#""openai/gpt-5""#] {
      let error = parse_jsonc(raw, Path::new("opencode.jsonc")).unwrap_err();
      assert!(error.to_string().contains("must contain a JSON object"));
    }
  }

  #[test]
  fn cst_edits_preserve_comments() {
    let root = parse_cst("{\n  // keep me\n  \"provider\": {},\n}\n", Path::new("opencode.jsonc")).unwrap();
    let object = root.object_value_or_set();
    let providers = object.object_value_or_set("provider");
    set_property(&providers, "openai", json!({"name": "router"}));

    let output = root.to_string();
    assert!(output.contains("// keep me"));
    assert!(output.contains("\"openai\""));
  }
}
