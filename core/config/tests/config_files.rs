use std::{fs, io::ErrorKind};
use tokn_config::{
  error::Error,
  files::{fragment_paths, ConfigFragments},
  paths::config_fragment_dir,
  ConfigSchema,
};

#[test]
fn source_lookup_and_merged_scores_use_the_same_ordered_files() {
  let dir = tempfile::tempdir().unwrap();
  let primary = dir.path().join("work.toml");
  let fragments = config_fragment_dir(&primary);
  fs::create_dir(&fragments).unwrap();
  let first = fragments.join("a.toml");
  let last = fragments.join("z.toml");
  fs::write(&last, "[model_scores.'gpt-*']\nopenai = 30\n").unwrap();
  fs::write(&first, "[model_scores.'gpt-*']\nopenai = 10\ndeepseek = 20\n").unwrap();
  fs::create_dir(fragments.join("ignored.toml")).unwrap();
  fs::write(fragments.join("notes.txt"), "not TOML").unwrap();

  let files = ConfigFragments::read(&primary, ConfigSchema::Legacy).unwrap();
  fs::remove_file(&last).unwrap();
  let snapshot = files.parse_legacy("[profiles.local]\nmode = 'route'").unwrap();
  let scores = &snapshot.config.model_scores["gpt-*"];
  assert_eq!(scores["openai"], 30);
  assert_eq!(scores["deepseek"], 20);
  assert_eq!(snapshot.fragment_source("model_scores", "gpt-*"), Some(first.as_path()));
  assert_eq!(snapshot.fragment_source("profiles", "local"), None);
  assert_eq!(fragment_paths(&primary).unwrap(), vec![first]);
}

#[test]
fn invalid_fragment_bytes_are_captured_and_reported_at_the_original_path() {
  let dir = tempfile::tempdir().unwrap();
  let primary = dir.path().join("work.toml");
  let fragments = config_fragment_dir(&primary);
  fs::create_dir(&fragments).unwrap();
  let path = fragments.join("invalid.toml");
  fs::write(&path, [0xff]).unwrap();

  let files = ConfigFragments::read(&primary, ConfigSchema::Legacy).unwrap();
  fs::remove_file(&path).unwrap();
  assert_eq!(files.iter().next(), Some((path.as_path(), &[0xff][..])));
  assert!(matches!(
    files.parse_legacy(""),
    Err(Error::Read { path: source_path, source }) if source_path == path && source.kind() == ErrorKind::InvalidData
  ));
}

#[test]
fn v2_skips_even_an_unreadable_fragment_directory() {
  let dir = tempfile::tempdir().unwrap();
  let primary = dir.path().join("work.toml");
  fs::write(config_fragment_dir(&primary), "a file where a directory would be").unwrap();
  let files = ConfigFragments::read(&primary, ConfigSchema::V2).unwrap();
  assert_eq!(files.iter().count(), 0);
}
