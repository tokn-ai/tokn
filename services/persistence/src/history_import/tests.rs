use super::*;

struct TestDirectory(PathBuf);

impl TestDirectory {
  fn new() -> Self {
    let path = std::env::temp_dir().join(format!("tokn-history-safeguards-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&path).unwrap();
    Self(path)
  }
}

impl Drop for TestDirectory {
  fn drop(&mut self) {
    let _ = fs::remove_dir_all(&self.0);
  }
}

fn capture_table(conn: &Connection, name: &str) -> TableCapture {
  let schema = columns(conn, "main", name).unwrap();
  let columns: Vec<_> = schema.iter().map(|column| column.name.clone()).collect();
  let key_indexes = schema
    .iter()
    .enumerate()
    .filter_map(|(index, column)| (column.primary_key != 0).then_some(index))
    .collect();
  let selected = columns
    .iter()
    .map(|column| quote(column))
    .collect::<Vec<_>>()
    .join(", ");
  TableCapture {
    name: name.into(),
    schema,
    columns,
    key_indexes,
    foreign_keys: rows(conn, &format!("PRAGMA foreign_key_list({})", quote(name))).unwrap(),
    rows: rows(conn, &format!("SELECT {selected} FROM {}", quote(name))).unwrap(),
  }
}

#[test]
fn deferred_foreign_keys_check_complete_composite_keys_and_nullable_references() {
  let mut conn = Connection::open_in_memory().unwrap();
  conn
    .execute_batch(
      "PRAGMA foreign_keys=ON;
     CREATE TABLE parent (namespace TEXT, id TEXT, PRIMARY KEY(namespace, id));
     CREATE TABLE tags (id TEXT PRIMARY KEY);
     CREATE TABLE child (id INTEGER PRIMARY KEY, namespace TEXT, parent_id TEXT, tag TEXT,
       FOREIGN KEY(namespace, parent_id) REFERENCES parent(namespace, id),
       FOREIGN KEY(tag) REFERENCES tags(id));
     INSERT INTO parent VALUES ('a', 'one'), ('b', 'two');
     INSERT INTO tags VALUES ('known');",
    )
    .unwrap();
  let tx = conn.transaction().unwrap();
  tx.execute_batch(
    "PRAGMA defer_foreign_keys=ON;
     INSERT INTO child VALUES (1, 'a', 'one', 'known'), (2, NULL, 'absent', NULL), (3, 'absent', NULL, NULL);",
  )
  .unwrap();
  validate_imported_foreign_keys(&tx, "main", &capture_table(&tx, "child")).unwrap();

  // Both components exist individually, but their pair does not.
  tx.execute("INSERT INTO child VALUES (4, 'a', 'two', 'known')", [])
    .unwrap();
  assert!(
    validate_imported_foreign_keys(&tx, "main", &capture_table(&tx, "child"))
      .unwrap_err()
      .to_string()
      .contains("Imported foreign key missing")
  );
  tx.execute("INSERT INTO parent VALUES ('a', 'two')", []).unwrap();
  validate_imported_foreign_keys(&tx, "main", &capture_table(&tx, "child")).unwrap();

  tx.execute("INSERT INTO child VALUES (5, 'a', 'one', 'missing-tag')", [])
    .unwrap();
  assert!(validate_imported_foreign_keys(&tx, "main", &capture_table(&tx, "child")).is_err());
  tx.rollback().unwrap();
  assert_eq!(
    rows(&conn, "SELECT count(*) FROM child").unwrap(),
    vec![vec![Value::Integer(0)]]
  );
}

#[test]
fn implicit_foreign_key_columns_are_reported_as_unsupported() {
  let conn = Connection::open_in_memory().unwrap();
  conn
    .execute_batch(
      "CREATE TABLE parent (id INTEGER PRIMARY KEY);
     CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent);
     INSERT INTO parent VALUES (1);
     INSERT INTO child VALUES (1, 1);",
    )
    .unwrap();
  let error = validate_imported_foreign_keys(&conn, "main", &capture_table(&conn, "child")).unwrap_err();
  assert!(error.to_string().contains("Unsupported implicit foreign key schema"));
}

#[test]
fn cleanup_preserves_a_replacement_file_at_the_original_path() {
  let directory = TestDirectory::new();
  let path = directory.0.join("new-day.db");
  fs::write(&path, b"original").unwrap();
  let guard = CreatedDays(vec![(path.clone(), same_file::Handle::from_path(&path).unwrap())]);
  let renamed = directory.0.join("renamed.db");
  fs::rename(&path, &renamed).unwrap();
  fs::write(&path, b"replacement").unwrap();
  drop(guard);
  assert_eq!(fs::read(&path).unwrap(), b"replacement");
  assert_eq!(fs::read(renamed).unwrap(), b"original");
}

#[test]
fn cleanup_tolerates_a_file_already_removed() {
  let directory = TestDirectory::new();
  let path = directory.0.join("new-day.db");
  fs::write(&path, []).unwrap();
  let guard = CreatedDays(vec![(path.clone(), same_file::Handle::from_path(&path).unwrap())]);
  fs::remove_file(&path).unwrap();
  drop(guard);
  assert!(!path.exists());
}

#[test]
fn source_revalidation_rejects_committed_changes_and_new_sidecars() {
  let directory = TestDirectory::new();
  let path = directory.0.join("source.db");
  let conn = Connection::open(&path).unwrap();
  conn
    .execute_batch("CREATE TABLE captured (value TEXT); INSERT INTO captured VALUES ('before');")
    .unwrap();
  drop(conn);
  let fingerprint = digest_file(&path).unwrap();
  verify_source_unchanged(&path, &fingerprint).unwrap();

  let conn = Connection::open(&path).unwrap();
  conn.execute("UPDATE captured SET value='after'", []).unwrap();
  drop(conn);
  assert!(verify_source_unchanged(&path, &fingerprint)
    .unwrap_err()
    .to_string()
    .contains("Source changed while reading"));

  let fingerprint = digest_file(&path).unwrap();
  fs::write(sidecar(&path, "-journal"), []).unwrap();
  assert!(verify_source_unchanged(&path, &fingerprint)
    .unwrap_err()
    .to_string()
    .contains("stopped and checkpointed"));
}
