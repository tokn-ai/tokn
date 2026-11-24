use rusqlite::{params_from_iter, types::Value as SqlValue, Connection};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};
use tokn_persistence::migrate::{self, Bootstrap, Migration};

const REQUESTS_V0_0_0: &str = include_str!("../schemas/snapshot/requests/v0.0.0.sql");
const REQUESTS_V0_1_1: &str = include_str!("../schemas/snapshot/requests/v0.1.1.sql");
const REQUESTS_V0_2_0: &str = include_str!("../schemas/snapshot/requests/v0.2.0.sql");
const REQUESTS_SQUASH_V0_1_1: &str = include_str!("../schemas/squash/requests/v0.0.0_v0.1.1_0001_0006.sql");
const REQUESTS_SQUASH_V0_2_0: &str = include_str!("../schemas/squash/requests/v0.1.1_v0.2.0_0006_0008.sql");
const REQUESTS_MIGRATIONS: &[Migration] = &[
  Migration {
    version: 1,
    name: "initial",
    sql: REQUESTS_V0_0_0,
  },
  Migration {
    version: 2,
    name: "add_correlation_and_error",
    sql: include_str!("../schemas/migrations/requests/0002_add_correlation_and_error.sql"),
  },
  Migration {
    version: 3,
    name: "add_usage_breakdown",
    sql: include_str!("../schemas/migrations/requests/0003_add_usage_breakdown.sql"),
  },
  Migration {
    version: 4,
    name: "add_response_header_latency",
    sql: include_str!("../schemas/migrations/requests/0004_add_response_header_latency.sql"),
  },
  Migration {
    version: 5,
    name: "add_source_and_method",
    sql: include_str!("../schemas/migrations/requests/0005_add_source_and_method.sql"),
  },
  Migration {
    version: 6,
    name: "add_context_and_metrics",
    sql: include_str!("../schemas/migrations/requests/0006_add_context_and_metrics.sql"),
  },
  Migration {
    version: 7,
    name: "split_requests",
    sql: include_str!("../schemas/migrations/requests/0007_split_requests.sql"),
  },
  Migration {
    version: 8,
    name: "metadata_json",
    sql: include_str!("../schemas/migrations/requests/0008_metadata_json.sql"),
  },
];

const SESSIONS_V0_0_0: &str = include_str!("../schemas/snapshot/sessions/v0.0.0.sql");
const SESSIONS_V0_1_1: &str = include_str!("../schemas/snapshot/sessions/v0.1.1.sql");
const SESSIONS_V0_2_0: &str = include_str!("../schemas/snapshot/sessions/v0.2.0.sql");
const SESSIONS_V0_2_1: &str = include_str!("../schemas/snapshot/sessions/v0.2.1.sql");
const SESSIONS_SQUASH_V0_1_1: &str = include_str!("../schemas/squash/sessions/v0.0.0_v0.1.1_0001_0001.sql");
const SESSIONS_SQUASH_V0_2_0: &str = include_str!("../schemas/squash/sessions/v0.1.1_v0.2.0_0001_0002.sql");
const SESSIONS_SQUASH_V0_2_1: &str = include_str!("../schemas/squash/sessions/v0.2.0_v0.2.1_0004_0005.sql");
const SESSIONS_MIGRATIONS: &[Migration] = &[
  Migration {
    version: 1,
    name: "initial",
    sql: SESSIONS_V0_0_0,
  },
  Migration {
    version: 2,
    name: "tree_nodes",
    sql: include_str!("../schemas/migrations/sessions/0002_tree_nodes.sql"),
  },
  Migration {
    version: 3,
    name: "session_views",
    sql: include_str!("../schemas/migrations/sessions/0003_session_views.sql"),
  },
  Migration {
    version: 4,
    name: "thread_lineage",
    sql: include_str!("../schemas/migrations/sessions/0004_thread_lineage.sql"),
  },
  Migration {
    version: 5,
    name: "message_tree",
    sql: include_str!("../schemas/migrations/sessions/0005_message_tree.sql"),
  },
];

const USAGE_V0_0_0: &str = include_str!("../schemas/snapshot/usage/v0.0.0.sql");
const USAGE_V0_1_1: &str = include_str!("../schemas/snapshot/usage/v0.1.1.sql");
const USAGE_V0_2_0: &str = include_str!("../schemas/snapshot/usage/v0.2.0.sql");
const USAGE_V0_2_1: &str = include_str!("../schemas/snapshot/usage/v0.2.1.sql");
const USAGE_SQUASH_V0_1_1: &str = include_str!("../schemas/squash/usage/v0.0.0_v0.1.1_0001_0004.sql");
const USAGE_SQUASH_V0_2_0: &str = include_str!("../schemas/squash/usage/v0.1.1_v0.2.0_0004_0005.sql");
const USAGE_SQUASH_V0_2_1: &str = include_str!("../schemas/squash/usage/v0.2.0_v0.2.1_0005_0006.sql");
const USAGE_MIGRATIONS: &[Migration] = &[
  Migration {
    version: 1,
    name: "initial",
    sql: USAGE_V0_0_0,
  },
  Migration {
    version: 2,
    name: "add_correlation_ids",
    sql: include_str!("../schemas/migrations/usage/0002_add_correlation_ids.sql"),
  },
  Migration {
    version: 3,
    name: "lifecycle_columns",
    sql: include_str!("../schemas/migrations/usage/0003_lifecycle_columns.sql"),
  },
  Migration {
    version: 4,
    name: "add_usage_breakdown",
    sql: include_str!("../schemas/migrations/usage/0004_add_usage_breakdown.sql"),
  },
  Migration {
    version: 5,
    name: "request_metadata",
    sql: include_str!("../schemas/migrations/usage/0005_request_metadata.sql"),
  },
  Migration {
    version: 6,
    name: "add_user",
    sql: include_str!("../schemas/migrations/usage/0006_add_user.sql"),
  },
];

#[derive(Clone, Copy)]
struct DbCase {
  name: &'static str,
  v0_0_0: &'static str,
  target_snapshot: &'static str,
  target_squash: &'static str,
  target_version: u32,
  squash_start_version: u32,
  migrations: &'static [Migration],
  meta_json: &'static str,
  seed_jsonl: &'static str,
  expected_jsonl: &'static str,
}

const REQUESTS_V0_1_1_CASE: DbCase = DbCase {
  name: "requests",
  v0_0_0: REQUESTS_V0_0_0,
  target_snapshot: REQUESTS_V0_1_1,
  target_squash: REQUESTS_SQUASH_V0_1_1,
  target_version: 6,
  squash_start_version: 1,
  migrations: REQUESTS_MIGRATIONS,
  meta_json: include_str!("fixtures/requests_meta_v0.1.1.json"),
  seed_jsonl: include_str!("fixtures/requests_seed_v0.1.1.jsonl"),
  expected_jsonl: include_str!("fixtures/requests_expected_v0.1.1.jsonl"),
};

const REQUESTS_V0_2_0_CASE: DbCase = DbCase {
  name: "requests",
  v0_0_0: REQUESTS_V0_0_0,
  target_snapshot: REQUESTS_V0_2_0,
  target_squash: REQUESTS_SQUASH_V0_2_0,
  target_version: 8,
  squash_start_version: 6,
  migrations: REQUESTS_MIGRATIONS,
  meta_json: include_str!("fixtures/requests_meta_v0.2.0.json"),
  seed_jsonl: include_str!("fixtures/requests_seed_v0.2.0.jsonl"),
  expected_jsonl: include_str!("fixtures/requests_expected_v0.2.0.jsonl"),
};

const SESSIONS_CASE: DbCase = DbCase {
  name: "sessions",
  v0_0_0: SESSIONS_V0_0_0,
  target_snapshot: SESSIONS_V0_1_1,
  target_squash: SESSIONS_SQUASH_V0_1_1,
  target_version: 1,
  squash_start_version: 1,
  migrations: SESSIONS_MIGRATIONS,
  meta_json: include_str!("fixtures/sessions_meta_v0.1.1.json"),
  seed_jsonl: include_str!("fixtures/sessions_seed_v0.1.1.jsonl"),
  expected_jsonl: include_str!("fixtures/sessions_expected_v0.1.1.jsonl"),
};

const SESSIONS_V0_2_0_CASE: DbCase = DbCase {
  name: "sessions",
  v0_0_0: SESSIONS_V0_0_0,
  target_snapshot: SESSIONS_V0_2_0,
  target_squash: SESSIONS_SQUASH_V0_2_0,
  target_version: 4,
  squash_start_version: 1,
  migrations: SESSIONS_MIGRATIONS,
  meta_json: include_str!("fixtures/sessions_meta_v0.2.0.json"),
  seed_jsonl: include_str!("fixtures/sessions_seed_v0.1.1.jsonl"),
  expected_jsonl: include_str!("fixtures/sessions_expected_v0.2.0.jsonl"),
};

const SESSIONS_V0_2_1_CASE: DbCase = DbCase {
  name: "sessions",
  v0_0_0: SESSIONS_V0_0_0,
  target_snapshot: SESSIONS_V0_2_1,
  target_squash: SESSIONS_SQUASH_V0_2_1,
  target_version: 5,
  squash_start_version: 4,
  migrations: SESSIONS_MIGRATIONS,
  meta_json: include_str!("fixtures/sessions_meta_v0.2.1.json"),
  seed_jsonl: include_str!("fixtures/sessions_seed_v0.1.1.jsonl"),
  expected_jsonl: include_str!("fixtures/sessions_expected_v0.2.1.jsonl"),
};

const USAGE_CASE: DbCase = DbCase {
  name: "usage",
  v0_0_0: USAGE_V0_0_0,
  target_snapshot: USAGE_V0_1_1,
  target_squash: USAGE_SQUASH_V0_1_1,
  target_version: 4,
  squash_start_version: 1,
  migrations: USAGE_MIGRATIONS,
  meta_json: include_str!("fixtures/usage_meta_v0.1.1.json"),
  seed_jsonl: include_str!("fixtures/usage_seed_v0.1.1.jsonl"),
  expected_jsonl: include_str!("fixtures/usage_expected_v0.1.1.jsonl"),
};

const USAGE_V0_2_0_CASE: DbCase = DbCase {
  name: "usage",
  v0_0_0: USAGE_V0_0_0,
  target_snapshot: USAGE_V0_2_0,
  target_squash: USAGE_SQUASH_V0_2_0,
  target_version: 5,
  squash_start_version: 4,
  migrations: USAGE_MIGRATIONS,
  meta_json: include_str!("fixtures/usage_meta_v0.2.0.json"),
  seed_jsonl: include_str!("fixtures/usage_seed_v0.1.1.jsonl"),
  expected_jsonl: include_str!("fixtures/usage_expected_v0.2.0.jsonl"),
};

const USAGE_V0_2_1_CASE: DbCase = DbCase {
  name: "usage",
  v0_0_0: USAGE_V0_0_0,
  target_snapshot: USAGE_V0_2_1,
  target_squash: USAGE_SQUASH_V0_2_1,
  target_version: 6,
  squash_start_version: 5,
  migrations: USAGE_MIGRATIONS,
  meta_json: include_str!("fixtures/usage_meta_v0.2.1.json"),
  seed_jsonl: include_str!("fixtures/usage_seed_v0.1.1.jsonl"),
  expected_jsonl: include_str!("fixtures/usage_expected_v0.2.1.jsonl"),
};

#[test]
fn requests_v0_1_1_migrations_and_squash_match_fixture() {
  assert_case(REQUESTS_V0_1_1_CASE);
}

#[test]
fn requests_v0_2_0_migrations_and_squash_match_fixture() {
  assert_case(REQUESTS_V0_2_0_CASE);
}

#[test]
fn sessions_v0_1_1_migrations_and_squash_match_fixture() {
  assert_case(SESSIONS_CASE);
}

#[test]
fn sessions_v0_2_0_migrations_and_squash_match_fixture() {
  assert_case(SESSIONS_V0_2_0_CASE);
}

#[test]
fn sessions_v0_2_1_migrations_and_squash_match_fixture() {
  assert_case(SESSIONS_V0_2_1_CASE);
}

#[test]
fn usage_v0_1_1_migrations_and_squash_match_fixture() {
  assert_case(USAGE_CASE);
}

#[test]
fn usage_v0_2_0_migrations_and_squash_match_fixture() {
  assert_case(USAGE_V0_2_0_CASE);
}

#[test]
fn usage_v0_2_1_migrations_and_squash_match_fixture() {
  assert_case(USAGE_V0_2_1_CASE);
}

fn assert_case(case: DbCase) {
  let fixture = parse_fixture(case);
  let dir = tempdir(case.name);

  let incremental_path = dir.join("incremental.db");
  seed_v0_db(&incremental_path, case, &fixture);
  let incremental_state = run_incremental(&incremental_path, case);

  let squash_path = dir.join("squash.db");
  seed_v0_db(&squash_path, case, &fixture);
  let squash_state = run_squash(&squash_path, case);

  assert_eq!(incremental_state.version, case.target_version);
  assert_eq!(squash_state.version, case.target_version);
  assert_expected_tables(&incremental_state.tables, &fixture);
  assert_expected_tables(&squash_state.tables, &fixture);
  assert_eq!(
    incremental_state.tables, squash_state.tables,
    "{} migrated and squashed results diverged",
    case.name
  );
}

fn run_incremental(path: &Path, case: DbCase) -> DbState {
  let mut conn = Connection::open(path).unwrap();
  // The fixture seeding path already created the v0.0.0 schema and marked
  // migration version 1 as applied, so `migrate::apply` skips the bootstrap
  // branch here and only runs pending migrations.
  migrate::apply(
    &mut conn,
    path,
    case.name,
    Bootstrap {
      sql: case.target_snapshot,
    },
    migrations_through(case),
  )
  .unwrap();
  dump_state(&conn)
}

fn run_squash(path: &Path, case: DbCase) -> DbState {
  if case.squash_start_version > 1 {
    let mut conn = Connection::open(path).unwrap();
    migrate::apply(
      &mut conn,
      path,
      case.name,
      Bootstrap { sql: REQUESTS_V0_1_1 },
      migrations_through_version(case.migrations, case.squash_start_version),
    )
    .unwrap();
  }
  let conn = Connection::open(path).unwrap();
  conn.execute_batch(case.target_squash).unwrap();
  mark_versions_after(&conn, case);
  dump_state(&conn)
}

fn seed_v0_db(path: &Path, case: DbCase, fixture: &Fixture) {
  let conn = Connection::open(path).unwrap();
  ensure_schema_table(&conn);
  conn.execute_batch(case.v0_0_0).unwrap();
  for row in &fixture.seed_rows {
    insert_row(&conn, row, &fixture.meta.input_columns);
  }
  conn
    .execute(
      "INSERT INTO schema_migrations (version, name, applied_ts) VALUES (1, ?1, 0)",
      [case.migrations[0].name],
    )
    .unwrap();
}

fn insert_row(conn: &Connection, row: &FixtureRow, columns_by_table: &std::collections::HashMap<String, Vec<String>>) {
  let columns = columns_by_table
    .get(&row.table)
    .unwrap_or_else(|| panic!("missing input columns for {}", row.table));
  let placeholders = (1..=columns.len())
    .map(|i| format!("?{i}"))
    .collect::<Vec<_>>()
    .join(", ");
  let sql = format!(
    "INSERT INTO \"{}\" ({}) VALUES ({})",
    row.table.replace('"', "\"\""),
    columns.join(", "),
    placeholders
  );
  let values = columns
    .iter()
    .map(|column| json_to_sql(row.values.get(column).unwrap_or(&Value::Null)))
    .collect::<Vec<_>>();
  conn.execute(&sql, params_from_iter(values)).unwrap();
}

fn ensure_schema_table(conn: &Connection) {
  conn
    .execute_batch(
      "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_ts INTEGER NOT NULL);",
    )
    .unwrap();
}

fn migrations_through(case: DbCase) -> &'static [Migration] {
  migrations_through_version(case.migrations, case.target_version)
}

fn migrations_through_version(migrations: &'static [Migration], version: u32) -> &'static [Migration] {
  let end = migrations
    .iter()
    .position(|m| m.version == version)
    .map(|idx| idx + 1)
    .expect("target migration version must exist");
  &migrations[..end]
}

fn mark_versions_after(conn: &Connection, case: DbCase) {
  for migration in case
    .migrations
    .iter()
    .filter(|m| m.version > case.squash_start_version && m.version <= case.target_version)
  {
    conn
      .execute(
        "INSERT INTO schema_migrations (version, name, applied_ts) VALUES (?1, ?2, 0)",
        rusqlite::params![migration.version, migration.name],
      )
      .unwrap();
  }
}

#[derive(Debug, PartialEq)]
struct DbState {
  version: u32,
  tables: Map<String, Value>,
}

fn dump_state(conn: &Connection) -> DbState {
  let version = migrate::read_current_version(conn).unwrap();
  let table_names = conn
    .prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name <> 'schema_migrations' ORDER BY name",
    )
    .unwrap()
    .query_map([], |row| row.get::<_, String>(0))
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap();
  let mut tables = Map::new();
  for table in table_names {
    tables.insert(table.clone(), Value::Array(read_table_rows(conn, &table)));
  }
  DbState { version, tables }
}

fn read_table_rows(conn: &Connection, table: &str) -> Vec<Value> {
  let sql = format!("SELECT rowid, * FROM \"{}\" ORDER BY rowid", table.replace('"', "\"\""));
  let mut stmt = conn.prepare(&sql).unwrap();
  let col_names: Vec<String> = (1..stmt.column_count())
    .map(|i| stmt.column_name(i).unwrap_or("").to_string())
    .collect();
  let rows = stmt
    .query_map([], |row| {
      let mut out = Map::new();
      for (offset, name) in col_names.iter().enumerate() {
        let value = row.get_ref(offset + 1)?;
        out.insert(name.clone(), value_to_json(value));
      }
      Ok(Value::Object(out))
    })
    .unwrap();
  rows.collect::<rusqlite::Result<Vec<_>>>().unwrap()
}

fn value_to_json(value: rusqlite::types::ValueRef<'_>) -> Value {
  match value {
    rusqlite::types::ValueRef::Null => Value::Null,
    rusqlite::types::ValueRef::Integer(n) => Value::Number(n.into()),
    rusqlite::types::ValueRef::Real(f) => serde_json::Number::from_f64(f)
      .map(Value::Number)
      .unwrap_or(Value::Null),
    rusqlite::types::ValueRef::Text(bytes) => match std::str::from_utf8(bytes) {
      Ok(s) => serde_json::from_str::<Value>(s).unwrap_or_else(|_| Value::String(s.to_string())),
      Err(_) => Value::Array(bytes.iter().map(|b| Value::from(*b)).collect()),
    },
    rusqlite::types::ValueRef::Blob(bytes) => match std::str::from_utf8(bytes) {
      Ok(s) => serde_json::from_str::<Value>(s).unwrap_or_else(|_| Value::String(s.to_string())),
      Err(_) => Value::Array(bytes.iter().map(|b| Value::from(*b)).collect()),
    },
  }
}

fn assert_expected_tables(actual_tables: &Map<String, Value>, fixture: &Fixture) {
  let mut actual_table_names: Vec<&str> = actual_tables.keys().map(String::as_str).collect();
  let mut expected_table_names: Vec<&str> = fixture.meta.output_order.iter().map(String::as_str).collect();
  actual_table_names.sort_unstable();
  expected_table_names.sort_unstable();
  assert_eq!(actual_table_names, expected_table_names, "table set mismatch");

  for table in &fixture.meta.output_order {
    let actual_rows = actual_tables
      .get(table)
      .unwrap_or_else(|| panic!("missing table {table}"));
    let empty = Vec::new();
    let expected_rows = fixture.expected_rows.get(table).unwrap_or(&empty);
    let actual_rows = actual_rows.as_array().expect("actual row array");
    assert_eq!(actual_rows.len(), expected_rows.len(), "row count mismatch for {table}");
    for (actual, expected) in actual_rows.iter().zip(expected_rows) {
      assert_expected_row(table, actual, expected, &fixture.meta.output_columns);
    }
  }
}

fn assert_expected_row(
  table: &str,
  actual: &Value,
  expected: &Value,
  output_columns: &std::collections::HashMap<String, Vec<String>>,
) {
  let actual = actual.as_object().expect("actual row object");
  let expected = expected.as_object().expect("expected row object");
  let expected_columns = output_columns
    .get(table)
    .unwrap_or_else(|| panic!("missing output columns for {table}"));
  let mut actual_columns: Vec<&str> = actual.keys().map(String::as_str).collect();
  let mut expected_columns_refs: Vec<&str> = expected_columns.iter().map(String::as_str).collect();
  actual_columns.sort_unstable();
  expected_columns_refs.sort_unstable();
  assert_eq!(actual_columns, expected_columns_refs, "column set mismatch for {table}");

  for key in expected_columns {
    let expected_value = expected.get(key).unwrap_or(&Value::Null);
    let actual_value = actual
      .get(key)
      .unwrap_or_else(|| panic!("missing column {table}.{key}"));
    assert_eq!(actual_value, expected_value, "value mismatch for {table}.{key}");
  }
}

#[derive(Debug)]
struct Fixture {
  meta: FixtureMeta,
  seed_rows: Vec<FixtureRow>,
  expected_rows: std::collections::HashMap<String, Vec<Value>>,
}

#[derive(Debug)]
struct FixtureMeta {
  input_columns: std::collections::HashMap<String, Vec<String>>,
  output_columns: std::collections::HashMap<String, Vec<String>>,
  output_order: Vec<String>,
}

#[derive(Debug)]
struct FixtureRow {
  table: String,
  values: Map<String, Value>,
}

fn parse_fixture(case: DbCase) -> Fixture {
  let meta_json: Value = serde_json::from_str(case.meta_json).expect("valid metadata json");
  Fixture {
    meta: parse_meta(&meta_json),
    seed_rows: parse_jsonl_rows(case.seed_jsonl),
    expected_rows: group_expected_rows(parse_jsonl_rows(case.expected_jsonl)),
  }
}

fn parse_meta(meta: &Value) -> FixtureMeta {
  FixtureMeta {
    input_columns: parse_table_columns(meta.get("input").and_then(Value::as_array).expect("input array")),
    output_columns: parse_table_columns(meta.get("output").and_then(Value::as_array).expect("output array")),
    output_order: meta
      .get("output")
      .and_then(Value::as_array)
      .expect("output array")
      .iter()
      .map(|entry| {
        entry
          .get("table")
          .and_then(Value::as_str)
          .expect("output table")
          .to_string()
      })
      .collect(),
  }
}

fn parse_table_columns(entries: &[Value]) -> std::collections::HashMap<String, Vec<String>> {
  entries
    .iter()
    .map(|entry| {
      let table = entry
        .get("table")
        .and_then(Value::as_str)
        .expect("table name")
        .to_string();
      let columns = entry
        .get("columns")
        .and_then(Value::as_array)
        .expect("columns array")
        .iter()
        .map(|value| value.as_str().expect("column name").to_string())
        .collect();
      (table, columns)
    })
    .collect()
}

fn parse_jsonl_rows(contents: &str) -> Vec<FixtureRow> {
  contents
    .lines()
    .filter(|line| !line.trim().is_empty())
    .map(|line| {
      let value: Value = serde_json::from_str(line).expect("valid jsonl row");
      FixtureRow {
        table: value
          .get("table")
          .and_then(Value::as_str)
          .expect("row table")
          .to_string(),
        values: value
          .get("values")
          .and_then(Value::as_object)
          .expect("row values object")
          .clone(),
      }
    })
    .collect()
}

fn group_expected_rows(rows: Vec<FixtureRow>) -> std::collections::HashMap<String, Vec<Value>> {
  let mut grouped = std::collections::HashMap::<String, Vec<Value>>::new();
  for row in rows {
    grouped.entry(row.table).or_default().push(Value::Object(row.values));
  }
  grouped
}

fn json_to_sql(value: &Value) -> SqlValue {
  match value {
    Value::Null => SqlValue::Null,
    Value::Bool(v) => SqlValue::Text(if *v { "true" } else { "false" }.to_string()),
    Value::Number(n) => {
      if let Some(i) = n.as_i64() {
        SqlValue::Integer(i)
      } else if let Some(f) = n.as_f64() {
        SqlValue::Real(f)
      } else {
        SqlValue::Null
      }
    }
    Value::String(s) => SqlValue::Text(s.clone()),
    Value::Array(_) | Value::Object(_) => SqlValue::Text(serde_json::to_string(value).unwrap()),
  }
}

fn tempdir(name: &str) -> PathBuf {
  let path = std::env::temp_dir().join(format!("tokn-router-schema-test-{name}-{}", uuid::Uuid::new_v4()));
  std::fs::create_dir_all(&path).unwrap();
  path
}
