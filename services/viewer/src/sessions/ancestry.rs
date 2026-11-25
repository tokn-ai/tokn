use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};

use crate::Result;

type PrefixId = [u8; 32];

#[derive(Debug)]
struct PrefixNode {
  parent_id: Option<PrefixId>,
  depth: u64,
}

#[derive(Debug)]
struct Observation {
  node_id: String,
  thread_id: String,
  ts: i64,
  row_id: i64,
  tip_id: PrefixId,
  input_count: u64,
  output_count: u64,
}

#[derive(Debug, Clone)]
struct Anchor {
  node_id: String,
  output_count: u64,
}

#[derive(Debug, Default)]
struct ThreadAnchors {
  input: HashMap<PrefixId, Anchor>,
  output: HashMap<PrefixId, Anchor>,
}

#[derive(Debug, Clone)]
pub(super) struct DerivedAncestry {
  pub parent_node_id: Option<String>,
  pub parent_source: &'static str,
  pub common_prefix_messages: u64,
}

pub(super) fn derive_session_ancestry(conn: &Connection, session_id: &str) -> Result<HashMap<String, DerivedAncestry>> {
  let tree = load_prefix_tree(conn, session_id)?;
  let mut observations = load_observations(conn, session_id)?;
  observations.sort_by(|left, right| {
    left
      .ts
      .cmp(&right.ts)
      .then_with(|| left.row_id.cmp(&right.row_id))
      .then_with(|| left.node_id.cmp(&right.node_id))
  });

  let mut anchors_by_thread = HashMap::<String, ThreadAnchors>::new();
  let mut ancestry = HashMap::with_capacity(observations.len());
  for observation in observations {
    let input_tip = input_tip(&tree, &observation)?;
    let anchors = anchors_by_thread.entry(observation.thread_id.clone()).or_default();
    let derived = find_parent(&tree, anchors, &observation, input_tip)?;
    ancestry.insert(observation.node_id.clone(), derived);

    anchors.output.insert(
      observation.tip_id,
      Anchor {
        node_id: observation.node_id.clone(),
        output_count: observation.output_count,
      },
    );
    if let Some(input_tip) = input_tip {
      anchors.input.insert(
        input_tip,
        Anchor {
          node_id: observation.node_id,
          output_count: observation.output_count,
        },
      );
    }
  }
  Ok(ancestry)
}

fn load_prefix_tree(conn: &Connection, session_id: &str) -> Result<HashMap<PrefixId, PrefixNode>> {
  let mut stmt = conn.prepare(
    "WITH RECURSIVE path(id, parent_id, depth) AS (
       SELECT tree.id, tree.parent_id, tree.depth
       FROM session_nodes node
       JOIN message_tree tree ON tree.id = node.message_id
       WHERE node.session_id = ?1
       UNION
       SELECT parent.id, parent.parent_id, parent.depth
       FROM path child
       JOIN message_tree parent ON parent.id = child.parent_id
     )
     SELECT id, parent_id, depth FROM path",
  )?;
  let rows = stmt.query_map(params![session_id], |row| {
    Ok((
      row.get::<_, Vec<u8>>(0)?,
      row.get::<_, Option<Vec<u8>>>(1)?,
      row.get::<_, i64>(2)?,
    ))
  })?;
  let mut tree = HashMap::new();
  for row in rows {
    let (id, parent_id, depth) = row?;
    let id = decode_prefix_id(&id)?;
    let parent_id = parent_id.as_deref().map(decode_prefix_id).transpose()?;
    let depth = u64::try_from(depth)
      .ok()
      .filter(|depth| *depth > 0)
      .ok_or_else(|| invalid_tree(&id))?;
    tree.insert(id, PrefixNode { parent_id, depth });
  }
  Ok(tree)
}

fn load_observations(conn: &Connection, session_id: &str) -> Result<Vec<Observation>> {
  let mut stmt = conn.prepare(
    "SELECT id, COALESCE(thread_id, session_id), ts, rowid, message_id,
            input_message_count, output_message_count
     FROM session_nodes
     WHERE session_id = ?1 AND message_id IS NOT NULL",
  )?;
  let rows = stmt.query_map(params![session_id], |row| {
    Ok((
      row.get::<_, String>(0)?,
      row.get::<_, String>(1)?,
      row.get::<_, i64>(2)?,
      row.get::<_, i64>(3)?,
      row.get::<_, Vec<u8>>(4)?,
      row.get::<_, Option<i64>>(5)?,
      row.get::<_, Option<i64>>(6)?,
    ))
  })?;
  rows
    .map(|row| {
      let (node_id, thread_id, ts, row_id, tip_id, input_count, output_count) = row?;
      let tip_id = decode_prefix_id(&tip_id)?;
      let input_count = nonnegative_count(input_count).ok_or_else(|| invalid_tree(&tip_id))?;
      let output_count = nonnegative_count(output_count).ok_or_else(|| invalid_tree(&tip_id))?;
      Ok(Observation {
        node_id,
        thread_id,
        ts,
        row_id,
        tip_id,
        input_count,
        output_count,
      })
    })
    .collect()
}

fn input_tip(tree: &HashMap<PrefixId, PrefixNode>, observation: &Observation) -> Result<Option<PrefixId>> {
  let expected_depth = observation
    .input_count
    .checked_add(observation.output_count)
    .ok_or_else(|| invalid_tree(&observation.tip_id))?;
  let tip = tree
    .get(&observation.tip_id)
    .ok_or_else(|| invalid_tree(&observation.tip_id))?;
  if tip.depth != expected_depth {
    return Err(invalid_tree(&observation.tip_id));
  }

  let mut current = Some(observation.tip_id);
  for _ in 0..observation.output_count {
    current = checked_parent(tree, current.ok_or_else(|| invalid_tree(&observation.tip_id))?)?;
  }
  match (observation.input_count, current) {
    (0, None) => Ok(None),
    (0, Some(_)) | (_, None) => Err(invalid_tree(&observation.tip_id)),
    (input_count, Some(input_tip)) => {
      let depth = tree
        .get(&input_tip)
        .map(|node| node.depth)
        .ok_or_else(|| invalid_tree(&observation.tip_id))?;
      if depth == input_count {
        Ok(Some(input_tip))
      } else {
        Err(invalid_tree(&observation.tip_id))
      }
    }
  }
}

fn find_parent(
  tree: &HashMap<PrefixId, PrefixNode>,
  anchors: &ThreadAnchors,
  observation: &Observation,
  input_tip: Option<PrefixId>,
) -> Result<DerivedAncestry> {
  let Some(mut current) = input_tip else {
    return Ok(no_parent());
  };
  let mut visited = HashSet::new();
  loop {
    if !visited.insert(current) {
      return Err(invalid_tree(&observation.tip_id));
    }
    let node = tree.get(&current).ok_or_else(|| invalid_tree(&observation.tip_id))?;
    if let Some(anchor) = anchors.output.get(&current) {
      return Ok(DerivedAncestry {
        parent_node_id: Some(anchor.node_id.clone()),
        parent_source: "message_ancestor",
        common_prefix_messages: node.depth,
      });
    }
    if node.depth < observation.input_count {
      if let Some(anchor) = anchors.input.get(&current) {
        return Ok(DerivedAncestry {
          parent_node_id: Some(anchor.node_id.clone()),
          parent_source: if anchor.output_count == 0 {
            "message_ancestor"
          } else {
            "input_ancestor"
          },
          common_prefix_messages: node.depth,
        });
      }
    }
    let Some(parent_id) = checked_parent(tree, current)? else {
      break;
    };
    current = parent_id;
  }
  Ok(no_parent())
}

fn checked_parent(tree: &HashMap<PrefixId, PrefixNode>, id: PrefixId) -> Result<Option<PrefixId>> {
  let node = tree.get(&id).ok_or_else(|| invalid_tree(&id))?;
  match node.parent_id {
    Some(parent_id) => {
      let parent = tree.get(&parent_id).ok_or_else(|| invalid_tree(&id))?;
      if parent.depth.checked_add(1) != Some(node.depth) {
        return Err(invalid_tree(&id));
      }
      Ok(Some(parent_id))
    }
    None if node.depth == 1 => Ok(None),
    None => Err(invalid_tree(&id)),
  }
}

fn no_parent() -> DerivedAncestry {
  DerivedAncestry {
    parent_node_id: None,
    parent_source: "none",
    common_prefix_messages: 0,
  }
}

fn decode_prefix_id(value: &[u8]) -> Result<PrefixId> {
  value.try_into().map_err(|_| crate::Error::InvalidMessageTree {
    message_id: encode_prefix_id(value),
  })
}

fn nonnegative_count(value: Option<i64>) -> Option<u64> {
  value.and_then(|value| u64::try_from(value).ok())
}

fn invalid_tree(id: &PrefixId) -> crate::Error {
  crate::Error::InvalidMessageTree {
    message_id: encode_prefix_id(id),
  }
}

fn encode_prefix_id(value: &[u8]) -> String {
  value.iter().map(|byte| format!("{byte:02x}")).collect()
}
