use crate::requests::open_day_db;

use super::super::{is_valid_request_day, list_request_days, RequestDay, RequestDayState};
use super::support::{tempdir, write_request};

#[test]
fn lists_request_day_states_newest_first() {
  let dir = tempdir();
  write_request(
    &dir,
    "2026-07-14",
    "request-available",
    1_784_444_800_000,
    Some("session-available"),
    Some("openai"),
  );
  drop(open_day_db(&dir.join("2026-07-15.db")).unwrap());
  std::fs::write(dir.join("2026-07-16.db"), b"not a sqlite database").unwrap();
  drop(open_day_db(&dir.join("not-a-day.db")).unwrap());
  drop(open_day_db(&dir.join("2026-02-30.db")).unwrap());

  let days = list_request_days(&dir).unwrap();
  assert_eq!(
    days,
    vec![
      RequestDay {
        day: "2026-07-16".to_string(),
        state: RequestDayState::Unavailable,
      },
      RequestDay {
        day: "2026-07-15".to_string(),
        state: RequestDayState::Empty,
      },
      RequestDay {
        day: "2026-07-14".to_string(),
        state: RequestDayState::Available,
      },
    ]
  );
  assert_eq!(
    serde_json::to_value(RequestDayState::Unavailable).unwrap(),
    serde_json::json!("unavailable")
  );
  assert!(is_valid_request_day("2026-07-14"));
  assert!(!is_valid_request_day("2026-7-14"));
  assert!(!is_valid_request_day("2026-02-30"));
}
