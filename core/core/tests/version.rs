use tokn_core::util::version::{self, BuildVersion};

// A separate integration-test process keeps installation isolated from tests
// that exercise the default metadata used by library consumers.
#[test]
fn application_metadata_reaches_shared_consumers_and_cannot_be_replaced() {
  assert_eq!(version::full(), concat!("v", env!("CARGO_PKG_VERSION")));
  assert_eq!(version::commit_id(), "unknown");
  assert!(!version::is_dirty());

  let build = BuildVersion {
    base: "v1.2.3",
    commit_id: "abcdef0",
    full: "v1.2.3+abcdef0+dev",
    dirty: true,
  };
  version::install(build).unwrap();
  assert_eq!(version::base(), build.base);
  assert_eq!(version::commit_id(), build.commit_id);
  assert_eq!(version::full(), build.full);
  assert!(version::is_dirty());
  assert_eq!(version::tokn_router_user_agent(), "tokn-router/v1.2.3+abcdef0+dev");

  assert!(version::install(BuildVersion {
    full: "replacement",
    ..build
  })
  .is_err());
  assert_eq!(version::full(), build.full);
}
