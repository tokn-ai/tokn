use super::*;

#[tokio::test]
async fn banner_status_reads_active_and_cancelled_experiments_over_control_socket() {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("frontend.sock");
  let listener = UnixListener::bind(&path).unwrap();
  let server = tokio::spawn(async move {
    for ab_test in [
      Some(AbTestStatus {
        baseline_worker_id: "old".into(),
        worker_id: "new".into(),
        elapsed_seconds: 12 * 3600,
        duration_seconds: 24 * 3600,
        traffic_percent: 50,
        rollout_policy: None,
      }),
      None,
    ] {
      let (stream, _) = listener.accept().await.unwrap();
      let mut stream = BufReader::new(stream);
      assert!(matches!(
        read_message::<ControlRequest>(&mut stream).await.unwrap(),
        ControlRequest::AbTestStatus
      ));
      write_message(stream.get_mut(), &ControlReply::AbTest { ab_test })
        .await
        .unwrap();
    }
  });
  let status = read_ab_test_status(&path).await.unwrap().unwrap();
  assert_eq!(status.traffic_percent, 50);
  assert_eq!(status.elapsed_seconds, 12 * 3600);
  assert!(read_ab_test_status(&path).await.unwrap().is_none());
  server.await.unwrap();
}

#[test]
fn optional_ab_capability_preserves_control_v2_compatibility() {
  let info = FrontendInfo {
    protocol_version: CONTROL_PROTOCOL_VERSION,
    supports_ab_test: true,
    supports_ab_test_status: true,
    capabilities: vec!["rollout_policy_v1".into()],
    frontend_pid: 123,
    maintenance_requests_dir: None,
    args: serde_json::json!({"ab_test": false}),
  };
  let mut legacy_info = serde_json::to_value(info).unwrap();
  legacy_info.as_object_mut().unwrap().remove("supports_ab_test");
  legacy_info.as_object_mut().unwrap().remove("supports_ab_test_status");
  legacy_info.as_object_mut().unwrap().remove("capabilities");
  legacy_info["args"].as_object_mut().unwrap().remove("ab_test");
  let decoded: FrontendInfo<serde_json::Value> = serde_json::from_value(legacy_info).unwrap();
  assert!(!decoded.supports_ab_test);
  assert!(!decoded.supports_ab_test_status);
  assert!(decoded.args.get("ab_test").is_none());
  assert!(decoded.capabilities.is_empty());
  assert!(decoded.maintenance_requests_dir.is_none());
  let register = ControlRequest::Register {
    worker_id: "fixture".into(),
    socket_path: "/tmp/fixture.sock".into(),
    candidate: false,
    ab_test: false,
    rollout_policy: None,
  };
  let legacy_message = serde_json::to_value(register).unwrap();
  assert!(
    legacy_message.get("ab_test").is_none(),
    "normal registration must stay readable by v2 frontends"
  );
  let ControlRequest::Register { ab_test, .. } = serde_json::from_value(legacy_message).unwrap() else {
    panic!("registration");
  };
  assert!(!ab_test, "new frontends must accept older worker registration messages");
}

#[test]
fn discovery_advertises_storage_only_when_the_frontend_manages_it() {
  let mut info = FrontendInfo {
    protocol_version: CONTROL_PROTOCOL_VERSION,
    supports_ab_test: true,
    supports_ab_test_status: true,
    capabilities: Vec::new(),
    frontend_pid: 42,
    maintenance_requests_dir: None,
    args: (),
  };
  let without_maintenance = serde_json::to_value(&info).unwrap();
  assert!(without_maintenance.get("maintenance_requests_dir").is_none());
  info.maintenance_requests_dir = Some(PathBuf::from("/tmp/gateway-fixture/requests"));
  let advertised = serde_json::to_value(&info).unwrap();
  let received: FrontendInfo<()> = serde_json::from_value(advertised).unwrap();
  assert_eq!(received.maintenance_requests_dir, info.maintenance_requests_dir);
}
