use std::process::Command;

#[test]
fn cli_installs_its_build_metadata_before_parsing_arguments() {
  let output = Command::new(env!("CARGO_BIN_EXE_tokn-gateway"))
    .arg("--version")
    .output()
    .unwrap();
  assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
  assert_eq!(
    String::from_utf8(output.stdout).unwrap().trim(),
    concat!("tokn-router ", env!("tokn_ROUTER_VERSION"))
  );
}
