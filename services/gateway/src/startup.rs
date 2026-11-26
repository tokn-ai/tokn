use anyhow::Result;
use std::future::Future;

/// Select the frontend/worker arrangement for ordinary Unix serving.
/// The explicit worker-socket entry point and other platforms use the direct runtime.
pub async fn serve(
  direct: impl Future<Output = Result<()>>,
  #[cfg(unix)] explicit_worker_socket: bool,
  #[cfg(unix)] split: impl Future<Output = Result<()>>,
) -> Result<()> {
  #[cfg(unix)]
  if !explicit_worker_socket {
    return split.await;
  }
  direct.await
}
