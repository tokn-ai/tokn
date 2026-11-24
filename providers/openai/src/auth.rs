//! Re-exports the per-flavor `ProviderAuth` impls so existing callers
//! keep importing `crate::auth::{openai_auth, codex_auth}`.

pub use crate::auth_codex::codex_auth;
pub use crate::auth_openai::openai_auth;
