//! Shared utilities for requests stages.
//!
//! `codec` ports the content-encoding helpers from the legacy router so
//! the new pipeline can negotiate / inflate / re-deflate request and
//! response bodies without depending on `axum` or the legacy `ApiError`
//! type.

pub mod codec;
