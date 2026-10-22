#[cfg(feature = "std")]
pub mod digest;
#[cfg(feature = "std")]
pub mod http;
pub mod initiator;
#[cfg(feature = "std")]
pub mod paths;
#[cfg(feature = "std")]
pub mod redact;
pub mod secret;
#[cfg(feature = "std")]
pub mod shutdown;
#[cfg(feature = "std")]
pub mod timefmt;
pub mod version;

#[cfg(feature = "std")]
pub fn now_unix_ms() -> i64 {
  let ns = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
  (ns / 1_000_000) as i64
}
