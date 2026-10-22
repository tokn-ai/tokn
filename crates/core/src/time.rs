//! Monotonic timestamp operations used by shared execution decisions.
//!
//! Runtime adapters own the clock. Algorithms receive a timestamp explicitly
//! so tests and embedded callers do not need an operating-system timer.

use core::time::Duration;

/// A timestamp from one monotonic clock, with saturating arithmetic.
///
/// Implementations must order moments chronologically. Callers must not mix
/// epochs or clocks. Elapsed time saturates at zero for an earlier `self`;
/// this prevents arithmetic underflow but does not make clock rollback valid.
pub trait Moment: Copy + Ord {
  fn saturating_add(self, duration: Duration) -> Self;
  fn elapsed_since(self, earlier: Self) -> Duration;
}

impl Moment for Duration {
  fn saturating_add(self, duration: Duration) -> Self {
    Duration::saturating_add(self, duration)
  }

  fn elapsed_since(self, earlier: Self) -> Duration {
    self.saturating_sub(earlier)
  }
}

#[cfg(feature = "std")]
impl Moment for std::time::Instant {
  fn saturating_add(self, duration: Duration) -> Self {
    if let Some(deadline) = self.checked_add(duration) {
      return deadline;
    }

    // The maximum representable Instant is platform-dependent. Locate the
    // largest representable offset without discarding subsecond precision.
    let mut lower_nanos = 0_u128;
    let mut upper_nanos = duration.as_nanos();
    while lower_nanos < upper_nanos {
      let midpoint = lower_nanos + (upper_nanos - lower_nanos).div_ceil(2);
      if self.checked_add(duration_from_nanos(midpoint)).is_some() {
        lower_nanos = midpoint;
      } else {
        upper_nanos = midpoint - 1;
      }
    }
    self.checked_add(duration_from_nanos(lower_nanos)).unwrap_or(self)
  }

  fn elapsed_since(self, earlier: Self) -> Duration {
    self.saturating_duration_since(earlier)
  }
}

#[cfg(feature = "std")]
fn duration_from_nanos(nanos: u128) -> Duration {
  const NANOS_PER_SECOND: u128 = 1_000_000_000;
  Duration::new((nanos / NANOS_PER_SECOND) as u64, (nanos % NANOS_PER_SECOND) as u32)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn duration_arithmetic_keeps_precision_and_saturates() {
    let now = Duration::from_nanos(19);
    assert_eq!(
      Moment::saturating_add(now, Duration::from_nanos(7)),
      Duration::from_nanos(26)
    );
    assert_eq!(now.elapsed_since(Duration::from_nanos(2)), Duration::from_nanos(17));
    assert_eq!(now.elapsed_since(Duration::from_nanos(20)), Duration::ZERO);
    assert_eq!(
      Moment::saturating_add(Duration::MAX, Duration::from_nanos(1)),
      Duration::MAX
    );
  }

  #[cfg(feature = "std")]
  #[test]
  fn standard_instant_preserves_nanoseconds_and_saturates_at_platform_limit() {
    let now = std::time::Instant::now();
    let small = Duration::from_nanos(37);
    assert_eq!(now.saturating_add(small), now.checked_add(small).unwrap());
    assert_eq!(now.saturating_add(small).elapsed_since(now), small);
    assert_eq!(now.elapsed_since(now.saturating_add(small)), Duration::ZERO);

    let deadline = now.saturating_add(Duration::MAX);
    assert!(deadline >= now);
    assert!(now.checked_add(deadline.duration_since(now)).is_some());
    if now.checked_add(Duration::MAX).is_none() {
      assert!(deadline.checked_add(Duration::from_nanos(1)).is_none());
    }
  }
}
