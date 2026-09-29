//! Lease (ticket) length.
//!
//! animus-postgres v0.2.9 reads `ANIMUS_QUEUE_LEASE_TTL_SECS` but never
//! declares it in its manifest, so the host never forwards it and its tickets
//! always last 30 minutes. This queue declares it (difference 6), with the
//! same 1800-second default.

/// Environment variable that sets the lease length in seconds.
pub const LEASE_TTL_ENV: &str = "ANIMUS_QUEUE_LEASE_TTL_SECS";

/// Lease length when the variable is unset or invalid.
pub const DEFAULT_LEASE_TTL_SECS: i64 = 1800;

/// Longest accepted lease length (7 days). Larger values fall back to the
/// default rather than risk timestamp overflow.
pub const MAX_LEASE_TTL_SECS: i64 = 7 * 24 * 60 * 60;

/// Parse a lease length. Anything but a whole number from 1 to
/// [`MAX_LEASE_TTL_SECS`] gives [`DEFAULT_LEASE_TTL_SECS`].
pub fn parse_lease_ttl(raw: Option<&str>) -> i64 {
    raw.and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|secs| (1..=MAX_LEASE_TTL_SECS).contains(secs))
        .unwrap_or(DEFAULT_LEASE_TTL_SECS)
}

/// Lease length from [`LEASE_TTL_ENV`].
pub fn lease_ttl_from_env() -> i64 {
    parse_lease_ttl(std::env::var(LEASE_TTL_ENV).ok().as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_values_are_used() {
        assert_eq!(parse_lease_ttl(Some("60")), 60);
        assert_eq!(parse_lease_ttl(Some(" 90 ")), 90);
        assert_eq!(parse_lease_ttl(Some("1")), 1);
        assert_eq!(parse_lease_ttl(Some("604800")), MAX_LEASE_TTL_SECS);
    }

    #[test]
    fn missing_or_invalid_values_fall_back_to_the_default() {
        for raw in [
            None,
            Some(""),
            Some("0"),
            Some("-5"),
            Some("abc"),
            Some("30s"),
            Some("604801"),
        ] {
            assert_eq!(parse_lease_ttl(raw), DEFAULT_LEASE_TTL_SECS, "{raw:?}");
        }
    }
}
