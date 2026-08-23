/// Does `topic` match subscription `pattern`?
///
/// A pattern is a dot-separated list of segments, for example `logs.error`
/// or `logs.*`. Matching is exact segment by segment, except a pattern
/// segment of `*` matches exactly one arbitrary topic segment in that
/// position. Both sides must have the same number of segments, so `logs.*`
/// matches `logs.error` but not `logs` and not `logs.error.detail`.
///
/// An empty pattern or empty topic never matches.
pub fn matches(pattern: &str, topic: &str) -> bool {
    if pattern.is_empty() || topic.is_empty() {
        return false;
    }
    let pat: Vec<&str> = pattern.split('.').collect();
    let top: Vec<&str> = topic.split('.').collect();
    if pat.len() != top.len() {
        return false;
    }
    pat.iter().zip(top.iter()).all(|(p, t)| *p == "*" || p == t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(matches("logs.error", "logs.error"));
        assert!(!matches("logs.error", "logs.info"));
    }

    #[test]
    fn wildcard_matches_one_segment() {
        assert!(matches("logs.*", "logs.error"));
        assert!(matches("logs.*", "logs.info"));
        assert!(!matches("logs.*", "logs"));
        assert!(!matches("logs.*", "logs.error.detail"));
    }

    #[test]
    fn wildcard_can_be_any_segment_position() {
        assert!(matches("*.error", "logs.error"));
        assert!(matches("*.error", "app.error"));
        assert!(!matches("*.error", "logs.info"));
    }

    #[test]
    fn empty_inputs_never_match() {
        assert!(!matches("", "logs.error"));
        assert!(!matches("logs.error", ""));
    }
}
