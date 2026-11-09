//! Verifies a recorded history of list-append operations.
//!
//! The workload appends unique tokens to keys, so the final list for a key
//! reveals the exact order the cluster applied them in. That turns the normally
//! NP-hard linearizability check into a linear pass, and catches these directly:
//!
//! - lost write: an acknowledged token is missing from the final list
//! - double apply: a token appears twice
//! - phantom write: a token nobody ever issued appears
//! - divergent read: a read is not a prefix of the final list
//! - real-time violation: the forced order contradicts wall-clock order

use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpKind {
    Append { token: String },
    Read { observed: Vec<String> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The cluster acknowledged it.
    Ok,
    /// The client gave up; it may or may not have happened.
    Indeterminate,
}

#[derive(Clone, Debug)]
pub struct OpRecord {
    pub client: u64,
    pub key: String,
    pub kind: OpKind,
    pub invoke: u64,
    pub complete: u64,
    pub outcome: Outcome,
}

#[derive(Clone, Debug, Default)]
pub struct CheckReport {
    pub violations: Vec<String>,
    pub acked_writes: u64,
    pub indeterminate_writes: u64,
    pub indeterminate_applied: u64,
    pub ok_reads: u64,
    pub keys: u64,
}

impl CheckReport {
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Split a stored value of the form `tok,tok,tok,` into tokens.
pub fn parse_list(v: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(v).split(',').filter(|s| !s.is_empty()).map(|s| s.to_string()).collect()
}

const MAX_REPORTED: usize = 50;

pub fn check(history: &[OpRecord], final_state: &HashMap<String, Vec<String>>) -> CheckReport {
    let mut report = CheckReport::default();
    let mut by_key: BTreeMap<&str, Vec<&OpRecord>> = BTreeMap::new();
    for op in history {
        by_key.entry(&op.key).or_default().push(op);
    }
    report.keys = by_key.len() as u64;
    let empty = Vec::new();

    for (key, ops) in &by_key {
        let list = final_state.get(*key).unwrap_or(&empty);

        let mut position: HashMap<&str, usize> = HashMap::with_capacity(list.len());
        for (i, t) in list.iter().enumerate() {
            if position.insert(t.as_str(), i).is_some() {
                report.violations.push(format!("DOUBLE_APPLY key={key} token={t}"));
            }
        }

        let mut issued: HashMap<&str, &OpRecord> = HashMap::new();
        for op in ops {
            if let OpKind::Append { token } = &op.kind {
                issued.insert(token.as_str(), op);
            }
        }
        for t in list {
            if !issued.contains_key(t.as_str()) {
                report.violations.push(format!("PHANTOM_WRITE key={key} token={t}"));
            }
        }

        // (position, invoke, complete, description). Appends sit on odd slots
        // and a read that saw L items sits on even slot 2L, between appends.
        let mut sequence: Vec<(usize, u64, u64, String)> = Vec::with_capacity(ops.len());
        for op in ops {
            match &op.kind {
                OpKind::Append { token } => match (&op.outcome, position.get(token.as_str())) {
                    (Outcome::Ok, Some(p)) => {
                        report.acked_writes += 1;
                        sequence.push((2 * p + 1, op.invoke, op.complete, format!("append {token}")));
                    }
                    (Outcome::Ok, None) => {
                        report.acked_writes += 1;
                        report.violations.push(format!("LOST_WRITE key={key} token={token} acked_at={}", op.complete));
                    }
                    (Outcome::Indeterminate, Some(p)) => {
                        report.indeterminate_writes += 1;
                        report.indeterminate_applied += 1;
                        // It took effect at some unknown time after it began.
                        sequence.push((2 * p + 1, op.invoke, u64::MAX, format!("append {token} (indeterminate)")));
                    }
                    (Outcome::Indeterminate, None) => report.indeterminate_writes += 1,
                },
                OpKind::Read { observed } => {
                    if op.outcome != Outcome::Ok {
                        continue;
                    }
                    report.ok_reads += 1;
                    let is_prefix = observed.len() <= list.len() && observed.iter().zip(list).all(|(a, b)| a == b);
                    if !is_prefix {
                        report.violations.push(format!(
                            "DIVERGENT_READ key={key} client={} read_len={} final_len={}",
                            op.client,
                            observed.len(),
                            list.len()
                        ));
                        continue;
                    }
                    sequence.push((2 * observed.len(), op.invoke, op.complete, format!("read len={}", observed.len())));
                }
            }
        }

        // Positions are forced by the final list, and ties are reads, for which
        // invocation order is always a valid choice. So the history is
        // linearizable exactly when no operation finished before an earlier one
        // in this order began.
        sequence.sort_by_key(|s| (s.0, s.1));
        let mut latest_invoke = 0u64;
        let mut latest_desc = String::new();
        for (_, invoke, complete, desc) in &sequence {
            if *complete < latest_invoke {
                report.violations.push(format!(
                    "REALTIME_VIOLATION key={key} [{desc}] finished at {complete} but is ordered after [{latest_desc}] which began at {latest_invoke}"
                ));
            }
            if *invoke > latest_invoke {
                latest_invoke = *invoke;
                latest_desc = desc.clone();
            }
        }
    }
    if report.violations.len() > MAX_REPORTED {
        let extra = report.violations.len() - MAX_REPORTED;
        report.violations.truncate(MAX_REPORTED);
        report.violations.push(format!("... and {extra} more"));
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append(token: &str, invoke: u64, complete: u64, outcome: Outcome) -> OpRecord {
        OpRecord { client: 1, key: "k".into(), kind: OpKind::Append { token: token.into() }, invoke, complete, outcome }
    }

    fn read(seen: &[&str], invoke: u64, complete: u64) -> OpRecord {
        OpRecord {
            client: 2,
            key: "k".into(),
            kind: OpKind::Read { observed: seen.iter().map(|s| s.to_string()).collect() },
            invoke,
            complete,
            outcome: Outcome::Ok,
        }
    }

    fn state(list: &[&str]) -> HashMap<String, Vec<String>> {
        HashMap::from([("k".to_string(), list.iter().map(|s| s.to_string()).collect())])
    }

    fn kinds(r: &CheckReport) -> Vec<String> {
        r.violations.iter().map(|v| v.split(' ').next().unwrap().to_string()).collect()
    }

    #[test]
    fn clean_history_passes() {
        let h = vec![
            append("a", 1, 2, Outcome::Ok),
            append("b", 3, 4, Outcome::Ok),
            read(&["a"], 2, 3),
            read(&["a", "b"], 5, 6),
        ];
        assert!(check(&h, &state(&["a", "b"])).ok());
    }

    #[test]
    fn detects_lost_write() {
        let h = vec![append("a", 1, 2, Outcome::Ok), append("b", 3, 4, Outcome::Ok)];
        assert_eq!(kinds(&check(&h, &state(&["a"]))), ["LOST_WRITE"]);
    }

    #[test]
    fn detects_double_apply() {
        let h = vec![append("a", 1, 2, Outcome::Ok)];
        assert_eq!(kinds(&check(&h, &state(&["a", "a"]))), ["DOUBLE_APPLY"]);
    }

    #[test]
    fn detects_phantom() {
        let h = vec![append("a", 1, 2, Outcome::Ok)];
        assert_eq!(kinds(&check(&h, &state(&["a", "z"]))), ["PHANTOM_WRITE"]);
    }

    #[test]
    fn detects_stale_read() {
        let h = vec![append("a", 1, 2, Outcome::Ok), read(&[], 5, 6)];
        assert_eq!(kinds(&check(&h, &state(&["a"]))), ["REALTIME_VIOLATION"]);
    }

    #[test]
    fn detects_write_order_inversion() {
        let h = vec![append("a", 1, 2, Outcome::Ok), append("b", 3, 4, Outcome::Ok)];
        assert_eq!(kinds(&check(&h, &state(&["b", "a"]))), ["REALTIME_VIOLATION"]);
    }

    #[test]
    fn detects_divergent_read() {
        let h = vec![append("a", 1, 2, Outcome::Ok), read(&["x"], 3, 4)];
        let k = kinds(&check(&h, &state(&["a"])));
        assert_eq!(k, ["DIVERGENT_READ"]);
    }

    #[test]
    fn indeterminate_may_or_may_not_apply() {
        let h = vec![append("a", 1, 2, Outcome::Indeterminate), append("b", 3, 4, Outcome::Indeterminate)];
        let r = check(&h, &state(&["b"]));
        assert!(r.ok());
        assert_eq!(r.indeterminate_writes, 2);
        assert_eq!(r.indeterminate_applied, 1);
    }

    #[test]
    fn concurrent_operations_may_order_either_way() {
        let h = vec![append("a", 1, 10, Outcome::Ok), append("b", 2, 9, Outcome::Ok), read(&["b"], 3, 8)];
        assert!(check(&h, &state(&["b", "a"])).ok());
    }
}
