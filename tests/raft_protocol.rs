//! Directed protocol scenarios, driven message by message.

use raftkv::raft::*;
use raftkv::storage::mem::MemStorage;
use raftkv::storage::Storage;
use raftkv::types::*;
use std::collections::VecDeque;

fn store(hs: HardState, log: &[(Term, Index)]) -> MemStorage {
    let mut s = MemStorage::new(false);
    s.save_hard_state(hs);
    let e: Vec<Entry> = log.iter().map(|(t, i)| Entry::new(*t, *i, vec![1])).collect();
    s.append(&e);
    s.sync();
    s
}

fn config(id: NodeId, n: u64, bugs: Bugs) -> Config {
    let mut c = Config::new(id, (1..=n).collect());
    c.pre_vote = false;
    c.check_quorum = false;
    c.bugs = bugs;
    c
}

fn run_until_candidate<S: Storage>(r: &mut Raft<S>) {
    for _ in 0..200 {
        r.tick();
        if r.role() == Role::Candidate {
            return;
        }
    }
    panic!("never became candidate");
}

fn elect(r: &mut Raft<MemStorage>, voters: &[NodeId]) {
    run_until_candidate(r);
    let term = r.term();
    for v in voters {
        r.step(Envelope { from: *v, to: r.id(), msg: Msg::VoteResp { pre: false, term, granted: true } });
    }
    assert_eq!(r.role(), Role::Leader);
    r.take_outbox();
}

fn ack(r: &mut Raft<MemStorage>, from: NodeId, match_index: Index) {
    let term = r.term();
    r.step(Envelope {
        from,
        to: r.id(),
        msg: Msg::AppendResp {
            term,
            success: true,
            match_index,
            rejected_prev: 0,
            conflict_index: 0,
            conflict_term: 0,
            read_ctx: 0,
        },
    });
}

fn figure8_leader(bugs: Bugs) -> Raft<MemStorage> {
    // Node 1 holds an entry from old term 2 and leads at term 4.
    let s = store(HardState { term: 3, voted_for: None }, &[(1, 1), (2, 2)]);
    let mut r = Raft::new(config(1, 5, bugs), s);
    elect(&mut r, &[2, 3]);
    assert_eq!(r.term(), 4);
    r
}

/// Raft paper §5.4.2 and Figure 8. A replica count alone cannot commit an
/// entry from an earlier term, because it can still be overwritten.
#[test]
fn figure8_old_term_entry_needs_a_current_term_entry() {
    let mut r = figure8_leader(Bugs::default());
    ack(&mut r, 2, 2);
    ack(&mut r, 3, 2);
    assert_eq!(r.commit(), 0, "index 2 is on a majority but is from term 2");
    ack(&mut r, 2, 3);
    ack(&mut r, 3, 3);
    assert_eq!(r.commit(), 3, "the term-4 no-op commits itself and everything before it");
}

/// Proves the test above would notice the bug.
#[test]
fn figure8_mutation_is_detected() {
    let bugs = Bugs::single("commit_old_term").unwrap();
    let mut r = figure8_leader(bugs);
    ack(&mut r, 2, 2);
    ack(&mut r, 3, 2);
    assert_eq!(r.commit(), 2, "the injected bug should commit the old entry");
}

#[test]
fn vote_is_refused_to_a_stale_log() {
    let s = store(HardState { term: 2, voted_for: None }, &[(1, 1), (2, 2)]);
    let mut r = Raft::new(config(1, 3, Bugs::default()), s);
    r.step(Envelope {
        from: 2,
        to: 1,
        msg: Msg::Vote { pre: false, force: false, term: 3, last_index: 5, last_term: 1 },
    });
    let out = r.take_outbox();
    assert!(matches!(out[0].msg, Msg::VoteResp { granted: false, .. }), "{out:?}");
    assert_eq!(r.term(), 3, "the higher term is still adopted");
}

#[test]
fn only_one_vote_per_term_and_it_is_durable() {
    let mut r = Raft::new(config(1, 3, Bugs::default()), store(HardState::default(), &[]));
    let vote = |c: NodeId| Envelope {
        from: c,
        to: 1,
        msg: Msg::Vote { pre: false, force: false, term: 1, last_index: 0, last_term: 0 },
    };
    r.step(vote(2));
    assert!(matches!(r.take_outbox()[0].msg, Msg::VoteResp { granted: true, .. }));
    r.step(vote(3));
    assert!(matches!(r.take_outbox()[0].msg, Msg::VoteResp { granted: false, .. }));
    assert_eq!(r.store.hard_state(), HardState { term: 1, voted_for: Some(2) });
}

#[test]
fn prevote_does_not_disturb_the_term() {
    let mut r = Raft::new(config(1, 3, Bugs::default()), store(HardState { term: 5, voted_for: None }, &[]));
    r.step(Envelope {
        from: 2,
        to: 1,
        msg: Msg::Vote { pre: true, force: false, term: 6, last_index: 0, last_term: 0 },
    });
    assert!(matches!(r.take_outbox()[0].msg, Msg::VoteResp { granted: true, .. }));
    assert_eq!(r.term(), 5);
    assert_eq!(r.store.hard_state().voted_for, None);
}

#[test]
fn follower_truncates_conflicts_but_never_past_a_valid_prefix() {
    let s = store(HardState { term: 1, voted_for: None }, &[(1, 1), (1, 2), (1, 3)]);
    let mut r = Raft::new(config(2, 3, Bugs::default()), s);
    r.step(Envelope {
        from: 1,
        to: 2,
        msg: Msg::Append {
            term: 2,
            prev_index: 1,
            prev_term: 1,
            entries: vec![Entry::new(2, 2, vec![9])],
            commit: 0,
            read_ctx: 0,
        },
    });
    assert_eq!(r.store.last_index(), 2);
    assert_eq!(r.store.term_at(2), Some(2));
}

#[test]
fn stale_append_cannot_remove_newer_entries() {
    let s = store(HardState { term: 1, voted_for: None }, &[(1, 1), (1, 2), (1, 3)]);
    let mut r = Raft::new(config(2, 3, Bugs::default()), s);
    r.step(Envelope {
        from: 1,
        to: 2,
        msg: Msg::Append {
            term: 1,
            prev_index: 0,
            prev_term: 0,
            entries: vec![Entry::new(1, 1, vec![1])],
            commit: 0,
            read_ctx: 0,
        },
    });
    assert_eq!(r.store.last_index(), 3);
}

#[test]
fn follower_commit_is_limited_to_what_the_message_verified() {
    let s = store(HardState { term: 1, voted_for: None }, &[(1, 1), (1, 2), (9, 3)]);
    let mut r = Raft::new(config(2, 3, Bugs::default()), s);
    r.step(Envelope {
        from: 1,
        to: 2,
        msg: Msg::Append { term: 9, prev_index: 1, prev_term: 1, entries: vec![], commit: 3, read_ctx: 0 },
    });
    assert_eq!(r.commit(), 1, "entries 2 and 3 have not been checked against the leader");
}

#[test]
fn read_needs_a_commit_in_the_current_term() {
    let s = store(HardState { term: 3, voted_for: None }, &[(1, 1)]);
    let mut r = Raft::new(config(1, 3, Bugs::default()), s);
    elect(&mut r, &[2]);
    assert!(!r.read_index(1), "no entry from this term has committed yet");
    ack(&mut r, 2, 2);
    assert!(r.read_index(1));
}

#[test]
fn read_waits_for_a_quorum_to_confirm_leadership() {
    let s = store(HardState { term: 3, voted_for: None }, &[(1, 1)]);
    let mut r = Raft::new(config(1, 3, Bugs::default()), s);
    elect(&mut r, &[2]);
    ack(&mut r, 2, 2);
    assert!(r.read_index(7));
    r.flush();
    assert!(r.take_ready_reads().is_empty());
    let out = r.take_outbox();
    let ctx = out
        .iter()
        .find_map(|e| match e.msg {
            Msg::Append { read_ctx, .. } if e.to == 2 => Some(read_ctx),
            _ => None,
        })
        .unwrap();
    assert!(ctx > 0);
    let term = r.term();
    r.step(Envelope {
        from: 2,
        to: 1,
        msg: Msg::AppendResp {
            term,
            success: true,
            match_index: 2,
            rejected_prev: 0,
            conflict_index: 0,
            conflict_term: 0,
            read_ctx: ctx,
        },
    });
    assert_eq!(r.take_ready_reads(), vec![(7, 2)]);
}

#[test]
fn isolated_leader_steps_down_with_check_quorum() {
    let mut c = config(1, 3, Bugs::default());
    c.check_quorum = true;
    let mut r = Raft::new(c, store(HardState::default(), &[]));
    elect(&mut r, &[2]);
    for _ in 0..200 {
        r.tick();
    }
    assert_ne!(r.role(), Role::Leader, "no peer answered, so it must give up leadership");
}

/// Regression test for a bug the simulator found: heartbeat replies kept the
/// peer looking healthy, so a lost Append was never resent when no further
/// writes arrived, and the follower stayed one entry behind forever.
#[test]
fn lost_append_is_retransmitted_despite_heartbeat_replies() {
    let mut r = Raft::new(config(1, 3, Bugs::default()), store(HardState::default(), &[]));
    elect(&mut r, &[2]);
    ack(&mut r, 2, 1);
    r.propose(vec![7]).unwrap();
    r.flush();
    r.take_outbox(); // the Append carrying index 2 is lost
    let term = r.term();
    let mut resent = false;
    for _ in 0..60 {
        r.tick();
        for e in r.take_outbox() {
            if e.to != 2 {
                continue;
            }
            if let Msg::Append { entries, read_ctx, .. } = &e.msg {
                if entries.iter().any(|x| x.index == 2) {
                    resent = true;
                } else {
                    // The peer keeps answering heartbeats.
                    let ctx = *read_ctx;
                    r.step(Envelope {
                        from: 2,
                        to: 1,
                        msg: Msg::AppendResp {
                            term,
                            success: true,
                            match_index: 1,
                            rejected_prev: 0,
                            conflict_index: 0,
                            conflict_term: 0,
                            read_ctx: ctx,
                        },
                    });
                }
            }
        }
    }
    assert!(resent);
}

/// Full message-passing run to check the pieces work together.
#[test]
fn three_nodes_elect_and_replicate() {
    let mut nodes: Vec<Raft<MemStorage>> = (1..=3)
        .map(|id| {
            let mut c = Config::new(id, vec![1, 2, 3]);
            c.seed = id * 77;
            Raft::new(c, MemStorage::new(false))
        })
        .collect();
    let mut q: VecDeque<Envelope> = VecDeque::new();
    let mut proposed = false;
    for _ in 0..2000 {
        for n in nodes.iter_mut() {
            n.tick();
            n.flush();
            n.store.sync();
            q.extend(n.take_outbox());
        }
        while let Some(e) = q.pop_front() {
            let to = (e.to - 1) as usize;
            nodes[to].step(e);
            nodes[to].flush();
            nodes[to].store.sync();
            q.extend(nodes[to].take_outbox());
        }
        if !proposed {
            if let Some(l) = nodes.iter_mut().find(|n| n.is_leader()) {
                if l.propose(vec![42]).is_some() {
                    proposed = true;
                }
            }
        }
    }
    assert!(proposed);
    assert_eq!(nodes.iter().filter(|n| n.is_leader()).count(), 1);
    let c = nodes[0].commit();
    assert!(c >= 2);
    assert!(nodes.iter().all(|n| n.commit() == c));
}
