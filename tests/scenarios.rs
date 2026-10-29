//! Scripted scenarios on bare nodes: each one drives messages by hand so the
//! failure (or its absence) is shown step by step, independent of seeds.

use raft_ctrl_rs::disk::Cmd;
use raft_ctrl_rs::node::{Mode, Node, Reply, Role, Timing};

struct Net {
    nodes: Vec<Node>,
    up: Vec<bool>,
    cut: Vec<bool>,
    now: u64,
    replies: Vec<(usize, u64, Reply)>,
}

impl Net {
    fn new(n: usize, mode: Mode) -> Self {
        // Distinct jitters: node 0 times out first.
        let nodes = (0..n)
            .map(|i| Node::new(i, n, mode, Timing::default(), 10 + 30 * i as u64))
            .collect();
        Net {
            nodes,
            up: vec![true; n],
            cut: vec![false; n],
            now: 0,
            replies: Vec::new(),
        }
    }

    fn linked(&self, a: usize, b: usize) -> bool {
        self.up[a] && self.up[b] && !self.cut[a] && !self.cut[b]
    }

    /// Deliver messages until none are left.
    fn pump(&mut self) {
        for _ in 0..10_000 {
            let mut msgs = Vec::new();
            for nd in self.nodes.iter_mut() {
                let from = nd.id;
                for (to, m) in nd.out.drain(..) {
                    msgs.push((from, to, m));
                }
                self.replies.append(&mut nd.replies);
            }
            if msgs.is_empty() {
                return;
            }
            for (from, to, m) in msgs {
                if self.linked(from, to) {
                    let now = self.now;
                    self.nodes[to].recv(from, m, now, 0);
                }
            }
        }
        panic!("message storm");
    }

    /// Advance time by `dt`, ticking every live node each tick.
    fn run(&mut self, dt: u64) {
        for _ in 0..dt {
            self.now += 1;
            for i in 0..self.nodes.len() {
                if self.up[i] {
                    let now = self.now;
                    self.nodes[i].tick(now, 7 * i as u64);
                }
            }
            self.pump();
        }
    }

    fn leader(&self) -> Option<usize> {
        (0..self.nodes.len())
            .filter(|&i| self.up[i] && self.nodes[i].role == Role::Leader)
            .max_by_key(|&i| self.nodes[i].term)
    }

    fn put(&mut self, node: usize, key: u8, val: u64, tag: u64) {
        let now = self.now;
        self.nodes[node].client(0, tag, Cmd::Put { key, val }, now, 0);
        self.pump();
    }

    fn done(&self, tag: u64) -> bool {
        self.replies
            .iter()
            .any(|&(_, t, r)| t == tag && matches!(r, Reply::Done(_)))
    }
}

/// Three nodes. A write is committed on {A, B} while C is partitioned. B's
/// copy of that entry is corrupted and B restarts; then A dies. C asks for
/// votes. Returns the network after that election attempt.
fn lost_write_scenario(mode: Mode) -> Net {
    let mut net = Net::new(3, mode);
    net.run(300);
    let a = net.leader().expect("initial leader");
    assert_eq!(a, 0, "node 0 has the shortest timeout");
    net.run(50);
    net.cut[2] = true;
    net.put(0, 0, 7, 1);
    net.run(30);
    assert!(net.done(1), "write must be committed and acknowledged");
    let idx = net.nodes[0].last_idx();
    assert!(net.nodes[0].commit >= idx);
    assert!(net.nodes[2].last_idx() < idx, "C must not have the write");
    // Corrupt B's copy, restart B, kill A, heal C.
    net.nodes[1].disk.corrupt(idx, 0x1234_5678_9abc);
    let now = net.now;
    net.nodes[1].restart(now, 0);
    net.up[0] = false;
    net.cut[2] = false;
    net.run(600);
    net
}

#[test]
fn vanilla_loses_an_acknowledged_write() {
    let mut net = lost_write_scenario(Mode::Vanilla);
    let l = net.leader().expect("vanilla elects C with B's vote");
    assert_eq!(l, 2);
    net.run(100);
    assert_eq!(net.nodes[1].value(0), 0, "B applied the new history");
    assert_eq!(net.nodes[2].value(0), 0, "acknowledged write 7 is gone");
}

#[test]
fn ctrl_refuses_to_elect_a_stale_leader_and_repairs_when_a_returns() {
    let mut net = lost_write_scenario(Mode::Ctrl);
    // B keeps the entry's identifier, so it never votes for C; B itself
    // may lead but cannot apply the entry until a good copy exists.
    assert_ne!(net.leader(), Some(2), "C must not win without the write");
    assert_eq!(net.nodes[2].value(0), 0);
    assert!(!net.nodes[1].faulty.is_empty() || net.nodes[1].value(0) == 0);
    // A comes back with an intact copy.
    net.up[0] = true;
    let now = net.now;
    net.nodes[0].restart(now, 0);
    net.run(800);
    assert!(net.leader().is_some());
    for i in 0..3 {
        assert!(
            net.nodes[i].faulty.is_empty(),
            "node {i} still has faulty entries"
        );
        assert_eq!(net.nodes[i].value(0), 7, "node {i} lost the write");
    }
}

#[test]
fn ctrl_follower_repairs_from_leader() {
    let mut net = Net::new(3, Mode::Ctrl);
    net.run(300);
    for k in 0..5u64 {
        net.put(0, 1, 100 + k, 10 + k);
    }
    net.run(60);
    net.nodes[2].disk.corrupt(3, 0xdead_beef);
    let now = net.now;
    net.nodes[2].restart(now, 0);
    assert!(net.nodes[2].faulty.contains(&3));
    net.run(100);
    assert!(net.nodes[2].faulty.is_empty());
    assert_eq!(net.nodes[2].value(1), 104);
    assert!(net.nodes[2].stats.repaired >= 1);
}

#[test]
fn ctrl_leader_repairs_its_own_entry_from_a_follower() {
    let mut net = Net::new(3, Mode::Ctrl);
    net.run(300);
    for k in 0..4u64 {
        net.put(0, 2, 200 + k, 20 + k);
    }
    net.run(60);
    // Corrupt the leader's copy; it notices on the next read.
    net.nodes[0].disk.corrupt(2, 0x00ff_00ff);
    let now = net.now;
    net.nodes[0].restart(now, 0);
    net.run(800);
    for i in 0..3 {
        assert!(net.nodes[i].faulty.is_empty(), "node {i}");
        assert_eq!(net.nodes[i].value(2), 203, "node {i}");
    }
    assert!(net.nodes.iter().map(|n| n.stats.repaired).sum::<u64>() >= 1);
}

#[test]
fn ctrl_discards_uncommitted_entry_only_with_a_majority_lacking_it() {
    let mut net = Net::new(3, Mode::Ctrl);
    net.run(300);
    net.run(50);
    // Leader appends while isolated: the entry is not committed.
    net.cut[1] = true;
    net.cut[2] = true;
    net.put(0, 3, 99, 30);
    let idx = net.nodes[0].last_idx();
    assert!(net.nodes[0].commit < idx);
    net.nodes[0].disk.corrupt(idx, 0x4242);
    let now = net.now;
    net.nodes[0].restart(now, 0);
    assert!(net.nodes[0].faulty.contains(&idx));
    net.cut[1] = false;
    net.cut[2] = false;
    net.run(1500);
    assert!(net.leader().is_some());
    let discarded: u64 = net.nodes.iter().map(|n| n.stats.discarded).sum();
    for i in 0..3 {
        assert!(net.nodes[i].faulty.is_empty(), "node {i}");
        assert_eq!(
            net.nodes[i].value(3),
            0,
            "uncommitted write must not appear"
        );
    }
    assert!(
        net.nodes[0].stats.discarded >= 1 && discarded == net.nodes[0].stats.discarded,
        "A must drop the entry itself after a majority reports it lacks it"
    );
}

#[test]
fn torn_tail_is_dropped_and_corrupt_tail_with_id_is_kept() {
    let mut net = Net::new(3, Mode::Ctrl);
    net.run(300);
    net.put(0, 0, 5, 40);
    net.run(40);
    let len = net.nodes[1].last_idx();
    net.nodes[1].disk.torn_append(9);
    let now = net.now;
    net.nodes[1].restart(now, 0);
    assert_eq!(net.nodes[1].last_idx(), len, "torn tail without id dropped");
    assert_eq!(net.nodes[1].stats.torn_tails, 1);
    net.nodes[1].disk.corrupt(len, 77);
    net.nodes[1].restart(now, 0);
    assert_eq!(net.nodes[1].last_idx(), len, "corrupt entry with id kept");
    assert!(net.nodes[1].faulty.contains(&len));
}
