//! Raft node as a pure state machine over a [`Disk`], with two policies for
//! corrupted entries ([`Mode`]).
//!
//! Term and vote are metadata kept outside the corruptible log (CTRL stores
//! them redundantly; corrupting them is out of scope here). Commit index,
//! applied index and the key-value state are volatile and rebuilt from the
//! log after a restart.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::disk::{Cmd, Disk, Entry, ReadErr};

/// Node id, `0..n`.
pub type NodeId = usize;
/// Simulated time in ticks.
pub type Time = u64;
/// Number of keys in the store.
pub const KEYS: usize = 4;
/// Max entries per AppendEntries.
const BATCH: u64 = 32;

/// Corruption handling policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Restart and truncate the log at the first entry that fails its
    /// checksum, as for a torn tail.
    Vanilla,
    /// CTRL protocol-aware recovery.
    Ctrl,
}

/// Timing parameters (ticks).
#[derive(Debug, Clone, Copy)]
pub struct Timing {
    /// Heartbeat interval.
    pub heartbeat: Time,
    /// Election timeout lower bound.
    pub election_min: Time,
    /// Election timeout upper bound (exclusive).
    pub election_max: Time,
}

impl Default for Timing {
    fn default() -> Self {
        Timing {
            heartbeat: 20,
            election_min: 100,
            election_max: 200,
        }
    }
}

/// Deliberate bugs for negative controls. All off by default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Bugs {
    /// CTRL leader discards a corrupted entry after a single peer says it
    /// lacks it, instead of a majority.
    pub discard_on_one_lack: bool,
}

/// Answer to a CTRL fix request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixAns {
    /// I have the entry with this term and it verified.
    Have(Entry),
    /// I have an entry with this index and term but it is corrupted too.
    HaveFaulty,
    /// I do not have an entry with this index and term.
    Lack,
}

/// Peer message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    /// RequestVote.
    Vote {
        /// Candidate term.
        term: u64,
        /// Candidate last log index.
        last_idx: u64,
        /// Candidate last log term.
        last_term: u64,
    },
    /// RequestVote response.
    VoteResp {
        /// Responder term.
        term: u64,
        /// Granted.
        granted: bool,
    },
    /// AppendEntries.
    Append {
        /// Leader term.
        term: u64,
        /// Index before `entries`.
        prev_idx: u64,
        /// Term at `prev_idx`.
        prev_term: u64,
        /// Entries.
        entries: Vec<Entry>,
        /// Leader commit index.
        commit: u64,
    },
    /// AppendEntries response.
    AppendResp {
        /// Responder term.
        term: u64,
        /// Consistency check passed.
        ok: bool,
        /// On success: highest index known to hold verified data matching
        /// the leader. On failure: hint for the leader's next index - 1.
        match_idx: u64,
    },
    /// CTRL: leader asks for a copy of a corrupted entry.
    FixReq {
        /// Leader term.
        term: u64,
        /// Index.
        idx: u64,
        /// Term of the entry (from the identifier record).
        eterm: u64,
    },
    /// CTRL: answer to [`Msg::FixReq`].
    FixResp {
        /// Responder term.
        term: u64,
        /// Index.
        idx: u64,
        /// Entry term asked for.
        eterm: u64,
        /// Answer.
        ans: FixAns,
    },
}

/// Node role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Follower.
    Follower,
    /// Candidate.
    Candidate,
    /// Leader.
    Leader,
}

/// Result returned to a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reply {
    /// Not the leader; maybe a hint.
    NotLeader(Option<NodeId>),
    /// Committed and applied. `Some(v)` for reads.
    Done(Option<u64>),
}

/// An applied entry, reported to the simulator's oracle.
#[derive(Debug, Clone, Copy)]
pub struct Applied {
    /// Node that applied it.
    pub node: NodeId,
    /// Entry.
    pub entry: Entry,
}

/// Recovery event counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeStats {
    /// Corruptions detected (load or read).
    pub detected: u64,
    /// CTRL: entries repaired in place.
    pub repaired: u64,
    /// CTRL: uncommitted corrupted entries discarded after a majority said
    /// it lacked them.
    pub discarded: u64,
    /// Entries removed by vanilla truncation on corruption.
    pub truncated_by_corruption: u64,
    /// Torn tails removed at load.
    pub torn_tails: u64,
    /// Conflict truncations below the commit index (safety violation).
    pub committed_truncated: u64,
}

/// A Raft node.
#[derive(Debug)]
pub struct Node {
    /// Id.
    pub id: NodeId,
    n: usize,
    /// Policy.
    pub mode: Mode,
    timing: Timing,
    /// Injected bugs (negative controls).
    pub bugs: Bugs,
    // Metadata (persistent, not corruptible in this model).
    /// Current term.
    pub term: u64,
    voted_for: Option<NodeId>,
    /// Log device (persistent).
    pub disk: Disk,
    // Volatile.
    terms: Vec<u64>,
    /// CTRL: indices whose data sector is known to be corrupted.
    pub faulty: BTreeSet<u64>,
    /// Role.
    pub role: Role,
    leader_hint: Option<NodeId>,
    /// Commit index.
    pub commit: u64,
    /// Applied index.
    pub applied: u64,
    kv: [u64; KEYS],
    votes: u64,
    next: Vec<u64>,
    matched: Vec<u64>,
    deadline: Time,
    hb_due: Time,
    waiting: HashMap<u64, (usize, u64)>,
    fix: BTreeMap<u64, (u64, u64)>,
    /// Outgoing peer messages.
    pub out: Vec<(NodeId, Msg)>,
    /// Client replies `(client, tag, reply)`.
    pub replies: Vec<(usize, u64, Reply)>,
    /// Applied entries since last drain.
    pub applied_log: Vec<Applied>,
    /// Counters.
    pub stats: NodeStats,
}

fn majority(n: usize) -> usize {
    n / 2 + 1
}

impl Node {
    /// New node with an empty disk.
    pub fn new(id: NodeId, n: usize, mode: Mode, timing: Timing, jitter: u64) -> Self {
        let mut nd = Node {
            id,
            n,
            mode,
            timing,
            bugs: Bugs::default(),
            term: 0,
            voted_for: None,
            disk: Disk::new(mode == Mode::Ctrl),
            terms: Vec::new(),
            faulty: BTreeSet::new(),
            role: Role::Follower,
            leader_hint: None,
            commit: 0,
            applied: 0,
            kv: [0; KEYS],
            votes: 0,
            next: vec![1; n],
            matched: vec![0; n],
            deadline: 0,
            hb_due: 0,
            waiting: HashMap::new(),
            fix: BTreeMap::new(),
            out: Vec::new(),
            replies: Vec::new(),
            applied_log: Vec::new(),
            stats: NodeStats::default(),
        };
        nd.reset_deadline(0, jitter);
        nd
    }

    fn reset_deadline(&mut self, now: Time, jitter: u64) {
        let span = self.timing.election_max - self.timing.election_min;
        self.deadline = now + self.timing.election_min + jitter % span;
    }

    /// Last log index.
    pub fn last_idx(&self) -> u64 {
        self.terms.len() as u64
    }

    fn term_at(&self, i: u64) -> u64 {
        if i == 0 {
            0
        } else {
            self.terms[i as usize - 1]
        }
    }

    fn last_term(&self) -> u64 {
        self.term_at(self.last_idx())
    }

    /// Restart: drop volatile state and rebuild the in-memory index from
    /// the disk, applying the mode's policy to bad sectors.
    pub fn restart(&mut self, now: Time, jitter: u64) {
        self.terms.clear();
        self.faulty.clear();
        self.role = Role::Follower;
        self.leader_hint = None;
        self.commit = 0;
        self.applied = 0;
        self.kv = [0; KEYS];
        self.votes = 0;
        self.waiting.clear();
        self.fix.clear();
        let len = self.disk.len();
        for i in 1..=len {
            match self.disk.read(i) {
                Ok(e) => {
                    if let (Mode::Ctrl, Some(id)) = (self.mode, self.disk.id(i)) {
                        debug_assert_eq!(id.term, e.term);
                    }
                    self.terms.push(e.term);
                }
                Err(ReadErr::Corrupt { id }) => match (self.mode, id) {
                    (Mode::Ctrl, Some(id)) => {
                        self.stats.detected += 1;
                        self.terms.push(id.term);
                        self.faulty.insert(i);
                    }
                    (Mode::Ctrl, None) => {
                        // No identifier record: the append never completed,
                        // so it was never acknowledged. Safe to drop.
                        self.stats.torn_tails += len - i + 1;
                        self.disk.truncate_from(i);
                        break;
                    }
                    (Mode::Vanilla, _) => {
                        if i < len {
                            self.stats.detected += 1;
                            self.stats.truncated_by_corruption += len - i + 1;
                        } else {
                            self.stats.torn_tails += 1;
                        }
                        self.disk.truncate_from(i);
                        break;
                    }
                },
            }
        }
        self.reset_deadline(now, jitter);
    }

    /// Read entry `i`; on corruption apply the mode's policy and return
    /// `None`. In vanilla mode this restarts the node, so callers must stop.
    fn read(&mut self, i: u64, now: Time, jitter: u64) -> Option<Entry> {
        match self.disk.read(i) {
            Ok(e) => Some(e),
            Err(ReadErr::Corrupt { .. }) => {
                self.stats.detected += 1;
                match self.mode {
                    Mode::Vanilla => {
                        // Vanilla recovery restarts the process; its log scan
                        // truncates at the bad entry.
                        self.stats.detected -= 1; // counted again by restart
                        self.restart(now, jitter);
                    }
                    Mode::Ctrl => {
                        self.faulty.insert(i);
                        if self.role == Role::Leader {
                            let t = self.term_at(i);
                            self.fix.entry(i).or_insert((t, 0));
                        }
                    }
                }
                None
            }
        }
    }

    fn truncate_from(&mut self, from: u64) {
        if from <= self.commit {
            // Raft never does this; it only happens after a policy lost a
            // committed entry. Record it so the simulator flags the run.
            self.stats.committed_truncated += 1;
            self.commit = from - 1;
        }
        self.disk.truncate_from(from);
        self.terms.truncate(from as usize - 1);
        self.faulty.retain(|&i| i < from);
        self.fix.retain(|&i, _| i < from);
        self.waiting.retain(|&i, _| i < from);
    }

    fn append(&mut self, e: Entry) {
        self.disk.append(&e);
        self.terms.push(e.term);
    }

    fn step_down(&mut self, term: u64, now: Time, jitter: u64) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        if self.role != Role::Follower {
            self.role = Role::Follower;
            self.waiting.clear();
            self.fix.clear();
        }
        self.reset_deadline(now, jitter);
    }

    /// Clock tick.
    pub fn tick(&mut self, now: Time, jitter: u64) {
        match self.role {
            Role::Leader => {
                if now >= self.hb_due {
                    self.hb_due = now + self.timing.heartbeat;
                    self.broadcast_append(now, jitter);
                    self.send_fix_requests();
                }
            }
            _ => {
                if now >= self.deadline {
                    self.term += 1;
                    self.voted_for = Some(self.id);
                    self.role = Role::Candidate;
                    self.votes = 1 << self.id;
                    self.leader_hint = None;
                    self.reset_deadline(now, jitter);
                    let (last_idx, last_term) = (self.last_idx(), self.last_term());
                    for p in 0..self.n {
                        if p != self.id {
                            self.out.push((
                                p,
                                Msg::Vote {
                                    term: self.term,
                                    last_idx,
                                    last_term,
                                },
                            ));
                        }
                    }
                    if self.n == 1 {
                        self.become_leader(now, jitter);
                    }
                }
            }
        }
    }

    fn become_leader(&mut self, now: Time, jitter: u64) {
        self.role = Role::Leader;
        self.leader_hint = Some(self.id);
        let last = self.last_idx();
        self.next = vec![last + 1; self.n];
        self.matched = vec![0; self.n];
        self.fix.clear();
        let faulty: Vec<u64> = self.faulty.iter().copied().collect();
        for i in faulty {
            self.fix.insert(i, (self.term_at(i), 0));
        }
        let e = Entry {
            index: last + 1,
            term: self.term,
            cmd: Cmd::Noop,
            tag: 0,
        };
        self.append(e);
        self.hb_due = now + self.timing.heartbeat;
        self.broadcast_append(now, jitter);
        self.send_fix_requests();
        self.advance_commit(now, jitter);
    }

    fn send_fix_requests(&mut self) {
        let reqs: Vec<(u64, u64)> = self
            .fix
            .iter()
            .take(8)
            .map(|(&i, &(t, _))| (i, t))
            .collect();
        for (idx, eterm) in reqs {
            for p in 0..self.n {
                if p != self.id {
                    self.out.push((
                        p,
                        Msg::FixReq {
                            term: self.term,
                            idx,
                            eterm,
                        },
                    ));
                }
            }
        }
    }

    fn broadcast_append(&mut self, now: Time, jitter: u64) {
        for p in 0..self.n {
            if p != self.id {
                self.send_append(p, now, jitter);
                if self.role != Role::Leader {
                    return;
                }
            }
        }
    }

    fn send_append(&mut self, to: NodeId, now: Time, jitter: u64) {
        let last = self.last_idx();
        let next = self.next[to].clamp(1, last + 1);
        let prev_idx = next - 1;
        let prev_term = self.term_at(prev_idx);
        let mut entries = Vec::new();
        let mut i = next;
        while i <= last && i < next + BATCH {
            if self.faulty.contains(&i) {
                break;
            }
            match self.read(i, now, jitter) {
                Some(e) => entries.push(e),
                None => {
                    if self.role != Role::Leader {
                        return;
                    }
                    break;
                }
            }
            i += 1;
        }
        self.out.push((
            to,
            Msg::Append {
                term: self.term,
                prev_idx,
                prev_term,
                entries,
                commit: self.commit,
            },
        ));
    }

    fn self_match(&self) -> u64 {
        match self.faulty.first() {
            Some(&f) => f - 1,
            None => self.last_idx(),
        }
        .min(self.last_idx())
    }

    fn advance_commit(&mut self, now: Time, jitter: u64) {
        if self.role != Role::Leader {
            return;
        }
        let maj = majority(self.n);
        let sm = self.self_match();
        let mut nidx = self.last_idx();
        while nidx > self.commit {
            if self.term_at(nidx) == self.term {
                let mut c = usize::from(sm >= nidx);
                for p in 0..self.n {
                    if p != self.id && self.matched[p] >= nidx {
                        c += 1;
                    }
                }
                if c >= maj {
                    self.commit = nidx;
                    break;
                }
            } else {
                break;
            }
            nidx -= 1;
        }
        self.apply(now, jitter);
    }

    fn apply(&mut self, now: Time, jitter: u64) {
        while self.applied < self.commit {
            let i = self.applied + 1;
            if self.faulty.contains(&i) {
                return;
            }
            let Some(e) = self.read(i, now, jitter) else {
                return;
            };
            self.applied = i;
            let res = match e.cmd {
                Cmd::Noop => None,
                Cmd::Put { key, val } => {
                    self.kv[key as usize % KEYS] = val;
                    None
                }
                Cmd::Get { key } => Some(self.kv[key as usize % KEYS]),
            };
            self.applied_log.push(Applied {
                node: self.id,
                entry: e,
            });
            if self.role == Role::Leader {
                if let Some((client, tag)) = self.waiting.remove(&i) {
                    if tag == e.tag {
                        self.replies.push((client, tag, Reply::Done(res)));
                    }
                }
            }
        }
    }

    /// Client request.
    pub fn client(&mut self, client: usize, tag: u64, cmd: Cmd, now: Time, jitter: u64) {
        if self.role != Role::Leader {
            self.replies
                .push((client, tag, Reply::NotLeader(self.leader_hint)));
            return;
        }
        let e = Entry {
            index: self.last_idx() + 1,
            term: self.term,
            cmd,
            tag,
        };
        self.append(e);
        self.waiting.insert(e.index, (client, tag));
        self.broadcast_append(now, jitter);
        self.advance_commit(now, jitter);
    }

    /// Deliver a peer message.
    pub fn recv(&mut self, from: NodeId, msg: Msg, now: Time, jitter: u64) {
        match msg {
            Msg::Vote {
                term,
                last_idx,
                last_term,
            } => {
                if term > self.term {
                    self.step_down(term, now, jitter);
                }
                let up_to_date = last_term > self.last_term()
                    || (last_term == self.last_term() && last_idx >= self.last_idx());
                let granted = term == self.term
                    && match self.voted_for {
                        None => true,
                        Some(v) => v == from,
                    }
                    && up_to_date;
                if granted {
                    self.voted_for = Some(from);
                    self.reset_deadline(now, jitter);
                }
                self.out.push((
                    from,
                    Msg::VoteResp {
                        term: self.term,
                        granted,
                    },
                ));
            }
            Msg::VoteResp { term, granted } => {
                if term > self.term {
                    self.step_down(term, now, jitter);
                    return;
                }
                if self.role == Role::Candidate && term == self.term && granted {
                    self.votes |= 1 << from;
                    if self.votes.count_ones() as usize >= majority(self.n) {
                        self.become_leader(now, jitter);
                    }
                }
            }
            Msg::Append {
                term,
                prev_idx,
                prev_term,
                entries,
                commit,
            } => self.on_append(
                from, term, prev_idx, prev_term, entries, commit, now, jitter,
            ),
            Msg::AppendResp {
                term,
                ok,
                match_idx,
            } => {
                if term > self.term {
                    self.step_down(term, now, jitter);
                    return;
                }
                if self.role != Role::Leader || term != self.term {
                    return;
                }
                if ok {
                    self.matched[from] = self.matched[from].max(match_idx);
                    self.next[from] = match_idx + 1;
                    self.advance_commit(now, jitter);
                    let nx = self.next[from];
                    // Only push more if there is a verified entry to send;
                    // a corrupted one waits for its repair.
                    if self.role == Role::Leader
                        && nx <= self.last_idx()
                        && !self.faulty.contains(&nx)
                    {
                        self.send_append(from, now, jitter);
                    }
                } else {
                    self.next[from] = (match_idx + 1).max(1);
                    self.send_append(from, now, jitter);
                }
            }
            Msg::FixReq { term, idx, eterm } => {
                if term < self.term {
                    return;
                }
                if term > self.term {
                    self.step_down(term, now, jitter);
                }
                let ans = if idx >= 1 && idx <= self.last_idx() && self.term_at(idx) == eterm {
                    if self.faulty.contains(&idx) {
                        FixAns::HaveFaulty
                    } else {
                        match self.read(idx, now, jitter) {
                            Some(e) => FixAns::Have(e),
                            None if self.mode == Mode::Ctrl => FixAns::HaveFaulty,
                            None => return,
                        }
                    }
                } else {
                    FixAns::Lack
                };
                self.out.push((
                    from,
                    Msg::FixResp {
                        term: self.term,
                        idx,
                        eterm,
                        ans,
                    },
                ));
            }
            Msg::FixResp {
                term,
                idx,
                eterm,
                ans,
            } => {
                if term > self.term {
                    self.step_down(term, now, jitter);
                    return;
                }
                if self.role != Role::Leader || term != self.term {
                    return;
                }
                let Some(&(fterm, lack)) = self.fix.get(&idx) else {
                    return;
                };
                if fterm != eterm || !self.faulty.contains(&idx) || self.term_at(idx) != eterm {
                    return;
                }
                match ans {
                    FixAns::Have(e) if e.index == idx && e.term == eterm => {
                        self.disk.overwrite(&e);
                        self.faulty.remove(&idx);
                        self.fix.remove(&idx);
                        self.stats.repaired += 1;
                        self.advance_commit(now, jitter);
                        self.broadcast_append(now, jitter);
                    }
                    FixAns::Lack => {
                        let lack = lack | (1 << from);
                        self.fix.insert(idx, (fterm, lack));
                        let need = if self.bugs.discard_on_one_lack {
                            1
                        } else {
                            majority(self.n)
                        };
                        if lack.count_ones() as usize >= need && idx > self.commit {
                            // A majority lacks (idx, eterm): it cannot be
                            // committed, and neither can anything after it.
                            self.stats.discarded += self.last_idx() - idx + 1;
                            self.truncate_from(idx);
                            // Never reuse (index, term) pairs: give up the
                            // term so the next leader has a higher one.
                            self.step_down(self.term, now, jitter);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append(
        &mut self,
        from: NodeId,
        term: u64,
        prev_idx: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
        now: Time,
        jitter: u64,
    ) {
        if term < self.term {
            self.out.push((
                from,
                Msg::AppendResp {
                    term: self.term,
                    ok: false,
                    match_idx: 0,
                },
            ));
            return;
        }
        if term > self.term || self.role != Role::Follower {
            self.step_down(term, now, jitter);
        }
        self.leader_hint = Some(from);
        self.reset_deadline(now, jitter);
        if prev_idx > self.last_idx() {
            self.out.push((
                from,
                Msg::AppendResp {
                    term: self.term,
                    ok: false,
                    match_idx: self.last_idx(),
                },
            ));
            return;
        }
        if self.term_at(prev_idx) != prev_term {
            let ct = self.term_at(prev_idx);
            let mut h = prev_idx - 1;
            while h > self.commit && self.term_at(h) == ct {
                h -= 1;
            }
            self.out.push((
                from,
                Msg::AppendResp {
                    term: self.term,
                    ok: false,
                    match_idx: h,
                },
            ));
            return;
        }
        let end = prev_idx + entries.len() as u64;
        for e in entries {
            let j = e.index;
            if j <= self.last_idx() {
                if self.term_at(j) == e.term {
                    if self.faulty.remove(&j) {
                        // Same (index, term) as the leader's verified copy:
                        // by log matching it is the original entry.
                        self.disk.overwrite(&e);
                        self.stats.repaired += 1;
                    }
                    continue;
                }
                self.truncate_from(j);
            }
            self.append(e);
        }
        if commit > self.commit {
            self.commit = commit.min(end).max(self.commit);
        }
        self.apply(now, jitter);
        // The vanilla read path may have restarted the node.
        let verified = match self.faulty.first() {
            Some(&f) => (f - 1).min(end),
            None => end,
        };
        self.out.push((
            from,
            Msg::AppendResp {
                term: self.term,
                ok: true,
                match_idx: verified.min(self.last_idx()),
            },
        ));
    }

    /// Value of `key` in the applied state (tests).
    pub fn value(&self, key: u8) -> u64 {
        self.kv[key as usize % KEYS]
    }
}
