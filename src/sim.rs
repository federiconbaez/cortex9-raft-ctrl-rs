//! Deterministic cluster simulation with sector corruption, crashes, torn
//! writes and message loss, plus the checks that decide each run.

use cortex9_raft_sim::lin::{self, Event, RegOp, Verdict};
use cortex9_raft_sim::rng::Rng;

use crate::disk::{Cmd, DiskStats, Entry};
use crate::node::{Bugs, Mode, Msg, Node, NodeId, Reply, Time, Timing, KEYS};

/// Which corruption faults to inject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Faults {
    /// No corruption (crashes, torn tails and message loss still happen).
    None,
    /// Corruption, but for every log index at most a minority of replicas
    /// ever get that index corrupted.
    Minority,
    /// Corruption of the same index on every replica that holds it.
    Majority,
}

/// Simulation parameters.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Cluster size.
    pub nodes: usize,
    /// Recovery policy.
    pub mode: Mode,
    /// Corruption model.
    pub faults: Faults,
    /// Concurrent clients.
    pub clients: usize,
    /// Ticks with faults enabled.
    pub fault_ticks: Time,
    /// Ticks after the scrub restarts, with no faults, to recover.
    pub heal_ticks: Time,
    /// Message drop probability (fault phase only).
    pub drop: f64,
    /// Max one-way message delay.
    pub delay_max: Time,
    /// Per node per tick crash probability.
    pub crash_rate: f64,
    /// Downtime range after a crash.
    pub down: (Time, Time),
    /// Probability that a crash leaves a torn tail sector.
    pub torn_p: f64,
    /// Per tick probability of a corruption event.
    pub corrupt_rate: f64,
    /// Client gives up after this many ticks without a reply.
    pub client_timeout: Time,
    /// Raft timing.
    pub timing: Timing,
    /// Linearizability search budget per key.
    pub lin_budget: u64,
    /// Deliberate bugs (negative controls).
    pub bugs: Bugs,
}

impl Config {
    /// Defaults for `nodes`, `mode`, `faults`.
    pub fn new(nodes: usize, mode: Mode, faults: Faults) -> Self {
        Config {
            nodes,
            mode,
            faults,
            clients: 3,
            fault_ticks: 3000,
            heal_ticks: 1500,
            drop: 0.01,
            delay_max: 8,
            crash_rate: 0.0004,
            down: (20, 200),
            torn_p: 0.3,
            corrupt_rate: 0.004,
            client_timeout: 300,
            timing: Timing::default(),
            lin_budget: 2_000_000,
            bugs: Bugs::default(),
        }
    }
}

/// Client operation record.
#[derive(Debug, Clone, Copy)]
pub struct HistOp {
    /// Key.
    pub key: u8,
    /// Write value, or `None` for a read.
    pub write: Option<u64>,
    /// Read result.
    pub read: Option<u64>,
    /// Invocation time.
    pub call: Time,
    /// Response time, `None` if the client gave up.
    pub ret: Option<Time>,
}

/// Outcome of one seed.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// Seed.
    pub seed: u64,
    /// Corruption events injected (sectors).
    pub injected: u64,
    /// Corruptions detected by nodes.
    pub detected: u64,
    /// CTRL repairs.
    pub repaired: u64,
    /// CTRL discards of uncommitted corrupted entries.
    pub discarded: u64,
    /// Entries removed by vanilla truncation.
    pub truncated_by_corruption: u64,
    /// Linearizability verdict over all keys (worst).
    pub lin: Option<Verdict>,
    /// State-machine-safety violations seen by the apply oracle.
    pub sms_violations: u64,
    /// Conflict truncations below a commit index.
    pub committed_truncated: u64,
    /// Every replica ended with no corrupted entry, applied everything
    /// committed before the fault phase ended, and clients made progress
    /// after the scrub.
    pub recovered: bool,
    /// Client operations that completed.
    pub completed: u64,
    /// Sum of latencies of completed operations.
    pub latency_sum: u64,
    /// Highest committed index seen by the oracle.
    pub max_committed: u64,
    /// Disk counters summed over nodes.
    pub disk: DiskStats,
    /// History (kept for tests and debugging).
    pub history: Vec<HistOp>,
}

impl Report {
    /// True if any safety check failed.
    pub fn violated(&self) -> bool {
        self.lin == Some(Verdict::Illegal)
            || self.sms_violations > 0
            || self.committed_truncated > 0
    }
}

enum Ev {
    Peer {
        from: NodeId,
        to: NodeId,
        msg: Msg,
    },
    ToNode {
        client: usize,
        node: NodeId,
        tag: u64,
        cmd: Cmd,
    },
    ToClient {
        client: usize,
        tag: u64,
        reply: Reply,
    },
}

struct Pending {
    tag: u64,
    cmd: Cmd,
    hist: usize,
    sent_at: Time,
}

struct Client {
    target: NodeId,
    next_at: Time,
    pending: Option<Pending>,
}

/// Run one seed.
pub fn run(cfg: &Config, seed: u64) -> Report {
    let mut root = Rng::new(seed);
    let mut net = root.fork();
    let mut faults = root.fork();
    let mut jit = root.fork();
    let mut crng = root.fork();
    let n = cfg.nodes;
    let fault_end = cfg.fault_ticks;
    let scrub_gap: Time = 150;
    let scrub_end = fault_end + scrub_gap * n as Time;
    let end = scrub_end + cfg.heal_ticks;

    let mut nodes: Vec<Node> = (0..n)
        .map(|i| {
            let mut nd = Node::new(i, n, cfg.mode, cfg.timing, jit.next_u64());
            nd.bugs = cfg.bugs;
            nd
        })
        .collect();
    let mut down_until: Vec<Option<Time>> = vec![None; n];
    let mut crash_at: Vec<Option<Time>> = vec![None; n];
    let mut queue: Vec<Vec<Ev>> = (0..=end + cfg.delay_max + 2).map(|_| Vec::new()).collect();
    let mut clients: Vec<Client> = (0..cfg.clients)
        .map(|c| Client {
            target: c % n,
            next_at: 1 + c as Time,
            pending: None,
        })
        .collect();
    let mut history: Vec<HistOp> = Vec::new();
    let mut next_tag: u64 = 1;
    let mut next_val: u64 = 1;
    let mut oracle: Vec<Option<Entry>> = Vec::new();
    let mut rep = Report {
        seed,
        ..Report::default()
    };
    let mut corrupted: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
    let mut committed_at_fault_end: u64 = 0;
    let mut progress_after_scrub = false;

    for t in 0..end {
        let faulty_phase = t < fault_end;
        // Faults.
        if t == fault_end {
            committed_at_fault_end = rep.max_committed;
            for (i, d) in down_until.iter_mut().enumerate() {
                if d.take().is_some() {
                    nodes[i].restart(t, jit.next_u64());
                }
                crash_at[i] = None;
            }
        }
        if t >= fault_end && t < scrub_end && (t - fault_end) % scrub_gap == 0 {
            // Rolling restart: every replica re-reads (scrubs) its log.
            let k = ((t - fault_end) / scrub_gap) as usize;
            nodes[k].restart(t, jit.next_u64());
        }
        if faulty_phase {
            for i in 0..n {
                if let Some(u) = down_until[i] {
                    if t >= u {
                        down_until[i] = None;
                        nodes[i].restart(t, jit.next_u64());
                    }
                    continue;
                }
                let forced = crash_at[i].is_some_and(|c| c <= t);
                if forced || faults.chance(cfg.crash_rate) {
                    crash_at[i] = None;
                    if faults.chance(cfg.torn_p) {
                        let b = faults.next_u64();
                        nodes[i].disk.torn_append(b);
                    }
                    down_until[i] = Some(t + faults.range(cfg.down.0, cfg.down.1));
                }
            }
            if cfg.faults != Faults::None && faults.chance(cfg.corrupt_rate) {
                inject(
                    cfg,
                    &mut nodes,
                    &mut faults,
                    &mut corrupted,
                    &mut crash_at,
                    &down_until,
                    t,
                    &mut rep,
                );
            }
        }

        // Deliver.
        let evs = std::mem::take(&mut queue[t as usize]);
        for ev in evs {
            match ev {
                Ev::Peer { from, to, msg } => {
                    if down_until[to].is_none() {
                        nodes[to].recv(from, msg, t, jit.next_u64());
                    }
                }
                Ev::ToNode {
                    client,
                    node,
                    tag,
                    cmd,
                } => {
                    if down_until[node].is_none() {
                        nodes[node].client(client, tag, cmd, t, jit.next_u64());
                    }
                }
                Ev::ToClient { client, tag, reply } => {
                    let c = &mut clients[client];
                    let Some(p) = c.pending.as_ref() else {
                        continue;
                    };
                    if p.tag != tag {
                        continue;
                    }
                    match reply {
                        Reply::Done(v) => {
                            let h = &mut history[p.hist];
                            h.ret = Some(t);
                            if h.write.is_none() {
                                h.read = v;
                            }
                            rep.completed += 1;
                            rep.latency_sum += t - h.call;
                            if h.call >= scrub_end {
                                progress_after_scrub = true;
                            }
                            c.pending = None;
                            c.next_at = t + crng.range(2, 15);
                        }
                        Reply::NotLeader(hint) => {
                            c.target = match hint {
                                Some(h) => h,
                                None => crng.range(0, n as u64) as usize,
                            };
                            let p = c.pending.as_mut().expect("pending");
                            p.sent_at = t;
                            let d = 1 + crng.range(0, cfg.delay_max);
                            queue[(t + d) as usize].push(Ev::ToNode {
                                client,
                                node: c.target,
                                tag: p.tag,
                                cmd: p.cmd,
                            });
                        }
                    }
                }
            }
        }

        // Node clocks.
        for (i, nd) in nodes.iter_mut().enumerate() {
            if down_until[i].is_none() {
                nd.tick(t, jit.next_u64());
            }
        }

        // Clients.
        if t + 200 < end {
            for (ci, c) in clients.iter_mut().enumerate() {
                if let Some(p) = c.pending.as_ref() {
                    if t >= p.sent_at + cfg.client_timeout {
                        // Give up: the outcome stays unknown to the checker.
                        c.pending = None;
                        c.target = crng.range(0, n as u64) as usize;
                        c.next_at = t + 1;
                    }
                    continue;
                }
                if t < c.next_at {
                    continue;
                }
                let key = crng.range(0, KEYS as u64) as u8;
                let cmd = if crng.chance(0.5) {
                    let v = next_val;
                    next_val += 1;
                    Cmd::Put { key, val: v }
                } else {
                    Cmd::Get { key }
                };
                let tag = next_tag;
                next_tag += 1;
                history.push(HistOp {
                    key,
                    write: match cmd {
                        Cmd::Put { val, .. } => Some(val),
                        _ => None,
                    },
                    read: None,
                    call: t,
                    ret: None,
                });
                c.pending = Some(Pending {
                    tag,
                    cmd,
                    hist: history.len() - 1,
                    sent_at: t,
                });
                let d = 1 + crng.range(0, cfg.delay_max);
                queue[(t + d) as usize].push(Ev::ToNode {
                    client: ci,
                    node: c.target,
                    tag,
                    cmd,
                });
            }
        }

        // Drain node outputs.
        for nd in nodes.iter_mut() {
            for a in nd.applied_log.drain(..) {
                let i = a.entry.index as usize;
                if oracle.len() < i {
                    oracle.resize(i, None);
                }
                match oracle[i - 1] {
                    None => oracle[i - 1] = Some(a.entry),
                    Some(prev) if prev != a.entry => rep.sms_violations += 1,
                    _ => {}
                }
                rep.max_committed = rep.max_committed.max(a.entry.index);
            }
            for (client, tag, reply) in nd.replies.drain(..) {
                let d = 1 + net.range(0, cfg.delay_max);
                queue[(t + d) as usize].push(Ev::ToClient { client, tag, reply });
            }
            let from = nd.id;
            for (to, msg) in nd.out.drain(..) {
                if faulty_phase && net.chance(cfg.drop) {
                    continue;
                }
                let d = 1 + net.range(0, cfg.delay_max);
                queue[(t + d) as usize].push(Ev::Peer { from, to, msg });
            }
        }
    }

    // Verdicts.
    let mut worst = Verdict::Ok;
    for k in 0..KEYS as u8 {
        let evs: Vec<Event> = history
            .iter()
            .filter(|h| h.key == k)
            .map(|h| Event {
                call: h.call,
                ret: h.ret,
                op: match h.write {
                    Some(v) => RegOp::Write(v),
                    None => RegOp::Read(h.read.unwrap_or(0)),
                },
            })
            .filter(|e| e.ret.is_some() || matches!(e.op, RegOp::Write(_)))
            .collect();
        match lin::check(&evs, 0, cfg.lin_budget) {
            Verdict::Illegal => worst = Verdict::Illegal,
            Verdict::Unknown if worst == Verdict::Ok => worst = Verdict::Unknown,
            _ => {}
        }
    }
    rep.lin = Some(worst);
    let mut clean = true;
    for nd in &nodes {
        rep.detected += nd.stats.detected;
        rep.repaired += nd.stats.repaired;
        rep.discarded += nd.stats.discarded;
        rep.truncated_by_corruption += nd.stats.truncated_by_corruption;
        rep.committed_truncated += nd.stats.committed_truncated;
        rep.disk.data_bytes += nd.disk.stats.data_bytes;
        rep.disk.id_bytes += nd.disk.stats.id_bytes;
        rep.disk.entries_written += nd.disk.stats.entries_written;
        if !nd.faulty.is_empty() || nd.applied < committed_at_fault_end {
            clean = false;
        }
    }
    rep.recovered = clean && progress_after_scrub;
    rep.history = history;
    rep
}

#[allow(clippy::too_many_arguments)]
fn inject(
    cfg: &Config,
    nodes: &mut [Node],
    rng: &mut Rng,
    corrupted: &mut std::collections::HashMap<u64, u64>,
    crash_at: &mut [Option<Time>],
    down_until: &[Option<Time>],
    t: Time,
    rep: &mut Report,
) {
    let n = nodes.len();
    let target = rng.range(0, n as u64) as usize;
    let len = nodes[target].disk.len();
    if len == 0 {
        return;
    }
    // Bias towards recent entries, where the interesting cases live.
    let idx = if rng.chance(0.7) {
        len - rng.range(0, len.min(6))
    } else {
        1 + rng.range(0, len)
    };
    let victims: Vec<usize> = match cfg.faults {
        Faults::None => return,
        Faults::Minority => {
            let m = corrupted.entry(idx).or_insert(0);
            let next = *m | (1 << target);
            if next.count_ones() as usize > (n - 1) / 2 {
                return;
            }
            *m = next;
            vec![target]
        }
        Faults::Majority => (0..n).filter(|&i| nodes[i].disk.len() >= idx).collect(),
    };
    for v in victims {
        let b = rng.next_u64();
        nodes[v].disk.corrupt(idx, b);
        rep.injected += 1;
        if down_until[v].is_none() && crash_at[v].is_none() && rng.chance(0.5) {
            crash_at[v] = Some(t + rng.range(1, 40));
        }
    }
}

/// Aggregate over a seed range.
#[derive(Debug, Clone, Default)]
pub struct Summary {
    /// Seeds run.
    pub seeds: u64,
    /// Seeds with a safety violation.
    pub violations: u64,
    /// Seeds whose checker ran out of budget.
    pub unknown: u64,
    /// Seeds with at least one corruption injected.
    pub corrupted_seeds: u64,
    /// Of those, seeds that recovered.
    pub recovered_seeds: u64,
    /// Sum of reports' counters.
    pub injected: u64,
    /// Detected.
    pub detected: u64,
    /// Repaired.
    pub repaired: u64,
    /// Discarded.
    pub discarded: u64,
    /// Truncated by corruption.
    pub truncated_by_corruption: u64,
    /// Completed ops.
    pub completed: u64,
    /// Latency sum.
    pub latency_sum: u64,
    /// Disk counters.
    pub disk: DiskStats,
    /// First violating seeds (up to 10).
    pub bad_seeds: Vec<u64>,
}

impl Summary {
    /// Fold one report in.
    pub fn add(&mut self, r: &Report) {
        self.seeds += 1;
        if r.violated() {
            self.violations += 1;
            if self.bad_seeds.len() < 10 {
                self.bad_seeds.push(r.seed);
            }
        }
        if r.lin == Some(Verdict::Unknown) {
            self.unknown += 1;
        }
        if r.injected > 0 {
            self.corrupted_seeds += 1;
            if r.recovered {
                self.recovered_seeds += 1;
            }
        }
        self.injected += r.injected;
        self.detected += r.detected;
        self.repaired += r.repaired;
        self.discarded += r.discarded;
        self.truncated_by_corruption += r.truncated_by_corruption;
        self.completed += r.completed;
        self.latency_sum += r.latency_sum;
        self.disk.data_bytes += r.disk.data_bytes;
        self.disk.id_bytes += r.disk.id_bytes;
        self.disk.entries_written += r.disk.entries_written;
    }

    /// Recovery rate among corrupted seeds.
    pub fn recovery_rate(&self) -> f64 {
        if self.corrupted_seeds == 0 {
            return f64::NAN;
        }
        self.recovered_seeds as f64 / self.corrupted_seeds as f64
    }
}

/// Run seeds `start..start + count`.
pub fn run_range(cfg: &Config, start: u64, count: u64) -> Summary {
    let mut s = Summary::default();
    for seed in start..start + count {
        let mut r = run(cfg, seed);
        r.history.clear();
        s.add(&r);
    }
    s
}
