//! Seeded runs. The 10k-seed gates run in CI in release mode; these are the
//! fast versions that run with `cargo test`.

use cortex9_raft_sim::lin::Verdict;
use raft_ctrl_rs::node::{Bugs, Mode};
use raft_ctrl_rs::sim::{run, run_range, Config, Faults};

#[test]
fn ctrl_is_safe_and_recovers_under_minority_corruption() {
    for nodes in [3, 5] {
        let cfg = Config::new(nodes, Mode::Ctrl, Faults::Minority);
        let s = run_range(&cfg, 0, 25);
        assert_eq!(s.violations, 0, "{nodes} nodes: {:?}", s.bad_seeds);
        assert_eq!(s.unknown, 0);
        assert!(s.injected > 0 && s.detected > 0 && s.repaired > 0);
        assert!(s.recovery_rate() >= 0.99, "{nodes}: {}", s.recovery_rate());
    }
}

#[test]
fn vanilla_loses_data_under_the_same_faults() {
    let cfg = Config::new(5, Mode::Vanilla, Faults::Minority);
    let s = run_range(&cfg, 0, 25);
    assert!(s.violations > 0, "vanilla should lose data in 25 seeds");
    assert!(s.truncated_by_corruption > 0);
}

#[test]
fn ctrl_stays_safe_when_a_majority_is_corrupted() {
    let cfg = Config::new(5, Mode::Ctrl, Faults::Majority);
    let s = run_range(&cfg, 0, 25);
    assert_eq!(s.violations, 0, "{:?}", s.bad_seeds);
    assert!(
        s.discarded > 0,
        "majority-lack discard path must be exercised"
    );
}

#[test]
fn negative_control_unsafe_discard_is_caught() {
    let mut cfg = Config::new(5, Mode::Ctrl, Faults::Majority);
    cfg.bugs = Bugs {
        discard_on_one_lack: true,
    };
    // Seed 18 is the first one caught by the 200-seed run (see README).
    let r = run(&cfg, 18);
    assert!(r.violated(), "{:?} sms={}", r.lin, r.sms_violations);
}

#[test]
fn deterministic_and_fault_free_modes_behave_identically() {
    let a = run(&Config::new(5, Mode::Ctrl, Faults::Minority), 3);
    let b = run(&Config::new(5, Mode::Ctrl, Faults::Minority), 3);
    assert_eq!(a.history.len(), b.history.len());
    assert_eq!(a.completed, b.completed);
    assert_eq!(a.injected, b.injected);
    for seed in 0..5 {
        let v = run(&Config::new(5, Mode::Vanilla, Faults::None), seed);
        let c = run(&Config::new(5, Mode::Ctrl, Faults::None), seed);
        assert_eq!(v.completed, c.completed, "seed {seed}");
        assert_eq!(v.latency_sum, c.latency_sum, "seed {seed}");
        assert_eq!(v.disk.data_bytes, c.disk.data_bytes);
        assert_eq!(c.disk.id_bytes * 16, c.disk.data_bytes);
        assert_eq!(v.lin, Some(Verdict::Ok));
        assert_eq!(c.lin, Some(Verdict::Ok));
    }
}

#[test]
fn checker_catches_a_lost_write_in_a_hand_built_history() {
    use cortex9_raft_sim::lin::{check, Event, RegOp};
    // Write 7 acknowledged, then a later read returns 0.
    let h = [
        Event {
            call: 0,
            ret: Some(5),
            op: RegOp::Write(7),
        },
        Event {
            call: 10,
            ret: Some(12),
            op: RegOp::Read(0),
        },
    ];
    assert_eq!(check(&h, 0, 1000), Verdict::Illegal);
}
