//! CLI: run seeds and print a one-line summary; optional gates set the exit
//! code so CI can enforce the acceptance criteria.

use std::process::ExitCode;
use std::time::Instant;

use raft_ctrl_rs::node::{Bugs, Mode};
use raft_ctrl_rs::sim::{run_range, Config, Faults, Summary};

fn usage() -> ExitCode {
    eprintln!(
        "usage:\n  raft-ctrl-rs run --mode ctrl|vanilla [--faults none|minority|majority] [--seeds N] [--start S] [--nodes N]\n                  [--bug discard-on-one-lack]\n                  [--require-safe] [--require-violation] [--min-recovery F]\n  raft-ctrl-rs overhead [--seeds N] [--reps R] [--max-overhead F]"
    );
    ExitCode::from(2)
}

fn summary_line(cfg: &Config, start: u64, s: &Summary, secs: f64) -> String {
    format!(
        "mode={:?} faults={:?} bugs={:?} nodes={} seeds={}..{} violations={} unknown={} corrupted_seeds={} recovered={} recovery_rate={:.4} injected={} detected={} repaired={} discarded={} truncated_by_corruption={} completed_ops={} mean_latency={:.2} data_bytes={} id_bytes={} entries_written={} wall_s={:.3} bad_seeds={:?}",
        cfg.mode,
        cfg.faults,
        cfg.bugs,
        cfg.nodes,
        start,
        start + s.seeds,
        s.violations,
        s.unknown,
        s.corrupted_seeds,
        s.recovered_seeds,
        s.recovery_rate(),
        s.injected,
        s.detected,
        s.repaired,
        s.discarded,
        s.truncated_by_corruption,
        s.completed,
        s.latency_sum as f64 / s.completed.max(1) as f64,
        s.disk.data_bytes,
        s.disk.id_bytes,
        s.disk.entries_written,
        secs,
        s.bad_seeds
    )
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first().cloned() else {
        return usage();
    };
    let mut mode = Mode::Ctrl;
    let mut faults = Faults::Minority;
    let mut seeds = 1000u64;
    let mut start = 0u64;
    let mut nodes = 5usize;
    let mut reps = 5usize;
    let mut require_safe = false;
    let mut require_violation = false;
    let mut min_recovery: Option<f64> = None;
    let mut max_overhead: Option<f64> = None;
    let mut bugs = Bugs::default();
    let mut it = args.iter().skip(1);
    while let Some(a) = it.next() {
        let mut val = || it.next().cloned().unwrap_or_default();
        match a.as_str() {
            "--mode" => {
                mode = match val().as_str() {
                    "ctrl" => Mode::Ctrl,
                    "vanilla" => Mode::Vanilla,
                    _ => return usage(),
                }
            }
            "--faults" => {
                faults = match val().as_str() {
                    "none" => Faults::None,
                    "minority" => Faults::Minority,
                    "majority" => Faults::Majority,
                    _ => return usage(),
                }
            }
            "--seeds" => seeds = val().parse().unwrap_or(0),
            "--start" => start = val().parse().unwrap_or(0),
            "--nodes" => nodes = val().parse().unwrap_or(5),
            "--reps" => reps = val().parse().unwrap_or(5),
            "--bug" => match val().as_str() {
                "discard-on-one-lack" => bugs.discard_on_one_lack = true,
                _ => return usage(),
            },
            "--require-safe" => require_safe = true,
            "--require-violation" => require_violation = true,
            "--min-recovery" => min_recovery = val().parse().ok(),
            "--max-overhead" => max_overhead = val().parse().ok(),
            _ => return usage(),
        }
    }
    if seeds == 0 || !(1..=63).contains(&nodes) {
        return usage();
    }
    match cmd.as_str() {
        "run" => {
            let mut cfg = Config::new(nodes, mode, faults);
            cfg.bugs = bugs;
            let t0 = Instant::now();
            let s = run_range(&cfg, start, seeds);
            println!(
                "{}",
                summary_line(&cfg, start, &s, t0.elapsed().as_secs_f64())
            );
            let mut ok = true;
            if require_safe && s.violations > 0 {
                eprintln!("FAIL: {} seeds violated safety", s.violations);
                ok = false;
            }
            if require_safe && s.unknown > 0 {
                eprintln!("FAIL: {} seeds not decided by the checker", s.unknown);
                ok = false;
            }
            if require_violation && s.violations == 0 {
                eprintln!("FAIL: expected at least one safety violation, saw none");
                ok = false;
            }
            if let Some(m) = min_recovery {
                if s.corrupted_seeds == 0 || s.recovery_rate().is_nan() || s.recovery_rate() < m {
                    eprintln!("FAIL: recovery rate {:.4} < {m}", s.recovery_rate());
                    ok = false;
                }
            }
            if ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        "overhead" => overhead(nodes, start, seeds, reps.max(1), max_overhead),
        _ => usage(),
    }
}

fn overhead(nodes: usize, start: u64, seeds: u64, reps: usize, max: Option<f64>) -> ExitCode {
    let cv = Config::new(nodes, Mode::Vanilla, Faults::None);
    let cc = Config::new(nodes, Mode::Ctrl, Faults::None);
    let mut tv = Vec::new();
    let mut tc = Vec::new();
    let mut sv = Summary::default();
    let mut sc = Summary::default();
    // Interleave repetitions so drift on a shared machine hits both.
    for _ in 0..reps {
        let t0 = Instant::now();
        sv = run_range(&cv, start, seeds);
        tv.push(t0.elapsed().as_secs_f64());
        let t0 = Instant::now();
        sc = run_range(&cc, start, seeds);
        tc.push(t0.elapsed().as_secs_f64());
    }
    tv.sort_by(|a, b| a.partial_cmp(b).unwrap());
    tc.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let (mv, mc) = (tv[reps / 2], tc[reps / 2]);
    let cpu = mc / mv - 1.0;
    let ops = 1.0 - sc.completed as f64 / sv.completed as f64;
    let lat_v = sv.latency_sum as f64 / sv.completed.max(1) as f64;
    let lat_c = sc.latency_sum as f64 / sc.completed.max(1) as f64;
    let lat = lat_c / lat_v - 1.0;
    let bytes_v = sv.disk.data_bytes + sv.disk.id_bytes;
    let bytes_c = sc.disk.data_bytes + sc.disk.id_bytes;
    let bytes = bytes_c as f64 / bytes_v as f64 - 1.0;
    println!("{}", summary_line(&cv, start, &sv, mv));
    println!("{}", summary_line(&cc, start, &sc, mc));
    println!(
        "overhead seeds={seeds} reps={reps} median_wall_vanilla_s={mv:.3} median_wall_ctrl_s={mc:.3} cpu_overhead={:.2}% throughput_loss={:.2}% latency_overhead={:.2}% disk_bytes_overhead={:.2}%",
        cpu * 100.0,
        ops * 100.0,
        lat * 100.0,
        bytes * 100.0
    );
    if let Some(m) = max {
        let worst = cpu.max(ops).max(lat).max(bytes);
        if sv.violations + sc.violations > 0 {
            eprintln!("FAIL: fault-free runs violated safety");
            return ExitCode::FAILURE;
        }
        if worst > m {
            eprintln!(
                "FAIL: worst overhead {:.2}% > {:.2}%",
                worst * 100.0,
                m * 100.0
            );
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}
