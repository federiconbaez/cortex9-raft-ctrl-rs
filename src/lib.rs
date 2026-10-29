//! raft-ctrl-rs: Raft that survives corrupted disks.
//!
//! A Raft key-value store whose log lives on a simulated sector device that
//! can silently corrupt sectors. Two recovery policies share the same Raft
//! core:
//!
//! - [`node::Mode::Vanilla`]: on a checksum mismatch the node restarts and
//!   truncates its log at the first bad entry, the way a torn tail is
//!   handled. This is the behaviour that loses committed data.
//! - [`node::Mode::Ctrl`]: CTRL, protocol-aware recovery (Alagappan et al.,
//!   FAST'18). A redundant identifier record per entry separates a crashed
//!   tail from corruption, a corrupted entry keeps its place in the log, and
//!   it is repaired from other replicas. A leader may drop a corrupted entry
//!   only when a majority reports it does not have it.
//!
//! The simulator ([`sim`]) injects corruption, crashes, torn writes and
//! message loss, and checks every history for linearizability with the
//! checker from `cortex9-raft-sim`.

pub mod crc;
pub mod disk;
