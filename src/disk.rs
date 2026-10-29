//! Simulated sector device holding a Raft log.
//!
//! Every log entry occupies one 512-byte sector: index, term, command, a
//! deterministic filler and a CRC32C over the first 508 bytes. In CTRL mode a
//! second region holds a 32-byte identifier record per entry (index, term,
//! CRC of the data sector, CRC of the record). The identifier region is a
//! different place on the device, so a single sector fault does not hit both
//! (the same assumption the CTRL paper makes). Identifier records are written
//! after the data sector, so a crash can leave a torn data sector without an
//! identifier record but never the other way round.

use crate::crc::crc32c;

/// Bytes per data sector.
pub const SECTOR: usize = 512;
/// Bytes per identifier record.
pub const ID_REC: usize = 32;

/// Register key.
pub type Key = u8;

/// A replicated command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cmd {
    /// Leader no-op appended on election.
    Noop,
    /// Write `val` to `key`.
    Put {
        /// Key.
        key: Key,
        /// Value.
        val: u64,
    },
    /// Read `key` through the log.
    Get {
        /// Key.
        key: Key,
    },
}

/// One log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    /// 1-based log index.
    pub index: u64,
    /// Term in which a leader created it.
    pub term: u64,
    /// Command.
    pub cmd: Cmd,
    /// Client request tag (0 for no-ops).
    pub tag: u64,
}

/// Decoded identifier record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdRec {
    /// Log index.
    pub index: u64,
    /// Term.
    pub term: u64,
    /// CRC32C of the data sector it describes.
    pub data_crc: u32,
}

/// Why a data sector could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadErr {
    /// Checksum mismatch or undecodable content. `id` is the identifier
    /// record if one exists (CTRL only).
    Corrupt {
        /// Identifier record, if present and valid.
        id: Option<IdRec>,
    },
}

/// Byte and operation counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiskStats {
    /// Data-sector bytes written.
    pub data_bytes: u64,
    /// Identifier-record bytes written.
    pub id_bytes: u64,
    /// Entries written (appends and overwrites).
    pub entries_written: u64,
}

/// The device. Index `i` (1-based) lives at sector `i - 1`.
#[derive(Debug, Clone)]
pub struct Disk {
    data: Vec<u8>,
    ids: Vec<Option<[u8; ID_REC]>>,
    with_ids: bool,
    /// Counters.
    pub stats: DiskStats,
}

fn encode(e: &Entry) -> [u8; SECTOR] {
    let mut s = [0u8; SECTOR];
    s[0..8].copy_from_slice(&e.index.to_le_bytes());
    s[8..16].copy_from_slice(&e.term.to_le_bytes());
    let (kind, key, val) = match e.cmd {
        Cmd::Noop => (0u8, 0u8, 0u64),
        Cmd::Put { key, val } => (1, key, val),
        Cmd::Get { key } => (2, key, 0),
    };
    s[16] = kind;
    s[17] = key;
    s[18..26].copy_from_slice(&val.to_le_bytes());
    s[26..34].copy_from_slice(&e.tag.to_le_bytes());
    // Deterministic filler so the payload is sector sized.
    let mut x = e.index ^ e.term.rotate_left(17) ^ e.tag;
    for b in s[34..SECTOR - 4].iter_mut() {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *b = (x >> 56) as u8;
    }
    let c = crc32c(&s[..SECTOR - 4]);
    s[SECTOR - 4..].copy_from_slice(&c.to_le_bytes());
    s
}

fn u64_at(s: &[u8], o: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&s[o..o + 8]);
    u64::from_le_bytes(b)
}

fn u32_at(s: &[u8], o: usize) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&s[o..o + 4]);
    u32::from_le_bytes(b)
}

fn decode(s: &[u8], index: u64) -> Option<Entry> {
    if crc32c(&s[..SECTOR - 4]) != u32_at(s, SECTOR - 4) {
        return None;
    }
    if u64_at(s, 0) != index {
        return None;
    }
    let cmd = match s[16] {
        0 => Cmd::Noop,
        1 => Cmd::Put {
            key: s[17],
            val: u64_at(s, 18),
        },
        2 => Cmd::Get { key: s[17] },
        _ => return None,
    };
    Some(Entry {
        index,
        term: u64_at(s, 8),
        cmd,
        tag: u64_at(s, 26),
    })
}

fn encode_id(r: &IdRec) -> [u8; ID_REC] {
    let mut b = [0u8; ID_REC];
    b[0..8].copy_from_slice(&r.index.to_le_bytes());
    b[8..16].copy_from_slice(&r.term.to_le_bytes());
    b[16..20].copy_from_slice(&r.data_crc.to_le_bytes());
    let c = crc32c(&b[..ID_REC - 4]);
    b[ID_REC - 4..].copy_from_slice(&c.to_le_bytes());
    b
}

fn decode_id(b: &[u8; ID_REC]) -> Option<IdRec> {
    if crc32c(&b[..ID_REC - 4]) != u32_at(b, ID_REC - 4) {
        return None;
    }
    Some(IdRec {
        index: u64_at(b, 0),
        term: u64_at(b, 8),
        data_crc: u32_at(b, 16),
    })
}

impl Disk {
    /// Empty device. `with_ids` enables the CTRL identifier region.
    pub fn new(with_ids: bool) -> Self {
        Disk {
            data: Vec::new(),
            ids: Vec::new(),
            with_ids,
            stats: DiskStats::default(),
        }
    }

    /// Number of sectors in the log (including a torn tail, if any).
    pub fn len(&self) -> u64 {
        (self.data.len() / SECTOR) as u64
    }

    /// True if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn sector(&self, i: u64) -> &[u8] {
        let o = (i as usize - 1) * SECTOR;
        &self.data[o..o + SECTOR]
    }

    fn write_id(&mut self, e: &Entry, sector: &[u8; SECTOR]) {
        if !self.with_ids {
            return;
        }
        let rec = IdRec {
            index: e.index,
            term: e.term,
            data_crc: u32_at(sector, SECTOR - 4),
        };
        let i = e.index as usize - 1;
        if self.ids.len() <= i {
            self.ids.resize(i + 1, None);
        }
        self.ids[i] = Some(encode_id(&rec));
        self.stats.id_bytes += ID_REC as u64;
    }

    /// Append `e`, which must be at index `len() + 1`.
    pub fn append(&mut self, e: &Entry) {
        assert_eq!(e.index, self.len() + 1, "append out of order");
        let s = encode(e);
        self.data.extend_from_slice(&s);
        self.stats.data_bytes += SECTOR as u64;
        self.stats.entries_written += 1;
        self.write_id(e, &s);
    }

    /// Overwrite an existing entry in place (repair).
    pub fn overwrite(&mut self, e: &Entry) {
        assert!(e.index >= 1 && e.index <= self.len());
        let s = encode(e);
        let o = (e.index as usize - 1) * SECTOR;
        self.data[o..o + SECTOR].copy_from_slice(&s);
        self.stats.data_bytes += SECTOR as u64;
        self.stats.entries_written += 1;
        self.write_id(e, &s);
    }

    /// Drop entries `from..`.
    pub fn truncate_from(&mut self, from: u64) {
        let keep = from.saturating_sub(1) as usize;
        self.data.truncate(keep * SECTOR);
        self.ids.truncate(keep);
    }

    /// Read and verify entry `i`.
    pub fn read(&self, i: u64) -> Result<Entry, ReadErr> {
        assert!(i >= 1 && i <= self.len());
        match decode(self.sector(i), i) {
            Some(e) => Ok(e),
            None => Err(ReadErr::Corrupt { id: self.id(i) }),
        }
    }

    /// Identifier record for `i`, if present and valid.
    pub fn id(&self, i: u64) -> Option<IdRec> {
        let r = self.ids.get(i as usize - 1)?.as_ref()?;
        decode_id(r).filter(|r| r.index == i)
    }

    /// Fault injection: flip bits in the data sector of entry `i`. `bits`
    /// supplies randomness; at least one bit always flips.
    pub fn corrupt(&mut self, i: u64, bits: u64) {
        assert!(i >= 1 && i <= self.len());
        let o = (i as usize - 1) * SECTOR;
        let pos = (bits % SECTOR as u64) as usize;
        let mask = ((bits >> 9) as u8) | 1;
        self.data[o + pos] ^= mask;
        if bits & (1 << 20) != 0 {
            // Sometimes a whole-sector fault (zeroed, as after a lost write).
            self.data[o..o + SECTOR].fill(0);
        }
    }

    /// Fault injection: a crash in the middle of an append leaves a torn data
    /// sector at the tail and no identifier record.
    pub fn torn_append(&mut self, bits: u64) {
        let mut s = [0u8; SECTOR];
        let mut x = bits | 1;
        for b in s.iter_mut() {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *b = (x >> 56) as u8;
        }
        self.data.extend_from_slice(&s);
        if self.ids.len() > self.len() as usize {
            self.ids.truncate(self.len() as usize);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(i: u64, t: u64) -> Entry {
        Entry {
            index: i,
            term: t,
            cmd: Cmd::Put {
                key: 1,
                val: i * 10,
            },
            tag: i,
        }
    }

    #[test]
    fn roundtrip_and_detects_any_corruption() {
        let mut d = Disk::new(true);
        for i in 1..=50 {
            d.append(&e(i, 1 + i / 10));
        }
        for i in 1..=50 {
            assert_eq!(d.read(i), Ok(e(i, 1 + i / 10)));
            assert_eq!(d.id(i).unwrap().term, 1 + i / 10);
        }
        let rounds: u64 = if cfg!(miri) { 60 } else { 2000 };
        for k in 0..rounds {
            let mut c = d.clone();
            let i = 1 + k % 50;
            c.corrupt(i, k.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            match c.read(i) {
                Err(ReadErr::Corrupt { id: Some(r) }) => assert_eq!(r.term, 1 + i / 10),
                other => panic!("undetected corruption at {i}: {other:?}"),
            }
        }
    }

    #[test]
    fn torn_tail_has_no_id_and_vanilla_has_no_ids() {
        let mut d = Disk::new(true);
        d.append(&e(1, 1));
        d.torn_append(77);
        assert_eq!(d.len(), 2);
        assert_eq!(d.read(2), Err(ReadErr::Corrupt { id: None }));
        d.truncate_from(2);
        d.append(&e(2, 1));
        assert!(d.read(2).is_ok());
        let mut v = Disk::new(false);
        v.append(&e(1, 1));
        assert!(v.id(1).is_none());
        assert_eq!(v.stats.id_bytes, 0);
    }
}
