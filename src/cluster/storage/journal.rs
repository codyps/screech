//! Accepted entries and the restart promise share one synchronized file. SQLite
//! between signed FULL checkpoint roots is a replayable query projection.
use super::*;
use std::io::{Read, SeekFrom};

const MAGIC: &[u8; 8] = b"SCSEAL04";
const ROOT_KEY: &str = "journal_root";
const MAX_RECORD: u64 = 34 * 1024 * 1024;
const ALLOCATION_BYTES: u64 = 1024 * 1024;

fn reserve(file: &mut File, end: u64) -> io::Result<u64> {
    let capacity = end
        .checked_add(ALLOCATION_BYTES - 1)
        .ok_or_else(|| error("journal allocation overflow"))?
        / ALLOCATION_BYTES
        * ALLOCATION_BYTES;
    let previous = file.metadata()?.len();
    if previous >= capacity {
        return Ok(previous);
    }
    #[cfg(target_os = "linux")]
    match rustix::fs::fallocate(&*file, rustix::fs::FallocateFlags::empty(), 0, capacity) {
        Ok(()) => (),
        Err(rustix::io::Errno::OPNOTSUPP | rustix::io::Errno::NOSYS) => (),
        Err(failure) => return Err(failure.into()),
    }
    // Unsupported allocation (including some FUSE filesystems) can still grow
    // safely. No I/O or out-of-space error is ignored; acceptance always syncs.
    file.set_len(capacity)?;
    // Materialize newly reserved extents together. Otherwise every small append
    // can make fdatasync wait for an unwritten-extent conversion. Never touch
    // existing bytes, including unpublished tails. These zeros confer no
    // authority: only the signed seal and its normal sync publish acceptance.
    if previous < capacity {
        let cursor = file.stream_position()?;
        file.seek(SeekFrom::Start(previous))?;
        let zeros = [0; 64 * 1024];
        let mut remaining = capacity - previous;
        while remaining > 0 {
            let count = remaining.min(zeros.len() as u64) as usize;
            file.write_all(&zeros[..count])?;
            remaining -= count as u64;
        }
        file.seek(SeekFrom::Start(cursor))?;
    }
    Ok(capacity)
}

#[cfg(test)]
mod allocation_tests {
    use super::*;

    #[test]
    fn reservation_initializes_only_new_extents_and_preserves_the_cursor() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"existing-prefix").unwrap();
        let cursor = file.stream_position().unwrap();
        let capacity = reserve(&mut file, ALLOCATION_BYTES + 1).unwrap();
        assert_eq!(capacity, ALLOCATION_BYTES * 2);
        assert_eq!(file.stream_position().unwrap(), cursor);
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(&bytes[..cursor as usize], b"existing-prefix");
        assert!(bytes[cursor as usize..].iter().all(|byte| *byte == 0));
        // Preserve every old byte on growth, including data past a logical seal.
        file.seek(SeekFrom::End(-4)).unwrap();
        file.write_all(b"tail").unwrap();
        let grown = reserve(&mut file, capacity + 1).unwrap();
        assert_eq!(grown, ALLOCATION_BYTES * 3);
        assert_eq!(file.stream_position().unwrap(), capacity);
        file.seek(SeekFrom::Start(capacity - 4)).unwrap();
        let mut tail = Vec::new();
        file.read_to_end(&mut tail).unwrap();
        assert_eq!(&tail[..4], b"tail");
        assert!(tail[4..].iter().all(|byte| *byte == 0));
        assert_eq!(reserve(&mut file, 1).unwrap(), grown);
        assert_eq!(file.metadata().unwrap().len(), grown);
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Tip {
    root: [u8; 16],
    end: u64,
    hash: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Root {
    id: [u8; 16],
    parent: Option<Tip>,
    position: Position,
    snapshot: (u64, u64, u64),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Seal {
    root: Root,
    position: Position,
    end: u64,
    hash: [u8; 32],
}
#[derive(Serialize, Deserialize)]
struct Accepted {
    position: Position,
    entries: Vec<Vec<u8>>,
}
pub(super) struct Journal {
    file: File,
    seal: Seal,
    generation: u64,
    capacity: u64,
}

impl Watermark {
    pub(crate) fn next_root_bytes(
        &self,
        position: Position,
        snapshot: (u64, u64, u64),
    ) -> io::Result<Vec<u8>> {
        let mut id = [0; 16];
        getrandom::fill(&mut id).map_err(error)?;
        self.root_bytes(&Root {
            id,
            parent: self.tip(),
            position,
            snapshot,
        })
    }
    fn root_bytes(&self, root: &Root) -> io::Result<Vec<u8>> {
        let body = postcard::to_allocvec(root).map_err(error)?;
        let mut signed = body.clone();
        signed.extend_from_slice(&self.journal_mac(b"root", &body));
        Ok(signed)
    }
    fn read_root(&self, bytes: &[u8]) -> io::Result<Root> {
        if bytes.len() > SLOT_BYTES {
            return Err(error("journal root exceeds metadata budget"));
        }
        let at = bytes
            .len()
            .checked_sub(32)
            .ok_or_else(|| error("short journal root"))?;
        self.journal_auth(b"root", &bytes[..at])
            .verify_slice(&bytes[at..])
            .map_err(error)?;
        postcard::from_bytes(&bytes[..at]).map_err(error)
    }
    fn journal_mac(&self, kind: &[u8], bytes: &[u8]) -> [u8; 32] {
        self.journal_auth(kind, bytes)
            .finalize()
            .into_bytes()
            .into()
    }
    fn journal_auth(&self, kind: &[u8], bytes: &[u8]) -> hmac::Hmac<sha2::Sha256> {
        let mut mac = auth(
            &self.secret,
            &self.cluster,
            self.member,
            b"screech-journal-v4",
        );
        mac.update(kind);
        mac.update(bytes);
        mac
    }
    fn encode_seal(&self, seal: &Seal, generation: u64, slot: u64) -> io::Result<[u8; SLOT_BYTES]> {
        let mut frame = [0; SLOT_BYTES];
        frame[..8].copy_from_slice(MAGIC);
        frame[8..16].copy_from_slice(&generation.to_le_bytes());
        frame[16..24].copy_from_slice(&slot.to_le_bytes());
        let body = postcard::to_allocvec(seal).map_err(error)?;
        if body.len() > MAC_AT - 28 {
            return Err(error("journal seal too large"));
        }
        frame[24..28].copy_from_slice(&(body.len() as u32).to_le_bytes());
        frame[28..28 + body.len()].copy_from_slice(&body);
        let mac = self.journal_mac(b"seal", &frame[..MAC_AT]);
        frame[MAC_AT..].copy_from_slice(&mac);
        Ok(frame)
    }
    fn decode_seal(&self, frame: &[u8], slot: u64) -> io::Result<(u64, Seal)> {
        self.journal_auth(b"seal", &frame[..MAC_AT])
            .verify_slice(&frame[MAC_AT..])
            .map_err(error)?;
        if &frame[..8] != MAGIC || u64::from_le_bytes(frame[16..24].try_into().unwrap()) != slot {
            return Err(error("journal seal authentication failed"));
        }
        let generation = u64::from_le_bytes(frame[8..16].try_into().unwrap());
        let len = u32::from_le_bytes(frame[24..28].try_into().unwrap()) as usize;
        if len > MAC_AT - 28 || (generation != 0 && generation % 2 != slot) {
            return Err(error("invalid journal seal bounds"));
        }
        let seal: Seal = postcard::from_bytes(&frame[28..28 + len]).map_err(error)?;
        if seal.end < SEAL_BYTES as u64
            || seal.position.incarnation != seal.root.position.incarnation
        {
            return Err(error("invalid journal seal position"));
        }
        Ok((generation, seal))
    }
    pub(super) fn open_journal(&mut self) -> io::Result<()> {
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let mut frames = [0; SEAL_BYTES];
        file.read_exact(&mut frames)?;
        let (a, left) = self.decode_seal(&frames[..SLOT_BYTES], 0)?;
        let (b, right) = self.decode_seal(&frames[SLOT_BYTES..], 1)?;
        if left.root != right.root || (a == b && (a != 0 || left != right)) || a.abs_diff(b) > 1 {
            return Err(error("invalid journal seal generations"));
        }
        let (generation, seal, previous) = if a >= b {
            (a, left, right)
        } else {
            (b, right, left)
        };
        if previous.end > seal.end {
            return Err(error("journal seal moved backward"));
        }
        let mut journal = Journal {
            capacity: file.metadata()?.len(),
            file,
            seal,
            generation,
        };
        self.walk_journal(&mut journal, |_, _| Ok(()), Some(&previous))?;
        self.sealed = Some(journal.seal.position.clone());
        self.journal = Some(journal);
        Ok(())
    }
    fn tip(&self) -> Option<Tip> {
        self.journal.as_ref().map(|j| Tip {
            root: j.seal.root.id,
            end: j.seal.end,
            hash: j.seal.hash,
        })
    }
    fn walk_journal(
        &self,
        journal: &mut Journal,
        mut visit: impl FnMut(Accepted, u64) -> io::Result<()>,
        previous: Option<&Seal>,
    ) -> io::Result<()> {
        journal.file.seek(SeekFrom::Start(SEAL_BYTES as u64))?;
        let mut end = SEAL_BYTES as u64;
        let mut hash = [0; 32];
        let mut position = journal.seal.root.position.clone();
        let mut saw_previous =
            previous.is_none_or(|p| p.end == end && p.hash == hash && p.position == position);
        while end < journal.seal.end {
            let mut size = [0; 8];
            journal.file.read_exact(&mut size)?;
            let size = u64::from_le_bytes(size);
            if size == 0
                || size > MAX_RECORD
                || end
                    .checked_add(8 + size + 32)
                    .is_none_or(|n| n > journal.seal.end)
            {
                return Err(error("invalid journal record bounds"));
            }
            let mut body = vec![0; size as usize];
            journal.file.read_exact(&mut body)?;
            let mut signed = Vec::with_capacity(56 + body.len());
            signed.extend_from_slice(&journal.seal.root.id);
            signed.extend_from_slice(&end.to_le_bytes());
            signed.extend_from_slice(&hash);
            signed.extend_from_slice(&size.to_le_bytes());
            signed.extend_from_slice(&body);
            journal.file.read_exact(&mut hash)?;
            self.journal_auth(b"accepted", &signed)
                .verify_slice(&hash)
                .map_err(error)?;
            let record: Accepted = postcard::from_bytes(&body).map_err(error)?;
            if record.position.incarnation != position.incarnation
                || record.position.term < position.term
                || record.position.commit < position.commit
                || record.position.commit > record.position.last
                || (record.position.term == position.term
                    && position.vote != 0
                    && record.position.vote != position.vote)
            {
                return Err(error("invalid journal promise transition"));
            }
            position = record.position.clone();
            end += 8 + size + 32;
            if previous.is_some_and(|p| p.end == end && p.hash == hash && p.position == position) {
                saw_previous = true;
            }
            visit(record, end)?;
        }
        if !saw_previous || hash != journal.seal.hash || position != journal.seal.position {
            return Err(error("journal published prefix is missing"));
        }
        Ok(())
    }
    fn create_journal(&mut self, root: Root) -> io::Result<()> {
        let seal = Seal {
            position: root.position.clone(),
            root,
            end: SEAL_BYTES as u64,
            hash: [0; 32],
        };
        let temporary = self.path.with_extension("cluster-watermark.tmp");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        let capacity = reserve(&mut file, SEAL_BYTES as u64)?;
        file.write_all(&self.encode_seal(&seal, 0, 0)?)?;
        file.write_all(&self.encode_seal(&seal, 0, 1)?)?;
        file.sync_all()?;
        crate::fault::point("before_cluster_watermark");
        std::fs::rename(&temporary, &self.path)?;
        File::open(
            self.path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?
        .sync_all()?;
        self.sealed = Some(seal.position.clone());
        self.journal = Some(Journal {
            file,
            seal,
            generation: 0,
            capacity,
        });
        self.file = None;
        crate::fault::point("after_cluster_watermark");
        Ok(())
    }
    fn append_accepted(&mut self, position: Position, entries: &[Entry]) -> io::Result<()> {
        let j = self
            .journal
            .as_ref()
            .ok_or_else(|| error("missing accepted journal"))?;
        if position.incarnation != j.seal.position.incarnation {
            return Err(error("accepted journal identity cannot change"));
        }
        let body = postcard::to_allocvec(&Accepted {
            position: position.clone(),
            entries: entries
                .iter()
                .map(|e| e.write_to_bytes().map_err(error))
                .collect::<io::Result<_>>()?,
        })
        .map_err(error)?;
        if body.len() as u64 > MAX_RECORD {
            return Err(error("accepted record exceeds journal budget"));
        }
        let mut signed = Vec::with_capacity(64 + body.len());
        signed.extend_from_slice(&j.seal.root.id);
        signed.extend_from_slice(&j.seal.end.to_le_bytes());
        signed.extend_from_slice(&j.seal.hash);
        signed.extend_from_slice(&(body.len() as u64).to_le_bytes());
        signed.extend_from_slice(&body);
        let hash = self.journal_mac(b"accepted", &signed);
        // The signed suffix is also the complete on-disk record. Send its
        // length, body and digest together, retaining the same sync barrier.
        signed.extend_from_slice(&hash);
        let generation = j
            .generation
            .checked_add(1)
            .ok_or_else(|| error("journal generations exhausted"))?;
        let seal = Seal {
            root: j.seal.root.clone(),
            position,
            end: j.seal.end + 8 + body.len() as u64 + 32,
            hash,
        };
        let frame = self.encode_seal(&seal, generation, generation % 2)?;
        let j = self.journal.as_mut().unwrap();
        if seal.end > j.capacity {
            j.capacity = reserve(&mut j.file, seal.end)?;
        }
        crate::fault::point("before_cluster_watermark");
        j.file.seek(SeekFrom::Start(j.seal.end))?;
        j.file.write_all(&signed[16 + 8 + 32..])?;
        j.file
            .seek(SeekFrom::Start(generation % 2 * SLOT_BYTES as u64))?;
        j.file.write_all(&frame)?;
        crate::fault::point("after_cluster_watermark_write");
        j.file.sync_data()?;
        j.seal = seal;
        j.generation = generation;
        self.sealed = Some(j.seal.position.clone());
        crate::fault::point("after_cluster_watermark");
        Ok(())
    }
}

impl Database {
    pub(crate) fn journal_bytes(&self) -> u64 {
        self.watermark
            .journal
            .as_ref()
            .map_or(0, |j| j.seal.end - SEAL_BYTES as u64)
    }
    pub(crate) fn recover_journal(&mut self) -> io::Result<()> {
        let Some(mut journal) = self.watermark.journal.take() else {
            return Ok(());
        };
        let root = self
            .meta(ROOT_KEY)?
            .ok_or_else(|| error("missing signed journal root"))?;
        let root = self.watermark.read_root(&root)?;
        if root != journal.seal.root {
            let tip = Tip {
                root: journal.seal.root.id,
                end: journal.seal.end,
                hash: journal.seal.hash,
            };
            if root.parent.as_ref() != Some(&tip)
                || root.snapshot != self.snapshot_position
                || self.position()? != root.position
            {
                return Err(error("journal/checkpoint root mismatch"));
            }
            self.verify_watermark()?;
            return self.watermark.create_journal(root);
        }
        if root.snapshot != self.snapshot_position {
            return Err(error("journal snapshot root changed"));
        }
        let old = self.hard()?;
        let latest = &journal.seal.position;
        if old.term > latest.term
            || old.term < root.position.term
            || (old.term == latest.term && old.vote != 0 && old.vote != latest.vote)
        {
            return Err(error("SQLite voting promise is outside its journal"));
        }
        let mut maximum = root.position.last;
        self.watermark.walk_journal(
            &mut journal,
            |record, _| {
                maximum = maximum.max(record.position.last);
                Ok(())
            },
            None,
        )?;
        if self.last()? > maximum {
            return Err(error("SQLite accepted suffix is ahead of its journal"));
        }
        let base = root.snapshot.0;
        let hard = HardState {
            term: journal.seal.position.term,
            vote: journal.seal.position.vote,
            commit: old.commit.max(journal.seal.position.commit),
            ..Default::default()
        };
        if hard.commit > journal.seal.position.last {
            return Err(error("SQLite committed prefix is outside its journal"));
        }
        self.conn
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(error)?;
        let tx = self.conn.transaction().map_err(error)?;
        self.watermark.walk_journal(
            &mut journal,
            |record, _| {
                let entries = record
                    .entries
                    .iter()
                    .map(|b| Entry::parse_from_bytes(b).map_err(error))
                    .collect::<io::Result<Vec<_>>>()?;
                if let Some(first) = entries.first() {
                    if first.index <= base
                        || entries.windows(2).any(|p| p[1].index != p[0].index + 1)
                        || entries.last().unwrap().index != record.position.last
                        || entries.last().unwrap().term != record.position.last_term
                    {
                        return Err(error("invalid journaled suffix"));
                    }
                    tx.execute("DELETE FROM entries WHERE idx>=?1", [sql(first.index)?])
                        .map_err(error)?;
                    for (entry, bytes) in entries.iter().zip(&record.entries) {
                        tx.execute(
                            "INSERT INTO entries VALUES(?1,?2,?3,?4)",
                            params![
                                sql(entry.index)?,
                                sql(entry.term)?,
                                bytes,
                                crc32fast::hash(bytes)
                            ],
                        )
                        .map_err(error)?;
                    }
                }
                Ok(())
            },
            None,
        )?;
        tx.execute(
            "INSERT INTO cluster_meta VALUES('hard',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [hard.write_to_bytes().map_err(error)?],
        )
        .map_err(error)?;
        tx.commit().map_err(error)?;
        self.conn
            .pragma_update(None, "synchronous", "FULL")
            .map_err(error)?;
        self.watermark.journal = Some(journal);
        self.refresh_log_bytes()?;
        Ok(())
    }
    pub(crate) fn seal_journal(&mut self) -> io::Result<()> {
        let position = self.position()?;
        if self.watermark.journal.is_none() {
            let root = self
                .watermark
                .next_root_bytes(position.clone(), self.snapshot_position)?;
            // A legacy FULL store is the first root; no NORMAL accepted writes
            // are allowed until this root and its journal are both published.
            self.commit_transaction(true, |tx| {
                tx.execute(
                    "INSERT INTO cluster_meta VALUES('journal_root',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    [&root],
                )
                .map_err(error)?;
                Ok(())
            })?;
            return self
                .watermark
                .create_journal(self.watermark.read_root(&root)?);
        }
        let bytes = self
            .meta(ROOT_KEY)?
            .ok_or_else(|| error("missing checkpoint journal root"))?;
        let root = self.watermark.read_root(&bytes)?;
        if root != self.watermark.journal.as_ref().unwrap().seal.root {
            if root.parent != self.watermark.tip()
                || root.position != position
                || root.snapshot != self.snapshot_position
            {
                return Err(error("invalid journal root rotation"));
            }
            self.watermark.create_journal(root)
        } else if self.watermark.sealed.as_ref() != Some(&position) {
            self.watermark.append_accepted(position, &[])
        } else {
            Ok(())
        }
    }
    pub(crate) fn accept_journal(&mut self, entries: &[Entry], hard: &HardState) -> io::Result<()> {
        // persist() already validated the accepted suffix and HardState. A new
        // suffix supplies the complete resulting frontier without rereading
        // the preceding SQL entry and metadata that it replaces.
        let (last, last_term) = if let Some(last) = entries.last() {
            (last.index, last.term)
        } else {
            let previous = self.position()?;
            (previous.last, previous.last_term)
        };
        let position = Position {
            incarnation: self.incarnation,
            term: hard.term,
            vote: hard.vote,
            commit: hard.commit,
            last,
            last_term,
        };
        if let Err(failure) = self.watermark.append_accepted(position, entries) {
            self.failed = true;
            return Err(failure);
        }
        Ok(())
    }
}
