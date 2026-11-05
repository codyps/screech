//! Volatile committed history avoids full snapshots for briefly lagging voters.
//! The FULL root still reclaims its covered SQL prefix. Restart discards this
//! cache and advertises the durable snapshot boundary instead.
use super::*;
use std::collections::VecDeque;

const MAX_ENTRIES: u64 = 256;
const MAX_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub(super) struct Recent {
    entries: VecDeque<Entry>,
    bytes: usize,
}

impl Recent {
    pub(super) fn get(&self, index: u64) -> Option<&Entry> {
        let offset = index.checked_sub(self.entries.front()?.index)?;
        self.entries
            .get(usize::try_from(offset).ok()?)
            .filter(|entry| entry.index == index)
    }

    fn push(&mut self, entry: Entry) {
        let bytes = entry.compute_size() as usize;
        if bytes > MAX_BYTES {
            self.entries.clear();
            self.bytes = 0;
            return;
        }
        self.bytes += bytes;
        self.entries.push_back(entry);
        while self.entries.len() > MAX_ENTRIES as usize || self.bytes > MAX_BYTES {
            self.bytes -= self.entries.pop_front().unwrap().compute_size() as usize;
        }
    }

    pub(super) fn capture(db: &Database, index: u64) -> io::Result<Self> {
        let mut recent = Self::default();
        for at in index.saturating_sub(MAX_ENTRIES - 1).max(1)..=index {
            if let Some(entry) = db.recent.get(at) {
                recent.push(entry.clone());
            } else if at > db.snapshot_position.0 {
                // Decode and verify the real committed SQL entry before the
                // root reclaims it. No uncommitted suffix enters this cache.
                recent.push(db.entry(at)?);
            }
        }
        Ok(recent)
    }
}

impl Database {
    pub(super) fn first_log_index(&self) -> io::Result<u64> {
        if self.failed {
            return Err(error("replica quarantined"));
        }
        Ok(self
            .recent
            .entries
            .front()
            // Raft also needs the term immediately before first_index. The
            // oldest cached entry supplies that boundary, as a snapshot does
            // when the cache is empty.
            .map_or(self.snapshot_position.0 + 1, |entry| entry.index + 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_history_is_bounded_by_both_entries_and_bytes() {
        let mut recent = Recent::default();
        for index in 1..=MAX_ENTRIES * 2 {
            recent.push(Entry {
                index,
                term: 1,
                ..Default::default()
            });
        }
        assert_eq!(recent.entries.len(), MAX_ENTRIES as usize);
        assert!(recent.get(MAX_ENTRIES).is_none());
        assert_eq!(recent.get(MAX_ENTRIES + 1).unwrap().index, MAX_ENTRIES + 1);
        for index in MAX_ENTRIES * 2 + 1..=MAX_ENTRIES * 2 + 10 {
            recent.push(Entry {
                index,
                term: 1,
                data: vec![7; 1024 * 1024].into(),
                ..Default::default()
            });
        }
        assert!(recent.bytes <= MAX_BYTES);
        assert!(recent.entries.len() < 8);
        let next = MAX_ENTRIES * 2 + 11;
        recent.push(Entry {
            index: next,
            data: vec![8; MAX_BYTES + 1].into(),
            ..Default::default()
        });
        assert!(recent.entries.is_empty());
        assert_eq!(recent.bytes, 0);
        recent.push(Entry {
            index: next + 1,
            term: 2,
            ..Default::default()
        });
        assert!(recent.get(next).is_none());
        assert_eq!(recent.get(next + 1).unwrap().term, 2);
    }
}
