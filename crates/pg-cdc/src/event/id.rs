use std::fmt;

use pg_cdc_decoder::Lsn;
use serde::{Serialize, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId {
    commit_lsn: Lsn,
    ordinal: u32,
}

impl EventId {
    pub fn new(commit_lsn: Lsn, ordinal: u32) -> Self {
        Self {
            commit_lsn,
            ordinal,
        }
    }

    pub fn commit_lsn(self) -> Lsn {
        self.commit_lsn
    }

    pub fn ordinal(self) -> u32 {
        self.ordinal
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}-{:08x}", self.commit_lsn.0, self.ordinal)
    }
}

impl Serialize for EventId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn formats_as_padded_lowercase_hex() {
        assert_eq!(
            EventId::new(Lsn(0x1A2B_3C4D), 3).to_string(),
            "000000001a2b3c4d-00000003"
        );
        assert_eq!(
            EventId::new(Lsn(u64::MAX), u32::MAX).to_string(),
            "ffffffffffffffff-ffffffff"
        );
        assert_eq!(
            serde_json::to_string(&EventId::new(Lsn(1), 0)).unwrap(),
            "\"0000000000000001-00000000\""
        );
    }

    proptest! {
        #[test]
        fn string_order_matches_stream_order(a in any::<(u64, u32)>(), b in any::<(u64, u32)>()) {
            let left = EventId::new(Lsn(a.0), a.1);
            let right = EventId::new(Lsn(b.0), b.1);
            prop_assert_eq!(left.cmp(&right), left.to_string().cmp(&right.to_string()));
        }
    }
}
