use std::fmt;
use std::str::FromStr;

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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0:?} is not an event id")]
pub struct InvalidEventId(pub String);

impl FromStr for EventId {
    type Err = InvalidEventId;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidEventId(text.to_owned());
        let (lsn, ordinal) = text.split_once('-').ok_or_else(invalid)?;
        if !lower_hex(lsn, 16) || !lower_hex(ordinal, 8) {
            return Err(invalid());
        }
        let lsn = u64::from_str_radix(lsn, 16).map_err(|_| invalid())?;
        let ordinal = u32::from_str_radix(ordinal, 16).map_err(|_| invalid())?;
        Ok(Self::new(Lsn(lsn), ordinal))
    }
}

pub(crate) fn lower_hex(text: &str, width: usize) -> bool {
    text.len() == width
        && text
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
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

    #[test]
    fn parses_its_own_text_and_nothing_else() {
        let id = EventId::new(Lsn(0x1A2B_3C4D), 3);
        assert_eq!("000000001a2b3c4d-00000003".parse(), Ok(id));
        for bad in [
            "",
            "000000001a2b3c4d",
            "000000001A2B3C4D-00000003",
            "000000001a2b3c4d-0000003",
            "00000001a2b3c4d-00000003",
            "000000001a2b3c4d_00000003",
            "000000001a2b3c4d-00000003.json",
            "+00000001a2b3c4d-00000003",
        ] {
            assert_eq!(
                bad.parse::<EventId>(),
                Err(InvalidEventId(bad.to_owned())),
                "{bad:?}"
            );
        }
    }

    proptest! {
        #[test]
        fn text_round_trips(lsn in any::<u64>(), ordinal in any::<u32>()) {
            let id = EventId::new(Lsn(lsn), ordinal);
            prop_assert_eq!(id.to_string().parse::<EventId>(), Ok(id));
        }

        #[test]
        fn string_order_matches_stream_order(a in any::<(u64, u32)>(), b in any::<(u64, u32)>()) {
            let left = EventId::new(Lsn(a.0), a.1);
            let right = EventId::new(Lsn(b.0), b.1);
            prop_assert_eq!(left.cmp(&right), left.to_string().cmp(&right.to_string()));
        }
    }
}
