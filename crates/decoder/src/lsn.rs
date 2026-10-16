use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid LSN {0:?}")]
pub struct ParseLsnError(String);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl FromStr for Lsn {
    type Err = ParseLsnError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || ParseLsnError(text.to_owned());
        let (high, low) = text.split_once('/').ok_or_else(invalid)?;
        let half = |part: &str| {
            if part.is_empty()
                || part.len() > 8
                || !part.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(invalid());
            }
            u32::from_str_radix(part, 16).map_err(|_| invalid())
        };
        Ok(Self((u64::from(half(high)?) << 32) | u64::from(half(low)?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_in_postgres_notation() {
        assert_eq!(Lsn(0x1A2B_3C4D).to_string(), "0/1A2B3C4D");
        assert_eq!(Lsn(0x1_0000_00FF).to_string(), "1/FF");
        assert_eq!(Lsn(0).to_string(), "0/0");
    }

    #[test]
    fn parses_padded_unpadded_and_lowercase() {
        assert_eq!("0/1A2B3C4D".parse::<Lsn>(), Ok(Lsn(0x1A2B_3C4D)));
        assert_eq!("0/016B3748".parse::<Lsn>(), Ok(Lsn(0x016B_3748)));
        assert_eq!("1/ff".parse::<Lsn>(), Ok(Lsn(0x1_0000_00FF)));
    }

    #[test]
    fn rejects_malformed_text() {
        for bad in [
            "",
            "1",
            "/1",
            "1/",
            "g/1",
            "+1/1",
            "123456789/1",
            "1/2/3",
            " 0/1",
        ] {
            assert!(bad.parse::<Lsn>().is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn orders_by_position() {
        assert!(Lsn(1) < Lsn(0x1_0000_0000));
    }
}
