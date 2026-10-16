use std::path::Path;

use crate::error::TestkitError;

pub fn write_fixture(path: &Path, messages: &[Vec<u8>]) -> Result<(), TestkitError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut bytes = Vec::new();
    for message in messages {
        let len = u32::try_from(message.len())
            .map_err(|_| TestkitError::MalformedFixture(path.display().to_string()))?;
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(message);
    }
    std::fs::write(path, bytes)?;
    Ok(())
}

pub fn read_fixture(path: &Path) -> Result<Vec<Vec<u8>>, TestkitError> {
    let bytes = std::fs::read(path)?;
    let malformed = || TestkitError::MalformedFixture(path.display().to_string());
    let mut rest = bytes.as_slice();
    let mut messages = Vec::new();
    while !rest.is_empty() {
        let (len, tail) = rest.split_first_chunk::<4>().ok_or_else(malformed)?;
        let len = usize::try_from(u32::from_be_bytes(*len)).map_err(|_| malformed())?;
        let (message, tail) = tail.split_at_checked(len).ok_or_else(malformed)?;
        messages.push(message.to_vec());
        rest = tail;
    }
    Ok(messages)
}
