mod builder;
mod envelope;
mod id;

pub use builder::{BuildError, Builder, Built, ColumnInfo, Fallback, RelationInfo};
pub use envelope::{ENVELOPE_VERSION, Envelope, Op, Row, Source};
pub(crate) use id::lower_hex;
pub use id::{EventId, InvalidEventId};

#[cfg(test)]
pub(crate) use envelope::sample_envelope;
