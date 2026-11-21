use std::cmp::Ordering;
use std::fmt;
use std::str::Utf8Error;

use serde::{Deserialize, Serialize};

/// Compares identifiers using unsigned bytes with no Unicode normalization.
#[must_use]
pub fn compare_bytes(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
}

/// A range in a phase-owned immutable byte arena.
///
/// The arena owner must keep the referenced bytes alive and unchanged while this
/// span crosses a crate boundary. Offsets are deliberately 32-bit to keep parsed
/// rows compact; an arena larger than 4 GiB must be split before spans are made.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ByteSpan {
    pub offset: u32,
    pub length: u32,
}

impl ByteSpan {
    /// Creates a span when its exclusive end fits in the 32-bit arena address space.
    #[must_use]
    pub fn new(offset: u32, length: u32) -> Option<Self> {
        offset.checked_add(length)?;
        Some(Self { offset, length })
    }

    #[must_use]
    pub const fn end(self) -> u64 {
        self.offset as u64 + self.length as u64
    }

    /// Resolves this span without copying.
    #[must_use]
    pub fn resolve(self, arena: &[u8]) -> Option<&[u8]> {
        let start = usize::try_from(self.offset).ok()?;
        let end = usize::try_from(self.end()).ok()?;
        arena.get(start..end)
    }
}

/// Stable index into an immutable byte arena owned by the producing slice.
///
/// Handles remain valid for the lifetime of one pipeline run. Consumers never
/// compare numeric handle values; they resolve bytes through the arena owner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ByteHandle(pub u32);

/// Owned, validated UTF-8 for output rows and other ownership boundaries.
///
/// Ordering is exact unsigned-byte ordering, which is also the required ID and
/// category tie-break ordering. No normalization, trimming, or case folding occurs.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OwnedUtf8(Box<str>);

impl OwnedUtf8 {
    /// Copies byte-exact UTF-8 into an owned output value.
    ///
    /// # Errors
    ///
    /// Returns the UTF-8 validation error without normalizing the input.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Utf8Error> {
        let text = std::str::from_utf8(bytes)?;
        Ok(Self(text.into()))
    }

    #[must_use]
    pub fn from_static(text: &'static str) -> Self {
        Self(text.into())
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OwnedUtf8 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("OwnedUtf8").field(&self.0).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparison_is_unsigned_bytewise() {
        assert_eq!(compare_bytes(b"U1", b"u1"), Ordering::Less);
        assert_eq!(compare_bytes("é".as_bytes(), b"z"), Ordering::Greater);
        assert_eq!(compare_bytes(b"u1", b"u1 "), Ordering::Less);
    }

    #[test]
    fn spans_are_checked_and_resolved() {
        let span = ByteSpan::new(2, 3).unwrap();
        assert_eq!(span.resolve(b"abcdef"), Some(&b"cde"[..]));
        assert_eq!(span.resolve(b"abcd"), None);
        assert!(ByteSpan::new(u32::MAX, 1).is_none());
    }

    #[test]
    fn owned_utf8_preserves_exact_bytes() {
        let value = OwnedUtf8::from_bytes(b" u1 ").unwrap();
        assert_eq!(value.as_bytes(), b" u1 ");
        assert!(OwnedUtf8::from_bytes(&[0xff]).is_err());
    }
}
