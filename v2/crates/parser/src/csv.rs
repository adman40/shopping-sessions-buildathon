use shopping_sessions_domain::{ByteSpan, ParseOutcome, ParseStatus};

/// Parsed records and the immutable bytes referenced by their spans.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedBatch<T> {
    arena: Box<[u8]>,
    pub rows: Vec<ParseOutcome<T>>,
}

impl<T> ParsedBatch<T> {
    pub(crate) fn new(arena: Vec<u8>, rows: Vec<ParseOutcome<T>>) -> Self {
        Self {
            arena: arena.into_boxed_slice(),
            rows,
        }
    }

    #[must_use]
    pub fn arena(&self) -> &[u8] {
        &self.arena
    }

    #[must_use]
    pub fn resolve(&self, span: ByteSpan) -> Option<&[u8]> {
        span.resolve(&self.arena)
    }
}

/// Parses one physical line of the restricted CSV grammar without copying.
///
/// Only whole-field quotes are removed. Commas always delimit fields, including
/// inside quotes, because the input contract forbids commas in values.
#[must_use]
pub fn parse_fields(line: &[u8], width: usize) -> ParseOutcome<Vec<&[u8]>> {
    if line.is_empty() {
        return ParseOutcome::Rejected(ParseStatus::EmptyLine);
    }

    if field_count(line) != width {
        return ParseOutcome::Rejected(ParseStatus::WrongFieldCount);
    }

    let mut fields = Vec::with_capacity(width);
    for piece in line.split(|byte| *byte == b',') {
        match parse_field(piece) {
            Ok(value) => fields.push(value),
            Err(status) => return ParseOutcome::Rejected(status),
        }
    }
    ParseOutcome::Accepted(fields)
}

pub(crate) fn parse_fixed_fields<const N: usize>(line: &[u8]) -> ParseOutcome<[&[u8]; N]> {
    if line.is_empty() {
        return ParseOutcome::Rejected(ParseStatus::EmptyLine);
    }
    if field_count(line) != N {
        return ParseOutcome::Rejected(ParseStatus::WrongFieldCount);
    }

    let mut fields = [&[][..]; N];
    for (index, piece) in line.split(|byte| *byte == b',').enumerate() {
        match parse_field(piece) {
            Ok(value) => fields[index] = value,
            Err(status) => return ParseOutcome::Rejected(status),
        }
    }
    ParseOutcome::Accepted(fields)
}

pub(crate) fn append_fixed_fields<const N: usize>(
    arena: &mut Vec<u8>,
    fields: &[&[u8]; N],
    arena_limit: usize,
) -> Option<[ByteSpan; N]> {
    let additional = fields
        .iter()
        .try_fold(0_usize, |total, field| total.checked_add(field.len()))?;
    let final_length = arena.len().checked_add(additional)?;
    if final_length > arena_limit || final_length > u32::MAX as usize {
        return None;
    }

    let empty = ByteSpan::new(0, 0)?;
    let mut spans = [empty; N];
    for (index, field) in fields.iter().enumerate() {
        let offset = u32::try_from(arena.len()).ok()?;
        let length = u32::try_from(field.len()).ok()?;
        spans[index] = ByteSpan::new(offset, length)?;
        arena.extend_from_slice(field);
    }
    Some(spans)
}

fn field_count(line: &[u8]) -> usize {
    let mut count = 1_usize;
    for byte in line {
        if *byte == b',' {
            count = count.saturating_add(1);
        }
    }
    count
}

fn parse_field(piece: &[u8]) -> Result<&[u8], ParseStatus> {
    let value = if piece.first() == Some(&b'"') {
        if piece.len() < 2
            || piece.last() != Some(&b'"')
            || piece[1..piece.len() - 1].contains(&b'"')
        {
            return Err(ParseStatus::MisplacedQuote);
        }
        &piece[1..piece.len() - 1]
    } else {
        if piece.contains(&b'"') {
            return Err(ParseStatus::MisplacedQuote);
        }
        piece
    };
    if std::str::from_utf8(value).is_err() {
        return Err(ParseStatus::InvalidUtf8);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grammar_status_precedence_matches_the_contract() {
        assert_eq!(
            parse_fields(b"", 2),
            ParseOutcome::Rejected(ParseStatus::EmptyLine)
        );
        assert_eq!(
            parse_fields(b"\"bad,x", 2),
            ParseOutcome::Rejected(ParseStatus::MisplacedQuote)
        );
        assert_eq!(
            parse_fields(b"\"bad,x,y", 2),
            ParseOutcome::Rejected(ParseStatus::WrongFieldCount)
        );
        assert_eq!(
            parse_fields(b"ok,\xff", 2),
            ParseOutcome::Rejected(ParseStatus::InvalidUtf8)
        );
    }

    #[test]
    fn quotes_strip_only_the_outer_bytes() {
        assert_eq!(
            parse_fields(b"\"\",\" u1\",u1 ", 3),
            ParseOutcome::Accepted(vec![&b""[..], &b" u1"[..], &b"u1 "[..]])
        );
        assert_eq!(
            parse_fields(b"\"a\"x,b", 2),
            ParseOutcome::Rejected(ParseStatus::MisplacedQuote)
        );
        assert_eq!(
            parse_fields(b"\"a\"\",b", 2),
            ParseOutcome::Rejected(ParseStatus::MisplacedQuote)
        );
    }
}
