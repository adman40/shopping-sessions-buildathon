use shopping_sessions_domain::{ParseOutcome, ParseStatus};
use shopping_sessions_parser::parse_fields;

fn reference(line: &[u8], width: usize) -> ParseOutcome<Vec<&[u8]>> {
    if line.is_empty() {
        return ParseOutcome::Rejected(ParseStatus::EmptyLine);
    }
    let pieces: Vec<_> = line.split(|byte| *byte == b',').collect();
    if pieces.len() != width {
        return ParseOutcome::Rejected(ParseStatus::WrongFieldCount);
    }
    let mut values = Vec::new();
    for piece in pieces {
        let value = match (piece.first(), piece.last()) {
            (Some(b'"'), Some(b'"')) if piece.len() >= 2 => {
                let inside = &piece[1..piece.len() - 1];
                if inside.contains(&b'"') {
                    return ParseOutcome::Rejected(ParseStatus::MisplacedQuote);
                }
                inside
            }
            _ if piece.contains(&b'"') => {
                return ParseOutcome::Rejected(ParseStatus::MisplacedQuote);
            }
            _ => piece,
        };
        if std::str::from_utf8(value).is_err() {
            return ParseOutcome::Rejected(ParseStatus::InvalidUtf8);
        }
        values.push(value);
    }
    ParseOutcome::Accepted(values)
}

#[test]
fn exhaustive_short_lines_match_the_reference() {
    const ALPHABET: &[u8] = &[b'a', b',', b'"', b' ', b'\r', 0xff];
    for length in 0_usize..=6 {
        let cases = ALPHABET
            .len()
            .pow(u32::try_from(length).expect("short test length"));
        for mut code in 0..cases {
            let mut line = vec![0; length];
            for byte in &mut line {
                *byte = ALPHABET[code % ALPHABET.len()];
                code /= ALPHABET.len();
            }
            for width in 1..=4 {
                assert_eq!(
                    parse_fields(&line, width),
                    reference(&line, width),
                    "line={line:?}, width={width}"
                );
            }
        }
    }
}

#[test]
fn deterministic_fuzz_lines_match_the_reference() {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    for _ in 0..50_000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let length = usize::try_from(state % 96).expect("bounded fuzz length");
        let mut line = Vec::with_capacity(length);
        for _ in 0..length {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            line.push((state >> 56) as u8);
        }
        let width = usize::try_from(state % 8).expect("bounded fuzz width") + 1;
        assert_eq!(parse_fields(&line, width), reference(&line, width));
    }
}
