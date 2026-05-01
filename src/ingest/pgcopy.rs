//! Helpers para escrever linhas no formato COPY TEXT do Postgres.
//! Especificação: <https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.2>
//!
//! Regras: `\t` separa colunas, `\n` separa linhas, `\N` representa NULL,
//! e os literais `\\`, `\t`, `\n`, `\r` precisam ser escapados.

/// Escreve um campo de texto, escapando os caracteres especiais do COPY TEXT.
pub fn write_text(buf: &mut Vec<u8>, s: &str) {
    for &b in s.as_bytes() {
        match b {
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            _ => buf.push(b),
        }
    }
}

pub fn write_null(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"\\N");
}

pub fn write_tab(buf: &mut Vec<u8>) {
    buf.push(b'\t');
}

pub fn write_newline(buf: &mut Vec<u8>) {
    buf.push(b'\n');
}

/// Escreve texto, ou NULL se vazio.
pub fn write_text_or_null(buf: &mut Vec<u8>, s: &str) {
    if s.is_empty() {
        write_null(buf);
    } else {
        write_text(buf, s);
    }
}

/// Inteiro em string (smallint/integer/bigint), ou NULL se vazio/zero.
pub fn write_int_or_null(buf: &mut Vec<u8>, s: &str) {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        write_null(buf);
        return;
    }
    // Postgres aceita inteiros literais; só repassamos
    buf.extend_from_slice(trimmed.as_bytes());
}

/// Decimal brasileiro ("1234,56") → Postgres ("1234.56"), ou NULL se vazio.
pub fn write_decimal_or_null(buf: &mut Vec<u8>, s: &str) {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        write_null(buf);
        return;
    }
    for b in trimmed.bytes() {
        if b == b',' {
            buf.push(b'.');
        } else {
            buf.push(b);
        }
    }
}

/// Data no formato YYYYMMDD → YYYY-MM-DD, ou NULL para vazio ou "00000000".
pub fn write_date_or_null(buf: &mut Vec<u8>, s: &str) {
    let s = s.trim();
    if s.is_empty() || s == "00000000" || s == "0" {
        write_null(buf);
        return;
    }
    if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        buf.extend_from_slice(&s.as_bytes()[0..4]);
        buf.push(b'-');
        buf.extend_from_slice(&s.as_bytes()[4..6]);
        buf.push(b'-');
        buf.extend_from_slice(&s.as_bytes()[6..8]);
    } else {
        // formato inesperado; gravar como NULL pra não quebrar a carga
        write_null(buf);
    }
}

/// "S"/"N" → "t"/"f" para BOOLEAN. Vazio → NULL.
pub fn write_bool_sn_or_null(buf: &mut Vec<u8>, s: &str) {
    match s.trim() {
        "" => write_null(buf),
        "S" | "s" => buf.push(b't'),
        "N" | "n" => buf.push(b'f'),
        _ => write_null(buf),
    }
}

/// Lista CSV-em-CSV (ex.: "1234567,8901234") → literal de array Postgres "{1234567,8901234}".
/// Vazio → NULL.
pub fn write_text_array_or_null(buf: &mut Vec<u8>, s: &str) {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        write_null(buf);
        return;
    }
    // Como queremos guardar o literal {a,b,c} dentro de um campo COPY TEXT,
    // os caracteres `{`, `}`, `,` não precisam de escape COPY (eles fazem parte
    // do valor, e o array literal é válido).
    buf.push(b'{');
    let mut first = true;
    for code in trimmed.split(',').filter(|p| !p.is_empty()) {
        if !first {
            buf.push(b',');
        }
        first = false;
        // códigos numéricos não precisam de aspas
        for b in code.trim().bytes() {
            buf.push(b);
        }
    }
    buf.push(b'}');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(buf: &[u8]) -> &str {
        std::str::from_utf8(buf).unwrap()
    }

    #[test]
    fn text_escapes_specials() {
        let mut buf = Vec::new();
        write_text(&mut buf, "a\tb\nc\\d");
        assert_eq!(s(&buf), "a\\tb\\nc\\\\d");
    }

    #[test]
    fn null_for_empty() {
        let mut buf = Vec::new();
        write_text_or_null(&mut buf, "");
        assert_eq!(s(&buf), "\\N");
    }

    #[test]
    fn date_yyyymmdd() {
        let mut buf = Vec::new();
        write_date_or_null(&mut buf, "20260415");
        assert_eq!(s(&buf), "2026-04-15");
        buf.clear();
        write_date_or_null(&mut buf, "00000000");
        assert_eq!(s(&buf), "\\N");
        buf.clear();
        write_date_or_null(&mut buf, "");
        assert_eq!(s(&buf), "\\N");
    }

    #[test]
    fn decimal_comma_to_dot() {
        let mut buf = Vec::new();
        write_decimal_or_null(&mut buf, "1234,56");
        assert_eq!(s(&buf), "1234.56");
    }

    #[test]
    fn bool_sn() {
        let mut buf = Vec::new();
        write_bool_sn_or_null(&mut buf, "S");
        assert_eq!(s(&buf), "t");
        buf.clear();
        write_bool_sn_or_null(&mut buf, "N");
        assert_eq!(s(&buf), "f");
        buf.clear();
        write_bool_sn_or_null(&mut buf, "");
        assert_eq!(s(&buf), "\\N");
    }

    #[test]
    fn array_literal() {
        let mut buf = Vec::new();
        write_text_array_or_null(&mut buf, "1234567,8901234,5678901");
        assert_eq!(s(&buf), "{1234567,8901234,5678901}");
        buf.clear();
        write_text_array_or_null(&mut buf, "");
        assert_eq!(s(&buf), "\\N");
    }
}
