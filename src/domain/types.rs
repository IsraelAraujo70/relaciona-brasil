//! Newtypes pra documentos brasileiros (CNPJ, CPF). A validação aqui é
//! sintática — o dump da Receita usa CPF mascarado `***NNNNNN**` (apenas os
//! 6 dígitos do meio são visíveis), e nós persistimos exatamente como vem.

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Cnpj(String);

impl Cnpj {
    /// Aceita qualquer entrada com 14 dígitos (com ou sem pontuação).
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() != 14 {
            return Err("CNPJ deve ter 14 dígitos");
        }
        Ok(Self(digits))
    }

    /// 8 primeiros dígitos — chave em `empresa` e `socio`.
    pub fn basico(&self) -> &str {
        &self.0[..8]
    }

    pub fn full(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Cpf {
    masked: String,
}

impl Cpf {
    /// Aceita CPF não-mascarado (11 dígitos) e produz a versão mascarada
    /// `***NNNNNN**` que casa com `socio.cnpj_cpf_socio` no dump.
    /// Também aceita CPF já mascarado (o formato exato).
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        // Se já estiver no formato mascarado, repassa.
        if s.len() == 11 && s.starts_with("***") && s.ends_with("**") {
            let middle = &s[3..9];
            if middle.chars().all(|c| c.is_ascii_digit()) {
                return Ok(Self {
                    masked: s.to_string(),
                });
            }
        }

        let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() != 11 {
            return Err("CPF deve ter 11 dígitos (ou já estar mascarado)");
        }
        // Mascarar: ***NNNNNN**
        let mut masked = String::with_capacity(11);
        masked.push_str("***");
        masked.push_str(&digits[3..9]);
        masked.push_str("**");
        Ok(Self { masked })
    }

    pub fn masked(&self) -> &str {
        &self.masked
    }
}

/// Identificador de pessoa OU empresa, usado em `/relacionamento/:doc`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Doc {
    Cpf(Cpf),
    Cnpj(Cnpj),
}

impl Doc {
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
        match digits.len() {
            11 => Ok(Doc::Cpf(Cpf::parse(s)?)),
            14 => Ok(Doc::Cnpj(Cnpj::parse(s)?)),
            _ => {
                // Talvez seja CPF já mascarado?
                if s.len() == 11 && s.starts_with("***") {
                    Ok(Doc::Cpf(Cpf::parse(s)?))
                } else {
                    Err("documento deve ser CPF (11 dígitos), CPF mascarado ou CNPJ (14 dígitos)")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cnpj_basico() {
        let c = Cnpj::parse("00.000.000/0001-91").unwrap();
        assert_eq!(c.basico(), "00000000");
        assert_eq!(c.full(), "00000000000191");
    }

    #[test]
    fn cpf_mask_from_unmasked() {
        let c = Cpf::parse("12345678901").unwrap();
        assert_eq!(c.masked(), "***456789**");
    }

    #[test]
    fn cpf_pass_through_masked() {
        let c = Cpf::parse("***456789**").unwrap();
        assert_eq!(c.masked(), "***456789**");
    }

    #[test]
    fn doc_dispatches() {
        match Doc::parse("12.345.678/0001-99").unwrap() {
            Doc::Cnpj(_) => {}
            Doc::Cpf(_) => panic!("esperava CNPJ"),
        }
        match Doc::parse("12345678901").unwrap() {
            Doc::Cpf(_) => {}
            Doc::Cnpj(_) => panic!("esperava CPF"),
        }
    }
}
