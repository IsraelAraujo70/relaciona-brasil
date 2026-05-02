//! UPSERT por linha das rows extraídas pelo scan no Postgres.
//!
//! Usado pelo worker depois que o scan termina. Trabalha em transação por job
//! pra que reads do cache não vejam estado parcial. Convertem strings cruas do
//! CSV pros tipos do schema; vazias viram NULL.

use anyhow::Result;
use chrono::NaiveDate;
use sqlx::PgPool;

use crate::source::parse::{
    EmpresaRow, EstabelecimentoRow, SimplesRow, SocioRow,
};
use crate::source::scan::ScanResult;

pub async fn persist_scan(pool: &PgPool, result: &ScanResult) -> Result<()> {
    let mut tx = pool.begin().await?;

    // 1. empresa primeiro (FK target).
    for row in &result.empresas {
        upsert_empresa(&mut tx, row, &result.vintage).await?;
    }

    // 2. simples depende de empresa (FK).
    for row in &result.simples {
        upsert_simples(&mut tx, row, &result.vintage).await?;
    }

    // 3. estabelecimento (FK em empresa).
    for row in &result.estabelecimentos {
        upsert_estabelecimento(&mut tx, row, &result.vintage).await?;
    }

    // 4. socio: sem PK natural — deletar todos os do cnpj_basico e reinserir.
    let cnpjs: std::collections::HashSet<&str> =
        result.socios.iter().map(|s| s.cnpj_basico.as_str()).collect();
    if !cnpjs.is_empty() {
        let cnpjs_vec: Vec<String> = cnpjs.iter().map(|s| s.to_string()).collect();
        sqlx::query("DELETE FROM socio WHERE cnpj_basico = ANY($1)")
            .bind(&cnpjs_vec)
            .execute(&mut *tx)
            .await?;
    }
    for row in &result.socios {
        insert_socio(&mut tx, row, &result.vintage).await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn upsert_empresa(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &EmpresaRow,
    vintage: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO empresa \
           (cnpj_basico, razao_social, natureza_juridica, qualificacao_resp, \
            capital_social, porte, ente_federativo, consultado_em, vintage) \
         VALUES ($1, $2, $3, $4, $5::NUMERIC, $6, $7, now(), $8) \
         ON CONFLICT (cnpj_basico) DO UPDATE SET \
           razao_social      = EXCLUDED.razao_social, \
           natureza_juridica = EXCLUDED.natureza_juridica, \
           qualificacao_resp = EXCLUDED.qualificacao_resp, \
           capital_social    = EXCLUDED.capital_social, \
           porte             = EXCLUDED.porte, \
           ente_federativo   = EXCLUDED.ente_federativo, \
           consultado_em     = now(), \
           vintage           = EXCLUDED.vintage",
    )
    .bind(&row.cnpj_basico)
    .bind(text_or_null(&row.razao_social))
    .bind(text_or_null(&row.natureza_juridica))
    .bind(int_or_null::<i16>(&row.qualificacao_resp))
    .bind(decimal_text_or_null(&row.capital_social))
    .bind(text_or_null(&row.porte))
    .bind(text_or_null(&row.ente_federativo))
    .bind(vintage)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_estabelecimento(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &EstabelecimentoRow,
    vintage: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO estabelecimento \
           (cnpj_basico, cnpj_ordem, cnpj_dv, matriz_filial, nome_fantasia, \
            situacao, data_situacao, motivo_situacao, nome_cidade_ext, pais, \
            data_inicio, cnae_principal, cnaes_secundarios, tipo_logradouro, \
            logradouro, numero, complemento, bairro, cep, uf, municipio, \
            ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax, email, \
            situacao_especial, data_sit_especial, consultado_em, vintage) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, \
                 $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, \
                 $27, $28, $29, $30, now(), $31) \
         ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO UPDATE SET \
           matriz_filial     = EXCLUDED.matriz_filial, \
           nome_fantasia     = EXCLUDED.nome_fantasia, \
           situacao          = EXCLUDED.situacao, \
           data_situacao     = EXCLUDED.data_situacao, \
           motivo_situacao   = EXCLUDED.motivo_situacao, \
           nome_cidade_ext   = EXCLUDED.nome_cidade_ext, \
           pais              = EXCLUDED.pais, \
           data_inicio       = EXCLUDED.data_inicio, \
           cnae_principal    = EXCLUDED.cnae_principal, \
           cnaes_secundarios = EXCLUDED.cnaes_secundarios, \
           tipo_logradouro   = EXCLUDED.tipo_logradouro, \
           logradouro        = EXCLUDED.logradouro, \
           numero            = EXCLUDED.numero, \
           complemento       = EXCLUDED.complemento, \
           bairro            = EXCLUDED.bairro, \
           cep               = EXCLUDED.cep, \
           uf                = EXCLUDED.uf, \
           municipio         = EXCLUDED.municipio, \
           ddd_1             = EXCLUDED.ddd_1, \
           telefone_1        = EXCLUDED.telefone_1, \
           ddd_2             = EXCLUDED.ddd_2, \
           telefone_2        = EXCLUDED.telefone_2, \
           ddd_fax           = EXCLUDED.ddd_fax, \
           fax               = EXCLUDED.fax, \
           email             = EXCLUDED.email, \
           situacao_especial = EXCLUDED.situacao_especial, \
           data_sit_especial = EXCLUDED.data_sit_especial, \
           consultado_em     = now(), \
           vintage           = EXCLUDED.vintage",
    )
    .bind(&row.cnpj_basico)
    .bind(&row.cnpj_ordem)
    .bind(&row.cnpj_dv)
    .bind(text_or_null(&row.matriz_filial))
    .bind(text_or_null(&row.nome_fantasia))
    .bind(text_or_null(&row.situacao))
    .bind(date_or_null(&row.data_situacao))
    .bind(int_or_null::<i16>(&row.motivo_situacao))
    .bind(text_or_null(&row.nome_cidade_ext))
    .bind(text_or_null(&row.pais))
    .bind(date_or_null(&row.data_inicio))
    .bind(text_or_null(&row.cnae_principal))
    .bind(text_array_or_null(&row.cnaes_secundarios))
    .bind(text_or_null(&row.tipo_logradouro))
    .bind(text_or_null(&row.logradouro))
    .bind(text_or_null(&row.numero))
    .bind(text_or_null(&row.complemento))
    .bind(text_or_null(&row.bairro))
    .bind(text_or_null(&row.cep))
    .bind(text_or_null(&row.uf))
    .bind(text_or_null(&row.municipio))
    .bind(text_or_null(&row.ddd_1))
    .bind(text_or_null(&row.telefone_1))
    .bind(text_or_null(&row.ddd_2))
    .bind(text_or_null(&row.telefone_2))
    .bind(text_or_null(&row.ddd_fax))
    .bind(text_or_null(&row.fax))
    .bind(text_or_null(&row.email))
    .bind(text_or_null(&row.situacao_especial))
    .bind(date_or_null(&row.data_sit_especial))
    .bind(vintage)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_socio(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &SocioRow,
    vintage: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO socio \
           (cnpj_basico, identificador, nome_socio, cnpj_cpf_socio, qualificacao, \
            data_entrada, pais, cpf_repr_legal, nome_repr_legal, qualif_repr_legal, \
            faixa_etaria, consultado_em, vintage) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), $12)",
    )
    .bind(&row.cnpj_basico)
    .bind(int_or_null::<i16>(&row.identificador).unwrap_or(0))
    .bind(text_or_null(&row.nome_socio))
    .bind(text_or_null(&row.cnpj_cpf_socio))
    .bind(int_or_null::<i16>(&row.qualificacao))
    .bind(date_or_null(&row.data_entrada))
    .bind(text_or_null(&row.pais))
    .bind(text_or_null(&row.cpf_repr_legal))
    .bind(text_or_null(&row.nome_repr_legal))
    .bind(int_or_null::<i16>(&row.qualif_repr_legal))
    .bind(int_or_null::<i16>(&row.faixa_etaria))
    .bind(vintage)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_simples(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &SimplesRow,
    vintage: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO simples \
           (cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao, \
            opcao_mei, data_opcao_mei, data_exclusao_mei, consultado_em, vintage) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, now(), $8) \
         ON CONFLICT (cnpj_basico) DO UPDATE SET \
           opcao_simples      = EXCLUDED.opcao_simples, \
           data_opcao_simples = EXCLUDED.data_opcao_simples, \
           data_exclusao      = EXCLUDED.data_exclusao, \
           opcao_mei          = EXCLUDED.opcao_mei, \
           data_opcao_mei     = EXCLUDED.data_opcao_mei, \
           data_exclusao_mei  = EXCLUDED.data_exclusao_mei, \
           consultado_em      = now(), \
           vintage            = EXCLUDED.vintage",
    )
    .bind(&row.cnpj_basico)
    .bind(bool_sn_or_null(&row.opcao_simples))
    .bind(date_or_null(&row.data_opcao_simples))
    .bind(date_or_null(&row.data_exclusao))
    .bind(bool_sn_or_null(&row.opcao_mei))
    .bind(date_or_null(&row.data_opcao_mei))
    .bind(date_or_null(&row.data_exclusao_mei))
    .bind(vintage)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

// ─── helpers ──────────────────────────────────────────────────────────────────

fn text_or_null(s: &str) -> Option<&str> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn int_or_null<T: std::str::FromStr>(s: &str) -> Option<T> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<T>().ok()
}

fn date_or_null(s: &str) -> Option<NaiveDate> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed == "00000000" {
        return None;
    }
    if trimmed.len() != 8 {
        return None;
    }
    let y: i32 = trimmed[0..4].parse().ok()?;
    let m: u32 = trimmed[4..6].parse().ok()?;
    let d: u32 = trimmed[6..8].parse().ok()?;
    NaiveDate::from_ymd_opt(y, m, d)
}

/// Capital social: vem como decimal usando `,` (vírgula) como separador.
/// Convertemos para `.` e mandamos como texto pra coluna NUMERIC; Postgres
/// faz o cast.
fn decimal_text_or_null(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.replace(',', "."))
}

fn bool_sn_or_null(s: &str) -> Option<bool> {
    match s.trim() {
        "S" => Some(true),
        "N" => Some(false),
        _ => None,
    }
}

/// Recebe lista CSV-style separada por vírgula. Postgres aceita `TEXT[]` via
/// `Vec<String>` via sqlx.
fn text_array_or_null(s: &str) -> Option<Vec<String>> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    let parts: Vec<String> = trimmed
        .split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some(parts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates() {
        assert_eq!(date_or_null(""), None);
        assert_eq!(date_or_null("00000000"), None);
        assert_eq!(
            date_or_null("20240315"),
            Some(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap())
        );
        assert_eq!(date_or_null("2024-03-15"), None);
    }

    #[test]
    fn sn() {
        assert_eq!(bool_sn_or_null("S"), Some(true));
        assert_eq!(bool_sn_or_null("N"), Some(false));
        assert_eq!(bool_sn_or_null(""), None);
        assert_eq!(bool_sn_or_null("X"), None);
    }

    #[test]
    fn decimals() {
        assert_eq!(decimal_text_or_null("10000,50"), Some("10000.50".into()));
        assert_eq!(decimal_text_or_null(""), None);
        assert_eq!(decimal_text_or_null("0"), Some("0".into()));
    }

    #[test]
    fn arrays() {
        assert_eq!(text_array_or_null(""), None);
        assert_eq!(
            text_array_or_null("4711301,4729699"),
            Some(vec!["4711301".into(), "4729699".into()])
        );
    }
}
