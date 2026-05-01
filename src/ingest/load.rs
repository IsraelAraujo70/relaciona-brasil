//! Carga em massa para Postgres. Para tabelas pequenas (lookups) usamos
//! INSERT via UNNEST; para as grandes (empresa, estabelecimento, sócio,
//! simples — Etapa 3) trocaremos por COPY FROM STDIN BINARY.

use anyhow::Result;
use sqlx::PgPool;

use super::parse::LookupRow;

/// Limpa e popula uma tabela de lookup com `(codigo, descricao)`. Idempotente:
/// `TRUNCATE` antes da inserção respeita o modelo de snapshot.
pub async fn replace_lookup(
    pool: &PgPool,
    table: &str,
    codigo_type: &str,
    rows: &[LookupRow],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await?;

    // CASCADE pra lidar com FKs vindas de tabelas filhas que ainda não foram
    // populadas (a primeira execução roda lookups → empresa → estab/sócio).
    sqlx::query(&format!("TRUNCATE TABLE {table} CASCADE"))
        .execute(&mut *tx)
        .await?;

    let codigos: Vec<&str> = rows.iter().map(|r| r.codigo.as_str()).collect();
    let descricoes: Vec<&str> = rows.iter().map(|r| r.descricao.as_str()).collect();

    let sql = format!(
        "INSERT INTO {table} (codigo, descricao) \
         SELECT codigo::{codigo_type}, descricao \
         FROM UNNEST($1::text[], $2::text[]) AS t(codigo, descricao)"
    );

    let inserted = sqlx::query(&sql)
        .bind(&codigos)
        .bind(&descricoes)
        .execute(&mut *tx)
        .await?
        .rows_affected();

    tx.commit().await?;
    Ok(inserted)
}
