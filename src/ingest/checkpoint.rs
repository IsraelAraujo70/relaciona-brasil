//! Controle de estado do pipeline mensal — tabela `ingestao_status`.
//! Cada vintage é uma linha; etapas avançam em ordem.

use anyhow::Result;
use sqlx::PgPool;

pub async fn start(pool: &PgPool, vintage: &str, etapa: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO ingestao_status (vintage, etapa) \
         VALUES ($1, $2) \
         ON CONFLICT (vintage) DO UPDATE SET \
           etapa = EXCLUDED.etapa, \
           iniciada_em = now(), \
           terminada_em = NULL, \
           erro = NULL",
    )
    .bind(vintage)
    .bind(etapa)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn step(pool: &PgPool, vintage: &str, etapa: &str) -> Result<()> {
    sqlx::query("UPDATE ingestao_status SET etapa = $2 WHERE vintage = $1")
        .bind(vintage)
        .bind(etapa)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn add_rows(pool: &PgPool, vintage: &str, n: i64) -> Result<()> {
    sqlx::query(
        "UPDATE ingestao_status \
         SET linhas_inseridas = COALESCE(linhas_inseridas, 0) + $2 \
         WHERE vintage = $1",
    )
    .bind(vintage)
    .bind(n)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn finish(pool: &PgPool, vintage: &str) -> Result<()> {
    sqlx::query(
        "UPDATE ingestao_status \
         SET etapa = 'concluida', terminada_em = now(), erro = NULL \
         WHERE vintage = $1",
    )
    .bind(vintage)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn fail(pool: &PgPool, vintage: &str, erro: &str) -> Result<()> {
    sqlx::query("UPDATE ingestao_status SET erro = $2 WHERE vintage = $1")
        .bind(vintage)
        .bind(erro)
        .execute(pool)
        .await?;
    Ok(())
}

/// Última etapa registrada para o vintage, se existir.
pub async fn current_step(pool: &PgPool, vintage: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT etapa FROM ingestao_status WHERE vintage = $1")
            .bind(vintage)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0))
}
