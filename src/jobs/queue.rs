//! Operações na fila `jobs`. Usa o índice único parcial `idx_jobs_inflight`
//! para deduplicar jobs com mesma raiz/profundidade ainda em andamento.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, sqlx::FromRow, ToSchema)]
pub struct EnqueuedJob {
    pub id: Uuid,
    pub cnpj_basico: String,
    pub profundidade: i16,
    pub callback_url: Option<String>,
    pub status: String,
    pub criado_em: DateTime<Utc>,
    pub iniciado_em: Option<DateTime<Utc>>,
    pub finalizado_em: Option<DateTime<Utc>>,
    pub erro: Option<String>,
    pub resultado: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct EnqueueRequest {
    pub cnpj_basico: String,
    pub profundidade: i16,
    pub callback_url: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum EnqueueOutcome {
    Created,
    AlreadyInflight,
}

/// Inserir um job. Se já existir um in-flight pra mesma raiz/profundidade
/// (`idx_jobs_inflight`), retorna o existente e marca o outcome.
pub async fn enqueue(
    pool: &PgPool,
    req: EnqueueRequest,
) -> Result<(EnqueuedJob, EnqueueOutcome)> {
    let mut tx = pool.begin().await?;

    let inserted: Option<EnqueuedJob> = sqlx::query_as(
        "INSERT INTO jobs (cnpj_basico, profundidade, callback_url) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (cnpj_basico, profundidade) WHERE status IN ('pending', 'running') \
           DO NOTHING \
         RETURNING id, cnpj_basico, profundidade, callback_url, status, \
                   criado_em, iniciado_em, finalizado_em, erro, resultado",
    )
    .bind(&req.cnpj_basico)
    .bind(req.profundidade)
    .bind(req.callback_url.as_deref())
    .fetch_optional(&mut *tx)
    .await?;

    if let Some(job) = inserted {
        tx.commit().await?;
        return Ok((job, EnqueueOutcome::Created));
    }

    let existing: EnqueuedJob = sqlx::query_as(
        "SELECT id, cnpj_basico, profundidade, callback_url, status, \
                criado_em, iniciado_em, finalizado_em, erro, resultado \
         FROM jobs \
         WHERE cnpj_basico = $1 AND profundidade = $2 \
           AND status IN ('pending', 'running') \
         ORDER BY criado_em DESC LIMIT 1",
    )
    .bind(&req.cnpj_basico)
    .bind(req.profundidade)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok((existing, EnqueueOutcome::AlreadyInflight))
}

/// Reivindica o próximo job pendente atomicamente. Usa `FOR UPDATE SKIP LOCKED`
/// pra permitir múltiplos workers em paralelo no futuro.
pub async fn claim_next(pool: &PgPool) -> Result<Option<EnqueuedJob>> {
    let job: Option<EnqueuedJob> = sqlx::query_as(
        "UPDATE jobs SET status = 'running', iniciado_em = now() \
         WHERE id = ( \
           SELECT id FROM jobs \
           WHERE status = 'pending' \
           ORDER BY criado_em ASC \
           FOR UPDATE SKIP LOCKED \
           LIMIT 1 \
         ) \
         RETURNING id, cnpj_basico, profundidade, callback_url, status, \
                   criado_em, iniciado_em, finalizado_em, erro, resultado",
    )
    .fetch_optional(pool)
    .await?;
    Ok(job)
}

pub async fn mark_completed(
    pool: &PgPool,
    id: Uuid,
    resultado: &serde_json::Value,
) -> Result<()> {
    sqlx::query(
        "UPDATE jobs SET status = 'completed', finalizado_em = now(), \
         resultado = $2, erro = NULL WHERE id = $1",
    )
    .bind(id)
    .bind(resultado)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_failed(pool: &PgPool, id: Uuid, erro: &str) -> Result<()> {
    sqlx::query(
        "UPDATE jobs SET status = 'failed', finalizado_em = now(), erro = $2 \
         WHERE id = $1",
    )
    .bind(id)
    .bind(erro)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn fetch(pool: &PgPool, id: Uuid) -> Result<Option<EnqueuedJob>> {
    let job: Option<EnqueuedJob> = sqlx::query_as(
        "SELECT id, cnpj_basico, profundidade, callback_url, status, \
                criado_em, iniciado_em, finalizado_em, erro, resultado \
         FROM jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(job)
}
