//! Consumidor da fila `jobs`. Polling simples (LISTEN/NOTIFY pode entrar
//! depois). Cada job:
//!   1. claim atômico (UPDATE … RETURNING + SKIP LOCKED)
//!   2. carrega vintage local mais recente
//!   3. scan paralelo dos zips por target (cnpj_basico)
//!   4. expansão pra profundidade > 1
//!   5. persist (UPSERT por linha numa transação)
//!   6. mark_completed com snapshot do grafo + webhook opcional

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use sqlx::PgPool;

use crate::config::Config;
use crate::db;
use crate::jobs::queue::{self, EnqueuedJob};
use crate::jobs::webhook;
use crate::persist::upsert;
use crate::source::{scan, vintage};

const POLL_IDLE_DELAY: Duration = Duration::from_secs(2);

pub async fn run(cfg: Config, once: bool) -> anyhow::Result<()> {
    let pool = db::pool(&cfg).await?;
    tracing::info!("worker iniciado");

    loop {
        match queue::claim_next(&pool).await? {
            Some(job) => {
                let id = job.id;
                let cnpj = job.cnpj_basico.clone();
                tracing::info!(%id, %cnpj, "job claimed");
                if let Err(err) = process(&pool, &cfg, &job).await {
                    tracing::error!(%id, error = ?err, "job falhou");
                    let msg = format!("{err:#}");
                    let _ = queue::mark_failed(&pool, id, &msg).await;
                    let _ = deliver_webhook(&job, "failed", None, Some(&msg)).await;
                }
            }
            None => {
                if once {
                    return Ok(());
                }
                tokio::time::sleep(POLL_IDLE_DELAY).await;
            }
        }
        if once {
            return Ok(());
        }
    }
}

async fn process(pool: &PgPool, _cfg: &Config, job: &EnqueuedJob) -> Result<()> {
    let (vintage_name, vintage_dir) = vintage::latest_recorded(pool)
        .await?
        .ok_or_else(|| anyhow!("nenhuma vintage baixada — rode --mode downloader primeiro"))?;

    let mut target: HashSet<String> = HashSet::new();
    target.insert(job.cnpj_basico.clone());

    let mut all_visited: HashSet<String> = target.clone();
    let mut accumulated = scan::ScanResult {
        vintage: vintage_name.clone(),
        ..Default::default()
    };

    for hop in 0..job.profundidade {
        if target.is_empty() {
            break;
        }
        tracing::info!(hop, target_size = target.len(), "scan hop");
        let result = scan::scan_target_set(&vintage_dir, &vintage_name, &target)
            .await
            .with_context(|| format!("scan hop {hop}"))?;

        let next = scan::next_target_layer(&result, &all_visited);

        accumulated.empresas.extend(result.empresas);
        accumulated.estabelecimentos.extend(result.estabelecimentos);
        accumulated.socios.extend(result.socios);
        accumulated.simples.extend(result.simples);

        target = next;
        all_visited.extend(target.iter().cloned());
    }

    upsert::persist_scan(pool, &accumulated)
        .await
        .context("persist scan no cache")?;

    let resultado = build_summary(&accumulated, &job.cnpj_basico, job.profundidade);
    queue::mark_completed(pool, job.id, &resultado).await?;
    deliver_webhook(job, "completed", Some(&resultado), None).await?;
    Ok(())
}

/// Constrói um snapshot mínimo do que foi descoberto pra anexar ao job.
/// O endpoint `/v1/relacionamento/{doc}` continua sendo a forma canônica de
/// montar o grafo a partir do cache; aqui só guardamos contagens e raiz.
fn build_summary(
    result: &scan::ScanResult,
    raiz_cnpj: &str,
    profundidade: i16,
) -> serde_json::Value {
    serde_json::json!({
        "raiz": { "cnpj_basico": raiz_cnpj, "tipo": "empresa" },
        "profundidade": profundidade,
        "vintage": result.vintage,
        "totais": {
            "empresas": result.empresas.len(),
            "estabelecimentos": result.estabelecimentos.len(),
            "socios": result.socios.len(),
            "simples": result.simples.len(),
        },
    })
}

async fn deliver_webhook(
    job: &EnqueuedJob,
    status: &str,
    resultado: Option<&serde_json::Value>,
    erro: Option<&str>,
) -> Result<()> {
    let Some(url) = job.callback_url.as_deref() else {
        return Ok(());
    };
    let payload = webhook::WebhookPayload {
        job_id: job.id,
        status,
        resultado,
        erro,
    };
    if let Err(err) = webhook::deliver(url, &payload).await {
        // Webhook delivery não invalida job; só loga.
        tracing::warn!(job_id = %job.id, error = ?err, "webhook delivery falhou");
    }
    Ok(())
}
