//! Job mensal de DOWNLOAD da vintage atual: dia 15 às 06:00 UTC (= 03:00 BRT).
//! A Receita publica o dump entre os dias 10 e 12; rodar dia 15 dá folga.
//!
//! O scheduler roda dentro do processo `--mode downloader`. `--once` baixa
//! a vintage corrente uma vez e sai (útil no boot do pod).

use std::sync::Arc;

use anyhow::{Context, Result};
use sqlx::PgPool;
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::config::Config;
use crate::db;
use crate::source::nextcloud::Source;
use crate::source::vintage;

/// `sec min hour day-of-month month day-of-week`
const SCHEDULE: &str = "0 0 6 15 * *";

pub async fn run_downloader(cfg: Config, once: bool) -> Result<()> {
    let pool = db::pool(&cfg).await?;

    if once {
        download_latest(&cfg, &pool).await?;
        return Ok(());
    }

    // Boot: baixa a vintage atual antes de virar agendador.
    if let Err(err) = download_latest(&cfg, &pool).await {
        tracing::error!(error = ?err, "download inicial falhou — continuando como scheduler");
    }

    let mut sched = JobScheduler::new().await?;

    let cfg = Arc::new(cfg);
    let pool = Arc::new(pool);

    let cfg_for_job = cfg.clone();
    let pool_for_job = pool.clone();

    let job = Job::new_async(SCHEDULE, move |_uuid, _l| {
        let cfg = cfg_for_job.clone();
        let pool = pool_for_job.clone();
        Box::pin(async move {
            tracing::info!("disparando download mensal agendado");
            match download_latest(&cfg, &pool).await {
                Ok(()) => tracing::info!("download mensal concluído"),
                Err(err) => tracing::error!(error = ?err, "download mensal falhou"),
            }
        })
    })?;

    sched.add(job).await?;
    sched.start().await?;
    tracing::info!(schedule = SCHEDULE, "downloader scheduler ativo");

    wait_for_shutdown().await;
    tracing::info!("encerrando downloader");
    sched.shutdown().await?;
    Ok(())
}

async fn download_latest(cfg: &Config, pool: &PgPool) -> Result<()> {
    let source = Source::from_share_url(&cfg.dump_base_url)
        .context("construindo Source")?;
    let latest = source
        .latest_vintage()
        .await
        .context("descobrindo última vintage")?;
    tracing::info!(vintage = %latest, "vintage alvo identificada");

    tokio::fs::create_dir_all(&cfg.dump_data_dir)
        .await
        .with_context(|| format!("criando {}", cfg.dump_data_dir.display()))?;

    let (path, bytes) = vintage::download_full(&source, &latest, &cfg.dump_data_dir)
        .await
        .with_context(|| format!("baixando vintage {latest}"))?;

    vintage::record(pool, &latest, bytes, &path).await?;
    tracing::info!(vintage = %latest, bytes, "vintage registrada");

    let removed = vintage::cleanup_old(&cfg.dump_data_dir, cfg.keep_vintages).await?;
    if !removed.is_empty() {
        tracing::info!(removidas = ?removed, "vintages antigas apagadas");
        // Limpa também o registro no DB.
        for v in &removed {
            sqlx::query("DELETE FROM vintages_baixadas WHERE vintage = $1")
                .bind(v)
                .execute(pool)
                .await?;
        }
    }
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("SIGTERM recebido"),
        _ = sigint.recv()  => tracing::info!("SIGINT recebido"),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("Ctrl+C recebido");
}
