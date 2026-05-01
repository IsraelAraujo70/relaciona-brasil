//! Cron in-process via tokio-cron-scheduler.
//!
//! Job mensal: dia 15 às 06:00 UTC (= 03:00 BRT). A Receita publica o dump
//! entre os dias 10 e 12; rodar dia 15 dá folga pra eventuais atrasos.

use std::sync::Arc;

use anyhow::Result;
use sqlx::PgPool;
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::config::Config;
use crate::ingest::pipeline::{self, PipelineConfig};

/// `sec min hour day-of-month month day-of-week`
const SCHEDULE: &str = "0 0 6 15 * *";

pub async fn run_forever(cfg: Config, pool: PgPool) -> Result<()> {
    let mut sched = JobScheduler::new().await?;

    let cfg = Arc::new(cfg);
    let pool = Arc::new(pool);

    let cfg_for_job = cfg.clone();
    let pool_for_job = pool.clone();

    let job = Job::new_async(SCHEDULE, move |_uuid, _l| {
        let cfg = cfg_for_job.clone();
        let pool = pool_for_job.clone();
        Box::pin(async move {
            tracing::info!("disparando ingestão mensal agendada");
            let result = pipeline::run(
                &pool,
                PipelineConfig {
                    share_url: cfg.dump_base_url.clone(),
                    vintage_override: None,
                    max_files_per_table: None,
                },
            )
            .await;
            match result {
                Ok(()) => tracing::info!("ingestão mensal concluída com sucesso"),
                Err(err) => tracing::error!(error = ?err, "ingestão mensal falhou"),
            }
        })
    })?;

    sched.add(job).await?;
    sched.start().await?;
    tracing::info!(schedule = SCHEDULE, "scheduler ativo");

    // Espera SIGTERM/SIGINT pra encerrar limpo.
    wait_for_shutdown().await;
    tracing::info!("encerrando scheduler");
    sched.shutdown().await?;
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
