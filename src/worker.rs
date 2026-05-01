use crate::config::Config;
use crate::db;
use crate::ingest::pipeline::{self, PipelineConfig};

pub async fn run(
    cfg: Config,
    once: bool,
    max_files_per_table: Option<usize>,
) -> anyhow::Result<()> {
    let pool = db::pool(&cfg).await?;

    if once {
        pipeline::run(
            &pool,
            PipelineConfig {
                share_url: cfg.dump_base_url.clone(),
                vintage_override: None,
                max_files_per_table,
            },
        )
        .await?;
        return Ok(());
    }

    tracing::warn!(
        "modo worker contínuo (com scheduler) ainda não implementado — chega na Etapa 4"
    );
    Ok(())
}
