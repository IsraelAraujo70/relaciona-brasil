use crate::config::Config;

pub async fn run(_cfg: Config, _once: bool) -> anyhow::Result<()> {
    tracing::warn!("modo worker ainda não implementado (chega na Etapa 2)");
    Ok(())
}
