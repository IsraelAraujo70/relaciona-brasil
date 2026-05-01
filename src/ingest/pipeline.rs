//! Orquestra o pipeline de ingestão de um vintage.
//!
//! Etapa 2 cobre apenas as tabelas de lookup. Etapa 3 estenderá com empresa,
//! estabelecimento, sócio e simples.

use anyhow::{Context, Result};
use sqlx::PgPool;
use tokio::io::BufReader;

use super::{checkpoint, decode, download, load, parse, source::Source, unzip};

/// Definição estática dos arquivos de lookup. (zip_filename, target_table, sql_codigo_type)
const LOOKUPS: &[(&str, &str, &str)] = &[
    ("Cnaes.zip", "cnae", "char(7)"),
    ("Motivos.zip", "motivo", "smallint"),
    ("Municipios.zip", "municipio", "char(4)"),
    ("Naturezas.zip", "natureza", "char(4)"),
    ("Paises.zip", "pais", "char(3)"),
    ("Qualificacoes.zip", "qualificacao", "smallint"),
];

pub struct PipelineConfig {
    pub share_url: String,
    pub vintage_override: Option<String>,
}

pub async fn run(pool: &PgPool, cfg: PipelineConfig) -> Result<()> {
    let source = Source::from_share_url(&cfg.share_url)?;

    let vintage = match cfg.vintage_override {
        Some(v) => v,
        None => source
            .latest_vintage()
            .await
            .context("descobrindo vintage mais recente")?,
    };
    tracing::info!(%vintage, "vintage selecionado");

    if let Err(err) = ingest_lookups(pool, &source, &vintage).await {
        let msg = format!("{err:?}");
        tracing::error!(%vintage, error = %msg, "falha na ingestão");
        let _ = checkpoint::fail(pool, &vintage, &msg).await;
        return Err(err);
    }

    checkpoint::finish(pool, &vintage).await?;
    tracing::info!(%vintage, "ingestão concluída");
    Ok(())
}

async fn ingest_lookups(pool: &PgPool, source: &Source, vintage: &str) -> Result<()> {
    checkpoint::start(pool, vintage, "lookups").await?;

    for (zip_name, table, codigo_type) in LOOKUPS {
        let span = tracing::info_span!("lookup", zip = zip_name, table = table);
        let _enter = span.enter();
        tracing::info!("baixando");

        let url = source.file_url(vintage, zip_name);
        let dl = download::fetch(source, &url).await?;
        let zip_reader = BufReader::with_capacity(64 * 1024, dl.reader);

        let (decompressed, unzip_task) = unzip::decompress_first_entry(zip_reader);
        let (utf8_stream, decode_task) = decode::latin1_to_utf8(decompressed);

        let rows = parse::read_lookup_rows(utf8_stream).await?;

        // Espera as tasks de fundo terminarem (propaga erros)
        unzip_task.await??;
        decode_task.await??;

        let inserted = load::replace_lookup(pool, table, codigo_type, &rows).await?;
        tracing::info!(rows = inserted, "carregado");

        checkpoint::add_rows(pool, vintage, inserted as i64).await?;
    }

    Ok(())
}
