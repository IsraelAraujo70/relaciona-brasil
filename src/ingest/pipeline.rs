//! Orquestra o pipeline de ingestão de um vintage.

use anyhow::{Context, Result};
use sqlx::PgPool;
use tokio::io::BufReader;

use super::{checkpoint, decode, download, load, parse, source::Source, unzip};

/// Definição estática das tabelas de lookup. (zip_filename, target_table, sql_codigo_type)
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
    /// Quando definido, limita a `n` arquivos por tabela grande (smoke / dev).
    pub max_files_per_table: Option<usize>,
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

    let result: Result<()> = async {
        ingest_lookups(pool, &source, &vintage).await?;
        ingest_empresas(pool, &source, &vintage, cfg.max_files_per_table).await?;
        ingest_estabelecimentos(pool, &source, &vintage, cfg.max_files_per_table).await?;
        ingest_socios(pool, &source, &vintage, cfg.max_files_per_table).await?;
        ingest_simples(pool, &source, &vintage).await?;
        Ok(())
    }
    .await;

    if let Err(err) = result {
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
        let rows = stream_csv_rows(source, &url, |stream| async move {
            parse::read_lookup_rows(stream).await
        })
        .await?;

        let inserted = load::replace_lookup(pool, table, codigo_type, &rows).await?;
        tracing::info!(rows = inserted, "carregado");
        checkpoint::add_rows(pool, vintage, inserted as i64).await?;
    }

    Ok(())
}

async fn ingest_empresas(
    pool: &PgPool,
    source: &Source,
    vintage: &str,
    max_files: Option<usize>,
) -> Result<()> {
    checkpoint::step(pool, vintage, "empresas").await?;

    load::truncate_empresa_cascade(pool).await?;
    tracing::info!("empresa truncada (CASCADE)");

    let n = max_files.unwrap_or(10).min(10);
    let mut total: u64 = 0;

    for i in 0..n {
        let zip_name = format!("Empresas{i}.zip");
        let span = tracing::info_span!("empresa", zip = %zip_name);
        let _enter = span.enter();
        tracing::info!("baixando");

        let url = source.file_url(vintage, &zip_name);
        let dl = download::fetch(source, &url).await?;
        let zip_reader = BufReader::with_capacity(256 * 1024, dl.reader);
        let (decompressed, unzip_task) = unzip::decompress_first_entry(zip_reader);
        let (utf8_stream, decode_task) = decode::latin1_to_utf8(decompressed);

        let count = load::copy_empresas(pool, utf8_stream).await?;

        unzip_task.await??;
        decode_task.await??;

        tracing::info!(rows = count, "carregado");
        total += count;
        checkpoint::add_rows(pool, vintage, count as i64).await?;
    }

    tracing::info!(rows = total, "empresa concluída");
    Ok(())
}

async fn ingest_estabelecimentos(
    pool: &PgPool,
    source: &Source,
    vintage: &str,
    max_files: Option<usize>,
) -> Result<()> {
    checkpoint::step(pool, vintage, "estabelecimentos").await?;

    // Drop FK e índices secundários pra acelerar COPY; tudo é recriado no fim.
    load::drop_fk_estab(pool).await?;
    load::drop_estab_indexes(pool).await?;
    tracing::info!("FK e índices estab dropados (serão recriados após carga)");

    let n = max_files.unwrap_or(10).min(10);
    let mut total_lidos: u64 = 0;
    let mut total_ativos: u64 = 0;

    for i in 0..n {
        let zip_name = format!("Estabelecimentos{i}.zip");
        let span = tracing::info_span!("estabelecimento", zip = %zip_name);
        let _enter = span.enter();
        tracing::info!("baixando");

        let url = source.file_url(vintage, &zip_name);
        let dl = download::fetch(source, &url).await?;
        let zip_reader = BufReader::with_capacity(256 * 1024, dl.reader);
        let (decompressed, unzip_task) = unzip::decompress_first_entry(zip_reader);
        let (utf8_stream, decode_task) = decode::latin1_to_utf8(decompressed);

        let (lidos, ativos) = load::copy_estabelecimentos(pool, utf8_stream).await?;

        unzip_task.await??;
        decode_task.await??;

        tracing::info!(lidos, ativos, "carregado");
        total_lidos += lidos;
        total_ativos += ativos;
        checkpoint::add_rows(pool, vintage, ativos as i64).await?;
    }

    let orfaos = load::delete_orphan_estab(pool).await?;
    if orfaos > 0 {
        tracing::warn!(orfaos, "estabelecimentos órfãos removidos");
    }
    tracing::info!("recriando índices estab (pode levar minutos)");
    load::recreate_estab_indexes(pool).await?;
    load::recreate_fk_estab(pool).await?;
    tracing::info!(total_lidos, total_ativos, "estabelecimento concluído");
    Ok(())
}

async fn ingest_socios(
    pool: &PgPool,
    source: &Source,
    vintage: &str,
    max_files: Option<usize>,
) -> Result<()> {
    checkpoint::step(pool, vintage, "socios").await?;
    load::drop_fk_socio(pool).await?;
    load::drop_socio_indexes(pool).await?;
    tracing::info!("FK e índices sócio dropados");

    let n = max_files.unwrap_or(10).min(10);
    let mut total: u64 = 0;

    for i in 0..n {
        let zip_name = format!("Socios{i}.zip");
        let span = tracing::info_span!("socio", zip = %zip_name);
        let _enter = span.enter();
        tracing::info!("baixando");

        let url = source.file_url(vintage, &zip_name);
        let dl = download::fetch(source, &url).await?;
        let zip_reader = BufReader::with_capacity(256 * 1024, dl.reader);
        let (decompressed, unzip_task) = unzip::decompress_first_entry(zip_reader);
        let (utf8_stream, decode_task) = decode::latin1_to_utf8(decompressed);

        let count = load::copy_socios(pool, utf8_stream).await?;

        unzip_task.await??;
        decode_task.await??;

        tracing::info!(rows = count, "carregado");
        total += count;
        checkpoint::add_rows(pool, vintage, count as i64).await?;
    }

    let orfaos = load::delete_orphan_socio(pool).await?;
    if orfaos > 0 {
        tracing::warn!(orfaos, "sócios órfãos removidos");
    }
    tracing::info!("recriando índices sócio (pode levar minutos por causa do GIN)");
    load::recreate_socio_indexes(pool).await?;
    load::recreate_fk_socio(pool).await?;
    tracing::info!(rows = total, "sócio concluído");
    Ok(())
}

async fn ingest_simples(pool: &PgPool, source: &Source, vintage: &str) -> Result<()> {
    checkpoint::step(pool, vintage, "simples").await?;
    load::drop_fk_simples(pool).await?;

    let span = tracing::info_span!("simples", zip = "Simples.zip");
    let _enter = span.enter();
    tracing::info!("baixando");

    let url = source.file_url(vintage, "Simples.zip");
    let dl = download::fetch(source, &url).await?;
    let zip_reader = BufReader::with_capacity(256 * 1024, dl.reader);
    let (decompressed, unzip_task) = unzip::decompress_first_entry(zip_reader);
    let (utf8_stream, decode_task) = decode::latin1_to_utf8(decompressed);

    let count = load::copy_simples(pool, utf8_stream).await?;

    unzip_task.await??;
    decode_task.await??;

    tracing::info!(rows = count, "carregado");
    checkpoint::add_rows(pool, vintage, count as i64).await?;

    let orfaos = load::delete_orphan_simples(pool).await?;
    if orfaos > 0 {
        tracing::warn!(orfaos, "simples órfãos removidos");
    }
    load::recreate_fk_simples(pool).await?;
    tracing::info!("simples concluído");
    Ok(())
}

/// Helper: baixa, descomprime, decodifica e passa o stream UTF-8 para um closure.
async fn stream_csv_rows<F, Fut, T>(source: &Source, url: &str, handler: F) -> Result<T>
where
    F: FnOnce(tokio::io::DuplexStream) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let dl = download::fetch(source, url).await?;
    let zip_reader = BufReader::with_capacity(64 * 1024, dl.reader);
    let (decompressed, unzip_task) = unzip::decompress_first_entry(zip_reader);
    let (utf8_stream, decode_task) = decode::latin1_to_utf8(decompressed);

    let result = handler(utf8_stream).await?;

    unzip_task.await??;
    decode_task.await??;
    Ok(result)
}
