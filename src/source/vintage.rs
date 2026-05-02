//! Gerencia o cache local de vintages: baixa o conjunto completo de zips de uma
//! vintage para `<dump_data_dir>/<YYYY-MM>/`, lista o que está pronto e expira
//! vintages antigas.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use sqlx::PgPool;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

use super::bucketize;
use super::download;
use super::index;
use super::nextcloud::Source;

/// Lista vintages presentes no diretório local. Cada subdiretório com nome
/// `YYYY-MM` é considerado uma vintage candidata.
pub async fn list_local(dump_data_dir: &Path) -> Result<Vec<String>> {
    if !dump_data_dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut rd = tokio::fs::read_dir(dump_data_dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let name = entry.file_name().into_string().unwrap_or_default();
        if is_vintage(&name) {
            out.push(name);
        }
    }
    out.sort();
    Ok(out)
}

/// Resolve o caminho local da vintage. Não verifica se existe.
pub fn vintage_path(dump_data_dir: &Path, vintage: &str) -> PathBuf {
    dump_data_dir.join(vintage)
}

/// Baixa todos os zips de uma vintage do share para o diretório local. Idempotente
/// por arquivo: se o tamanho local bater com o anunciado pelo PROPFIND, pula.
/// Retorna o caminho do diretório com a vintage e o total de bytes em disco.
pub async fn download_full(
    source: &Source,
    vintage: &str,
    dump_data_dir: &Path,
) -> Result<(PathBuf, u64)> {
    let target_dir = vintage_path(dump_data_dir, vintage);
    tokio::fs::create_dir_all(&target_dir)
        .await
        .with_context(|| format!("criando {}", target_dir.display()))?;

    let entries = source
        .list(vintage)
        .await
        .with_context(|| format!("listando vintage {vintage}"))?;

    let files: Vec<_> = entries.into_iter().filter(|e| !e.is_dir).collect();
    if files.is_empty() {
        return Err(anyhow!("vintage {vintage} sem arquivos"));
    }

    let mut total_bytes: u64 = 0;
    for entry in &files {
        let dest = target_dir.join(&entry.name);
        let needs_download = match tokio::fs::metadata(&dest).await {
            Ok(meta) => entry.size > 0 && meta.len() != entry.size,
            Err(_) => true,
        };

        if needs_download {
            tracing::info!(file = %entry.name, size = entry.size, "baixando");
            let url = source.file_url(vintage, &entry.name);
            let mut dl = download::fetch(source, &url).await?;
            let mut out = tokio::fs::File::create(&dest)
                .await
                .with_context(|| format!("criando {}", dest.display()))?;
            tokio::io::copy(&mut dl.reader, &mut out)
                .await
                .with_context(|| format!("escrevendo {}", dest.display()))?;
            out.flush().await?;
        } else {
            tracing::debug!(file = %entry.name, "já em disco, pulando");
        }

        let meta = tokio::fs::metadata(&dest).await?;
        total_bytes += meta.len();
    }

    if !has_minimum_files(&target_dir).await? {
        return Err(anyhow!(
            "vintage {vintage} incompleta após download (faltam zips esperados)"
        ));
    }

    build_indexes(&target_dir).await?;
    bucketize::bucketize_estabelecimentos(&target_dir).await?;
    build_bucket_indexes(&target_dir).await?;

    let final_bytes = recompute_dir_bytes(&target_dir).await.unwrap_or(total_bytes);
    Ok((target_dir, final_bytes))
}

/// Após bucketize, indexa cada `EstabBucket{i}.zip` igual aos Estab originais.
/// Usa o mesmo formato `.idx` consumido pelo scan.
async fn build_bucket_indexes(target_dir: &Path) -> Result<()> {
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();
    for i in 0..10usize {
        let zip_path = bucketize::bucket_zip_path(target_dir, i);
        if !tokio::fs::try_exists(&zip_path).await? {
            continue;
        }
        let zip_name = bucketize::bucket_zip_name(i);
        let idx_path = index::index_path(target_dir, &zip_name);
        if tokio::fs::try_exists(&idx_path).await? {
            continue;
        }
        tasks.spawn(async move {
            let started = std::time::Instant::now();
            let basicos = index::build_for_zip(&zip_path).await?;
            let count = basicos.len();
            index::write_index(&idx_path, &basicos).await?;
            tracing::info!(
                zip = %zip_name,
                cnpj_basicos = count,
                elapsed_secs = started.elapsed().as_secs(),
                "índice de bucket construído"
            );
            Ok(())
        });
    }
    while let Some(res) = tasks.join_next().await {
        res??;
    }
    Ok(())
}

async fn recompute_dir_bytes(dir: &Path) -> Result<u64> {
    let mut total = 0u64;
    let mut rd = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        if entry.file_type().await?.is_file() {
            total += entry.metadata().await?.len();
        }
    }
    Ok(total)
}

/// Para cada zip CSV, gera um índice sorted dos `cnpj_basico` que ele contém.
/// O scan usa o índice para pular zips sem matches (binary_search). Idempotente:
/// pula índices já presentes em disco.
async fn build_indexes(target_dir: &Path) -> Result<()> {
    let zip_names = indexable_zip_names(target_dir).await?;
    let mut tasks: JoinSet<Result<(String, usize)>> = JoinSet::new();

    for name in zip_names {
        let zip_path = target_dir.join(&name);
        let idx_path = index::index_path(target_dir, &name);
        if tokio::fs::try_exists(&idx_path).await? {
            tracing::debug!(zip = %name, "índice já existe, pulando");
            continue;
        }
        tasks.spawn(async move {
            let started = std::time::Instant::now();
            let basicos = index::build_for_zip(&zip_path).await?;
            let count = basicos.len();
            index::write_index(&idx_path, &basicos).await?;
            tracing::info!(
                zip = %name,
                cnpj_basicos = count,
                elapsed_secs = started.elapsed().as_secs(),
                "índice construído"
            );
            Ok((name, count))
        });
    }

    while let Some(res) = tasks.join_next().await {
        res??;
    }
    Ok(())
}

async fn indexable_zip_names(target_dir: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut rd = tokio::fs::read_dir(target_dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        let n = entry.file_name();
        let s = n.to_string_lossy().into_owned();
        if !s.ends_with(".zip") {
            continue;
        }
        // Lookups (Cnaes, Municipios, etc) não são keyed por cnpj_basico.
        if matches!(
            s.as_str(),
            "Cnaes.zip"
                | "Motivos.zip"
                | "Municipios.zip"
                | "Naturezas.zip"
                | "Paises.zip"
                | "Qualificacoes.zip"
        ) {
            continue;
        }
        names.push(s);
    }
    names.sort();
    Ok(names)
}

/// Verifica que o conjunto mínimo de arquivos esperados existe.
async fn has_minimum_files(dir: &Path) -> Result<bool> {
    let required = [
        "Empresas0.zip",
        "Estabelecimentos0.zip",
        "Socios0.zip",
        "Simples.zip",
    ];
    for name in required {
        if !tokio::fs::try_exists(dir.join(name)).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Apaga vintages locais mais antigas, mantendo apenas as `keep` mais recentes.
pub async fn cleanup_old(dump_data_dir: &Path, keep: usize) -> Result<Vec<String>> {
    let mut local = list_local(dump_data_dir).await?;
    local.sort();
    if local.len() <= keep {
        return Ok(Vec::new());
    }
    let to_delete: Vec<String> = local.drain(..local.len() - keep).collect();
    for v in &to_delete {
        let path = vintage_path(dump_data_dir, v);
        tracing::info!(vintage = %v, path = %path.display(), "apagando vintage antiga");
        tokio::fs::remove_dir_all(&path)
            .await
            .with_context(|| format!("removendo {}", path.display()))?;
    }
    Ok(to_delete)
}

/// Marca a vintage como pronta no Postgres.
pub async fn record(
    pool: &PgPool,
    vintage: &str,
    bytes: u64,
    caminho: &Path,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO vintages_baixadas (vintage, bytes, caminho) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (vintage) DO UPDATE SET \
           baixada_em = now(), bytes = EXCLUDED.bytes, caminho = EXCLUDED.caminho",
    )
    .bind(vintage)
    .bind(bytes as i64)
    .bind(caminho.to_string_lossy().as_ref())
    .execute(pool)
    .await?;
    Ok(())
}

/// Última vintage registrada no Postgres (qualquer baixada). Retorna
/// `(vintage, caminho)`.
pub async fn latest_recorded(pool: &PgPool) -> Result<Option<(String, PathBuf)>> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT vintage, caminho FROM vintages_baixadas ORDER BY vintage DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(v, c)| (v, PathBuf::from(c))))
}

fn is_vintage(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() == 7
        && bytes[4] == b'-'
        && bytes[..4].iter().all(|c| c.is_ascii_digit())
        && bytes[5..].iter().all(|c| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vintage_format_check() {
        assert!(is_vintage("2026-04"));
        assert!(!is_vintage("2026-4"));
        assert!(!is_vintage("not-a-vintage"));
    }
}
