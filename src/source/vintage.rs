//! Gerencia o cache local de vintages: baixa o conjunto completo de zips de uma
//! vintage para `<dump_data_dir>/<YYYY-MM>/`, lista o que está pronto e expira
//! vintages antigas.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use sqlx::PgPool;
use tokio::io::AsyncWriteExt;

use super::download;
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

    Ok((target_dir, total_bytes))
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
