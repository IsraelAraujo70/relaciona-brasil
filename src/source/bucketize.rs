//! Reshard de Estabelecimentos por range de `cnpj_basico`.
//!
//! Os zips originais `Estabelecimentos{0..9}.zip` da Receita são particionados
//! por hash do CNPJ completo, então os ~7900 estabelecimentos de uma empresa
//! grande tipo o BB ficam espalhados em todos os 10 zips. Como o scan da
//! query precisa achar TODAS as filiais de uma empresa, ele acaba lendo os
//! 5.2 GB inteiros, mesmo com índice por zip.
//!
//! Este módulo, rodado uma vez após o download, lê os 10 Estabs originais em
//! sequência e reescreve as rows em 10 buckets novos `EstabBucket{0..9}.zip`,
//! cada bucket cobrindo uma faixa contígua de `cnpj_basico`. A faixa é
//! herdada dos índices Empresas que já foram construídos antes.
//!
//! Em runtime, o scan resolve `cnpj_basico → bucket` com lookup linear nos 10
//! ranges (trivial) e abre apenas o bucket relevante.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use async_zip::base::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use futures::AsyncWriteExt as _;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use super::decode;
use super::index;
use super::unzip;

const NUM_BUCKETS: usize = 10;
const SOURCE_ZIPS: [&str; 10] = [
    "Estabelecimentos0.zip",
    "Estabelecimentos1.zip",
    "Estabelecimentos2.zip",
    "Estabelecimentos3.zip",
    "Estabelecimentos4.zip",
    "Estabelecimentos5.zip",
    "Estabelecimentos6.zip",
    "Estabelecimentos7.zip",
    "Estabelecimentos8.zip",
    "Estabelecimentos9.zip",
];

pub fn bucket_zip_name(idx: usize) -> String {
    format!("EstabBucket{idx}.zip")
}

pub fn bucket_zip_path(vintage_dir: &Path, idx: usize) -> PathBuf {
    vintage_dir.join(bucket_zip_name(idx))
}

/// Carrega os ranges de cnpj_basico a partir dos índices `Empresas{i}.zip.idx`,
/// ordenados ascendente por `min`. Cada range é `(min, max)` inclusive.
pub async fn load_ranges(vintage_dir: &Path) -> Result<Vec<(u32, u32)>> {
    let mut ranges = Vec::with_capacity(NUM_BUCKETS);
    for i in 0..NUM_BUCKETS {
        let idx_path = index::index_path(vintage_dir, &format!("Empresas{i}.zip"));
        let idx = index::load_index(&idx_path)
            .await
            .with_context(|| format!("carregando {}", idx_path.display()))?;
        if let (Some(&first), Some(&last)) = (idx.first(), idx.last()) {
            ranges.push((first, last));
        } else {
            return Err(anyhow!("Empresas{i}.zip.idx vazio — vintage incompleta"));
        }
    }
    ranges.sort_by_key(|(min, _)| *min);
    Ok(ranges)
}

/// Determina o bucket para um cnpj_basico, com lookup linear nos 10 ranges.
/// Em desempate (raro com gaps entre ranges) escolhe o primeiro que contém.
pub fn bucket_for(basico: u32, ranges: &[(u32, u32)]) -> Option<usize> {
    for (i, (min, max)) in ranges.iter().enumerate() {
        if basico >= *min && basico <= *max {
            return Some(i);
        }
    }
    // Sem match exato: cai no bucket cujo `max` é o maior abaixo do target,
    // ou no primeiro se target está antes de tudo. Defesa pra cnpj_basicos
    // entre gaps; o destinatário não precisa estar correto, só ser estável.
    let mut best = 0usize;
    let mut best_diff = u32::MAX;
    for (i, (min, max)) in ranges.iter().enumerate() {
        let diff = if basico < *min {
            *min - basico
        } else {
            basico.saturating_sub(*max)
        };
        if diff < best_diff {
            best_diff = diff;
            best = i;
        }
    }
    Some(best)
}

/// Re-particiona os 10 zips de Estabelecimentos por range de cnpj_basico.
/// Idempotente: pula se todos os buckets já existem com tamanho > 0.
pub async fn bucketize_estabelecimentos(vintage_dir: &Path) -> Result<()> {
    if all_buckets_present(vintage_dir).await? {
        tracing::info!("EstabBuckets já presentes, pulando bucketize");
        return Ok(());
    }

    let ranges = load_ranges(vintage_dir).await?;
    tracing::info!(?ranges, "ranges carregados pra bucketize");

    let started = std::time::Instant::now();

    let mut senders: Vec<mpsc::Sender<Vec<u8>>> = Vec::with_capacity(NUM_BUCKETS);
    let mut writer_set: JoinSet<Result<(usize, u64)>> = JoinSet::new();

    for i in 0..NUM_BUCKETS {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(8192);
        senders.push(tx);
        let target = bucket_zip_path(vintage_dir, i);
        writer_set.spawn(async move { bucket_writer(i, target, rx).await });
    }

    let mut total_lines: u64 = 0;
    let mut routed: u64 = 0;

    for src_name in SOURCE_ZIPS {
        let src_path = vintage_dir.join(src_name);
        if !tokio::fs::try_exists(&src_path).await? {
            tracing::warn!(file = src_name, "ausente, pulando");
            continue;
        }
        let zip_started = std::time::Instant::now();
        let (lines, sent) = stream_route(&src_path, &ranges, &senders).await?;
        total_lines += lines;
        routed += sent;
        tracing::info!(
            file = src_name,
            lines,
            routed = sent,
            elapsed_secs = zip_started.elapsed().as_secs(),
            "bucketize zip processado"
        );
    }

    drop(senders);

    let mut bytes_per_bucket = vec![0u64; NUM_BUCKETS];
    while let Some(joined) = writer_set.join_next().await {
        let (i, bytes) = joined??;
        bytes_per_bucket[i] = bytes;
    }

    tracing::info!(
        elapsed_secs = started.elapsed().as_secs(),
        total_lines,
        routed,
        bytes_per_bucket = ?bytes_per_bucket,
        "bucketize completo"
    );

    Ok(())
}

async fn all_buckets_present(vintage_dir: &Path) -> Result<bool> {
    for i in 0..NUM_BUCKETS {
        let path = bucket_zip_path(vintage_dir, i);
        match tokio::fs::metadata(&path).await {
            Ok(meta) if meta.len() > 0 => continue,
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// Lê 1 zip de Estabelecimentos linha-a-linha, parseia o cnpj_basico (1ª col)
/// e empurra a linha pro `Sender` do bucket correto. Retorna `(lidas, roteadas)`.
async fn stream_route(
    src_path: &Path,
    ranges: &[(u32, u32)],
    senders: &[mpsc::Sender<Vec<u8>>],
) -> Result<(u64, u64)> {
    let file = tokio::fs::File::open(src_path)
        .await
        .with_context(|| format!("abrindo {}", src_path.display()))?;
    let buffered = BufReader::with_capacity(256 * 1024, file);

    let (unzipped, unzip_task) = unzip::decompress_first_entry(buffered);
    let (decoded, decode_task) = decode::latin1_to_utf8(unzipped);

    let mut reader = BufReader::with_capacity(256 * 1024, decoded);
    let mut line = String::new();
    let mut lines: u64 = 0;
    let mut routed: u64 = 0;

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        lines += 1;

        let basico_opt = index::parse_first_column(&line)
            .and_then(|s| s.parse::<u32>().ok());
        let Some(basico) = basico_opt else { continue };
        let Some(bucket) = bucket_for(basico, ranges) else { continue };

        let bytes = line.as_bytes().to_vec();
        if senders[bucket].send(bytes).await.is_err() {
            return Err(anyhow!(
                "writer do bucket {bucket} morreu antes do reader terminar"
            ));
        }
        routed += 1;
    }

    let _ = unzip_task.await;
    let _ = decode_task.await;
    Ok((lines, routed))
}

/// Writer dum bucket: cria `EstabBucket{i}.zip` com 1 entry CSV, recebe
/// linhas no canal e escreve serialmente. Retorna bytes escritos.
async fn bucket_writer(
    idx: usize,
    target: PathBuf,
    mut rx: mpsc::Receiver<Vec<u8>>,
) -> Result<(usize, u64)> {
    let file = tokio::fs::File::create(&target)
        .await
        .with_context(|| format!("criando {}", target.display()))?;
    let compat = file.compat_write();
    let mut zw = ZipFileWriter::new(compat);

    let entry_name = format!("EstabBucket{idx}.csv");
    let entry = ZipEntryBuilder::new(entry_name.into(), Compression::Deflate);
    let mut entry_writer = zw
        .write_entry_stream(entry)
        .await
        .map_err(|e| anyhow!("abrindo entry no bucket {idx}: {e:?}"))?;

    let mut bytes_written: u64 = 0;
    while let Some(line) = rx.recv().await {
        entry_writer
            .write_all(&line)
            .await
            .with_context(|| format!("escrevendo bucket {idx}"))?;
        bytes_written += line.len() as u64;
    }

    entry_writer
        .close()
        .await
        .map_err(|e| anyhow!("fechando entry bucket {idx}: {e:?}"))?;
    zw.close()
        .await
        .map_err(|e| anyhow!("fechando zip bucket {idx}: {e:?}"))?;

    Ok((idx, bytes_written))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_for_ranges_in_range() {
        let ranges = vec![(0, 100), (200, 300), (400, 500)];
        assert_eq!(bucket_for(50, &ranges), Some(0));
        assert_eq!(bucket_for(250, &ranges), Some(1));
        assert_eq!(bucket_for(450, &ranges), Some(2));
    }

    #[test]
    fn bucket_for_falls_back_to_nearest() {
        let ranges = vec![(0, 100), (200, 300), (400, 500)];
        // 600 fora à direita — pega 2 (distância 100).
        assert_eq!(bucket_for(600, &ranges), Some(2));
        // 110 está logo após bucket 0 — bucket 0 é o mais próximo (distância 10).
        assert_eq!(bucket_for(110, &ranges), Some(0));
        // 190 está logo antes de bucket 1 — bucket 1 é o mais próximo (distância 10).
        assert_eq!(bucket_for(190, &ranges), Some(1));
    }
}
