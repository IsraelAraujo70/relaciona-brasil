//! Índice por zip que mapeia quais `cnpj_basico` ele contém. Usado pelo scan
//! para pular zips sem matches, evitando ler ~5 GB de Estabelecimentos quando
//! a query é por uma empresa que só aparece em 1 deles.
//!
//! Layout em disco: `<vintage>/index/<zipname>.idx` com cabeçalho de 4 bytes
//! (count: u32 LE) seguido de `count` u32 little-endian sorted ascendentemente.
//! Cada u32 é um `cnpj_basico` parseado como inteiro (range 0..=99_999_999).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use super::decode;
use super::unzip;

const HEADER_BYTES: usize = 4;

pub fn index_dir(vintage_dir: &Path) -> PathBuf {
    vintage_dir.join("index")
}

pub fn index_path(vintage_dir: &Path, zip_name: &str) -> PathBuf {
    index_dir(vintage_dir).join(format!("{zip_name}.idx"))
}

/// Constrói o índice de um zip varrendo só a primeira coluna de cada linha
/// (formato: `"00000000";"…";…`). `cnpj_basico` é sempre 8 dígitos numéricos.
pub async fn build_for_zip(zip_path: &Path) -> Result<Vec<u32>> {
    let file = File::open(zip_path)
        .await
        .with_context(|| format!("abrindo {}", zip_path.display()))?;
    let buffered = BufReader::with_capacity(256 * 1024, file);

    let (unzipped, unzip_task) = unzip::decompress_first_entry(buffered);
    let (decoded, decode_task) = decode::latin1_to_utf8(unzipped);

    let mut reader = BufReader::with_capacity(256 * 1024, decoded);
    let mut line = String::new();
    let mut set: BTreeSet<u32> = BTreeSet::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        if let Some(basico) = parse_first_column(&line) {
            if let Ok(v) = basico.parse::<u32>() {
                set.insert(v);
            }
        }
    }

    // Drena tasks de pipeline mesmo em erro pra evitar leaks.
    let _ = unzip_task.await;
    let _ = decode_task.await;

    Ok(set.into_iter().collect())
}

fn parse_first_column(line: &str) -> Option<&str> {
    let trimmed = line.trim_start_matches('"');
    let end = trimmed.find('"')?;
    Some(&trimmed[..end])
}

/// Persiste o índice como `[count: u32 LE][cnpj_basico: u32 LE x count]`.
pub async fn write_index(path: &Path, sorted: &[u32]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut f = File::create(path)
        .await
        .with_context(|| format!("criando {}", path.display()))?;

    let count = sorted.len() as u32;
    f.write_all(&count.to_le_bytes()).await?;

    let mut buf = Vec::with_capacity(sorted.len() * 4);
    for v in sorted {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    f.write_all(&buf).await?;
    f.flush().await?;
    Ok(())
}

pub async fn load_index(path: &Path) -> Result<Vec<u32>> {
    let mut f = File::open(path)
        .await
        .with_context(|| format!("abrindo {}", path.display()))?;
    let mut header = [0u8; HEADER_BYTES];
    f.read_exact(&mut header).await?;
    let count = u32::from_le_bytes(header) as usize;

    let mut buf = vec![0u8; count * 4];
    f.read_exact(&mut buf).await?;

    let mut out = Vec::with_capacity(count);
    for chunk in buf.chunks_exact(4) {
        out.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok(out)
}

/// Verdadeiro se algum `target` aparece no índice. Como `index` é sorted,
/// usa `binary_search` por target — eficiente até com índices de ~10M.
pub fn intersects(index: &[u32], targets: &[u32]) -> bool {
    targets.iter().any(|t| index.binary_search(t).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_first_col() {
        assert_eq!(
            parse_first_column("\"00000000\";\"BANCO DO BRASIL SA\";"),
            Some("00000000")
        );
        assert_eq!(parse_first_column(""), None);
        assert_eq!(parse_first_column("\""), None);
    }

    #[test]
    fn intersect_logic() {
        let idx: Vec<u32> = vec![10, 20, 30, 40];
        assert!(intersects(&idx, &[20]));
        assert!(intersects(&idx, &[15, 25, 30]));
        assert!(!intersects(&idx, &[5, 25, 35]));
        assert!(!intersects(&idx, &[]));
    }

    #[tokio::test]
    async fn roundtrip() {
        let dir = tempdir_path();
        let path = dir.join("test.idx");
        let data = vec![1u32, 5, 100_000, 99_999_999];
        write_index(&path, &data).await.unwrap();
        let loaded = load_index(&path).await.unwrap();
        assert_eq!(loaded, data);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn tempdir_path() -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("relaciona-test-{pid}-{nanos}"));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
