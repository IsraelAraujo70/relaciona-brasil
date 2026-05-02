//! Varre os zips de uma vintage local em paralelo, extraindo apenas linhas
//! cujo `cnpj_basico` está no conjunto-alvo. O filtro reduz ~60M linhas para
//! dezenas, então o resultado cabe em memória.
//!
//! Cada zip vira uma task: zip stream → latin-1→utf-8 → CSV → filter → send.
//! Tarefas escrevem num `mpsc::Sender<Match>`; a task principal drena e
//! agrega em `ScanResult`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;
use tokio::io::BufReader;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use super::decode;
use super::index;
use super::parse::{
    self, EmpresaRow, EstabelecimentoRow, SimplesRow, SocioRow,
};
use super::unzip;

#[derive(Debug, Default)]
pub struct ScanResult {
    pub vintage: String,
    pub empresas: Vec<EmpresaRow>,
    pub estabelecimentos: Vec<EstabelecimentoRow>,
    pub socios: Vec<SocioRow>,
    pub simples: Vec<SimplesRow>,
}

#[derive(Debug)]
enum Match {
    Empresa(EmpresaRow),
    Estabelecimento(Box<EstabelecimentoRow>),
    Socio(SocioRow),
    Simples(SimplesRow),
}

/// Varre uma vintage local procurando linhas cujo `cnpj_basico` está em `target`.
/// Spawnou uma task por arquivo (até 31 zips por vintage); o `JoinSet` paraleliza.
pub async fn scan_target_set(
    vintage_dir: &Path,
    vintage: &str,
    target: &HashSet<String>,
) -> Result<ScanResult> {
    if target.is_empty() {
        return Ok(ScanResult {
            vintage: vintage.to_string(),
            ..Default::default()
        });
    }

    let target_arc = Arc::new(target.clone());
    let target_u32: Vec<u32> = target.iter().filter_map(|s| s.parse().ok()).collect();
    let target_u32 = Arc::new(target_u32);

    let (tx, mut rx) = mpsc::channel::<Match>(4096);

    let mut tasks: JoinSet<Result<()>> = JoinSet::new();

    for i in 0..10u8 {
        let name = format!("Empresas{i}.zip");
        spawn_if_present(
            &mut tasks,
            vintage_dir,
            &name,
            ZipKind::Empresa,
            target_arc.clone(),
            target_u32.clone(),
            tx.clone(),
        )
        .await?;
    }
    for i in 0..10u8 {
        let name = format!("Estabelecimentos{i}.zip");
        spawn_if_present(
            &mut tasks,
            vintage_dir,
            &name,
            ZipKind::Estabelecimento,
            target_arc.clone(),
            target_u32.clone(),
            tx.clone(),
        )
        .await?;
    }
    for i in 0..10u8 {
        let name = format!("Socios{i}.zip");
        spawn_if_present(
            &mut tasks,
            vintage_dir,
            &name,
            ZipKind::Socio,
            target_arc.clone(),
            target_u32.clone(),
            tx.clone(),
        )
        .await?;
    }
    spawn_if_present(
        &mut tasks,
        vintage_dir,
        "Simples.zip",
        ZipKind::Simples,
        target_arc.clone(),
        target_u32.clone(),
        tx.clone(),
    )
    .await?;

    drop(tx);

    let mut result = ScanResult {
        vintage: vintage.to_string(),
        ..Default::default()
    };
    while let Some(m) = rx.recv().await {
        match m {
            Match::Empresa(r) => result.empresas.push(r),
            Match::Estabelecimento(r) => result.estabelecimentos.push(*r),
            Match::Socio(r) => result.socios.push(r),
            Match::Simples(r) => result.simples.push(r),
        }
    }

    while let Some(joined) = tasks.join_next().await {
        joined??;
    }

    Ok(result)
}

#[derive(Copy, Clone)]
enum ZipKind {
    Empresa,
    Estabelecimento,
    Socio,
    Simples,
}

async fn spawn_if_present(
    tasks: &mut JoinSet<Result<()>>,
    vintage_dir: &Path,
    zip_name: &str,
    kind: ZipKind,
    target: Arc<HashSet<String>>,
    target_u32: Arc<Vec<u32>>,
    tx: mpsc::Sender<Match>,
) -> Result<()> {
    let path = vintage_dir.join(zip_name);
    if !tokio::fs::try_exists(&path).await? {
        return Ok(());
    }

    // Se houver índice, checa se algum target pode estar no zip antes de gastar
    // IO descomprimindo. Sem índice (vintage antiga, downloader não rodou), cai
    // no fallback de varrer tudo.
    let idx_path = index::index_path(vintage_dir, zip_name);
    if tokio::fs::try_exists(&idx_path).await? {
        match index::load_index(&idx_path).await {
            Ok(idx) => {
                if !index::intersects(&idx, &target_u32) {
                    tracing::debug!(zip = %zip_name, "skip via índice");
                    return Ok(());
                }
            }
            Err(err) => {
                tracing::warn!(zip = %zip_name, error = ?err, "índice ilegível, varrendo zip inteiro");
            }
        }
    }

    tasks.spawn(async move { scan_one_zip(path, kind, target, tx).await });
    Ok(())
}

async fn scan_one_zip(
    path: PathBuf,
    kind: ZipKind,
    target: Arc<HashSet<String>>,
    tx: mpsc::Sender<Match>,
) -> Result<()> {
    let file = tokio::fs::File::open(&path)
        .await
        .with_context(|| format!("abrindo {}", path.display()))?;
    let buffered = BufReader::with_capacity(256 * 1024, file);

    let (unzipped, unzip_task) = unzip::decompress_first_entry(buffered);
    let (decoded, decode_task) = decode::latin1_to_utf8(unzipped);

    let res = match kind {
        ZipKind::Empresa => scan_empresa(decoded, &target, &tx).await,
        ZipKind::Estabelecimento => scan_estabelecimento(decoded, &target, &tx).await,
        ZipKind::Socio => scan_socio(decoded, &target, &tx).await,
        ZipKind::Simples => scan_simples(decoded, &target, &tx).await,
    };

    // Drena tasks de pipeline mesmo em erro pra evitar leaks.
    let _ = unzip_task.await;
    let _ = decode_task.await;
    res.with_context(|| format!("scanning {}", path.display()))
}

async fn scan_empresa<R>(
    input: R,
    target: &HashSet<String>,
    tx: &mpsc::Sender<Match>,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut rdr = parse::make_deserializer(input);
    let mut rows = rdr.deserialize::<EmpresaRow>();
    while let Some(row) = rows.next().await {
        let row = row?;
        if target.contains(&row.cnpj_basico)
            && tx.send(Match::Empresa(row)).await.is_err()
        {
            break;
        }
    }
    Ok(())
}

async fn scan_estabelecimento<R>(
    input: R,
    target: &HashSet<String>,
    tx: &mpsc::Sender<Match>,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut rdr = parse::make_deserializer(input);
    let mut rows = rdr.deserialize::<EstabelecimentoRow>();
    while let Some(row) = rows.next().await {
        let row = row?;
        if target.contains(&row.cnpj_basico)
            && tx.send(Match::Estabelecimento(Box::new(row))).await.is_err()
        {
            break;
        }
    }
    Ok(())
}

async fn scan_socio<R>(
    input: R,
    target: &HashSet<String>,
    tx: &mpsc::Sender<Match>,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut rdr = parse::make_deserializer(input);
    let mut rows = rdr.deserialize::<SocioRow>();
    while let Some(row) = rows.next().await {
        let row = row?;
        // Em socios, queremos linhas onde a empresa-pai está no target
        // (vínculos da empresa) OU onde o sócio é uma PJ no target
        // (empresas onde a PJ-alvo participa).
        let pj_socio = row.cnpj_cpf_socio.len() == 14
            && target.contains(&row.cnpj_cpf_socio[..8]);
        if (target.contains(&row.cnpj_basico) || pj_socio)
            && tx.send(Match::Socio(row)).await.is_err()
        {
            break;
        }
    }
    Ok(())
}

async fn scan_simples<R>(
    input: R,
    target: &HashSet<String>,
    tx: &mpsc::Sender<Match>,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut rdr = parse::make_deserializer(input);
    let mut rows = rdr.deserialize::<SimplesRow>();
    while let Some(row) = rows.next().await {
        let row = row?;
        if target.contains(&row.cnpj_basico)
            && tx.send(Match::Simples(row)).await.is_err()
        {
            break;
        }
    }
    Ok(())
}

/// Extrai cnpj_basico para o próximo hop de profundidade. Cobre dois casos:
///
/// 1. PJ-sócios das empresas no target atual: `socio.cnpj_cpf_socio` é um CNPJ
///    completo de 14 dígitos — extraímos o cnpj_basico (8 chars) para visitar
///    a empresa-sócia.
/// 2. Empresas onde o target atual figura como sócia: `socio.cnpj_basico` está
///    fora do target (caso emitido por `scan_socio` quando a PJ-sócia da row
///    está no target). Essas rows não são persistidas (FK quebraria), mas
///    seu cnpj_basico vira alvo do próximo hop.
pub fn next_target_layer(result: &ScanResult, current: &HashSet<String>) -> HashSet<String> {
    let mut next = HashSet::new();
    for socio in &result.socios {
        if socio.cnpj_cpf_socio.len() == 14 {
            let basico = &socio.cnpj_cpf_socio[..8];
            if !current.contains(basico) {
                next.insert(basico.to_string());
            }
        }
        if !current.contains(&socio.cnpj_basico) {
            next.insert(socio.cnpj_basico.clone());
        }
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_layer_extracts_pj_socios() {
        let mut result = ScanResult::default();
        result.socios.push(SocioRow {
            cnpj_basico: "11111111".into(),
            identificador: "1".into(),
            nome_socio: "ACME LTDA".into(),
            cnpj_cpf_socio: "22222222000199".into(),
            qualificacao: "22".into(),
            data_entrada: "20200101".into(),
            pais: "".into(),
            cpf_repr_legal: "".into(),
            nome_repr_legal: "".into(),
            qualif_repr_legal: "".into(),
            faixa_etaria: "".into(),
        });
        // PF: cpf_cpf_socio mascarado, len < 14, ignorado
        result.socios.push(SocioRow {
            cnpj_basico: "11111111".into(),
            identificador: "2".into(),
            nome_socio: "FULANO".into(),
            cnpj_cpf_socio: "***123456**".into(),
            qualificacao: "22".into(),
            data_entrada: "20200101".into(),
            pais: "".into(),
            cpf_repr_legal: "".into(),
            nome_repr_legal: "".into(),
            qualif_repr_legal: "".into(),
            faixa_etaria: "".into(),
        });

        let mut current = HashSet::new();
        current.insert("11111111".to_string());
        let next = next_target_layer(&result, &current);
        assert_eq!(next.len(), 1);
        assert!(next.contains("22222222"));
    }

    #[test]
    fn next_layer_extracts_companies_where_target_is_partner() {
        // Caso 3: row de socio onde a PJ-sócia está no target. cnpj_basico
        // dessa row é uma OUTRA empresa, que deve virar alvo do próximo hop.
        let mut result = ScanResult::default();
        result.socios.push(SocioRow {
            cnpj_basico: "33333333".into(),
            identificador: "1".into(),
            nome_socio: "BANCO X".into(),
            cnpj_cpf_socio: "11111111000122".into(),
            qualificacao: "22".into(),
            data_entrada: "20200101".into(),
            pais: "".into(),
            cpf_repr_legal: "".into(),
            nome_repr_legal: "".into(),
            qualif_repr_legal: "".into(),
            faixa_etaria: "".into(),
        });

        let mut current = HashSet::new();
        current.insert("11111111".to_string());
        let next = next_target_layer(&result, &current);
        assert!(next.contains("33333333"));
    }
}
