//! Parsers tipados para os CSVs da Receita. Ponto único de truthing do layout
//! oficial; mudanças no schema da Receita devem cair aqui.

use csv_async::{AsyncReaderBuilder, Trim};
use futures::StreamExt;
use serde::Deserialize;
use tokio::io::AsyncRead;

/// Linha das tabelas de lookup (Cnaes, Motivos, Municipios, Naturezas, Paises, Qualificacoes).
/// Todas têm o mesmo formato: `codigo;descricao` sem header.
#[derive(Debug, Deserialize)]
pub struct LookupRow {
    pub codigo: String,
    pub descricao: String,
}

pub async fn read_lookup_rows<R: AsyncRead + Unpin + Send + 'static>(
    input: R,
) -> anyhow::Result<Vec<LookupRow>> {
    let mut rdr = AsyncReaderBuilder::new()
        .delimiter(b';')
        .has_headers(false)
        .quoting(true)
        .quote(b'"')
        .trim(Trim::Fields)
        .flexible(true)
        .create_deserializer(input);

    let mut rows = rdr.deserialize::<LookupRow>();
    let mut out = Vec::new();
    while let Some(row) = rows.next().await {
        out.push(row?);
    }
    Ok(out)
}
