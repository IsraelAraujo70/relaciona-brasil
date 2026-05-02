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

/// Layout oficial Empresas — 7 colunas.
#[derive(Debug, Deserialize, Clone)]
pub struct EmpresaRow {
    pub cnpj_basico: String,
    pub razao_social: String,
    pub natureza_juridica: String,
    pub qualificacao_resp: String,
    pub capital_social: String,
    pub porte: String,
    pub ente_federativo: String,
}

/// Layout oficial Estabelecimentos — 30 colunas.
#[derive(Debug, Deserialize, Clone)]
pub struct EstabelecimentoRow {
    pub cnpj_basico: String,
    pub cnpj_ordem: String,
    pub cnpj_dv: String,
    pub matriz_filial: String,
    pub nome_fantasia: String,
    pub situacao: String,
    pub data_situacao: String,
    pub motivo_situacao: String,
    pub nome_cidade_ext: String,
    pub pais: String,
    pub data_inicio: String,
    pub cnae_principal: String,
    pub cnaes_secundarios: String,
    pub tipo_logradouro: String,
    pub logradouro: String,
    pub numero: String,
    pub complemento: String,
    pub bairro: String,
    pub cep: String,
    pub uf: String,
    pub municipio: String,
    pub ddd_1: String,
    pub telefone_1: String,
    pub ddd_2: String,
    pub telefone_2: String,
    pub ddd_fax: String,
    pub fax: String,
    pub email: String,
    pub situacao_especial: String,
    pub data_sit_especial: String,
}

/// Layout oficial Sócios — 11 colunas.
#[derive(Debug, Deserialize, Clone)]
pub struct SocioRow {
    pub cnpj_basico: String,
    pub identificador: String,
    pub nome_socio: String,
    pub cnpj_cpf_socio: String,
    pub qualificacao: String,
    pub data_entrada: String,
    pub pais: String,
    pub cpf_repr_legal: String,
    pub nome_repr_legal: String,
    pub qualif_repr_legal: String,
    pub faixa_etaria: String,
}

/// Layout oficial Simples — 7 colunas.
#[derive(Debug, Deserialize, Clone)]
pub struct SimplesRow {
    pub cnpj_basico: String,
    pub opcao_simples: String,
    pub data_opcao_simples: String,
    pub data_exclusao: String,
    pub opcao_mei: String,
    pub data_opcao_mei: String,
    pub data_exclusao_mei: String,
}

/// Cria um CSV deserializer-padrão (separador `;`, sem header, quotes `"`).
pub fn make_deserializer<R: AsyncRead + Unpin + Send + 'static>(
    input: R,
) -> csv_async::AsyncDeserializer<R> {
    AsyncReaderBuilder::new()
        .delimiter(b';')
        .has_headers(false)
        .quoting(true)
        .quote(b'"')
        .trim(Trim::Fields)
        .flexible(true)
        .create_deserializer(input)
}
