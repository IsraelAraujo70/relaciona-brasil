//! Parsers tipados para os CSVs da Receita. Ponto único de truthing do layout
//! oficial; mudanças no schema da Receita devem cair aqui.

use csv_async::{AsyncReaderBuilder, Trim};
use futures::StreamExt;
use serde::Deserialize;
use tokio::io::AsyncRead;

use super::pgcopy;

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
#[derive(Debug, Deserialize)]
pub struct EmpresaRow {
    pub cnpj_basico: String,
    pub razao_social: String,
    pub natureza_juridica: String,
    pub qualificacao_resp: String,
    pub capital_social: String,
    pub porte: String,
    pub ente_federativo: String,
}

impl EmpresaRow {
    pub fn write_copy_line(&self, buf: &mut Vec<u8>) {
        pgcopy::write_text(buf, &self.cnpj_basico);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.razao_social);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.natureza_juridica);
        pgcopy::write_tab(buf);
        pgcopy::write_int_or_null(buf, &self.qualificacao_resp);
        pgcopy::write_tab(buf);
        pgcopy::write_decimal_or_null(buf, &self.capital_social);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.porte);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.ente_federativo);
        pgcopy::write_newline(buf);
    }
}

/// Layout oficial Estabelecimentos — 30 colunas.
#[derive(Debug, Deserialize)]
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

impl EstabelecimentoRow {
    pub fn write_copy_line(&self, buf: &mut Vec<u8>) {
        // Ordem das colunas no COPY — sem o `cnpj` (gerado).
        pgcopy::write_text(buf, &self.cnpj_basico);
        pgcopy::write_tab(buf);
        pgcopy::write_text(buf, &self.cnpj_ordem);
        pgcopy::write_tab(buf);
        pgcopy::write_text(buf, &self.cnpj_dv);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.matriz_filial);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.nome_fantasia);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.situacao);
        pgcopy::write_tab(buf);
        pgcopy::write_date_or_null(buf, &self.data_situacao);
        pgcopy::write_tab(buf);
        pgcopy::write_int_or_null(buf, &self.motivo_situacao);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.nome_cidade_ext);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.pais);
        pgcopy::write_tab(buf);
        pgcopy::write_date_or_null(buf, &self.data_inicio);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.cnae_principal);
        pgcopy::write_tab(buf);
        pgcopy::write_text_array_or_null(buf, &self.cnaes_secundarios);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.tipo_logradouro);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.logradouro);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.numero);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.complemento);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.bairro);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.cep);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.uf);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.municipio);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.ddd_1);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.telefone_1);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.ddd_2);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.telefone_2);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.ddd_fax);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.fax);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.email);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.situacao_especial);
        pgcopy::write_tab(buf);
        pgcopy::write_date_or_null(buf, &self.data_sit_especial);
        pgcopy::write_newline(buf);
    }
}

/// Layout oficial Sócios — 11 colunas.
#[derive(Debug, Deserialize)]
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

impl SocioRow {
    pub fn write_copy_line(&self, buf: &mut Vec<u8>) {
        pgcopy::write_text(buf, &self.cnpj_basico);
        pgcopy::write_tab(buf);
        pgcopy::write_int_or_null(buf, &self.identificador);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.nome_socio);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.cnpj_cpf_socio);
        pgcopy::write_tab(buf);
        pgcopy::write_int_or_null(buf, &self.qualificacao);
        pgcopy::write_tab(buf);
        pgcopy::write_date_or_null(buf, &self.data_entrada);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.pais);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.cpf_repr_legal);
        pgcopy::write_tab(buf);
        pgcopy::write_text_or_null(buf, &self.nome_repr_legal);
        pgcopy::write_tab(buf);
        pgcopy::write_int_or_null(buf, &self.qualif_repr_legal);
        pgcopy::write_tab(buf);
        pgcopy::write_int_or_null(buf, &self.faixa_etaria);
        pgcopy::write_newline(buf);
    }
}

/// Layout oficial Simples — 7 colunas.
#[derive(Debug, Deserialize)]
pub struct SimplesRow {
    pub cnpj_basico: String,
    pub opcao_simples: String,
    pub data_opcao_simples: String,
    pub data_exclusao: String,
    pub opcao_mei: String,
    pub data_opcao_mei: String,
    pub data_exclusao_mei: String,
}

impl SimplesRow {
    pub fn write_copy_line(&self, buf: &mut Vec<u8>) {
        pgcopy::write_text(buf, &self.cnpj_basico);
        pgcopy::write_tab(buf);
        pgcopy::write_bool_sn_or_null(buf, &self.opcao_simples);
        pgcopy::write_tab(buf);
        pgcopy::write_date_or_null(buf, &self.data_opcao_simples);
        pgcopy::write_tab(buf);
        pgcopy::write_date_or_null(buf, &self.data_exclusao);
        pgcopy::write_tab(buf);
        pgcopy::write_bool_sn_or_null(buf, &self.opcao_mei);
        pgcopy::write_tab(buf);
        pgcopy::write_date_or_null(buf, &self.data_opcao_mei);
        pgcopy::write_tab(buf);
        pgcopy::write_date_or_null(buf, &self.data_exclusao_mei);
        pgcopy::write_newline(buf);
    }
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
