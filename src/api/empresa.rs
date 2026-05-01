use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::NaiveDate;
use serde::Serialize;
use serde_json::json;

use super::AppState;
use crate::domain::types::Cnpj;
use crate::error::AppError;

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/empresas/{cnpj}", get(get_empresa))
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct EmpresaRow {
    cnpj_basico: String,
    razao_social: Option<String>,
    capital_social: Option<String>,
    porte: Option<String>,
    natureza_juridica: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct EstabRow {
    cnpj: String,
    matriz_filial: Option<String>,
    nome_fantasia: Option<String>,
    situacao: Option<String>,
    data_inicio: Option<NaiveDate>,
    cnae_principal: Option<String>,
    uf: Option<String>,
    municipio: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct SocioRow {
    identificador: i16,
    nome_socio: Option<String>,
    cnpj_cpf_socio: Option<String>,
    qualificacao: Option<i16>,
    data_entrada: Option<NaiveDate>,
}

async fn get_empresa(
    State(state): State<AppState>,
    Path(cnpj): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let cnpj = Cnpj::parse(&cnpj).map_err(|e| AppError::BadRequest(e.into()))?;

    let empresa: Option<EmpresaRow> = sqlx::query_as(
        "SELECT cnpj_basico, razao_social, capital_social::text AS capital_social, \
                porte, natureza_juridica \
         FROM empresa WHERE cnpj_basico = $1",
    )
    .bind(cnpj.basico())
    .fetch_optional(&state.pool)
    .await?;

    let Some(empresa) = empresa else {
        return Err(AppError::NotFound);
    };

    let estabs: Vec<EstabRow> = sqlx::query_as(
        "SELECT cnpj, matriz_filial, nome_fantasia, situacao, data_inicio, \
                cnae_principal, uf, municipio \
         FROM estabelecimento WHERE cnpj_basico = $1",
    )
    .bind(cnpj.basico())
    .fetch_all(&state.pool)
    .await?;

    let socios: Vec<SocioRow> = sqlx::query_as(
        "SELECT identificador, nome_socio, cnpj_cpf_socio, qualificacao, data_entrada \
         FROM socio WHERE cnpj_basico = $1",
    )
    .bind(cnpj.basico())
    .fetch_all(&state.pool)
    .await?;

    Ok((
        StatusCode::OK,
        Json(json!({
            "empresa": empresa,
            "estabelecimentos": estabs,
            "socios": socios,
        })),
    ))
}
