use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::NaiveDate;
use serde::Serialize;
use utoipa::ToSchema;

use super::AppState;
use crate::domain::types::Cnpj;
use crate::error::AppError;

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/empresas/{cnpj}", get(get_empresa))
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct EmpresaDto {
    pub cnpj_basico: String,
    pub razao_social: Option<String>,
    pub capital_social: Option<String>,
    pub porte: Option<String>,
    pub natureza_juridica: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct EstabDto {
    pub cnpj: String,
    pub matriz_filial: Option<String>,
    pub nome_fantasia: Option<String>,
    pub situacao: Option<String>,
    pub data_inicio: Option<NaiveDate>,
    pub cnae_principal: Option<String>,
    pub uf: Option<String>,
    pub municipio: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct SocioDto {
    /// 1=PJ, 2=PF, 3=Estrangeira
    pub identificador: i16,
    pub nome_socio: Option<String>,
    /// CPF mascarado (***NNNNNN**) ou CNPJ completo (PJ-socio).
    pub cnpj_cpf_socio: Option<String>,
    pub qualificacao: Option<i16>,
    pub data_entrada: Option<NaiveDate>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct EmpresaDetalhe {
    pub empresa: EmpresaDto,
    pub estabelecimentos: Vec<EstabDto>,
    pub socios: Vec<SocioDto>,
}

#[utoipa::path(
    get,
    path = "/v1/empresas/{cnpj}",
    tag = "empresas",
    params(
        ("cnpj" = String, Path, description = "CNPJ completo de 14 dígitos (com ou sem pontuação)"),
    ),
    responses(
        (status = 200, description = "Empresa + estabelecimentos ativos + sócios", body = EmpresaDetalhe),
        (status = 400, description = "CNPJ inválido"),
        (status = 404, description = "Empresa não encontrada"),
    ),
)]
pub async fn get_empresa(
    State(state): State<AppState>,
    Path(cnpj): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let cnpj = Cnpj::parse(&cnpj).map_err(|e| AppError::BadRequest(e.into()))?;

    let empresa: Option<EmpresaDto> = sqlx::query_as(
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

    let estabelecimentos: Vec<EstabDto> = sqlx::query_as(
        "SELECT cnpj, matriz_filial, nome_fantasia, situacao, data_inicio, \
                cnae_principal, uf, municipio \
         FROM estabelecimento WHERE cnpj_basico = $1",
    )
    .bind(cnpj.basico())
    .fetch_all(&state.pool)
    .await?;

    let socios: Vec<SocioDto> = sqlx::query_as(
        "SELECT identificador, nome_socio, cnpj_cpf_socio, qualificacao, data_entrada \
         FROM socio WHERE cnpj_basico = $1",
    )
    .bind(cnpj.basico())
    .fetch_all(&state.pool)
    .await?;

    Ok((
        StatusCode::OK,
        Json(EmpresaDetalhe {
            empresa,
            estabelecimentos,
            socios,
        }),
    ))
}
