use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use super::AppState;
use crate::api::relacionamento::JobAceito;
use crate::domain::types::Cnpj;
use crate::error::AppError;
use crate::jobs::queue::{self, EnqueueRequest};

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/empresas/{cnpj}", get(get_empresa))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct EmpresaQuery {
    /// Webhook opcional disparado pelo worker quando a primeira ingestão concluir.
    pub callback: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct EmpresaDto {
    pub cnpj_basico: String,
    pub razao_social: Option<String>,
    pub capital_social: Option<String>,
    pub porte: Option<String>,
    pub natureza_juridica: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct EmpresaCacheRow {
    cnpj_basico: String,
    razao_social: Option<String>,
    capital_social: Option<String>,
    porte: Option<String>,
    natureza_juridica: Option<String>,
    consultado_em: DateTime<Utc>,
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
    /// Quando o conteúdo foi atualizado pela última vez via scan.
    pub atualizado_em: Option<DateTime<Utc>>,
}

#[utoipa::path(
    get,
    path = "/v1/empresas/{cnpj}",
    tag = "empresas",
    params(
        ("cnpj" = String, Path, description = "CNPJ completo de 14 dígitos (com ou sem pontuação)"),
        EmpresaQuery,
    ),
    responses(
        (status = 200, description = "Empresa + estabelecimentos + sócios (cache hit)", body = EmpresaDetalhe),
        (status = 202, description = "Job enfileirado para popular o cache", body = JobAceito),
        (status = 400, description = "CNPJ inválido"),
    ),
)]
pub async fn get_empresa(
    State(state): State<AppState>,
    Path(cnpj): Path<String>,
    Query(q): Query<EmpresaQuery>,
) -> Result<Response, AppError> {
    let cnpj = Cnpj::parse(&cnpj).map_err(|e| AppError::BadRequest(e.into()))?;

    let cached: Option<EmpresaCacheRow> = sqlx::query_as(
        "SELECT cnpj_basico, razao_social, capital_social::text AS capital_social, \
                porte, natureza_juridica, consultado_em \
         FROM empresa WHERE cnpj_basico = $1",
    )
    .bind(cnpj.basico())
    .fetch_optional(&state.pool)
    .await?;

    if let Some(row) = cached {
        let consultado_em = Some(row.consultado_em);
        let empresa = EmpresaDto {
            cnpj_basico: row.cnpj_basico,
            razao_social: row.razao_social,
            capital_social: row.capital_social,
            porte: row.porte,
            natureza_juridica: row.natureza_juridica,
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

        return Ok((
            StatusCode::OK,
            Json(EmpresaDetalhe {
                empresa,
                estabelecimentos,
                socios,
                atualizado_em: consultado_em,
            }),
        )
            .into_response());
    }

    let req = EnqueueRequest {
        cnpj_basico: cnpj.basico().to_string(),
        profundidade: 1,
        callback_url: q.callback,
    };
    let (job, _outcome) = queue::enqueue(&state.pool, req)
        .await
        .map_err(AppError::Other)?;

    let aceito = JobAceito {
        job_id: job.id,
        status: job.status.clone(),
        poll_url: format!("/v1/jobs/{}", job.id),
        mensagem: "consulta enfileirada — primeira leitura é fria, aguarde o worker".into(),
    };
    Ok((StatusCode::ACCEPTED, Json(aceito)).into_response())
}
