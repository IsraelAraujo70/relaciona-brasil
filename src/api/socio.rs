use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use super::AppState;
use crate::error::AppError;

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/socios", get(buscar))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct BuscaParams {
    /// Busca por nome via similaridade trigram (≥3 caracteres). Mutuamente
    /// exclusivo com `cpf`.
    pub nome: Option<String>,
    /// CPF mascarado (`***NNNNNN**`) ou CNPJ completo (PJ-socio) — match exato.
    pub cpf: Option<String>,
    /// Máximo de resultados (1-200, default 50).
    #[serde(default = "default_limit")]
    pub limite: u32,
}

fn default_limit() -> u32 {
    50
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct SocioMatch {
    pub cnpj_basico: String,
    pub razao_social: Option<String>,
    pub nome_socio: Option<String>,
    pub cnpj_cpf_socio: Option<String>,
    pub qualificacao: Option<i16>,
    pub data_entrada: Option<NaiveDate>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BuscaResposta {
    pub total: usize,
    pub matches: Vec<SocioMatch>,
}

#[utoipa::path(
    get,
    path = "/v1/socios",
    tag = "socios",
    params(BuscaParams),
    responses(
        (status = 200, description = "Lista de matches", body = BuscaResposta),
        (status = 400, description = "Sem `nome` nem `cpf`, ou nome muito curto"),
    ),
)]
pub async fn buscar(
    State(state): State<AppState>,
    Query(params): Query<BuscaParams>,
) -> Result<impl IntoResponse, AppError> {
    let limite = params.limite.clamp(1, 200) as i64;

    let matches: Vec<SocioMatch> = if let Some(cpf) = params.cpf.as_ref() {
        sqlx::query_as(
            "SELECT s.cnpj_basico, e.razao_social, s.nome_socio, s.cnpj_cpf_socio, \
                    s.qualificacao, s.data_entrada \
             FROM socio s LEFT JOIN empresa e USING (cnpj_basico) \
             WHERE s.cnpj_cpf_socio = $1 \
             ORDER BY s.data_entrada DESC NULLS LAST \
             LIMIT $2",
        )
        .bind(cpf)
        .bind(limite)
        .fetch_all(&state.pool)
        .await?
    } else if let Some(nome) = params.nome.as_ref() {
        if nome.trim().len() < 3 {
            return Err(AppError::BadRequest(
                "nome com pelo menos 3 caracteres".into(),
            ));
        }
        sqlx::query_as(
            "SELECT s.cnpj_basico, e.razao_social, s.nome_socio, s.cnpj_cpf_socio, \
                    s.qualificacao, s.data_entrada \
             FROM socio s LEFT JOIN empresa e USING (cnpj_basico) \
             WHERE s.nome_socio % $1 \
             ORDER BY similarity(s.nome_socio, $1) DESC \
             LIMIT $2",
        )
        .bind(nome)
        .bind(limite)
        .fetch_all(&state.pool)
        .await?
    } else {
        return Err(AppError::BadRequest("informe ?nome=… ou ?cpf=…".into()));
    };

    Ok((
        StatusCode::OK,
        Json(BuscaResposta {
            total: matches.len(),
            matches,
        }),
    ))
}
