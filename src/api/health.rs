use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::json;
use utoipa::ToSchema;

use super::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HealthOk {
    /// Sempre `"ok"`.
    pub status: String,
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "system",
    responses(
        (status = 200, description = "Liveness probe", body = HealthOk),
    ),
)]
pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

#[derive(sqlx::FromRow)]
struct StatusRow {
    etapa: String,
    terminada_em: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ReadyOk {
    /// `"ready"` quando `ingestao_status.etapa = 'concluida'`.
    pub status: String,
    /// Timestamp UTC da última ingestão concluída.
    pub ultima_ingestao: Option<DateTime<Utc>>,
}

#[utoipa::path(
    get,
    path = "/ready",
    tag = "system",
    responses(
        (status = 200, description = "Pelo menos uma ingestão concluída", body = ReadyOk),
        (status = 503, description = "Sem dados ainda ou banco indisponível"),
    ),
)]
pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let row = sqlx::query_as::<_, StatusRow>(
        "SELECT etapa, terminada_em FROM ingestao_status \
         WHERE terminada_em IS NOT NULL \
         ORDER BY terminada_em DESC LIMIT 1",
    )
    .fetch_optional(&state.pool)
    .await;

    match row {
        Ok(Some(row)) if row.etapa == "concluida" => (
            StatusCode::OK,
            Json(json!({
                "status": "ready",
                "ultima_ingestao": row.terminada_em,
            })),
        ),
        Ok(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "sem_dados" })),
        ),
        Err(err) => {
            tracing::error!(?err, "falha em /ready");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "db_indisponivel" })),
            )
        }
    }
}
