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
struct VintageRow {
    vintage: String,
    baixada_em: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ReadyOk {
    /// `"ready"` quando há ao menos uma vintage em disco.
    pub status: String,
    pub vintage: String,
    pub baixada_em: DateTime<Utc>,
}

#[utoipa::path(
    get,
    path = "/ready",
    tag = "system",
    responses(
        (status = 200, description = "Pelo menos uma vintage baixada", body = ReadyOk),
        (status = 503, description = "Sem vintage ou banco indisponível"),
    ),
)]
pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let row = sqlx::query_as::<_, VintageRow>(
        "SELECT vintage, baixada_em FROM vintages_baixadas \
         ORDER BY vintage DESC LIMIT 1",
    )
    .fetch_optional(&state.pool)
    .await;

    match row {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(json!({
                "status": "ready",
                "vintage": row.vintage,
                "baixada_em": row.baixada_em,
            })),
        ),
        Ok(None) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "sem_vintage" })),
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
