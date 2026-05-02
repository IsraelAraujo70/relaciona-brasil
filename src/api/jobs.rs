use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use uuid::Uuid;

use super::AppState;
use crate::error::AppError;
use crate::jobs::queue::{self, EnqueuedJob};

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/jobs/{id}", get(status))
}

#[utoipa::path(
    get,
    path = "/v1/jobs/{id}",
    tag = "jobs",
    params(
        ("id" = String, Path, description = "UUID do job"),
    ),
    responses(
        (status = 200, description = "Status do job", body = EnqueuedJob),
        (status = 400, description = "UUID inválido"),
        (status = 404, description = "Job não encontrado"),
    ),
)]
pub async fn status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let id = Uuid::parse_str(&id).map_err(|_| AppError::BadRequest("UUID inválido".into()))?;
    let job = queue::fetch(&state.pool, id)
        .await
        .map_err(AppError::Other)?
        .ok_or(AppError::NotFound)?;
    Ok((StatusCode::OK, Json(job)))
}
