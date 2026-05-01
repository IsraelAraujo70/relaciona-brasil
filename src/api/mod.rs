use axum::Router;
use sqlx::PgPool;
use tower_http::trace::TraceLayer;

use crate::config::Config;

pub mod health;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

pub async fn serve(cfg: Config) -> anyhow::Result<()> {
    let pool = crate::db::pool(&cfg).await?;
    let state = AppState { pool };

    let app = Router::new()
        .merge(health::router())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    tracing::info!(%addr, "ouvindo");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
