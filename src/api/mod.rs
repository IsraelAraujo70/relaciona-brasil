use std::sync::Arc;

use axum::Router;
use sqlx::PgPool;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::GovernorLayer;
use tower_http::trace::TraceLayer;

use crate::config::Config;

pub mod empresa;
pub mod health;
pub mod relacionamento;
pub mod socio;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

pub async fn serve(cfg: Config) -> anyhow::Result<()> {
    let pool = crate::db::pool(&cfg).await?;
    let state = AppState { pool };

    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(2)
            .burst_size(20)
            .finish()
            .ok_or_else(|| anyhow::anyhow!("falha ao construir rate limiter"))?,
    );

    let app = Router::new()
        .merge(health::router())
        .merge(empresa::router())
        .merge(socio::router())
        .merge(relacionamento::router())
        .layer(GovernorLayer::new(governor_conf))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", cfg.port)
        .parse()
        .expect("endereço inválido");
    tracing::info!(%addr, "ouvindo");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
