use std::sync::Arc;

use axum::Router;
use sqlx::PgPool;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::GovernorLayer;
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::config::Config;

pub mod empresa;
pub mod health;
pub mod jobs;
pub mod relacionamento;
pub mod socio;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Relaciona Brasil",
        version = env!("CARGO_PKG_VERSION"),
        description = "API pública de relacionamentos entre empresas e pessoas no Brasil.\n\n\
                       Dados originados do dump aberto da Receita Federal. \
                       Modelo lazy: a primeira consulta de um CNPJ enfileira um job (202) \
                       e o worker preenche o cache via scan dos zips em paralelo.",
        license(name = "AGPL-3.0-or-later", identifier = "AGPL-3.0-or-later"),
    ),
    paths(
        health::health,
        health::ready,
        empresa::get_empresa,
        socio::buscar,
        relacionamento::grafo,
        jobs::status,
    ),
    components(schemas(
        health::HealthOk,
        health::ReadyOk,
        empresa::EmpresaDto,
        empresa::EstabDto,
        empresa::SocioDto,
        empresa::EmpresaDetalhe,
        socio::SocioMatch,
        socio::BuscaResposta,
        relacionamento::No,
        relacionamento::Aresta,
        relacionamento::Raiz,
        relacionamento::Grafo,
        relacionamento::JobAceito,
        crate::jobs::queue::EnqueuedJob,
    )),
    tags(
        (name = "system", description = "Health/readiness"),
        (name = "empresas", description = "Empresa, estabelecimentos, sócios"),
        (name = "socios", description = "Busca de sócios (cache-only)"),
        (name = "relacionamento", description = "Grafo de relacionamentos"),
        (name = "jobs", description = "Status de jobs assíncronos"),
    ),
)]
struct ApiDoc;

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
        .merge(SwaggerUi::new("/docs").url("/api-doc/openapi.json", ApiDoc::openapi()))
        .merge(health::router())
        .merge(empresa::router())
        .merge(socio::router())
        .merge(relacionamento::router())
        .merge(jobs::router())
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
