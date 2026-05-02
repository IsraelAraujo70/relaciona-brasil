use anyhow::Result;
use clap::{Parser, ValueEnum};
use tracing_subscriber::EnvFilter;

use relaciona_brasil::{api, config::Config, db, scheduler, worker};

#[derive(Parser, Debug)]
#[command(version, about = "Relaciona Brasil — API + worker lazy lookup CNPJ")]
struct Args {
    #[arg(long, value_enum, default_value_t = Mode::Api)]
    mode: Mode,

    /// Modo `downloader`: apenas baixa a vintage atual e sai.
    /// Modo `worker`: processa um único job pendente e sai (smoke).
    #[arg(long)]
    once: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Mode {
    Api,
    Worker,
    Downloader,
    Migrate,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let cfg = Config::load()?;

    match args.mode {
        Mode::Migrate => {
            let pool = db::pool(&cfg).await?;
            sqlx::migrate!().run(&pool).await?;
            tracing::info!("migrações aplicadas");
            Ok(())
        }
        Mode::Api => api::serve(cfg).await,
        Mode::Worker => worker::run(cfg, args.once).await,
        Mode::Downloader => scheduler::run_downloader(cfg, args.once).await,
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("relaciona_brasil=info,info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}
