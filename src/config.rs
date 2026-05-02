use std::path::PathBuf;

use figment::providers::Env;
use figment::Figment;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub database_url: String,
    pub dump_base_url: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// Diretório onde as vintages baixadas ficam (`<dir>/<YYYY-MM>/*.zip`).
    #[serde(default = "default_dump_data_dir")]
    pub dump_data_dir: PathBuf,
    /// Quantas tasks paralelas o scan dispara para varrer zips.
    #[serde(default = "default_scan_parallelism")]
    pub scan_parallelism: usize,
    /// Quantas vintages mais recentes manter em disco. As demais são apagadas
    /// no início de cada execução do downloader.
    #[serde(default = "default_keep_vintages")]
    pub keep_vintages: usize,
}

fn default_port() -> u16 {
    8080
}

fn default_dump_data_dir() -> PathBuf {
    PathBuf::from("/app/data/dumps")
}

fn default_scan_parallelism() -> usize {
    4
}

fn default_keep_vintages() -> usize {
    2
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        Ok(Figment::new().merge(Env::raw()).extract()?)
    }
}
