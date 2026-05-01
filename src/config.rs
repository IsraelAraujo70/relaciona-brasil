use figment::providers::Env;
use figment::Figment;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub database_url: String,
    pub dump_base_url: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_port() -> u16 {
    8080
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        Ok(Figment::new().merge(Env::raw()).extract()?)
    }
}
