use anyhow::Context;
use serde_derive::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigTable {
    pub database: String,
    pub table: String,
    pub cols: Vec<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub table: Vec<ConfigTable>,
    pub connection: String,
}

impl Config {
    pub fn from_file(filename: &str) -> Result<Self, ::anyhow::Error> {
        let file = ::std::fs::read_to_string(filename).context("could not read config file")?;

        let config = ::toml::from_str(&file).context("could not parse toml")?;

        Ok(config)
    }
}
