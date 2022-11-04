use anyhow::Context;
use serde_derive::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct ConfigTable {
    database: String,
    table: String,
    col: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    table: Vec<ConfigTable>,
}

impl Config {
    pub fn from_file(filename : &str) -> Result<Self, ::anyhow::Error> {
        let file = ::std::fs::read_to_string(filename).context("could not read config file")?;

        let config = ::toml::from_str(&file).context("could not parse toml")?;

        Ok(config)
    }
}