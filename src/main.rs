use anyhow::Context;
use config::Config;
use mysql::binlog::events::EventData;
use mysql::consts::ColumnFlags;
use mysql::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use mysql::BinlogDumpFlags;
use mysql_common::packets::Interval;
use structopt::StructOpt;
use crate::gtid::format_gtid;
use crate::sink::console::SinkConsoleJsonValue;

use crate::tablemap::TableMap;

mod config;
mod tablemap;
mod sink;
mod gtid;
mod test;
mod cdc;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(name = "config")]
    config: String,
}

fn main() -> Result<(), ::anyhow::Error> {
    let opt = Opt::from_args();

    let config = Config::from_file(&opt.config)?;

    // let cdc_runn



    Ok(())
}
