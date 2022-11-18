use crate::gtid::format_gtid;
use crate::sink::console::SinkConsoleJsonValue;
use anyhow::Context;
use config::Config;
use mysql::binlog::events::EventData;
use mysql::consts::ColumnFlags;
use mysql::prelude::*;
use mysql::BinlogDumpFlags;
use mysql_common::packets::Interval;
use std::collections::HashMap;
use std::path::PathBuf;
use structopt::StructOpt;
use crate::control_handle::ControlHandle;

use crate::tablemap::TableMap;

mod cdc;
mod config;
mod gtid;
mod sink;
mod tablemap;
mod test;
mod control_handle;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(name = "config")]
    config: String,
}

fn main() -> Result<(), ::anyhow::Error> {
    let opt = Opt::from_args();

    let x = ControlHandle::new();

    let config = Config::from_file(&opt.config)?;

    // let cdc_runn

    Ok(())
}
