use std::collections::HashMap;
use std::path::PathBuf;
use anyhow::Context;
use config::Config;
use mysql::binlog::events::EventData;
use mysql::consts::ColumnFlags;
use mysql::prelude::*;
use structopt::StructOpt;

mod tablemap;
mod config;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(name = "config")]
    config: String,
}

fn main() -> Result<(), ::anyhow::Error> {

    let opt = Opt::from_args();
    
    let config = Config::from_file(&opt.config)?;

    println!("config : {:#?}", &config);

    let url = "mysql://root:password@localhost:3306/foo";

    let pool = ::mysql::Pool::new(url)?;

    let mut conn = pool.get_conn()?;

    let mut binlog_stream = conn.get_binlog_stream(::mysql::BinlogRequest::new(1))?;

    let mut table_maps = HashMap::new();

    loop {
        let item = match binlog_stream.next() {
            Some(s) => s,
            None => {
                println!("got an empty event");
                continue;
            }
        };

        let item = match item {
            Ok(s) => s,
            Err(e) => {
                panic!("could not read item. {:?}", e);
            }
        };

        let data = match item.read_data()? {
            Some(s) => s,
            None => {
                println!("unknown event");
                continue;
            }
        };

        let timestamp = item.header().timestamp();

        match data {
            EventData::TableMapEvent(t) => {
                table_maps.insert(t.table_id(), t.into_owned());
            },
            EventData::RowsEvent(row_event) => {
                // println!("row event {:#?}", row_event);
                let table = match table_maps.get(&row_event.table_id()) {
                    Some(s) => s,
                    None => {
                        println!("binlog event for unknwon table");
                        continue;
                    }
                };

                let rows = row_event.rows(table);

                for row in rows {
                    let (row) = match row {
                        Ok(k) => k,
                        Err(e) => {
                            println!("could not decode row");
                            continue;
                        }
                    };

                    match row {
                        (Some(before), Some(after)) => {
                            println!("update!");
                        },
                        (Some(before), None) => {
                            println!("delete!");
                        },
                        (None, Some(after)) => {
                            println!("insert!");
                        },
                        _ => unreachable!()
                    };


                }

                let a = 0;
            }
            _ => {
                // println!("data {:#?}", data);
            }
        };
    }

    Ok(())
}
