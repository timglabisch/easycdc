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

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(name = "config")]
    config: String,
}

fn main() -> Result<(), ::anyhow::Error> {
    let opt = Opt::from_args();

    let config = Config::from_file(&opt.config)?;

    let url = "mysql://root:password@localhost:3306/foo";

    let pool = ::mysql::Pool::new(url)?;

    let mut conn = pool.get_conn()?;

    let mut binlog_stream = conn.get_binlog_stream(
        ::mysql::BinlogRequest::new(1)
            .with_use_gtid(true)
            /*.with_sids(vec![
                ::mysql_common::packets::Sid::new([
                    177,
                    165,
                    142,
                    39,
                    97,
                    182,
                    17,
                    237,
                    160,
                    50,
                    2,
                    66,
                    172,
                    17,
                    0,
                    2,
                ]).with_interval(Interval::new(1, 6))
            ])*/
    )?;

    let mut tablemap = TableMap::from_config(&config);

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
                tablemap.record_table_map_event(&t);
            }
            EventData::RowsEvent(row_event) => {

                let table_info = match tablemap.get_cdc_info(&row_event.table_id()) {
                    None => {
                        continue;
                    },
                    Some(s) => s
                };

                let table_config = match table_info.table_config {
                    None => {
                        println!("skip table ...");
                        continue;
                    },
                    Some(ref s) => s,
                };

                let rows = row_event.rows(&table_info.table_map_event);

                for row in rows {
                    let (before, after)= match row {
                        Ok(k) => k,
                        Err(e) => {
                            println!("could not decode row");
                            continue;
                        }
                    };


                    let v = SinkConsoleJsonValue::from_row(
                        table_config,
                        &table_info.table_map_event,
                        &before,
                        &after
                    );

                    println!("{}", v.to_json());
                }
            }
            EventData::XidEvent(xid_event) => {
                println!("xid");
            },
            EventData::GtidEvent(gtid_event) => {

                let sid = gtid_event.sid();
                // dbg!(sid);

                dbg!(format_gtid(sid));
                dbg!(gtid_event.sequence_number());
                dbg!(gtid_event.sequence_number());
            },
            EventData::QueryEvent(e) => {
                // println!("{:#?}", e);
            },
            _ => {
                // println!("data {:#?}", data);
            }
        };
    }

    Ok(())
}
