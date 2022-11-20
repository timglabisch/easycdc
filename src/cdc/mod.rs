use crate::config::Config;
use crate::gtid::format_gtid;
use crate::sink::console::SinkConsoleJsonValue;
use crate::tablemap::TableMap;
use anyhow::Context;
use mysql_common::binlog::events::EventData;
use std::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use crate::control_handle::{ControlHandle, ControlHandleReceiver};

pub enum CdcRunnerControlMsg {}

#[derive(Debug)]
pub struct CdcStreamItemGtid {
    pub uuid: [u8; 16],
    pub sequence_number: u64,
}

#[derive(Debug)]
pub enum CdcStreamItem {
    Value(String),
    Gtid(CdcStreamItemGtid),
}


pub struct CdcRunner {
    pub config: Config,
    pub control_handle_recv: ControlHandleReceiver,
    pub cdc_thread: Option<::std::thread::JoinHandle<()>>,
}

pub type CdcStream = ::async_channel::Receiver<CdcStreamItem>;

impl CdcRunner {
    pub fn new(control_handle_recv: ControlHandleReceiver, config: Config) -> Self {
        Self {
            config,
            control_handle_recv,
            cdc_thread: None,
        }
    }

    pub async fn run(
        mut self,
    ) -> ::async_channel::Receiver<CdcStreamItem>
    {
        let config = self.config.clone();

        let (cdc_stream_sender, cdc_stream_recv) = ::async_channel::bounded(20000);

        self.cdc_thread = Some(::std::thread::spawn(move || {
            match Self::inner_run(config, cdc_stream_sender) {
                Err(e) => {
                    println!("CDC Worker Crashed! {}", e);
                }
                Ok(_) => {
                    println!("CDC Worker Finished, should not happen!");
                }
            }
        }));

        let control_handle_recv = self.control_handle_recv.clone();

        ::tokio::task::spawn(async move {
            loop {
                let x = control_handle_recv.inner.recv().await;
            }
        });

        cdc_stream_recv
    }

    fn inner_run(
        config: Config,
        cdc_stream_sender: ::async_channel::Sender<CdcStreamItem>,
    ) -> Result<(), ::anyhow::Error> {
        let pool = ::mysql::Pool::new(config.connection.as_str())?;

        let mut conn = pool.get_conn()?;

        let mut binlog_stream = conn.get_binlog_stream(
            ::mysql::BinlogRequest::new(1).with_use_gtid(true), /*.with_sids(vec![
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
                        }
                        Some(s) => s,
                    };

                    let table_config = match table_info.table_config {
                        None => {
                            println!("skip table ...");
                            continue;
                        }
                        Some(ref s) => s,
                    };

                    let rows = row_event.rows(&table_info.table_map_event);

                    for row in rows {
                        let (before, after) = match row {
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
                            &after,
                        );

                        cdc_stream_sender
                            .try_send(CdcStreamItem::Value(v.to_json().to_string()))
                            .context("could not send to channel")?

                        //println!("{}", v.to_json());
                    }
                }
                EventData::XidEvent(xid_event) => {
                    println!("xid");
                }
                EventData::GtidEvent(gtid_event) => {
                    let sid = gtid_event.sid();
                    // dbg!(sid);

                    // dbg!(format_gtid(sid));
                    // dbg!(gtid_event.sequence_number());
                    // dbg!(gtid_event.sequence_number());

                    cdc_stream_sender
                        .try_send(CdcStreamItem::Gtid(CdcStreamItemGtid {
                            uuid: gtid_event.sid(),
                            sequence_number: gtid_event.sequence_number(),
                        }))
                        .context("could not send to channel");
                }
                EventData::QueryEvent(e) => {
                    // println!("{:#?}", e);
                }
                _ => {
                    // println!("data {:#?}", data);
                }
            };
        }
    }
}
