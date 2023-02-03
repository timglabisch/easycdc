use std::sync::atomic::Ordering;
use crate::config::Config;
use crate::gtid::format_gtid;
use crate::sink::console::SinkConsoleJsonValue;
use crate::tablemap::TableMap;
use anyhow::Context;
use mysql_common::binlog::events::{EventData, RowsEvent, RowsEventData};
use std::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use crate::benchmark::{PERF_COUNTER_BINLOG_EVENT_GTID, PERF_COUNTER_BINLOG_EVENT_OTHER, PERF_COUNTER_BINLOG_EVENT_QUERY, PERF_COUNTER_BINLOG_EVENT_ROWS, PERF_COUNTER_BINLOG_EVENT_TABLEMAP, PERF_COUNTER_BINLOG_EVENT_XID, PERF_COUNTER_BINLOG_EVENTS, PERF_COUNTER_TABLE_SKIP, PERF_TIMER_BINLOG_FINISH, PERF_TIMER_BINLOG_READ_WAIT, PERF_TIMER_BINLOG_ROWS_EVENT};
use crate::control_handle::{ControlHandle, ControlHandleReceiver};

pub enum CdcRunnerControlMsg {}

#[derive(Debug)]
pub struct CdcStreamItemGtid {
    pub uuid: [u8; 16],
    pub sequence_number: u64,
}

#[derive(Debug)]
pub enum CdcStreamItem {
    Value(CdcStreamItemValue),
    Gtid(CdcStreamItemGtid),
}

#[derive(Debug)]
pub struct CdcStreamItemValue {
    pub table_name: String,
    pub database_name: String,
    pub data: String,
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

        let mut item_start = minstant::Instant::now();
        loop {
            PERF_TIMER_BINLOG_FINISH.fetch_add(item_start.elapsed().as_millis() as u64, Ordering::Relaxed);
            item_start = minstant::Instant::now();

            let item = match binlog_stream.next() {
                Some(s) => s,
                None => {
                    println!("got an empty event");
                    continue;
                }
            };
            PERF_COUNTER_BINLOG_EVENTS.fetch_add(1, Ordering::Relaxed);
            PERF_TIMER_BINLOG_READ_WAIT.fetch_add(item_start.elapsed().as_millis() as u64, Ordering::Relaxed);

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
                    PERF_COUNTER_BINLOG_EVENT_TABLEMAP.fetch_add(1, Ordering::Relaxed);

                    tablemap.record_table_map_event(&t);
                }
                EventData::RowsEvent(row_event) => {
                    let start_rows_event = minstant::Instant::now();
                    Self::on_rows_event(&mut tablemap, &row_event, &cdc_stream_sender);
                    PERF_TIMER_BINLOG_ROWS_EVENT.fetch_add(start_rows_event.elapsed().as_millis() as u64, Ordering::Relaxed);
                    PERF_COUNTER_BINLOG_EVENT_ROWS.fetch_add(1, Ordering::Relaxed);
                }
                EventData::XidEvent(xid_event) => {
                    PERF_COUNTER_BINLOG_EVENT_XID.fetch_add(1, Ordering::Relaxed);

                    // println!("xid");
                }
                EventData::GtidEvent(gtid_event) => {
                    PERF_COUNTER_BINLOG_EVENT_GTID.fetch_add(1, Ordering::Relaxed);

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
                    PERF_COUNTER_BINLOG_EVENT_QUERY.fetch_add(1, Ordering::Relaxed);

                    // println!("{:#?}", e);
                }
                _ => {
                    PERF_COUNTER_BINLOG_EVENT_OTHER.fetch_add(1, Ordering::Relaxed);

                    // println!("data {:#?}", data);
                }
            };
        }
    }

    fn on_rows_event(
        tablemap : &mut TableMap,
        row_event: &RowsEventData,
        cdc_stream_sender: &::async_channel::Sender<CdcStreamItem>
    ) {

        let table_info = match tablemap.get_cdc_info(&row_event.table_id()) {
            None => {
                PERF_COUNTER_TABLE_SKIP.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Some(s) => s,
        };

        let table_config = match table_info.table_config {
            None => {
                PERF_COUNTER_TABLE_SKIP.fetch_add(1, Ordering::Relaxed);
                return;
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
                .try_send(CdcStreamItem::Value(CdcStreamItemValue {
                    table_name: v.get_table_name().to_string(),
                    database_name: v.get_database_name().to_string(),
                    data: v.to_json().to_string(),
                }))
                .context("could not send to channel").expect("channel!")
        }
    }

}
