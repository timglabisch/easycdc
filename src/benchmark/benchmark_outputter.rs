use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use crate::benchmark::{PERF_COUNTER_BINLOG_EVENT_GTID, PERF_COUNTER_BINLOG_EVENT_OTHER, PERF_COUNTER_BINLOG_EVENT_QUERY, PERF_COUNTER_BINLOG_EVENT_ROWS, PERF_COUNTER_BINLOG_EVENT_TABLEMAP, PERF_COUNTER_BINLOG_EVENT_XID, PERF_COUNTER_BINLOG_EVENTS, PERF_COUNTER_TABLE_SKIP, PERF_TIMER_BINLOG_FINISH, PERF_TIMER_BINLOG_READ_WAIT, PERF_TIMER_BINLOG_ROWS_EVENT};

pub struct BenchmarkOutputter {

}

impl BenchmarkOutputter {
    pub fn run() {
        ::tokio::spawn(async move {
            Self::run_async().await;
        });
    }

    async fn run_async() {
        loop {
            ::tokio::time::sleep(Duration::from_secs(1)).await;

            let perf_counter_binlog_events = PERF_COUNTER_BINLOG_EVENTS.swap(0, Ordering::Relaxed);
            let perf_timer_binlog_read_wait = PERF_TIMER_BINLOG_READ_WAIT.swap(0, Ordering::Relaxed);
            let perf_timer_binlog_finish = PERF_TIMER_BINLOG_FINISH.swap(0, Ordering::Relaxed);
            let perf_counter_binlog_event_tablemap = PERF_COUNTER_BINLOG_EVENT_TABLEMAP.swap(0, Ordering::Relaxed);
            let perf_timer_binlog_rows_event = PERF_TIMER_BINLOG_ROWS_EVENT.swap(0, Ordering::Relaxed);
            let perf_counter_binlog_event_rows = PERF_COUNTER_BINLOG_EVENT_ROWS.swap(0, Ordering::Relaxed);
            let perf_counter_binlog_event_xid = PERF_COUNTER_BINLOG_EVENT_XID.swap(0, Ordering::Relaxed);
            let perf_counter_binlog_event_gtid = PERF_COUNTER_BINLOG_EVENT_GTID.swap(0, Ordering::Relaxed);
            let perf_counter_binlog_event_query = PERF_COUNTER_BINLOG_EVENT_QUERY.swap(0, Ordering::Relaxed);
            let perf_counter_binlog_event_other = PERF_COUNTER_BINLOG_EVENT_OTHER.swap(0, Ordering::Relaxed);
            let perf_counter_table_skip = PERF_COUNTER_TABLE_SKIP.swap(0, Ordering::Relaxed);

            print!(
                "perf_counter_binlog_events: {perf_counter_binlog_events}\n
                perf_timer_binlog_read_wait: {perf_timer_binlog_read_wait}\n
                perf_timer_binlog_finish: {perf_timer_binlog_finish}\n
                perf_counter_binlog_event_tablemap: {perf_counter_binlog_event_tablemap}\n
                perf_timer_binlog_rows_event: {perf_timer_binlog_rows_event}\n
                perf_counter_binlog_event_rows: {perf_counter_binlog_event_rows}\n
                perf_counter_binlog_event_xid: {perf_counter_binlog_event_xid}\n
                perf_counter_binlog_event_gtid: {perf_counter_binlog_event_gtid}\n
                perf_counter_binlog_event_query: {perf_counter_binlog_event_query}\n
                perf_counter_binlog_event_other: {perf_counter_binlog_event_other}\n
                perf_counter_table_skip: {perf_counter_table_skip}\n"
            );

        }
    }
}