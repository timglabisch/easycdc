use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use crate::benchmark::{PERF_COUNTER_BINLOG_EVENT_GTID, PERF_COUNTER_BINLOG_EVENT_OTHER, PERF_COUNTER_BINLOG_EVENT_QUERY, PERF_COUNTER_BINLOG_EVENT_ROWS, PERF_COUNTER_BINLOG_EVENT_TABLEMAP, PERF_COUNTER_BINLOG_EVENT_XID, PERF_COUNTER_BINLOG_EVENTS, PERF_COUNTER_TABLE_SKIP, PERF_TIMER_BINLOG_FINISH, PERF_TIMER_BINLOG_READ_WAIT, PERF_TIMER_BINLOG_ROWS_EVENT};

pub struct BenchmarkOutputter {

}

impl BenchmarkOutputter {
    pub fn run() {
        ::tokio::spawn(Self::run_async)
    }

    async fn run_async() {
        loop {
            ::tokio::time::sleep(Duration::from_secs(1)).await;

            let perf_counter_binlog_events = PERF_COUNTER_BINLOG_EVENTS.swap(0, Ordering::Relaxed);
            let perf_timer_binlog_read_wait = PERF_TIMER_BINLOG_READ_WAIT.swap(0, Ordering::Relaxed);
            let perf_timer_binlog_finish = PERF_TIMER_BINLOG_FINISH.swap(0, Ordering::Relaxed);
            PERF_COUNTER_BINLOG_EVENT_TABLEMAP.swap(0, Ordering::Relaxed);
            PERF_TIMER_BINLOG_ROWS_EVENT.swap(0, Ordering::Relaxed);
            PERF_COUNTER_BINLOG_EVENT_ROWS.swap(0, Ordering::Relaxed);
            PERF_COUNTER_BINLOG_EVENT_XID.swap(0, Ordering::Relaxed);
            PERF_COUNTER_BINLOG_EVENT_GTID.swap(0, Ordering::Relaxed);
            PERF_COUNTER_BINLOG_EVENT_QUERY.swap(0, Ordering::Relaxed);
            PERF_COUNTER_BINLOG_EVENT_OTHER.swap(0, Ordering::Relaxed);
            PERF_COUNTER_TABLE_SKIP.swap(0, Ordering::Relaxed);

        }
    }
}