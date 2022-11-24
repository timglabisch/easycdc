use std::sync::atomic::AtomicU64;

pub static PERF_COUNTER_BINLOG_EVENTS: AtomicU64 = AtomicU64::new(0);
pub static PERF_TIMER_BINLOG_READ_WAIT: AtomicU64 = AtomicU64::new(0);
pub static PERF_TIMER_BINLOG_FINISH: AtomicU64 = AtomicU64::new(0);

pub static PERF_COUNTER_BINLOG_EVENT_TABLEMAP: AtomicU64 = AtomicU64::new(0);

pub static PERF_TIMER_BINLOG_ROWS_EVENT: AtomicU64 = AtomicU64::new(0);
pub static PERF_COUNTER_BINLOG_EVENT_ROWS: AtomicU64 = AtomicU64::new(0);
pub static PERF_COUNTER_BINLOG_EVENT_XID: AtomicU64 = AtomicU64::new(0);
pub static PERF_COUNTER_BINLOG_EVENT_GTID: AtomicU64 = AtomicU64::new(0);
pub static PERF_COUNTER_BINLOG_EVENT_QUERY: AtomicU64 = AtomicU64::new(0);
pub static PERF_COUNTER_BINLOG_EVENT_OTHER: AtomicU64 = AtomicU64::new(0);
pub static PERF_COUNTER_TABLE_SKIP: AtomicU64 = AtomicU64::new(0);