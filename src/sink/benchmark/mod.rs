use std::sync::atomic::{AtomicU64, Ordering};
use crate::cdc::CdcStream;
use crate::config::ConfigSinkBenchmark;
use crate::control_handle::ControlHandleReceiver;

pub static COUNTER : AtomicU64 = AtomicU64::new(0);

pub struct SinkBenchmark {
    config: ConfigSinkBenchmark,
    control_handle_receiver: ControlHandleReceiver,
    cdc_stream: CdcStream,
}

impl SinkBenchmark {
    pub fn new(
        config: ConfigSinkBenchmark,
        control_handle_receiver: ControlHandleReceiver,
        cdc_stream: CdcStream,
    ) -> Self {
        Self {
            config,
            control_handle_receiver,
            cdc_stream
        }
    }

    pub fn run(mut self) {
        ::tokio::spawn(async move {
            self.run_inner().await.expect("sink benchmark panic");
            let number_if_items = COUNTER.swap(0, Ordering::Relaxed);

        });
    }

    pub async fn run_inner(&mut self) -> Result<(), ::anyhow::Error> {
        loop {
            let _ = self.cdc_stream.recv().await?;
            COUNTER.fetch_add(1, Ordering::Relaxed);
        }
    }
}