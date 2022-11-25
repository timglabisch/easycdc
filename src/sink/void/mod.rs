use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use crate::cdc::CdcStream;
use crate::config::ConfigSinkVoid;
use crate::control_handle::ControlHandleReceiver;

pub static COUNTER : AtomicU64 = AtomicU64::new(0);

pub struct SinkVoid {
    config: ConfigSinkVoid,
    control_handle_receiver: ControlHandleReceiver,
    cdc_stream: CdcStream,
}

impl SinkVoid {
    pub fn new(
        config: ConfigSinkVoid,
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
            self.run_inner().await.expect("sink void panic");
        });
    }

    pub async fn run_inner(&mut self) -> Result<(), ::anyhow::Error> {
        loop {
            let _ = self.cdc_stream.recv().await?;
            COUNTER.fetch_add(1, Ordering::Relaxed);
        }
    }
}