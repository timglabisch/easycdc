use crate::cdc::CdcStream;
use crate::config;
use crate::config::Config;
use crate::control_handle::ControlHandleReceiver;
use crate::sink::benchmark::SinkBenchmark;

pub mod console;
pub mod benchmark;

pub fn sinks_initialize(
    config : Config,
    control_handle_receiver : ControlHandleReceiver,
    cdc_stream :CdcStream,
) {
    if let Some(ref sink_benchmark) = config.sink_benchmark {
        SinkBenchmark::new(
            sink_benchmark.clone(),
            control_handle_receiver,
            cdc_stream.clone()
        ).run();
    }
}