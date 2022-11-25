use crate::cdc::CdcStream;
use crate::config;
use crate::config::Config;
use crate::control_handle::ControlHandleReceiver;
use crate::sink::void::SinkVoid;

pub mod console;
pub mod void;

pub fn sinks_initialize(
    config : Config,
    control_handle_receiver : ControlHandleReceiver,
    cdc_stream :CdcStream,
) {
    if let Some(ref sink_benchmark) = config.sink_void {
        SinkVoid::new(
            sink_benchmark.clone(),
            control_handle_receiver,
            cdc_stream.clone()
        ).run();
    }
}