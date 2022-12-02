use crate::cdc::CdcStream;
use crate::config;
use crate::config::Config;
use crate::control_handle::ControlHandleReceiver;
use crate::sink::scylla::{ConfigSinkScylla, SinkScylla};
use crate::sink::void::SinkVoid;

pub mod console;
pub mod void;
pub mod scylla;

pub fn sinks_initialize(
    config : Config,
    control_handle_receiver : ControlHandleReceiver,
    cdc_stream :CdcStream,
) {
    if let Some(ref config) = config.sink_void {
        SinkVoid::new(
            config.clone(),
            control_handle_receiver,
            cdc_stream.clone()
        ).run();
    } else if let Some(ref config) = config.sink_scylla {
        SinkScylla::new(
            config.clone(),
            control_handle_receiver,
            cdc_stream.clone()
        ).run();
    }
}