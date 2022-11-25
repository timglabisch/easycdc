#[derive(Clone)]
pub enum ControlHandleMsg {

}

pub struct ControlHandle {}

impl ControlHandle {
    pub fn new() -> (ControlHandleSender, ControlHandleReceiver) {
        let (sender, receiver) = ::async_channel::unbounded();

        (
            ControlHandleSender {
                inner: sender
            },
            ControlHandleReceiver {
                inner: receiver
            },
        )
    }
}

#[derive(Clone)]
pub struct ControlHandleReceiver {
    pub inner: ::async_channel::Receiver<ControlHandleMsg>
}

#[derive(Clone)]
pub struct ControlHandleSender {
    pub inner: ::async_channel::Sender<ControlHandleMsg>
}