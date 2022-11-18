#[derive(Clone)]
pub enum ControlHandleMsg {

}

#[derive(Clone)]
pub struct ControlHandle {
    pub sender: ::async_channel::Sender<ControlHandleMsg>,
    pub receiver: ::async_channel::Receiver<ControlHandleMsg>
}

impl ControlHandle {
    pub fn new() -> Self {
        let (sender, receiver) = ::async_channel::unbounded();

        Self {
            receiver,
            sender
        }
    }

    pub fn get_sender_handle(&self) -> ControlHandleSender {
        ControlHandleSender {
            sender: self.sender.clone(),
        }
    }

    pub fn get_recv_handle(&self) -> ControlHandleReceiver {
        ControlHandleReceiver {
            receiver: self.receiver.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ControlHandleReceiver {
    pub receiver: ::async_channel::Receiver<ControlHandleMsg>
}

#[derive(Clone)]
pub struct ControlHandleSender {
    pub sender: ::async_channel::Sender<ControlHandleMsg>
}