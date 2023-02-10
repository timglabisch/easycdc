use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use futures::Stream;
use scylla::{FromRow, IntoTypedRows, Session, SessionBuilder};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct CdcStreamPosition {
    pub database_name: String,
    pub database_table: String,
    pub server_uuid: String,
    pub sequence: u64,
    pub sequence_row: u64,
}

pub struct CdcStream {
    receiver: Receiver<CdcStreamItem>,
}

#[derive(Debug)]
pub struct CdcStreamItem {}

impl CdcStream {
    pub async fn new(position_start: CdcStreamPosition) -> Result<Self, ::anyhow::Error> {
        let (sender, receiver) = channel(1000);

        ::tokio::spawn(async move {
            ScyllaPoller::new(position_start, sender).run().await
        });

        Ok(Self {
            receiver,
        })
    }
}

impl Stream for CdcStream {
    type Item = CdcStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

struct ScyllaPoller {
    position: CdcStreamPosition,
    sender: Sender<CdcStreamItem>,
}

impl ScyllaPoller {
    pub fn new(position: CdcStreamPosition, sender: Sender<CdcStreamItem>) -> Self {
        ScyllaPoller {
            position,
            sender
        }
    }

    pub async fn run(&mut self) {
        loop {
            match self.run_inner().await {
                Ok(_) => {
                    ::tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    eprintln!("Err (ScyllaPoller): {:?}", e);
                    ::tokio::time::sleep(Duration::from_millis(300)).await;
                }
            };
        }
    }

    async fn run_inner(&mut self) -> Result<(), ::anyhow::Error> {

        #[derive(scylla::ValueList)]
        struct QType {
            server_uuid: String,
            name_database: String,
            name_table: String,
            sequence_number: i64,
        }

        let session = SessionBuilder::new().known_node("127.0.0.1:9042").build().await?;

        let query = r#"
                select
                    data,
                    sequence_number,
                    sequence_row
                from
                    easycdc.sequence
                    where server_uuid = ?
                    and name_database = ?
                    and name_table = ?
                    and sequence_number >= ?
                LIMIT 1000
        "#.trim();

        loop {

            let res = session.query(query, QType {
                name_table: self.position.database_table.to_string(),
                name_database: self.position.database_name.to_string(),
                server_uuid: self.position.server_uuid.to_string(),
                sequence_number: self.position.sequence as i64,
            }).await?;

            let rows = match res.rows {
                None => {
                    ::tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Some(s) if s.iter().len() == 0 => {
                    ::tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Some(s) => s,
            };


            for row in rows.into_typed::<(String, i64, i64)>() {
                let row = row.expect("could not convert row");

                if self.position.sequence > row.1 as u64 {
                    continue;
                }

                if self.position.sequence == row.1 as u64 && self.position.sequence_row >= row.2 as u64 {
                    continue;
                }

                println!("got row: {:?}", row);

                self.position.sequence = row.1 as u64;
                self.position.sequence_row = row.2 as u64;
            }
        }

        Ok(())
    }
}