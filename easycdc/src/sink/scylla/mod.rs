use scylla::{FromRow, IntoTypedRows, Session, SessionBuilder};
use crate::cdc::{CdcStream, CdcStreamItem, CdcStreamItemGtid, CdcStreamItemValue};
use crate::control_handle::ControlHandleReceiver;
use crate::gtid::{format_gtid, format_gtid_reverse};
use crate::sink::scylla::scylla_table_mapper::ScyllaTableMapper;

mod scylla_table_mapper;

pub struct ConfigSinkScylla {
    connection: String,
}

pub struct SinkScylla {
    config: ConfigSinkScylla,
    control_handle_receiver: ControlHandleReceiver,
    cdc_stream: CdcStream,
}

impl SinkScylla {
    pub fn new(
        config: ConfigSinkScylla,
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



    async fn run_inner(&mut self) -> Result<(), ::anyhow::Error> {

        let mut session: Session = SessionBuilder::new().known_node(&self.config.connection).build().await?;
        Self::initialize(&mut session).await?;
        let mut tablemap = Self::build_scylla_tablemap(&mut session).await?;


        let mut current_gtid = None;
        let mut row_sequence_number = 0;
        loop {
            let stream_item = self.cdc_stream.recv().await?;

            let value = match stream_item {
                CdcStreamItem::Gtid(gtid) => {
                    row_sequence_number = 0;
                    current_gtid = Some(gtid);
                    continue;
                },
                CdcStreamItem::Value(value) => value,
            };

            row_sequence_number = row_sequence_number + 1;

            let current_gtid = current_gtid.as_ref().expect("there must be a current gtid!");

            tablemap.insert(&mut session, &value.table_name, current_gtid.uuid).await?;

            Self::write_to_db(&mut session, row_sequence_number, value, current_gtid).await?;
        }
    }

    async fn write_to_db(session : &mut Session, row_sequence_number: i32, value: CdcStreamItemValue, gtid: &CdcStreamItemGtid) -> Result<(), ::anyhow::Error> {

        let query = format!(
            "INSERT INTO easycdc.stream_{}_{} (sequence_number, row_sequence_number, data) VALUES (?, ?, ?) IF NOT EXISTS",
            value.table_name,
            format_gtid(gtid.uuid)
        );

        session.query(query, &(gtid.sequence_number as i64, row_sequence_number, value.data)).await?;

        Ok(())
    }

    async fn initialize(session : &mut Session) -> Result<(), ::anyhow::Error> {

        session.query("CREATE KEYSPACE IF NOT EXISTS easycdc WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

        session
            .query(
                "CREATE TABLE IF NOT EXISTS easycdc.meta (
  table_name text,
  server_uuid text,
  PRIMARY KEY (table_name, server_uuid));",
                &[],
            )
            .await?;

        Ok(())
    }

    async fn build_scylla_tablemap(session : &mut Session) -> Result<ScyllaTableMapper, ::anyhow::Error> {
        #[derive(Debug, FromRow)]
        struct RowData {
            table_name: String,
            server_uuid: String,
        }

        let mut tablemap = ScyllaTableMapper::new();

        if let Some(rows) = session.query("SELECT table_name, server_uuid FROM easycdc.meta", &[]).await?.rows {
            for row_data in rows.into_typed::<RowData>() {
                let row_data = row_data?;

                tablemap.insert_mem(&row_data.table_name, format_gtid_reverse(&row_data.server_uuid));
            }
        }

        Ok(tablemap)
    }
}