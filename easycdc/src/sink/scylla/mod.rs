use scylla::{FromRow, IntoTypedRows, Session, SessionBuilder};
use crate::cdc::CdcStream;
use crate::control_handle::ControlHandleReceiver;
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
        Self::build_scylla_tablemap(&mut session).await?;


        loop {
            let _ = self.cdc_stream.recv().await?;
        }
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

                tablemap.insert_mem(&row_data.table_name, &row_data.server_uuid);
            }
        }

        Ok(tablemap)
    }
}