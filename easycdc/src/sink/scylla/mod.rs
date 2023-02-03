use anyhow::Context;
use md5::Digest;
use scylla::{FromRow, IntoTypedRows, Session, SessionBuilder};
use crate::cdc::{CdcStream, CdcStreamItem, CdcStreamItemGtid, CdcStreamItemValue};
use crate::control_handle::ControlHandleReceiver;
use crate::gtid::{format_gtid, format_gtid_for_table, format_gtid_reverse};
use serde_derive::Deserialize;

pub fn scylla_format_table_name(raw_table_name: &str, uuid: &[u8; 16]) -> String {
    let mut hasher = md5::Md5::default();
    hasher.update(format!("{}{}",raw_table_name, format_gtid_for_table(uuid)));
    format!("s_{}", base16ct::lower::encode_string(&hasher.finalize()))
}

#[derive(Debug, Clone, Deserialize)]
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
            match self.run_inner().await.context("sink scylla panic") {
                Ok(()) => unreachable!(),
                Err(e) => {
                    e.chain().skip(1).for_each(|cause| eprintln!("because: {}", cause));
                    std::process::exit(1);
                }
            }
        });
    }



    async fn run_inner(&mut self) -> Result<(), ::anyhow::Error> {

        let mut session: Session = SessionBuilder::new().known_node(&self.config.connection).build().await?;
        Self::initialize(&mut session).await?;

        let mut current_gtid = None;
        let mut row_sequence_number = 0;
        loop {
            let stream_item = self.cdc_stream.recv().await.context("read stream item")?;

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

            Self::write_to_db(&mut session, value, current_gtid, row_sequence_number).await.context("write to db")?;
        }
    }

    async fn write_to_db(session : &mut Session, value: CdcStreamItemValue, gtid: &CdcStreamItemGtid, sequence_row: i64) -> Result<(), ::anyhow::Error> {

        let gtid_formatted = format_gtid(&gtid.uuid);
        session.query(
            "insert into easycdc.by_id (sequence_number, sequence_row, server_uuid, name_database, name_table, data) values (?, ?, ?, ?, ?, ?);",
            &(gtid.sequence_number as i64, sequence_row,  gtid_formatted.clone(), value.database_name.clone(),  &value.table_name.clone(), value.data.clone())
        ).await?;

        session.query(
            "insert into easycdc.pk_by_id (name_database, name_table, pk, insert_at, deleted_at, last_update_sequence_number, last_update_timestamp) values (?, ?, ?, ?, ?, ?, ?);",
            &(gtid.sequence_number as i64, sequence_row,  gtid_formatted, value.database_name,  value.table_name, value.data)
        ).await?;

        Ok(())
    }

    async fn initialize(session : &mut Session) -> Result<(), ::anyhow::Error> {

        session.query("CREATE KEYSPACE IF NOT EXISTS easycdc WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};", &[]).await?;
        session.query(r#"
            CREATE TABLE IF NOT EXISTS easycdc.by_id (
             sequence_number BIGINT,
             sequence_row BIGINT,
             server_uuid text,
             timestamp TIMESTAMP,
             name_database text,
             name_table text,
             data text,
             PRIMARY KEY (sequence_number, server_uuid, sequence_row, timestamp)
            );
        "#, &[]).await?;
        session.query(r#"
            CREATE MATERIALIZED VIEW easycdc.by_date AS
            SELECT * FROM foo.by_id
            WHERE timestamp IS NOT NULL
              AND sequence_row IS NOT NULL
              AND server_uuid IS NOT NULL
                PRIMARY KEY(timestamp, server_uuid, sequence_number, sequence_row);
        "#, &[]).await?;
        session.query(r#"
            CREATE TABLE IF NOT EXISTS easycdc.pk_by_id (
            name_database text,
            name_table text,
            pk text,
            insert_at: TIMESTAMP,
            deleted_at: TIMESTAMP,
            last_update_sequence_number BIGINT,
            last_update_timestamp TIMESTAMP,
            PRIMARY KEY (pk, name_database, name_table));
        "#, &[]).await?;

        Ok(())
    }
}