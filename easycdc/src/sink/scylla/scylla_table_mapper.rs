use std::collections::HashMap;
use anyhow::Context;
use scylla::Session;
use crate::gtid::{format_gtid, format_gtid_for_table, format_gtid_reverse};

pub struct ScyllaTableMapper {
    map: HashMap<(String, [u8; 16]), ()>
}

impl ScyllaTableMapper {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub async fn insert(
        &mut self,
        session: &mut Session,
        table_name: &str,
        server_uuid: [u8; 16],
    ) -> Result<(), ::anyhow::Error> {
        if self.inner_exists(table_name, server_uuid) {
            return Ok(());
        }

        {
            let query = format!("CREATE TABLE IF NOT EXISTS easycdc.stream_{}_{} (
  sequence_number BIGINT,
  row_sequence_number INT,
  data text,
  PRIMARY KEY (sequence_number));", table_name, format_gtid_for_table(server_uuid));

            println!("{}", query);
            session
                .query(query, &[])
                .await.context("create stream table")?;

        }

        session
            .query("INSERT INTO easycdc.meta (table_name, server_uuid) VALUES (?, ?) IF NOT EXISTS", (
                table_name, format_gtid_for_table(server_uuid)
            ))
            .await.context("insert into meta")?;

        self.insert_mem(table_name, server_uuid);

        Ok(())
    }

    fn inner_exists(&self, table_name: &str, server_uuid: [u8; 16]) -> bool {
        self.map.get(&(table_name.to_string(), server_uuid)).is_some()
    }

    pub fn insert_mem(&mut self, table_name: &str, server_uuid: [u8; 16]) {
        self.map.insert((table_name.to_string(), server_uuid), ());
    }
}