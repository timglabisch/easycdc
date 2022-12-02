use std::collections::HashMap;
use scylla::Session;
use crate::gtid::format_gtid;

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

        session
            .query("INSERT INTO easycdc.meta (table_name, server_uuid) VALUES (?, ?) IF NOT EXISTS", (
                table_name, format_gtid(server_uuid)
            ))
            .await?;

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