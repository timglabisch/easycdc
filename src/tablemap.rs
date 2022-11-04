use std::collections::HashMap;

use fnv::FnvHashMap;
use mysql::binlog::events::TableMapEvent;

use crate::config::{Config, ConfigTable};

type TableId = u64;

struct TableInfoCdc {
    table: String,
    database: String,
    column: u32,
}

enum TableInfo {
    ShouldCdc(TableInfoCdc),
    NoCdc,
}

#[derive(Hash, PartialEq, Eq)]
pub struct DatabaseTable {
    database: String,
    table: String,
}

struct TableMap {
    map_name_shouldcdc: ::fnv::FnvHashMap<DatabaseTable, ConfigTable>,
    map_id_info: ::fnv::FnvHashMap<TableId, TableInfo>,
}

impl TableMap {
    pub fn from_config(config: &Config) -> Self {
        let map_name_shouldcdc = {
            let mut map = FnvHashMap::default();
            for c in &config.table {
                map.insert(
                    DatabaseTable {
                        database: c.database.clone(),
                        table: c.table.clone(),
                    },
                    c.clone(),
                );
            }
            map
        };

        Self {
            map_name_shouldcdc,
            map_id_info: FnvHashMap::default(),
        }
    }

    pub fn record_table_map_event(&mut self, event: &TableMapEvent) {
        match self.map_id_info.get(&event.table_id()) {
            Some(s) => return,
            None => {}
        };

        let config_table = self.map_name_shouldcdc.get(&DatabaseTable {
            table: event.table_name().to_string(),
            database: event.database_name().to_string(),
        });

        self.map_id_info.insert(event.table_id(), {
            match config_table {
                None => TableInfo::NoCdc,
                Some(config_table) => TableInfo::ShouldCdc(TableInfoCdc {
                    table: event.table_name().to_string(),
                    database: event.database_name().to_string(),
                    column: config_table.col,
                }),
            }
        });
    }
}
