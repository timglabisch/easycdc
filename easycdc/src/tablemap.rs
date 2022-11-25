use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;

use fnv::FnvHashMap;
use mysql::binlog::events::TableMapEvent;

use crate::config::{Config, ConfigTable};

type TableId = u64;

pub struct TableInfo<'a> {
    pub table_map_event: TableMapEvent<'a>,
    pub table_config: Option<ConfigTable>,
}

#[derive(Hash, PartialEq, Eq)]
pub struct DatabaseTable {
    database: String,
    table: String,
}

pub struct TableMap<'a> {
    map_name_shouldcdc: ::fnv::FnvHashMap<DatabaseTable, ConfigTable>,
    map_id_info: ::fnv::FnvHashMap<TableId, TableInfo<'a>>,
}

impl<'a> TableMap<'a> {
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

    pub fn get_cdc_info(&self, table_id: &TableId) -> Option<&TableInfo> {
        self.map_id_info.get(table_id)
    }

    pub fn record_table_map_event(&mut self, event: &TableMapEvent) {
        match self.map_id_info.entry(event.table_id()) {
            Occupied(mut entry) => {
                entry.get_mut().table_map_event = event.clone().into_owned();
            }
            Vacant(mut entry) => {
                let table_config = self
                    .map_name_shouldcdc
                    .get(&DatabaseTable {
                        table: event.table_name().to_string(),
                        database: event.database_name().to_string(),
                    })
                    .map(|x| x.clone());

                entry.insert(TableInfo {
                    table_map_event: event.clone().into_owned(),
                    table_config,
                });
            }
        };
    }
}
