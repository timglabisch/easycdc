use mysql::{
    binlog::{events::TableMapEvent, row::BinlogRow, value::BinlogValue},
    Value,
};

use crate::{config::ConfigTable, tablemap::TableInfo};

pub struct SinkConsoleJsonValue<'a> {
    pub table_map_event: &'a TableMapEvent<'a>,
    pub table_config: &'a ConfigTable,
    before: Vec<Option<&'a BinlogValue<'a>>>,
    after: Vec<Option<&'a BinlogValue<'a>>>,
}

impl<'a> SinkConsoleJsonValue<'a> {
    pub fn match_row(
        table_config: &ConfigTable,
        row: &'a Option<BinlogRow>,
    ) -> Vec<Option<&'a BinlogValue<'a>>> {
        match row {
            None => vec![],
            Some(before) => table_config
                .cols
                .iter()
                .map(|col| before.as_ref(col.clone() as usize))
                .collect::<Vec<_>>(),
        }
    }

    pub fn from_row(
        table_config: &'a ConfigTable,
        table_map_event: &'a TableMapEvent<'a>,
        before: &'a Option<BinlogRow>,
        after: &'a Option<BinlogRow>,
    ) -> Self {
        Self {
            table_config,
            table_map_event,
            before: Self::match_row(table_config, &before),
            after: Self::match_row(table_config, &after),
        }
    }
}

impl<'a> SinkConsoleJsonValue<'a> {

    pub fn get_table_name(&self) -> String {
        format!("{}.{}", self.table_map_event.database_name(), self.table_map_event.table_name())
    }

    pub fn to_json(&self) -> serde_json::Value {
        let db = self.table_map_event.database_name();
        let table = self.table_map_event.table_name();
        let before = self
            .before
            .iter()
            .map(|v| Self::binlog_value_to_json(v))
            .collect::<Vec<_>>();
        let after = self
            .after
            .iter()
            .map(|v| Self::binlog_value_to_json(v))
            .collect::<Vec<_>>();

        serde_json::json!({
            "db": db,
            "table": table,
            "before": before,
            "after": after,
        })
    }

    pub fn binlog_value_to_json(value: &Option<&BinlogValue>) -> serde_json::Value {
        match value {
            None => serde_json::Value::Null,
            Some(v) => match v {
                BinlogValue::Value(v) => match v {
                    Value::Int(v) => serde_json::json!(v),
                    Value::Double(v) => serde_json::json!(v),
                    Value::Float(v) => serde_json::json!(v),
                    Value::UInt(v) => serde_json::json!(v),
                    Value::NULL => serde_json::Value::Null,
                    Value::Bytes(ref bytes) => {
                        if bytes.len() <= 8 {
                            serde_json::json!(String::from_utf8_lossy(&*bytes))
                        } else {
                            serde_json::json!(String::from_utf8_lossy(&bytes[..8]))
                        }
                    }
                    _ => serde_json::Value::Null,
                },
                _ => serde_json::Value::Null,
            },
        }
    }
}
