use mysql::{binlog::value::BinlogValue, Value};

use crate::tablemap::TableInfo;

struct SinkConsoleJsonValue<'a> {
    table_info: TableInfo<'a>,
    before: Vec<BinlogValue<'a>>,
    after: Vec<BinlogValue<'a>>,
}

impl<'a> SinkConsoleJsonValue<'a> {
    pub fn from_row() {
        
    }
}

impl <'a> SinkConsoleJsonValue<'a> {
    pub fn to_json(&self) -> serde_json::Value {

        let db = self.table_info.table_map_event.database_name(); 
        let table = self.table_info.table_map_event.table_name();
        let before = self.before.iter().map(|v| Self::binlog_value_to_json(v)).collect::<Vec<_>>();
        let after = self.before.iter().map(|v| Self::binlog_value_to_json(v)).collect::<Vec<_>>();


        serde_json::json!({
            "db": db,
            "table": table,
            "before": before,
            "after": after,
        })
    }

    pub fn binlog_value_to_json(value : &BinlogValue) -> serde_json::Value {
        match value {
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
        }
    }
}