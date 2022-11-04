use std::collections::HashMap;

type TableId = u32;

struct TableInfoCdc {
    table_name: String,
    column: usize
}

enum TableInfo {
    ShouldCdc(TableInfoCdc),
    NoCdc,
}

struct TableMap {
    map_id_name: ::fnv::FnvHashMap<TableId, TableInfo>,
}