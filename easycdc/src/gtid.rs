use std::str::FromStr;

pub fn format_gtid(uuid: [u8; 16]) -> String {
    let mut buf = [b'!'; 40];
    ::uuid::Uuid::from_slice(&uuid)
        .expect("invalid uuid")
        .braced()
        .encode_lower(&mut buf)
        .to_string()
}

pub fn format_gtid_reverse(uuid: &str) -> [u8; 16] {
    let mut buf : [u8; 16] = [0; 16];
    let mut data = ::uuid::Uuid::from_str(uuid).expect("invalid uuid");
    let data_borrow = data.as_bytes();

    buf.copy_from_slice(&data_borrow[..]);
    buf
}
