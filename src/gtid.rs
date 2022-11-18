pub fn format_gtid(uuid: [u8; 16]) -> String {
    let mut buf = [b'!'; 40];
    ::uuid::Uuid::from_slice(&uuid)
        .expect("invalid uuid")
        .braced()
        .encode_lower(&mut buf)
        .to_string()
}
