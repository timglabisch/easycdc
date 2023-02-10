use futures::StreamExt;
use easycdc_scylla_subscriber::{CdcStream, CdcStreamPosition};

#[tokio::main]
async fn main() -> Result<(), ::anyhow::Error> {
    let mut stream = CdcStream::new(CdcStreamPosition {
        database_name: "foo".to_string(),
        database_table: "foo1".to_string(),
        server_uuid: "{db271012-a921-11ed-bb0d-0242ac130005}".to_string(),
        sequence: 0,
        sequence_row: 0,
    }).await?;

    while let Some(item) = stream.next().await {
        println!("item {:?}", item);
    }

    Ok(())
}
