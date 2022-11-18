use std::time::Duration;
use crate::cdc::CdcRunner;
use crate::config::{Config, ConfigTable};
use crate::test::docker::DockerMysql;

mod docker;

pub fn create_config() -> Config {
    Config {
        connection: "mysql://root:password@localhost:33069".to_string(),
        table: vec![
            ConfigTable {
                table: "foo".to_string(),
                cols: vec![0],
                database: "foo".to_string(),
            }
        ]
    }
}

#[tokio::test]
pub async fn test_integration() {
    let mut mysql = DockerMysql::new(1).await;

    ::tokio::time::sleep(Duration::from_secs(10)).await;

    let pool = mysql.get_pool().await.expect("pool!");

    let row: (String,) = sqlx::query_as("SELECT @@version")
        .fetch_one(&pool).await.expect("...");

    let (cdc_control_handle, mut cdc_stream) = CdcRunner::new(create_config()).run().await;

    loop {
        let item = match cdc_stream.recv().await {
            None => continue,
            Some(s) => s,
        };

        println!("got item! {:?}", item);
    }

    mysql.stop_mysql().await;
}