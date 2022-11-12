use std::time::Duration;
use crate::test::docker::DockerMysql;

mod docker;

#[tokio::test]
pub async fn test_integration() {
    let mut mysql = DockerMysql::new(1).await;

    ::tokio::time::sleep(Duration::from_secs(10)).await;

    let pool = mysql.get_pool().await.expect("pool!");

    let row: (String,) = sqlx::query_as("SELECT @@version")
        .fetch_one(&pool).await.expect("...");

    dbg!(row);

    mysql.stop_mysql().await;
}