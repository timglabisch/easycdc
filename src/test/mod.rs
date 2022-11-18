use std::time::Duration;
use sqlx::MySqlPool;
use crate::cdc::CdcRunner;
use crate::config::{Config, ConfigTable};
use crate::test::docker::DockerMysql;

mod docker;

pub fn create_config() -> Config {
    Config {
        connection: "mysql://root:password@localhost:33069".to_string(),
        table: vec![
            ConfigTable {
                table: "foo1".to_string(),
                cols: vec![0],
                database: "foo".to_string(),
            }
        ]
    }
}

pub async fn execute_queries(queries: Vec<String>, pool : &MySqlPool) {
    /*
    create database foo;
use foo;

create table foo.foo1 (
                      id bigint AUTO_INCREMENT,
                      val varchar(255) null,
                      PRIMARY KEY (id)
);
     */

    for q in queries {
        sqlx::query(&q).execute(pool).await.expect("could not execute query");
    }
}

pub async fn up(pool : &MySqlPool) {
    execute_queries(vec![
        "create database foo;".to_string(),
        r#"
            create table foo.foo1 (
              id bigint AUTO_INCREMENT,
              val varchar(255) null,
              PRIMARY KEY (id)
            );
        "#.to_string()
    ], pool).await
}

#[tokio::test]
pub async fn test_integration() {
    let mut mysql = DockerMysql::new(1).await;

    ::tokio::time::sleep(Duration::from_secs(10)).await;

    let pool = mysql.get_pool().await.expect("pool!");

    let row: (String,) = sqlx::query_as("SELECT @@version")
        .fetch_one(&pool).await.expect("...");

    up(&pool).await;

    println!("okay, start cdc!");


    sqlx::query(r#"INSERT INTO foo.foo1 (val) VALUES ('dfgdsfg');"#).execute(&pool).await.unwrap();

    let (cdc_control_handle, mut cdc_stream) = CdcRunner::new(create_config()).run().await;

    println!("okay, try to fetch data!");

    loop {
        let item = match cdc_stream.recv().await {
            None => continue,
            Some(s) => s,
        };

        println!("got item! {:?}", item);
        mysql.stop_cdc().await;
    }

    mysql.stop_mysql().await;
}