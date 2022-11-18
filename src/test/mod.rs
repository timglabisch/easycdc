use std::time::Duration;
use sqlx::MySqlPool;
use crate::cdc::{CdcRunner, CdcStreamItem};
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

pub enum TestFlow {
    Query(String),
    Expect(String),
}

#[tokio::test]
pub async fn test_integration() {
    test_runnter(vec![
       TestFlow::Query("INSERT INTO foo.foo1 (val) VALUES ('dfgdsfg');".to_string()),
       TestFlow::Expect(r#"{"after":[1],"before":[],"db":"foo","table":"foo1"}"#.to_string()),
       TestFlow::Query("INSERT INTO foo.foo1 (val) VALUES ('abcd');".to_string()),
       TestFlow::Expect(r#"{"after":[2],"before":[],"db":"foo","table":"foo1"}"#.to_string()),
       TestFlow::Query("UPDATE foo.foo1 SET val = 'updated' WHERE id = 2;".to_string()),
       TestFlow::Expect(r#"{"after":[2],"before":[2],"db":"foo","table":"foo1"}"#.to_string()),
       TestFlow::Query("UPDATE foo.foo1 SET val = 'updated!'".to_string()),
       TestFlow::Expect(r#"{"after":[1],"before":[1],"db":"foo","table":"foo1"}"#.to_string()),
       TestFlow::Expect(r#"{"after":[2],"before":[2],"db":"foo","table":"foo1"}"#.to_string()),
       TestFlow::Query("UPDATE foo.foo1 SET id = 3 WHERE id = 1;".to_string()),
       TestFlow::Expect(r#"{"after":[3],"before":[1],"db":"foo","table":"foo1"}"#.to_string()),
    ]).await;
}

pub async fn test_runnter(flow_items: Vec<TestFlow>) {
    let mut mysql = DockerMysql::new(1).await;

    ::tokio::time::sleep(Duration::from_secs(10)).await;

    let pool = mysql.get_pool().await.expect("pool!");

    let row: (String,) = sqlx::query_as("SELECT @@version")
        .fetch_one(&pool).await.expect("...");

    up(&pool).await;

    println!("okay, start cdc!");


    let (cdc_control_handle, mut cdc_stream) = CdcRunner::new(create_config()).run().await;

    for flow_item in flow_items {
        match flow_item {
            TestFlow::Query(q) => {
                sqlx::query(&q).execute(&pool).await.unwrap();
            },
            TestFlow::Expect(expect) => {
                let item = match cdc_stream.recv().await {
                    None => continue,
                    Some(s) => s,
                };

                match item {
                    CdcStreamItem::Value(v) => {
                        if expect != v {
                            mysql.stop_cdc().await;
                            mysql.stop_mysql().await;
                        }
                        assert_eq!(expect, v)
                    }
                };
            }
        }
    }

    mysql.stop_cdc().await;
    mysql.stop_mysql().await;
}