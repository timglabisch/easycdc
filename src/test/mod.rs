use std::collections::HashMap;
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
    QueryTransaction(usize, String),
    Expect(String),
    ExpectNoEvent,
    ExpectGtidSequence(u64),
}

#[tokio::test]
pub async fn test_insert_update() {
    test_runnter(vec![
        TestFlow::Query("INSERT INTO foo.foo1 (val) VALUES ('dfgdsfg');".to_string()),
        TestFlow::Expect(r#"{"after":[1],"before":[],"db":"foo","table":"foo1"}"#.to_string()),
        TestFlow::ExpectGtidSequence(3),
        TestFlow::Query("INSERT INTO foo.foo1 (val) VALUES ('abcd');".to_string()),
        TestFlow::Expect(r#"{"after":[2],"before":[],"db":"foo","table":"foo1"}"#.to_string()),
        TestFlow::ExpectGtidSequence(4),
        TestFlow::Query("UPDATE foo.foo1 SET val = 'updated' WHERE id = 2;".to_string()),
        TestFlow::Expect(r#"{"after":[2],"before":[2],"db":"foo","table":"foo1"}"#.to_string()),
        TestFlow::ExpectGtidSequence(5),
        TestFlow::Query("UPDATE foo.foo1 SET val = 'updated!'".to_string()),
        TestFlow::Expect(r#"{"after":[1],"before":[1],"db":"foo","table":"foo1"}"#.to_string()),
        TestFlow::Expect(r#"{"after":[2],"before":[2],"db":"foo","table":"foo1"}"#.to_string()),
        TestFlow::ExpectGtidSequence(6),
        TestFlow::Query("UPDATE foo.foo1 SET id = 3 WHERE id = 1;".to_string()),
        TestFlow::Expect(r#"{"after":[3],"before":[1],"db":"foo","table":"foo1"}"#.to_string()),
        TestFlow::ExpectGtidSequence(7),
    ]).await;
}

#[tokio::test]
pub async fn test_transaction_commit() {
    test_runnter(vec![
        TestFlow::QueryTransaction(1, "BEGIN;".to_string()),
        TestFlow::ExpectNoEvent,
        TestFlow::ExpectGtidSequence(2),
        TestFlow::QueryTransaction(1, "INSERT INTO foo.foo1 (val) VALUES ('dfgdsfg');".to_string()),
        TestFlow::ExpectNoEvent,
        TestFlow::ExpectGtidSequence(2),
        TestFlow::QueryTransaction(1, "INSERT INTO foo.foo1 (val) VALUES ('dfgdsfg');".to_string()),
        TestFlow::ExpectNoEvent,
        TestFlow::ExpectGtidSequence(2),
        TestFlow::QueryTransaction(1, "COMMIT;".to_string()),
        TestFlow::Expect(r#"{"after":[1],"before":[],"db":"foo","table":"foo1"}"#.to_string()),
        TestFlow::ExpectGtidSequence(3),
        TestFlow::Expect(r#"{"after":[2],"before":[],"db":"foo","table":"foo1"}"#.to_string()),
        TestFlow::ExpectGtidSequence(3),
    ]).await;
}

#[tokio::test]
pub async fn test_transaction_rollback() {
    test_runnter(vec![
        TestFlow::QueryTransaction(1, "BEGIN;".to_string()),
        TestFlow::ExpectNoEvent,
        TestFlow::ExpectGtidSequence(2),
        TestFlow::QueryTransaction(1, "INSERT INTO foo.foo1 (val) VALUES ('dfgdsfg');".to_string()),
        TestFlow::ExpectNoEvent,
        TestFlow::ExpectGtidSequence(2),
        TestFlow::QueryTransaction(1, "ROLLBACK;".to_string()),
        TestFlow::ExpectNoEvent,
        TestFlow::ExpectGtidSequence(2),
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

    let mut transactions = HashMap::new();

    let mut sequence_number = 0;

    'mainloop: for flow_item in flow_items {
        match flow_item {
            TestFlow::QueryTransaction(id, q) if q.trim_end_matches(";").trim() == "BEGIN" => {
                let con = pool.begin().await.expect("could not fetch transaction");
                transactions.insert(id, con);
            },
            TestFlow::QueryTransaction(id, q) if q.trim_end_matches(";").trim() == "COMMIT" => {
                transactions.remove_entry(&id).expect("could bot get transaction").1.commit().await.expect("could not commit!");
            },
            TestFlow::QueryTransaction(id, q) if q.trim_end_matches(";").trim() == "ROLLBACK" => {
                transactions.remove_entry(&id).expect("could bot get transaction").1.rollback().await.expect("could not commit!");
            },
            TestFlow::QueryTransaction(id, q) => {
                let transaction = transactions.get_mut(&id).expect("could bot get transaction");
                sqlx::query(&q).execute(transaction).await.unwrap();
            },
            TestFlow::Query(q) => {
                sqlx::query(&q).execute(&pool).await.unwrap();
            },
            TestFlow::ExpectNoEvent => {
                // we need to wait a bit for events ...
                ::tokio::time::sleep(Duration::from_millis(200)).await;

                let v = loop {
                     match cdc_stream.try_recv() {
                        Ok(CdcStreamItem::Value(v)) => break v,
                        Ok(CdcStreamItem::Gtid(gtid)) => {
                            // just consume gtid's
                            sequence_number = gtid.sequence_number;
                            continue;
                        },
                        Err(_) => continue 'mainloop,
                    };
                };

                mysql.stop_cdc().await;
                mysql.stop_mysql().await;
                assert_eq!("", v)
            }
            TestFlow::Expect(expect) => {
                loop {
                    match cdc_stream.recv().await {
                        None => continue,
                        Some(CdcStreamItem::Value(v)) => {
                            if expect != v {
                                mysql.stop_cdc().await;
                                mysql.stop_mysql().await;
                            }
                            assert_eq!(expect, v);
                            break;
                        },
                        Some(CdcStreamItem::Gtid(gtid)) => {
                            // just consume gtid's
                            sequence_number = gtid.sequence_number;
                            continue;
                        },
                    };
                };
            },
            TestFlow::ExpectGtidSequence(expected_sequence) => {
                assert_eq!(expected_sequence, sequence_number);
            }
        }
    }

    mysql.stop_cdc().await;
    mysql.stop_mysql().await;
}