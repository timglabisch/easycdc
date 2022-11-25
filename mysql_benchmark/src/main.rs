use anyhow::Context;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, pool};

#[tokio::main]
async fn main() -> Result<(), ::anyhow::Error> {
    let workers = 5;

    let pool = MySqlPoolOptions::new()
        .max_connections(workers + 5)
        .connect("mysql://root:password@localhost:33069")
        .await
        .context("pool...")?;


    sqlx::query("create database if not exists _easycdc_bench;")
        .execute(&pool)
        .await
        .expect("could not execute query");

    let mut jhs = vec![];
    for i in 0..workers {
        let pool_copy = pool.clone();
        jhs.push(::tokio::spawn(async move {
            run_update(i, pool_copy).await;
        }));
    }

    for jh in jhs {
        jh.await.expect("jh failed.")
    }

    Ok(())
}

pub async fn run_update(worker_id: u32, pool : MySqlPool) {
    sqlx::query(format!("
        drop table if exists _easycdc_bench.worker_{worker_id};
    ").trim())
        .execute(&pool)
        .await
        .expect("could not execute query #1");

    sqlx::query(format!("
        create table _easycdc_bench.worker_{worker_id} (
                  id bigint AUTO_INCREMENT,
                  val varchar(255) null,
                  PRIMARY KEY (id)
        );
    ").trim())
        .execute(&pool)
        .await
        .expect("could not execute query #2");


    sqlx::query(format!("
        INSERT INTO _easycdc_bench.worker_{worker_id} (val) VALUES (null);
    ").trim())
        .execute(&pool)
        .await
        .expect("could not execute query #3");

    let mut i = 0;
    loop {
        i = i + 1;
        sqlx::query(format!("
            UPDATE _easycdc_bench.worker_{worker_id} SET val = ? WHERE id = 1;
        ").trim())
            .bind(format!("value {i}"))
            .execute(&pool)
            .await
            .expect("could not execute query #4");

        if i % 1000 == 0 {
            println!("1000 updates")
        }
    }
}
