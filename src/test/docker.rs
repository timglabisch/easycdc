use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use anyhow::Context;
use mysql::Pool;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;
use tokio::process::Child;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

pub struct DockerMysql {
    pub id: usize,
    pub child: Option<Child>,
    pub pool: Option<MySqlPool>
}

impl DockerMysql {
    pub async fn new(id : usize) -> Self {
        let mut instance = Self {
            id,
            child: None,
            pool: None,
        };

        instance.start_mysql().await;

        loop {
            match instance.get_pool().await {
                Err(e) => {
                    println!("wait for server");
                    ::tokio::time::sleep(Duration::from_millis(100)).await
                },
                Ok(_) => break,
            };
        }

        instance
    }

    pub async fn get_pool(&mut self) -> Result<MySqlPool, ::anyhow::Error> {

        match self.pool {
            Some(ref p) => return Ok(p.clone()),
            None => {}
        };

        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect("mysql://root:password@localhost:33069").await.context("pool...")?;

        self.pool = Some(pool.clone());

        Ok(pool)
    }

    async fn start_mysql(&mut self) {

        if self.child.is_some() {
            panic!("could not start twice.");
        }

        let mut docker = ::tokio::process::Command::new("docker");
        let dir = ::std::env::current_dir().expect("current dir...").to_string_lossy().into_owned();
        let docker = docker.args(&[
                "run",
                "--rm",
                "--name", &format!("easycdc_integration_{}", self.id),
                "-e", "MYSQL_ROOT_PASSWORD=password",
                "-e", "MYSQL_ROOT_HOST=%",
                "-p", "33069:3306",
                "--mount", &format!("type=bind,src={dir}/docker/mysql/my.cnf,dst=/etc/my.cnf"),
                "mysql/mysql-server:latest"
            ]);

        self.child = Some(docker.spawn().expect("could not create test container"));
    }

    pub async fn stop_mysql(&mut self) {
        let mut docker = ::tokio::process::Command::new("docker");
        let docker = docker.args(&[
            "kill",
            &format!("easycdc_integration_{}", self.id),
        ]);

        docker.output().await.expect("could not kill container");

        self.child = None;
    }
}