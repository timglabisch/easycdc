create database foo;
use foo;

create table foo1 (
                      id bigint AUTO_INCREMENT,
                      val varchar(255) null,
                      PRIMARY KEY (id)
);

show variables like 'gtid%';
show variables like 'bin%';
show binary logs;

show master status ;