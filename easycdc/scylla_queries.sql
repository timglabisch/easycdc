CREATE KEYSPACE IF NOT EXISTS easycdc WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE IF NOT EXISTS easycdc.sequence (
    sequence_number BIGINT,
    sequence_row BIGINT,
    server_uuid text,
    timestamp TIMESTAMP,
    name_database text,
    name_table text,
    data text,
    PRIMARY KEY (sequence_number, server_uuid, sequence_row)
);

select data from easycdc.sequence where server_uuid = '1' and name_database = '1' and name_table = '2' and sequence_number > 1;

CREATE TABLE IF NOT EXISTS easycdc.by_pk (
 name_database text,
 name_table text,
 pk text,
 insert_at TIMESTAMP,
 deleted_at TIMESTAMP,
 last_update_sequence_number BIGINT,
 last_update_timestamp TIMESTAMP,
 PRIMARY KEY (pk, name_database, name_table)
 );


CREATE MATERIALIZED VIEW IF NOT EXISTS easycdc.by_date AS
SELECT * FROM easycdc.by_pk
WHERE last_update_timestamp IS NOT NULL
  AND name_database IS NOT NULL
  AND name_table IS NOT NULL
PRIMARY KEY(name_database, name_table, last_update_timestamp, pk);

select * from easycdc.by_date where name_database = 'foo' and name_table = 'foo1' and last_update_timestamp > 10;
select * from easycdc.by_pk where name_database = 'foo' and name_table = 'foo1' and pk = '[1]'