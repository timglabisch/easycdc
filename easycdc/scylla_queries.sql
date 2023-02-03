CREATE KEYSPACE IF NOT EXISTS foo WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};

drop materialized view foo.by_date;
drop table foo.by_id;

CREATE TABLE IF NOT EXISTS foo.by_id (
                                         sequence_number BIGINT,
                                         sequence_row BIGINT,
                                         server_uuid text,
                                         timestamp TIMESTAMP,
                                         name_database text,
                                         name_table text,
                                         data text,
                                         PRIMARY KEY (sequence_number, server_uuid, sequence_row, timestamp));

CREATE MATERIALIZED VIEW foo.by_date AS
SELECT * FROM foo.by_id
WHERE timestamp IS NOT NULL
  AND sequence_row IS NOT NULL
  AND server_uuid IS NOT NULL
    PRIMARY KEY(timestamp, server_uuid, sequence_number, sequence_row);

CREATE TABLE IF NOT EXISTS foo.pk_by_id (
                                            name_database text,
                                            name_table text,
                                            pk text,
                                            insert_at: TIMESTAMP,
                                            deleted_at: TIMESTAMP,
                                            last_update_sequence_number BIGINT,
                                            last_update_timestamp TIMESTAMP,
                                            PRIMARY KEY (pk, name_database, name_table));

truncate foo.by_id;
insert into foo.by_id (sequence_number, server_uuid, sequence_row, timestamp, data) values (1, 'a', 1, 1673618418, 'data1 - first');
insert into foo.by_id (sequence_number, server_uuid, sequence_row, timestamp, data) values (1, 'a', 1, 1673618419, 'data1 - later');
insert into foo.by_id (sequence_number, server_uuid, sequence_row, timestamp, data) values (2, 'a', 1, 1673618419, 'data2');
insert into foo.by_id (sequence_number, server_uuid, sequence_row, timestamp, data) values (2, 'a', 2, 1673618419, 'data2');

select * from foo.by_id where sequence_number = 1;
select * from foo.by_date where timestamp = 1673618419;