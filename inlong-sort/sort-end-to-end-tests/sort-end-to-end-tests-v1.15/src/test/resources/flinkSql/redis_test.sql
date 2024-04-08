CREATE TABLE dim_get
(
    aaa varchar,
    bbb varchar
) WITH (
      'connector' = 'redis-inlong',
      'command' = 'get',
      'host' = 'redis_source',
      'port' = '6379',
      'maxIdle' = '8',
      'minIdle' = '1',
      'maxTotal' = '2',
      'timeout' = '2000'
      );

create table source_get
(
    aaa varchar,
    proctime as procTime()
) with ('connector' = 'datagen', 'rows-per-second' = '1',
      'fields.aaa.kind' = 'sequence', 'fields.aaa.start' = '1', 'fields.aaa.end' = '2'
      );
CREATE TABLE sink_get
(
    aaa varchar,
    bbb varchar,
    PRIMARY KEY (`aaa`) NOT ENFORCED
) WITH (
      'connector' = 'redis-inlong',
      'sink.batch-size' = '1',
      'format' = 'csv',
      'data-type' = 'PLAIN',
      'redis-mode' = 'standalone',
      'host' = 'redis_sink',
      'port' = '6379',
      'maxIdle' = '8',
      'minIdle' = '1',
      'maxTotal' = '2',
      'timeout' = '2000'
      );


insert into sink_get

select concat_ws('_', s.aaa, s.aaa), concat_ws('_', d.bbb, s.aaa)
from source_get s
         left join dim_get for system_time as of s.proctime as d
                   on d.aaa = s.aaa;



CREATE TABLE dim_hget
(

    aaa varchar,
    bbb varchar
) WITH (
      'connector' = 'redis-inlong',
      'command' = 'hget',
      'host' = 'redis_source',
      'port' = '6379',
      'maxIdle' = '8',
      'minIdle' = '1',
      'maxTotal' = '2',
      'timeout' = '2000',
      'additional.key' = '3'
      );

create table source_hget
(
    aaa varchar,
    bbb varchar,
    proctime as procTime()
) with ('connector' = 'datagen', 'rows-per-second' = '1',
      'fields.aaa.kind' = 'sequence', 'fields.aaa.start' = '1', 'fields.aaa.end' = '2'
      );
CREATE TABLE sink_hget
(

    aaa varchar,
    bbb varchar,
    ccc varchar,
    PRIMARY KEY (`aaa`) NOT ENFORCED
) WITH (
      'connector' = 'redis-inlong',
      'sink.batch-size' = '1',
      'format' = 'csv',
      'data-type' = 'HASH',
      'redis-mode' = 'standalone',
      'host' = 'redis_sink',
      'port' = '6379',
      'maxIdle' = '8',
      'minIdle' = '1',
      'maxTotal' = '2',
      'timeout' = '2000'
      );

insert into sink_hget
select '3_3', d.aaa, d.bbb
from source_hget s
         left join dim_hget for system_time as of s.proctime as d
                   on d.aaa = s.aaa and d.bbb = s.bbb;

CREATE TABLE dim_zrevrank
(
    member_test varchar,
    member_rank BIGINT
) WITH (
      'connector' = 'redis-inlong',
      'command' = 'zrevrank',
      'host' = 'redis_source',
      'port' = '6379',
      'maxIdle' = '8',
      'minIdle' = '1',
      'maxTotal' = '2',
      'timeout' = '2000',
      'additional.key' = 'rank'
      );

create table source_zrevrank
(
    member_test varchar,
    proctime as procTime()
) with ('connector' = 'datagen', 'rows-per-second' = '1',
      'fields.member_test.kind' = 'sequence', 'fields.member_test.start' = '1', 'fields.member_test.end' = '3'
      );
CREATE TABLE sink_zrevrank
(

    aaa varchar,
    bbb varchar,
    ccc BIGINT,
    PRIMARY KEY(`aaa`) NOT ENFORCED
) WITH (
      'connector' = 'redis-inlong',
      'sink.batch-size' = '1',
      'format' = 'csv',
      'data-type' = 'HASH',
      'redis-mode' = 'standalone',
      'host' = 'redis_sink',
      'port' = '6379',
      'maxIdle' = '8',
      'minIdle' = '1',
      'maxTotal' = '2',
      'timeout' = '2000'
      );

insert into sink_zrevrank
select 'rank_test', s.member_test, d.member_rank
from source_zrevrank s
         left join dim_zrevrank for system_time as of s.proctime as d
                   on d.member_test = s.member_test;



CREATE TABLE dim_zscore
(

    member_test varchar,
    score double
) WITH (
      'connector' = 'redis-inlong',
      'command' = 'zscore',
      'host' = 'redis_source',
      'port' = '6379',
      'maxIdle' = '8',
      'minIdle' = '1',
      'maxTotal' = '2',
      'timeout' = '2000',
      'additional.key' = 'rank_score'
      );

create table source_zscore
(
    member_test varchar,
    proctime as procTime()
) with ('connector' = 'datagen', 'rows-per-second' = '1',
      'fields.member_test.kind' = 'sequence', 'fields.member_test.start' = '1', 'fields.member_test.end' = '3'
      );
CREATE TABLE sink_zscore
(

    aaa
        varchar,
    bbb
        varchar,
    ccc
        double,
    PRIMARY KEY(`aaa`) NOT ENFORCED
) WITH (
      'connector' = 'redis-inlong',
      'sink.batch-size' = '1',
      'format' = 'csv',
      'data-type' = 'HASH',
      'redis-mode' = 'standalone',
      'host' = 'redis_sink',
      'port' = '6379',
      'maxIdle' = '8',
      'minIdle' = '1',
      'maxTotal' = '2',
      'timeout' = '2000'
      );

insert into sink_zscore
select 'rank_score_test', d.member_test, d.score
from source_zscore s
         left join dim_zscore for system_time as of s.proctime as d
                   on d.member_test = s.member_test;