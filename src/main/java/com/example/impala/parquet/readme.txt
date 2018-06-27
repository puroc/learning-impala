[创建目录]
hdfs dfs -mkdir -p /user/hdfs/sample_data/parquet

[赋予权限]
sudo -u hdfs hadoop fs -chown -R impala:supergroup /user/hdfs/sample_data

[删除目录]
hdfs dfs -rm -r /user/hdfs/sample_data/parquet

[上传文件]
hdfs dfs -put -f device /user/hdfs/sample_data/parquet
hdfs dfs -put -f metrics /user/hdfs/sample_data/parquet

[查看文件]
hdfs dfs -ls /user/hdfs/sample_data/parquet

[impala建表，不带分区]（创建表之后，还需要通过下面的alter语句添加分区）
DROP TABLE IF EXISTS device_parquet;
CREATE EXTERNAL TABLE device_parquet
(
   deviceId STRING,
   deviceName STRING,
   orgId STRING
)

  STORED AS PARQUET
  LOCATION '/user/hdfs/sample_data/parquet/device';

[impala建表，带分区]
DROP TABLE IF EXISTS metrics_parquet;
CREATE EXTERNAL TABLE metrics_parquet
(
   deviceId STRING,
   reading BIGINT,
   time STRING
)
  partitioned by (year string)
  STORED AS PARQUET
  LOCATION '/user/hdfs/sample_data/parquet/metrics';

[添加表分区]
alter table metrics_parquet add partition (year="2017");
alter table metrics_parquet add partition (year="2018");

[删除分区]
alter table metrics_parquet drop partition (year="2017");
alter table metrics_parquet drop partition (year="2018");

[查看表分区]
show partitions metrics_parquet;

[不指定分区查询数据]
select
  T_3C75F1.`deviceId`,
  year(T_3C75F1.`time`),
  month(T_3C75F1.`time`),
  sum(T_3C75F1.`reading`),
  count(1)
from (select device_parquet.deviceId,reading,metrics_parquet.time as time from device_parquet,metrics_parquet where device_parquet.deviceId=metrics_parquet.deviceId) as `T_3C75F1`
group by
  T_3C75F1.`deviceId`,
  year(T_3C75F1.`time`),
  month(T_3C75F1.`time`);

耗时：device表50条，metrics表1亿条（261M）执行上面的查询语句，耗时平均135秒

[指定分区查询数据]
select
  T_3C75F1.`deviceId`,
  year(T_3C75F1.`time`),
  month(T_3C75F1.`time`),
  sum(T_3C75F1.`reading`),
  count(1)
from (select device_parquet.deviceId,reading,metrics_parquet.time as time from device_parquet,metrics_parquet where device_parquet.deviceId=metrics_parquet.deviceId and year='2017') as `T_3C75F1`
group by
  T_3C75F1.`deviceId`,
  year(T_3C75F1.`time`),
  month(T_3C75F1.`time`);

耗时：device表50条，metrics表1亿条（261M）执行上面的查询语句，耗时平均96秒

[查询多个分区的数据]
select
T_3C75F1.`deviceId`,
year(T_3C75F1.`time`),
month(T_3C75F1.`time`),
sum(T_3C75F1.`reading`),
count(1)
from (select device_parquet.deviceId,reading,metrics_parquet.time as time from device_parquet,metrics_parquet where device_parquet.deviceId=metrics_parquet.deviceId and year in ('2017','2018')) as `T_3C75F1`
group by
T_3C75F1.`deviceId`,
year(T_3C75F1.`time`),
month(T_3C75F1.`time`);

[刷新数据]（hdfs中数据发生变化时，需要执行以下命令更新impala）
refresh device_parquet;
refresh metrics_parquet;




