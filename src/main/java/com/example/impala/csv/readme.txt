[创建目录]
hdfs dfs -mkdir -p /user/hdfs/sample_data/csv/device
hdfs dfs -mkdir -p /user/hdfs/sample_data/csv/metrics

[赋予权限]
sudo -u hdfs hadoop fs -chown -R impala:supergroup /user/hdfs/sample_data
sudo -u hdfs hadoop fs -chmod -R 777 /user/hdfs/sample_data

[删除目录]
hdfs dfs -rm -r /user/hdfs/sample_data/csv

[上传文件]
hdfs dfs -put -f device.csv /user/hdfs/sample_data/csv/device
hdfs dfs -put -f metrics.csv /user/hdfs/sample_data/csv/metrics

[查看文件]
hdfs dfs -ls /user/hdfs/sample_data/csv/device

[impala建表]
DROP TABLE IF EXISTS device;
CREATE EXTERNAL TABLE device
(
   deviceId STRING,
   deviceName STRING,
   orgId INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hdfs/sample_data/csv/device';

DROP TABLE IF EXISTS metrics;
CREATE EXTERNAL TABLE metrics
(
   deviceId STRING,
   reading INT,
   time STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hdfs/sample_data/csv/metrics';

[查询数据]
select device.deviceId,reading,metrics.time as time from device,metrics where device.deviceId=metrics.deviceId limit 10;

select
  T_3C75F1.`deviceId`,
  year(T_3C75F1.`time`),
  month(T_3C75F1.`time`),
  sum(T_3C75F1.`reading`),
  count(1)
from (select device.deviceId,reading,metrics.time as time from device,metrics where device.deviceId=metrics.deviceId) as `T_3C75F1`
group by
  T_3C75F1.`deviceId`,
  year(T_3C75F1.`time`),
  month(T_3C75F1.`time`);

[加载数据]
LOAD DATA INPATH '/user/hdfs/sample_data/csv/device' INTO TABLE device;

[问题]

1、执行查询时抛异常：
Memory limit exceeded: The memory limit is set too low to initialize spilling operator (id=2). The minimum required memory to spill this operator is 136.00 MB.
Error occurred on backend cdh4:22000 by fragment 5140dddc4be44c9e:5cccbfe200000004
Memory left in process limit: 384.00 KB
Process: Limit=256.00 MB Total=255.62 MB Peak=264.16 MB
  RequestPool=root.root: Total=162.28 MB Peak=162.42 MB
    Query(5140dddc4be44c9e:5cccbfe200000000): Total=162.28 MB Peak=162.42 MB
      Fragment 5140dddc4be44c9e:5cccbfe200000006: Total=2.30 MB Peak=2.74 MB
        AGGREGATION_NODE (id=6): Total=2.28 MB Peak=2.28 MB
          Exprs: Total=4.00 KB Peak=4.00 KB
        EXCHANGE_NODE (id=5): Total=0 Peak=0
        DataStreamRecvr: Total=0 Peak=0
        DataStreamSender (dst_id=7): Total=3.12 KB Peak=3.12 KB
        CodeGen: Total=2.22 KB Peak=451.50 KB
      Block Manager: Limit=156.00 MB Total=104.50 MB Peak=104.50 MB
      Fragment 5140dddc4be44c9e:5cccbfe200000004: Total=132.81 MB Peak=132.95 MB
        Runtime Filter Bank: Total=1.00 MB Peak=1.00 MB
        AGGREGATION_NODE (id=3): Total=1.29 MB Peak=1.29 MB
          Exprs: Total=8.00 KB Peak=8.00 KB
        HASH_JOIN_NODE (id=2): Total=113.12 MB Peak=113.12 MB
          Hash Join Builder (join_node_id=2): Total=113.02 MB Peak=113.02 MB
        HDFS_SCAN_NODE (id=0): Total=0 Peak=0
        EXCHANGE_NODE (id=4): Total=0 Peak=0
        DataStreamRecvr: Total=17.37 MB Peak=28.05 MB
        DataStreamSender (dst_id=5): Total=6.23 KB Peak=6.23 KB
        CodeGen: Total=14.88 KB Peak=2.08 MB
      Fragment 5140dddc4be44c9e:5cccbfe200000001: Total=27.18 MB Peak=43.25 MB
        HDFS_SCAN_NODE (id=1): Total=27.13 MB Peak=43.20 MB
        DataStreamSender (dst_id=4): Total=6.91 KB Peak=6.91 KB
        CodeGen: Total=1.38 KB Peak=178.00 KB
  RequestPool=root.hue: Total=0 Peak=1.76 MB
  RequestPool=root.default: Total=0 Peak=7.40 MB
  Untracked Memory: Total=93.34 MB
  解决方案：调整impala的参数mem_limit，应大于数据文件的大小。



