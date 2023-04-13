CREATE EXTERNAL TABLE IF NOT EXISTS `{table}`(
  `brand` string COMMENT 'from deserializer',
  `resolution` string COMMENT 'from deserializer',
  `size` string,
  `selling_price` string COMMENT 'from deserializer',
  `oringinal_price` string COMMENT 'from deserializer',
  `os` string COMMENT 'from deserializer',
  `rating` string COMMENT 'from deserializer')
PARTITIONED BY (
  `day` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'skip.header.line.count'='1')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '{location}'
TBLPROPERTIES ('delimiter'=',', 'skip.header.line.count'='1')