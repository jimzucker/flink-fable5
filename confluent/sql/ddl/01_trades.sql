-- Source: raw JSON trades, exactly as the Java generator produces them.
-- raw key/value keeps the wire format identical to the AWS/DataStream stack,
-- so the same generator and the same validation checks work unchanged.
CREATE TABLE IF NOT EXISTS `trades` (
  `key` STRING,
  `val` STRING
) DISTRIBUTED BY HASH(`key`) INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'raw'
);
