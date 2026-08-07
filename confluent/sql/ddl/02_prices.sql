-- Source: raw JSON prices {symbol, price (decimal string), event_time}.
CREATE TABLE IF NOT EXISTS `prices` (
  `key` STRING,
  `val` STRING
) DISTRIBUTED BY HASH(`key`) INTO 48 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'raw'
);
