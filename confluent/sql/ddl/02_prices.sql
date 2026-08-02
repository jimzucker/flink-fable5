-- Source: raw JSON prices {symbol, price (decimal string), event_time}.
CREATE TABLE `prices` (
  `key` STRING,
  `val` STRING
) DISTRIBUTED BY HASH(`key`) INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'raw'
);
