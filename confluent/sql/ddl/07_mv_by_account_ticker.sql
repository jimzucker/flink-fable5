CREATE TABLE `mv-by-account-ticker` (
  `key` STRING,
  `val` STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'raw',
  'value.format' = 'raw'
);
