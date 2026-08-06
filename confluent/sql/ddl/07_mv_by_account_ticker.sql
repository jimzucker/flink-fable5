CREATE TABLE IF NOT EXISTS `mv-by-account-ticker` (
  `key` STRING,
  `val` STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED INTO 48 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'raw',
  'value.format' = 'raw'
);
