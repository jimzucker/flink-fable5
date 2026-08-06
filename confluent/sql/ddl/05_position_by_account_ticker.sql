-- Output: same topic name and JSON shape as the Java pipeline.
-- Upsert changelog: aggregation updates land as upserts keyed account|ticker.
CREATE TABLE IF NOT EXISTS `position-by-account-ticker` (
  `key` STRING,
  `val` STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED INTO 48 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'raw',
  'value.format' = 'raw'
);
