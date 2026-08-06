-- Deduplicated trades (first occurrence per trade_id wins).
-- Append-only: first-row deduplication emits inserts only.
CREATE TABLE IF NOT EXISTS `trades-dedup` (
  `key` STRING,
  `val` STRING
) DISTRIBUTED BY HASH(`key`) INTO 48 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'raw'
);
