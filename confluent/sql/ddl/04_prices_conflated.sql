-- Conflated prices: at most one price per symbol per 250 ms window — the
-- SQL twin of the Phase 7 per-ticker timer. Window deduplication (last row
-- per window) is append-only.
CREATE TABLE IF NOT EXISTS `prices-conflated` (
  `key` STRING,
  `val` STRING
) DISTRIBUTED BY HASH(`key`) INTO 16 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'raw'
);
