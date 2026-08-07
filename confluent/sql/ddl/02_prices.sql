-- Source: raw JSON prices {symbol, price (decimal string), event_time}.
--
-- ROUND-ROBIN, deliberately NOT `DISTRIBUTED BY HASH(key)`.
--
-- The producer keys prices by symbol. Hashing ten symbols into 48 buckets fills
-- at most ten and leaves 38 permanently empty, which caused BOTH of this
-- project's worst Confluent problems:
--   1. the source could only read ~10-way however many buckets or CFUs existed,
--      so the compute pool never drew more than 10 CFU and query-time salting
--      could not help (a downstream PARTITION BY cannot widen a source);
--   2. empty partitions never advance a watermark, so the event-time TUMBLE
--      never fired and the job consumed 119k/s while writing zero rows.
--
-- Co-location by symbol buys nothing: every consumer of this table immediately
-- does PARTITION BY symbol, which shuffles regardless.
--
-- NOTE: this clause alone does NOT fix it. DISTRIBUTED BY governs how FLINK
-- writes to the topic; the generator is a plain Kafka producer and Kafka's
-- default partitioner hashes the record key, so records still land in ~10
-- partitions. The actual fix is producer-side:
--     --generator.price.key.mode salted
-- which spreads records across every partition. This DDL keeps the table from
-- re-imposing the narrow distribution on anything Flink writes.
CREATE TABLE IF NOT EXISTS `prices` (
  `key` STRING,
  `val` STRING
) DISTRIBUTED INTO 48 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'raw'
);
