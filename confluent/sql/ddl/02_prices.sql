-- Source: raw JSON prices {symbol, price (decimal string), event_time}.
--
-- Distributed BY HASH(key), and that is correct — the spreading is done at the
-- PRODUCER, not here.
--
-- Round-robin (`DISTRIBUTED INTO n BUCKETS` with no BY HASH) is NOT available:
-- Confluent rejects it with "A key format 'key.format' requires the declaration
-- of one or more of key fields using DISTRIBUTED BY." Declaring a key format
-- forces a hash distribution.
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
-- THE FIX IS PRODUCER-SIDE:
--     --generator.price.key.mode salted
-- which writes keys as `symbol#N`. Those hash across all 48 buckets, so hashing
-- by key becomes the right thing to do rather than the problem. Keying by bare
-- symbol is what concentrated ten symbols into ten buckets.
CREATE TABLE IF NOT EXISTS `prices` (
  `key` STRING,
  `val` STRING
) DISTRIBUTED BY HASH(`key`) INTO 48 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'raw'
);
