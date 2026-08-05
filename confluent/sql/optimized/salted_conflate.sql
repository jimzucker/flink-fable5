-- Key salting: manufacture parallelism when the natural key space is too narrow.
--
-- Problem measured in phase 11: the conflation stage keys on symbol, and with
-- 10 symbols only 10 workers can ever be busy — so 10 -> 16 CFUs bought only
-- +8% throughput. Compute past the key count sits idle.
--
-- Fix (two-phase / local-global aggregation):
--   phase 1  key on (symbol, salt) where salt varies PER RECORD  -> 8x the keys
--            each shard picks its own latest tick
--   phase 2  key on symbol again, reduce the 8 candidates to the true latest
--
-- Correct because "latest of the per-shard latests" IS the global latest, the
-- same associativity argument that made the phase 7 conflation safe.
--
-- Two implementation notes:
--   * the salt must vary per RECORD, not per key — hashing the symbol would
--     produce a constant and change nothing. event_time MOD 8 spreads evenly.
--   * phase 2 cannot ORDER BY a time attribute: after TUMBLE every row in a
--     window shares one window_time, so the ordering collapses. And MAX_BY
--     does not exist on Confluent Cloud. The portable reduction is a
--     lexicographic encode: left-pad the timestamp to fixed width, prepend
--     it to the payload, take a plain MAX (string order == numeric order
--     once padded), then strip the prefix back off.
INSERT INTO `prices-conflated`
SELECT
  symbol AS `key`,
  SUBSTRING(MAX(CONCAT(LPAD(CAST(event_time AS STRING), 20, '0'), `val`)) FROM 21) AS `val`
FROM (
  -- phase 1: parallel across (symbol, salt)
  SELECT
    JSON_VALUE(`val`, '$.symbol') AS symbol,
    `val`,
    CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT) AS event_time,
    window_start,
    window_end
  FROM (
    SELECT
      `val`, window_start, window_end,
      ROW_NUMBER() OVER (
        PARTITION BY window_start, window_end,
                     JSON_VALUE(`val`, '$.symbol'),
                     MOD(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT), 8)
        ORDER BY `$rowtime` DESC) AS rn
    FROM TABLE(TUMBLE(TABLE `prices-bulk48`, DESCRIPTOR(`$rowtime`),
                      INTERVAL '0.25' SECOND))
  ) WHERE rn = 1
)
-- phase 2: reduce the 8 shard-candidates back to one row per symbol per window
GROUP BY window_start, window_end, symbol;
