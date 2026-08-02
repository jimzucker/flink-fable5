-- Dedup: keep the FIRST occurrence per trade_id (mirrors DedupByTradeId).
-- ROW_NUMBER ordered by $rowtime ASC + rn = 1 is Flink's deduplication
-- pattern — recognized as a Deduplicate operator, emits inserts only.
INSERT INTO `trades-dedup`
SELECT `key`, `val`
FROM (
  SELECT
    `key`,
    `val`,
    ROW_NUMBER() OVER (
      PARTITION BY JSON_VALUE(`val`, '$.trade_id')
      ORDER BY `$rowtime` ASC
    ) AS rn
  FROM `trades`
)
WHERE rn = 1;
