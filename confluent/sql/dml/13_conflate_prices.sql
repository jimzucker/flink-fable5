-- Conflation — the SQL twin of the Phase 7 fix. A 250 ms tumbling window
-- keeps only the LAST price per symbol per window; intermediate ticks are
-- absorbed before they ever reach the market-value joins. Window
-- deduplication (rn = 1 over window_start/window_end) is append-only.
-- The original price JSON passes through untouched.
INSERT INTO `prices-conflated`
SELECT `key`, `val`
FROM (
  SELECT
    `key`,
    `val`,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, window_end, JSON_VALUE(`val`, '$.symbol')
      ORDER BY `$rowtime` DESC
    ) AS rn
  FROM TABLE(
    TUMBLE(TABLE `prices`, DESCRIPTOR(`$rowtime`), INTERVAL '0.25' SECOND)
  )
)
WHERE rn = 1;
