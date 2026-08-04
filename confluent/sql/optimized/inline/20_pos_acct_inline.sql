-- Positions by account+ticker with dedup INLINED (no trades-dedup topic).
-- Same result as baseline dml/11, one fewer Kafka round-trip.
INSERT INTO `position-by-account-ticker`
SELECT
  CONCAT(account, '|', ticker) AS `key`,
  JSON_OBJECT(
    'account'  VALUE account,
    'ticker'   VALUE ticker,
    'net_qty'  VALUE net_qty,
    'as_of'    VALUE as_of
  ) AS `val`
FROM (
  SELECT
    JSON_VALUE(`val`, '$.account') AS account,
    JSON_VALUE(`val`, '$.ticker') AS ticker,
    SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
    MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM (
    SELECT `key`, `val` FROM (
      SELECT `key`, `val`,
        ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.trade_id')
                           ORDER BY `$rowtime` ASC) AS rn
      FROM `trades`
    ) WHERE rn = 1
  )
  GROUP BY
    JSON_VALUE(`val`, '$.account'),
    JSON_VALUE(`val`, '$.ticker')
);
