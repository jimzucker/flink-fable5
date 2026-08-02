-- Positions per account+ticker: net qty as a running SUM over deduped trades.
-- Output JSON matches the Java pipeline: {account, ticker, net_qty, as_of}.
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
  FROM `trades-dedup`
  GROUP BY
    JSON_VALUE(`val`, '$.account'),
    JSON_VALUE(`val`, '$.ticker')
);
