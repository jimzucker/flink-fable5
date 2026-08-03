-- Firm-wide position per ticker. Output JSON: {ticker, net_qty, as_of}.
INSERT INTO `position-by-ticker`
SELECT
  ticker AS `key`,
  JSON_OBJECT(
    'ticker'   VALUE ticker,
    'net_qty'  VALUE net_qty,
    'as_of'    VALUE as_of
  ) AS `val`
FROM (
  SELECT
    JSON_VALUE(`val`, '$.ticker') AS ticker,
    SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
    MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM `trades-dedup`
  GROUP BY JSON_VALUE(`val`, '$.ticker')
);
