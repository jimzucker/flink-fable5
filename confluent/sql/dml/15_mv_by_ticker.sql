-- Firm-wide market value per ticker. JSON: {ticker, net_qty, price, mv, as_of}
-- (no account field — matches the Java pipeline's NON_NULL serialization).
INSERT INTO `mv-by-ticker`
SELECT
  p.ticker AS `key`,
  JSON_OBJECT(
    'ticker'   VALUE p.ticker,
    'net_qty'  VALUE p.net_qty,
    'price'    VALUE CAST(lp.price AS STRING),
    'mv'       VALUE CAST(CAST(p.net_qty AS DECIMAL(18, 0)) * lp.price AS STRING),
    'as_of'    VALUE GREATEST(p.as_of, lp.event_time)
  ) AS `val`
FROM (
  SELECT
    JSON_VALUE(`val`, '$.ticker') AS ticker,
    SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
    MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM `trades-dedup`
  GROUP BY JSON_VALUE(`val`, '$.ticker')
) p
JOIN (
  SELECT symbol, price, event_time
  FROM (
    SELECT
      JSON_VALUE(`val`, '$.symbol') AS symbol,
      CAST(JSON_VALUE(`val`, '$.price') AS DECIMAL(18, 2)) AS price,
      CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT) AS event_time,
      ROW_NUMBER() OVER (
        PARTITION BY JSON_VALUE(`val`, '$.symbol')
        ORDER BY `$rowtime` DESC
      ) AS rn
    FROM `prices-conflated`
  )
  WHERE rn = 1
) lp ON p.ticker = lp.symbol;
