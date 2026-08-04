-- ONE JOB, four sinks — the structural equivalent of the Java DataStream job.
-- A statement set lets the planner share the source scan and the dedup /
-- conflation operators across all four outputs, handing records off in
-- memory instead of through intermediate Kafka topics. This is the fair
-- comparison against a fused DataStream job; the baseline dml/ scripts
-- deliberately mirror the Java topology one-operator-per-statement, which
-- is what costs the Kafka round-trips.
EXECUTE STATEMENT SET
BEGIN

INSERT INTO `position-by-account-ticker`
WITH deduped AS (
  SELECT `key`, `val` FROM (
    SELECT `key`, `val`,
      ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.trade_id')
                         ORDER BY `$rowtime` ASC) AS rn
    FROM `trades`
  ) WHERE rn = 1
)
SELECT
  CONCAT(account, '|', ticker),
  JSON_OBJECT('account' VALUE account, 'ticker' VALUE ticker,
              'net_qty' VALUE net_qty, 'as_of' VALUE as_of)
FROM (
  SELECT JSON_VALUE(`val`, '$.account') AS account,
         JSON_VALUE(`val`, '$.ticker') AS ticker,
         SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
         MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM deduped
  GROUP BY JSON_VALUE(`val`, '$.account'), JSON_VALUE(`val`, '$.ticker')
);

INSERT INTO `position-by-ticker`
WITH deduped AS (
  SELECT `key`, `val` FROM (
    SELECT `key`, `val`,
      ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.trade_id')
                         ORDER BY `$rowtime` ASC) AS rn
    FROM `trades`
  ) WHERE rn = 1
)
SELECT
  ticker,
  JSON_OBJECT('ticker' VALUE ticker, 'net_qty' VALUE net_qty, 'as_of' VALUE as_of)
FROM (
  SELECT JSON_VALUE(`val`, '$.ticker') AS ticker,
         SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
         MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM deduped
  GROUP BY JSON_VALUE(`val`, '$.ticker')
);

INSERT INTO `mv-by-account-ticker`
WITH deduped AS (
  SELECT `key`, `val` FROM (
    SELECT `key`, `val`,
      ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.trade_id')
                         ORDER BY `$rowtime` ASC) AS rn
    FROM `trades`
  ) WHERE rn = 1
),
conflated AS (
  -- NOTE: TUMBLE replaces $rowtime with window_time; carry window_time
  -- forward so the latest-price deduplication has a time attribute to
  -- order by. (Needing a fresh $rowtime is exactly why the baseline
  -- wrote an intermediate topic here.)
  SELECT `val`, window_time AS wt FROM (
    SELECT `val`, window_time,
      ROW_NUMBER() OVER (PARTITION BY window_start, window_end,
                                      JSON_VALUE(`val`, '$.symbol')
                         ORDER BY `$rowtime` DESC) AS rn
    FROM TABLE(TUMBLE(TABLE `prices`, DESCRIPTOR(`$rowtime`), INTERVAL '0.25' SECOND))
  ) WHERE rn = 1
),
latest_price AS (
  SELECT symbol, price, event_time FROM (
    SELECT JSON_VALUE(`val`, '$.symbol') AS symbol,
           CAST(JSON_VALUE(`val`, '$.price') AS DECIMAL(18, 2)) AS price,
           CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT) AS event_time,
           ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.symbol')
                              ORDER BY wt DESC) AS rn
    FROM conflated
  ) WHERE rn = 1
)
SELECT
  CONCAT(p.account, '|', p.ticker),
  JSON_OBJECT('account' VALUE p.account, 'ticker' VALUE p.ticker,
              'net_qty' VALUE p.net_qty,
              'price' VALUE CAST(lp.price AS STRING),
              'mv' VALUE CAST(CAST(p.net_qty AS DECIMAL(18, 0)) * lp.price AS STRING),
              'as_of' VALUE GREATEST(p.as_of, lp.event_time))
FROM (
  SELECT JSON_VALUE(`val`, '$.account') AS account,
         JSON_VALUE(`val`, '$.ticker') AS ticker,
         SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
         MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM deduped
  GROUP BY JSON_VALUE(`val`, '$.account'), JSON_VALUE(`val`, '$.ticker')
) p
JOIN latest_price lp ON p.ticker = lp.symbol;

INSERT INTO `mv-by-ticker`
WITH deduped AS (
  SELECT `key`, `val` FROM (
    SELECT `key`, `val`,
      ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.trade_id')
                         ORDER BY `$rowtime` ASC) AS rn
    FROM `trades`
  ) WHERE rn = 1
),
conflated AS (
  -- NOTE: TUMBLE replaces $rowtime with window_time; carry window_time
  -- forward so the latest-price deduplication has a time attribute to
  -- order by. (Needing a fresh $rowtime is exactly why the baseline
  -- wrote an intermediate topic here.)
  SELECT `val`, window_time AS wt FROM (
    SELECT `val`, window_time,
      ROW_NUMBER() OVER (PARTITION BY window_start, window_end,
                                      JSON_VALUE(`val`, '$.symbol')
                         ORDER BY `$rowtime` DESC) AS rn
    FROM TABLE(TUMBLE(TABLE `prices`, DESCRIPTOR(`$rowtime`), INTERVAL '0.25' SECOND))
  ) WHERE rn = 1
),
latest_price AS (
  SELECT symbol, price, event_time FROM (
    SELECT JSON_VALUE(`val`, '$.symbol') AS symbol,
           CAST(JSON_VALUE(`val`, '$.price') AS DECIMAL(18, 2)) AS price,
           CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT) AS event_time,
           ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.symbol')
                              ORDER BY wt DESC) AS rn
    FROM conflated
  ) WHERE rn = 1
)
SELECT
  p.ticker,
  JSON_OBJECT('ticker' VALUE p.ticker, 'net_qty' VALUE p.net_qty,
              'price' VALUE CAST(lp.price AS STRING),
              'mv' VALUE CAST(CAST(p.net_qty AS DECIMAL(18, 0)) * lp.price AS STRING),
              'as_of' VALUE GREATEST(p.as_of, lp.event_time))
FROM (
  SELECT JSON_VALUE(`val`, '$.ticker') AS ticker,
         SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
         MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM deduped
  GROUP BY JSON_VALUE(`val`, '$.ticker')
) p
JOIN latest_price lp ON p.ticker = lp.symbol;

END;
