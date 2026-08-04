-- CR-1 on Confluent: output cadence capped to human reading speed.
--
-- Throttling after a GROUP BY does not work: a plain aggregation emits an
-- UPDATING stream and windowing functions require append-only input. CUMULATE
-- is the idiomatic construct instead — it emits a RUNNING total from the start
-- of the window once per step, which is exactly what a position is. The daily
-- window gives a daily reset, which is correct for trading anyway.
--
--   positions     : CUMULATE step 0.5 s -> <= 2 updates/key/sec
--   market values : CUMULATE step 1 s   -> <= 1 update/key/sec
--
-- Note the windowing function takes a TABLE IDENTIFIER, not an inline
-- subquery, so the deduplication has to be a named CTE.
-- All four outputs stay in ONE fused job (Phase 12: a chain costs ~15x).
EXECUTE STATEMENT SET
BEGIN

INSERT INTO `position-by-account-ticker`
WITH deduped AS (
  SELECT `key`, `val`, `$rowtime` AS rt FROM (
    SELECT `key`, `val`, `$rowtime`,
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
  SELECT
    JSON_VALUE(`val`, '$.account') AS account,
    JSON_VALUE(`val`, '$.ticker') AS ticker,
    SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
    MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM TABLE(CUMULATE(TABLE deduped, DESCRIPTOR(rt), INTERVAL '0.5' SECOND, INTERVAL '1' DAY))
  GROUP BY window_start, window_end, JSON_VALUE(`val`, '$.account'), JSON_VALUE(`val`, '$.ticker')
);

INSERT INTO `position-by-ticker`
WITH deduped AS (
  SELECT `key`, `val`, `$rowtime` AS rt FROM (
    SELECT `key`, `val`, `$rowtime`,
      ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.trade_id')
                         ORDER BY `$rowtime` ASC) AS rn
    FROM `trades`
  ) WHERE rn = 1
)
SELECT
  ticker,
  JSON_OBJECT('ticker' VALUE ticker, 'net_qty' VALUE net_qty, 'as_of' VALUE as_of)
FROM (
  SELECT
    JSON_VALUE(`val`, '$.ticker') AS ticker,
    SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
    MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM TABLE(CUMULATE(TABLE deduped, DESCRIPTOR(rt), INTERVAL '0.5' SECOND, INTERVAL '1' DAY))
  GROUP BY window_start, window_end, JSON_VALUE(`val`, '$.ticker')
);

INSERT INTO `mv-by-account-ticker`
WITH deduped AS (
  SELECT `key`, `val`, `$rowtime` AS rt FROM (
    SELECT `key`, `val`, `$rowtime`,
      ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.trade_id')
                         ORDER BY `$rowtime` ASC) AS rn
    FROM `trades`
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
  SELECT
    JSON_VALUE(`val`, '$.account') AS account,
    JSON_VALUE(`val`, '$.ticker') AS ticker,
    SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
    MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM TABLE(CUMULATE(TABLE deduped, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '1' DAY))
  GROUP BY window_start, window_end, JSON_VALUE(`val`, '$.account'), JSON_VALUE(`val`, '$.ticker')
) p
JOIN (
  SELECT symbol, price, event_time FROM (
    SELECT JSON_VALUE(`val`, '$.symbol') AS symbol,
           CAST(JSON_VALUE(`val`, '$.price') AS DECIMAL(18, 2)) AS price,
           CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT) AS event_time,
           ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.symbol')
                              ORDER BY wt DESC) AS rn
    FROM (SELECT `val`, window_time AS wt FROM (
            SELECT `val`, window_time,
              ROW_NUMBER() OVER (PARTITION BY window_start, window_end,
                                              JSON_VALUE(`val`, '$.symbol')
                                 ORDER BY `$rowtime` DESC) AS rn
            FROM TABLE(TUMBLE(TABLE `prices`, DESCRIPTOR(`$rowtime`),
                              INTERVAL '0.25' SECOND))) WHERE rn = 1)
  ) WHERE rn = 1
) lp ON p.ticker = lp.symbol;

INSERT INTO `mv-by-ticker`
WITH deduped AS (
  SELECT `key`, `val`, `$rowtime` AS rt FROM (
    SELECT `key`, `val`, `$rowtime`,
      ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.trade_id')
                         ORDER BY `$rowtime` ASC) AS rn
    FROM `trades`
  ) WHERE rn = 1
)
SELECT
  p.ticker,
  JSON_OBJECT('ticker' VALUE p.ticker, 'net_qty' VALUE p.net_qty,
              'price' VALUE CAST(lp.price AS STRING),
              'mv' VALUE CAST(CAST(p.net_qty AS DECIMAL(18, 0)) * lp.price AS STRING),
              'as_of' VALUE GREATEST(p.as_of, lp.event_time))
FROM (
  SELECT
    JSON_VALUE(`val`, '$.ticker') AS ticker,
    SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
    MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM TABLE(CUMULATE(TABLE deduped, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '1' DAY))
  GROUP BY window_start, window_end, JSON_VALUE(`val`, '$.ticker')
) p
JOIN (
  SELECT symbol, price, event_time FROM (
    SELECT JSON_VALUE(`val`, '$.symbol') AS symbol,
           CAST(JSON_VALUE(`val`, '$.price') AS DECIMAL(18, 2)) AS price,
           CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT) AS event_time,
           ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.symbol')
                              ORDER BY wt DESC) AS rn
    FROM (SELECT `val`, window_time AS wt FROM (
            SELECT `val`, window_time,
              ROW_NUMBER() OVER (PARTITION BY window_start, window_end,
                                              JSON_VALUE(`val`, '$.symbol')
                                 ORDER BY `$rowtime` DESC) AS rn
            FROM TABLE(TUMBLE(TABLE `prices`, DESCRIPTOR(`$rowtime`),
                              INTERVAL '0.25' SECOND))) WHERE rn = 1)
  ) WHERE rn = 1
) lp ON p.ticker = lp.symbol;

END;
