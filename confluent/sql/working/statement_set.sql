-- WORKING CONFIG for Confluent Cloud Flink.
--
-- Identical to optimized/statement_set.sql except how prices are reduced:
--   optimized/ : 250ms tumbling window over the INPUT -> newest tick discarded
--   working/   : keep-last-row dedup                  -> newest tick always used
--
-- Validated on the equivalent OSS config: 100% exact market values, 0ms
-- staleness, zero ordering violations, all six checks pass -- while publishing
-- 3x FEWER records than the windowed version.
--
-- The other half of the fix is an OUTPUT reduce (publish at most once per
-- interval). On AWS that is the upsert-kafka option sink.buffer-flush.interval.
-- Whether Confluent Cloud exposes an equivalent is UNVERIFIED. If it does not,
-- this statement set is correct but publishes more records than the AWS working
-- config -- and that gap is itself a platform finding.

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
-- Group on the CONCATENATED key, not on its components. Projecting
-- CONCAT(account,'|',ticker) AFTER a GROUP BY account,ticker destroys the
-- derived upsert key: EXPLAIN reports Upsert key:(account,ticker) at the
-- aggregate and NOTHING at the sink, so the planner inserts a
-- state-intensive correction operator (the AWS SinkUpsertMaterializer, and
-- the "State size: high" operator that also triggers the no-TTL warning).
-- Grouping on the concatenation makes the upsert key a single column that
-- matches the sink PRIMARY KEY. account/ticker are constant within a group,
-- so MAX() over them is an identity, not an aggregation.
SELECT
  COALESCE(acct_key, '?'),
  JSON_OBJECT('account' VALUE account, 'ticker' VALUE ticker,
              'net_qty' VALUE net_qty, 'as_of' VALUE as_of)
FROM (
  SELECT COALESCE(CONCAT(JSON_VALUE(`val`, '$.account'), '|', JSON_VALUE(`val`, '$.ticker')), '?') AS acct_key,
         MAX(JSON_VALUE(`val`, '$.account')) AS account,
         MAX(JSON_VALUE(`val`, '$.ticker')) AS ticker,
         SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
         MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM deduped
  GROUP BY COALESCE(CONCAT(JSON_VALUE(`val`, '$.account'), '|', JSON_VALUE(`val`, '$.ticker')), '?')
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
  COALESCE(ticker, '?'),
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
latest_price AS (
  -- WORKING CONFIG: keep-last-row deduplication, NO windowing of the input.
  --
  -- optimized/ reduces the INPUT with a 250ms tumbling window, which discards
  -- the newest tick before it is used -- the published market value sits ~2-3s
  -- behind and never corrects. Measured on the equivalent OSS config: 0% exact,
  -- p50 2,929ms stale, and 3x MORE records published than this variant.
  --
  -- Dedup over the raw stream keeps the latest price per symbol always current.
  -- Volume is reduced at the SINK instead, which publishes the newest value
  -- less often rather than discarding the newest value.
  SELECT symbol, price, event_time FROM (
    SELECT JSON_VALUE(`val`, '$.symbol') AS symbol,
           CAST(JSON_VALUE(`val`, '$.price') AS DECIMAL(18, 2)) AS price,
           CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT) AS event_time,
           ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.symbol')
                              ORDER BY `$rowtime` DESC) AS rn
    FROM `prices`
  ) WHERE rn = 1
)
-- Same upsert-key fix as position-by-account-ticker: group on the
-- concatenated key so the sink's single-column PRIMARY KEY matches the
-- upsert key the planner derives, and the correction operator disappears.
SELECT
  COALESCE(p.acct_key, '?'),
  JSON_OBJECT('account' VALUE p.account, 'ticker' VALUE p.ticker,
              'net_qty' VALUE p.net_qty,
              'price' VALUE CAST(lp.price AS STRING),
              'mv' VALUE CAST(CAST(p.net_qty AS DECIMAL(18, 0)) * lp.price AS STRING),
              'as_of' VALUE GREATEST(p.as_of, lp.event_time))
FROM (
  SELECT COALESCE(CONCAT(JSON_VALUE(`val`, '$.account'), '|', JSON_VALUE(`val`, '$.ticker')), '?') AS acct_key,
         MAX(JSON_VALUE(`val`, '$.account')) AS account,
         MAX(JSON_VALUE(`val`, '$.ticker')) AS ticker,
         SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,
         MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of
  FROM deduped
  GROUP BY COALESCE(CONCAT(JSON_VALUE(`val`, '$.account'), '|', JSON_VALUE(`val`, '$.ticker')), '?')
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
latest_price AS (
  -- WORKING CONFIG: keep-last-row deduplication, NO windowing of the input.
  --
  -- optimized/ reduces the INPUT with a 250ms tumbling window, which discards
  -- the newest tick before it is used -- the published market value sits ~2-3s
  -- behind and never corrects. Measured on the equivalent OSS config: 0% exact,
  -- p50 2,929ms stale, and 3x MORE records published than this variant.
  --
  -- Dedup over the raw stream keeps the latest price per symbol always current.
  -- Volume is reduced at the SINK instead, which publishes the newest value
  -- less often rather than discarding the newest value.
  SELECT symbol, price, event_time FROM (
    SELECT JSON_VALUE(`val`, '$.symbol') AS symbol,
           CAST(JSON_VALUE(`val`, '$.price') AS DECIMAL(18, 2)) AS price,
           CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT) AS event_time,
           ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.symbol')
                              ORDER BY `$rowtime` DESC) AS rn
    FROM `prices`
  ) WHERE rn = 1
)
SELECT
  COALESCE(p.ticker, '?'),
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
