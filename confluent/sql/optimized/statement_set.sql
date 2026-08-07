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
conflated AS (
  -- NOTE: TUMBLE replaces $rowtime with window_time; carry window_time
  -- forward so the latest-price deduplication has a time attribute to
  -- order by. (Needing a fresh $rowtime is exactly why the baseline
  -- wrote an intermediate topic here.)
  --
  -- KEY SALTING (two-phase / local-global). Conflating on symbol alone gives
  -- one worker per symbol, and with ten symbols only ten workers can ever be
  -- busy no matter how large the pool is. Phase 1 partitions on
  -- (symbol, salt) to multiply the key space; phase 2 reduces the salt
  -- shards back to one row per symbol per window. Correct by associativity:
  -- the newest of the per-shard newest IS the global newest.
  --
  -- The salt MUST vary per RECORD. Hashing the symbol yields a constant per
  -- symbol and manufactures no parallelism at all; MOD over event_time
  -- spreads consecutive ticks across shards evenly.
  --
  -- Both phases stay ROW_NUMBER rather than GROUP BY on purpose: dedup
  -- preserves the time attribute, so `wt` remains orderable downstream. A
  -- GROUP BY reduction would strip it, and after TUMBLE there is no other
  -- time attribute left to recover (MAX_BY does not exist on Confluent, which
  -- is why the standalone salted_conflate.sql had to fall back to a
  -- lexicographic LPAD/CONCAT/MAX/SUBSTRING encode).
  SELECT `val`, wt FROM (
    SELECT `val`, wt, window_start, window_end, `$rowtime`,
      -- phase 2: reduce the salt shards (at most 8 rows per symbol/window)
      ROW_NUMBER() OVER (PARTITION BY window_start, window_end,
                                      JSON_VALUE(`val`, '$.symbol')
                         ORDER BY `$rowtime` DESC) AS rn2
    FROM (
      SELECT `val`, window_time AS wt, window_start, window_end, `$rowtime`,
        -- phase 1: parallel across (symbol, salt)
        ROW_NUMBER() OVER (PARTITION BY window_start, window_end,
                                        JSON_VALUE(`val`, '$.symbol'),
                                        MOD(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT), 8)
                           ORDER BY `$rowtime` DESC) AS rn
      FROM TABLE(TUMBLE(TABLE `prices`, DESCRIPTOR(`$rowtime`), INTERVAL '0.25' SECOND))
    ) WHERE rn = 1
  ) WHERE rn2 = 1
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
conflated AS (
  -- NOTE: TUMBLE replaces $rowtime with window_time; carry window_time
  -- forward so the latest-price deduplication has a time attribute to
  -- order by. (Needing a fresh $rowtime is exactly why the baseline
  -- wrote an intermediate topic here.)
  --
  -- KEY SALTING (two-phase / local-global). Conflating on symbol alone gives
  -- one worker per symbol, and with ten symbols only ten workers can ever be
  -- busy no matter how large the pool is. Phase 1 partitions on
  -- (symbol, salt) to multiply the key space; phase 2 reduces the salt
  -- shards back to one row per symbol per window. Correct by associativity:
  -- the newest of the per-shard newest IS the global newest.
  --
  -- The salt MUST vary per RECORD. Hashing the symbol yields a constant per
  -- symbol and manufactures no parallelism at all; MOD over event_time
  -- spreads consecutive ticks across shards evenly.
  --
  -- Both phases stay ROW_NUMBER rather than GROUP BY on purpose: dedup
  -- preserves the time attribute, so `wt` remains orderable downstream. A
  -- GROUP BY reduction would strip it, and after TUMBLE there is no other
  -- time attribute left to recover (MAX_BY does not exist on Confluent, which
  -- is why the standalone salted_conflate.sql had to fall back to a
  -- lexicographic LPAD/CONCAT/MAX/SUBSTRING encode).
  SELECT `val`, wt FROM (
    SELECT `val`, wt, window_start, window_end, `$rowtime`,
      -- phase 2: reduce the salt shards (at most 8 rows per symbol/window)
      ROW_NUMBER() OVER (PARTITION BY window_start, window_end,
                                      JSON_VALUE(`val`, '$.symbol')
                         ORDER BY `$rowtime` DESC) AS rn2
    FROM (
      SELECT `val`, window_time AS wt, window_start, window_end, `$rowtime`,
        -- phase 1: parallel across (symbol, salt)
        ROW_NUMBER() OVER (PARTITION BY window_start, window_end,
                                        JSON_VALUE(`val`, '$.symbol'),
                                        MOD(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT), 8)
                           ORDER BY `$rowtime` DESC) AS rn
      FROM TABLE(TUMBLE(TABLE `prices`, DESCRIPTOR(`$rowtime`), INTERVAL '0.25' SECOND))
    ) WHERE rn = 1
  ) WHERE rn2 = 1
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
