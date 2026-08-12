package com.demo.flink.pipeline;

import com.demo.flink.common.AppConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The same four outputs as {@link PositionPipeline}, expressed in Flink SQL.
 *
 * One job, one source scan, four sinks: every INSERT goes into a single
 * {@link StatementSet}, so the planner shares the trades scan, the dedup and
 * the latest-price state across all four outputs and hands records off in
 * memory. No intermediate Kafka topics — this is the structural equivalent of
 * the fused DataStream graph, and it mirrors
 * {@code confluent/sql/optimized/statement_set.sql}, which is already proven
 * correct on Confluent Cloud.
 *
 * Wire format is byte-identical to the DataStream job: raw JSON in, raw JSON
 * out, same topics, same keys, same field names and order. Money is DECIMAL
 * end to end — never a float.
 *
 * Selected at runtime with {@code pipeline.mode=sql}; the same jar still runs
 * the DataStream job by default.
 */
public final class SqlPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(SqlPipeline.class);

    /** Local table names. The Kafka topic is a table option, so these never contain hyphens. */
    static final String TRADES = "trades_src";
    static final String PRICES = "prices_src";
    static final String SINK_POSITION_ACCOUNT_TICKER = "position_by_account_ticker_sink";
    static final String SINK_POSITION_TICKER = "position_by_ticker_sink";
    static final String SINK_MV_ACCOUNT_TICKER = "mv_by_account_ticker_sink";
    static final String SINK_MV_TICKER = "mv_by_ticker_sink";

    private SqlPipeline() {
    }

    public static void main(String[] args) throws Exception {
        run(AppConfig.load(args));
    }

    /** Build and submit the SQL job. Config keys mirror the DataStream pipeline's. */
    public static void run(AppConfig params) throws Exception {
        long checkpointIntervalMs = params.getLong("checkpoint.interval.ms", 10_000L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (params.has("pipeline.parallelism")) {
            env.setParallelism(params.getInt("pipeline.parallelism", 2));
        }
        env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        configure(tableEnv, params);
        createTables(tableEnv, params);

        LOG.info("Starting flink-demo SQL pipeline against {} (delivery guarantee {})",
                params.get("kafka.bootstrap.servers", "localhost:29092"), deliveryGuarantee(params));
        buildStatementSet(tableEnv, params).execute();
    }

    /**
     * Planner knobs. Mini-batch is the SQL analogue of the DataStream job's
     * per-key emit timers; it is OFF by default so latency matches the
     * DataStream baseline unless the experiment asks for it.
     */
    static void configure(TableEnvironment tableEnv, AppConfig params) {
        Map<String, String> conf = new LinkedHashMap<>();
        long miniBatchMs = params.getLong("sql.minibatch.latency.ms", 0L);
        if (miniBatchMs > 0) {
            conf.put("table.exec.mini-batch.enabled", "true");
            conf.put("table.exec.mini-batch.allow-latency", miniBatchMs + " ms");
            conf.put("table.exec.mini-batch.size", String.valueOf(params.getLong("sql.minibatch.size", 5_000L)));
        }
        // Source idle timeout. This is a CORRECTNESS requirement for this
        // workload, not tuning, and it defaults ON for that reason.
        //
        // The price key space is 10 tickers hashed across 48 partitions, so at
        // most 10 partitions ever carry data and 38 are permanently empty. An
        // empty partition never advances its watermark, which holds the global
        // watermark at its initial value for ever: the event-time TUMBLE in the
        // conflation CTE never fires and the job emits NOTHING while happily
        // consuming at full speed. Observed on Confluent as a statement RUNNING
        // for an hour at 119k records/sec with zero rows on all four sinks, no
        // error and no DEGRADED status.
        //
        // Raising the partition count to lift the source-parallelism ceiling is
        // what creates this. "partitions >= parallelism" is only half the rule;
        // the other half is "partitions <= key cardinality, or set this".
        //
        // 0 disables it (restores the stalling behaviour) for A/B testing.
        long idleMs = params.getLong("sql.source.idle.timeout.ms", 5_000L);
        if (idleMs > 0) {
            conf.put("table.exec.source.idle-timeout", idleMs + " ms");
        }
        // Job-wide state TTL. NOT the same knob as dedup.state.ttl.ms: SQL cannot
        // scope a TTL to the dedup operator alone, and expiring the position
        // aggregates would change the answers, so this defaults to "never".
        long stateTtlMs = params.getLong("sql.state.ttl.ms", 0L);
        if (stateTtlMs > 0) {
            conf.put("table.exec.state.ttl", stateTtlMs + " ms");
        }
        // Escape hatch for the SinkUpsertMaterializer — an extra stateful operator
        // the DataStream job does not have, measured at parallelism 20 moving
        // ~219k records/sec, roughly ten times the pipeline's own source
        // consumption.
        //
        // Mostly obsolete now: POSITION_BY_ACCOUNT_TICKER_CTE groups on the
        // concatenated key, so the planner derives an upsert key that matches the
        // sink primary key and stops inserting the operator on its own. It still
        // appears on mv-by-account-ticker, whose left upsert key is acct_key but
        // whose join predicate is on ticker — a non-key column — so the planner
        // cannot carry the key through the join. Fixing that properly needs a
        // temporal join against a versioned table, which is not expressible
        // against a CTE inside a fused statement set.
        //
        // Values: AUTO (Flink default) | NONE | FORCE.
        String upsertMat = params.get("sql.sink.upsert.materialize", "AUTO");
        if (!"AUTO".equalsIgnoreCase(upsertMat)) {
            conf.put("table.exec.sink.upsert-materialize", upsertMat.toUpperCase());
        }
        conf.forEach((k, v) -> tableEnv.getConfig().getConfiguration().setString(k, v));
    }

    static void createTables(TableEnvironment tableEnv, AppConfig params) {
        tableEnv.executeSql(tradesDdl(params));
        tableEnv.executeSql(pricesDdl(params));
        tableEnv.executeSql(sinkDdl(params, SINK_POSITION_ACCOUNT_TICKER,
                params.get("topic.position.account.ticker", "position-by-account-ticker"),
                positionEmitIntervalMs(params)));
        tableEnv.executeSql(sinkDdl(params, SINK_POSITION_TICKER,
                params.get("topic.position.ticker", "position-by-ticker"),
                positionEmitIntervalMs(params)));
        tableEnv.executeSql(sinkDdl(params, SINK_MV_ACCOUNT_TICKER,
                params.get("topic.mv.account.ticker", "mv-by-account-ticker"),
                mvEmitIntervalMs(params)));
        tableEnv.executeSql(sinkDdl(params, SINK_MV_TICKER,
                params.get("topic.mv.ticker", "mv-by-ticker"),
                mvEmitIntervalMs(params)));
    }

    /** All four INSERTs in ONE statement set — one job graph, one source scan. */
    static StatementSet buildStatementSet(TableEnvironment tableEnv, AppConfig params) {
        StatementSet statements = tableEnv.createStatementSet();
        statements.addInsertSql(positionByAccountTickerSql());
        statements.addInsertSql(positionByTickerSql());
        statements.addInsertSql(mvByAccountTickerSql(params));
        statements.addInsertSql(mvByTickerSql(params));
        return statements;
    }

    // ------------------------------------------------------------------
    // DDL
    // ------------------------------------------------------------------

    static String tradesDdl(AppConfig params) {
        return "CREATE TEMPORARY TABLE `" + TRADES + "` (\n"
                + "  `val` STRING,\n"
                + "  `proc_ts` AS PROCTIME()\n"
                + ") WITH (\n"
                + options(sourceOptions(params, params.get("topic.trades", "trades"), "flink-demo-trades"))
                + ")";
    }

    static String pricesDdl(AppConfig params) {
        // event_ts (the Kafka record timestamp) is the stand-in for Confluent's
        // `$rowtime`; only the optional TUMBLE conflation path uses it.
        return "CREATE TEMPORARY TABLE `" + PRICES + "` (\n"
                + "  `val` STRING,\n"
                + "  `event_ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,\n"
                + "  `proc_ts` AS PROCTIME(),\n"
                + "  WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '1' SECOND\n"
                + ") WITH (\n"
                + options(sourceOptions(params, params.get("topic.prices", "prices"), "flink-demo-prices"))
                + ")";
    }

    static String sinkDdl(AppConfig params, String table, String topic, long bufferFlushMs) {
        return "CREATE TEMPORARY TABLE `" + table + "` (\n"
                + "  `key` STRING,\n"
                + "  `val` STRING,\n"
                + "  PRIMARY KEY (`key`) NOT ENFORCED\n"
                + ") WITH (\n"
                + options(sinkOptions(params, topic, bufferFlushMs))
                + ")";
    }

    static Map<String, String> sourceOptions(AppConfig params, String topic, String defaultGroup) {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("connector", "kafka");
        opts.put("topic", topic);
        opts.put("properties.bootstrap.servers", params.get("kafka.bootstrap.servers", "localhost:29092"));
        opts.put("properties.group.id", params.get("kafka.group.id", defaultGroup) + "-" + topic);
        opts.put("scan.startup.mode", "earliest-offset");
        opts.put("value.format", "raw");
        addKafkaProps(params, opts);
        return opts;
    }

    static Map<String, String> sinkOptions(AppConfig params, String topic, long bufferFlushMs) {
        Map<String, String> opts = new LinkedHashMap<>();
        // upsert-kafka, not kafka: the aggregates and the join emit an updating
        // stream. Same wire bytes as the DataStream sink (keyed raw JSON), with
        // the changelog's deletes expressed as tombstones — which never occur
        // here because no group key is ever retracted.
        opts.put("connector", "upsert-kafka");
        opts.put("topic", topic);
        opts.put("properties.bootstrap.servers", params.get("kafka.bootstrap.servers", "localhost:29092"));
        opts.put("key.format", "raw");
        opts.put("value.format", "raw");
        opts.put("value.fields-include", "EXCEPT_KEY");
        String guarantee = deliveryGuarantee(params);
        opts.put("sink.delivery-guarantee", guarantee);
        if ("exactly-once".equals(guarantee)) {
            // Must be unique per sink, and stable across restarts.
            opts.put("sink.transactional-id-prefix",
                    params.get("sink.transactional.id.prefix", "flink-demo-sql") + "-" + topic);
        }
        if (bufferFlushMs > 0) {
            // The SQL analogue of CR-1: buffer per key and emit the newest value
            // at most once per interval. Same ceiling on output rate, same
            // "an emitted record is never staler than its own interval".
            opts.put("sink.buffer-flush.interval", bufferFlushMs + " ms");
            opts.put("sink.buffer-flush.max-rows",
                    String.valueOf(params.getLong("sql.sink.buffer.max.rows", 5_000L)));
        }
        addKafkaProps(params, opts);
        return opts;
    }

    /** kafka.props.* passthrough — how MSK IAM auth reaches the client, exactly as in the DataStream job. */
    private static void addKafkaProps(AppConfig params, Map<String, String> opts) {
        Properties kafka = params.kafkaProps();
        for (String name : kafka.stringPropertyNames()) {
            opts.put("properties." + name, kafka.getProperty(name));
        }
    }

    static String deliveryGuarantee(AppConfig params) {
        String value = params.get("sink.delivery.guarantee", "at-least-once").trim().toLowerCase();
        if (!value.equals("at-least-once") && !value.equals("exactly-once") && !value.equals("none")) {
            throw new IllegalArgumentException(
                    "sink.delivery.guarantee must be at-least-once, exactly-once or none (was: " + value + ")");
        }
        return value;
    }

    private static long positionEmitIntervalMs(AppConfig params) {
        return params.getLong("position.emit.interval.ms", params.getLong("emit.interval.ms", 500L));
    }

    private static long mvEmitIntervalMs(AppConfig params) {
        return params.getLong("mv.emit.interval.ms", params.getLong("mv.reval.interval.ms", 1000L));
    }

    // ------------------------------------------------------------------
    // Shared CTEs
    // ------------------------------------------------------------------

    /**
     * Dedup by trade_id, first arrival wins — the SQL form of DedupByTradeId.
     * ORDER BY a processing-time attribute ASC makes this a "keep first row"
     * deduplication, which is insert-only and emits immediately, so downstream
     * aggregation sees exactly the stream the DataStream job sees.
     */
    /**
     * Trades, read straight through -- NO deduplication.
     *
     * The upstream publisher is guaranteed to emit each trade_id once, and the
     * pipeline runs exactly-once, so there is nothing to dedup. Removing the
     * operator removes a stateful stage that held every trade_id for
     * dedup.state.ttl.ms and measured 9-22% busy while the pipeline was
     * backpressured.
     *
     * NOTE for anyone reinstating a duplicate-capable source: exactly-once does
     * NOT cover this. It stops Flink reprocessing after a failure; it does
     * nothing about a duplicate already in Kafka. Two records with the same
     * trade_id are processed exactly once EACH and the position doubles. If the
     * source can retry, replay, or publish from more than one writer, the
     * dedup stage has to come back.
     */
    private static final String DEDUPED_CTE =
            "deduped AS (\n"
            + "  SELECT `val` FROM `" + TRADES + "`\n"
            + ")";

    /**
     * Positions keyed by account+ticker.
     *
     * Groups on the CONCATENATED key rather than on its two components. That
     * looks redundant but is the whole point: projecting
     * CONCAT(account,'|',ticker) *after* a GROUP BY account,ticker destroys the
     * upsert key the planner derived, so it can no longer prove the sink key is
     * a bijection of the grouping key and inserts a correction operator
     * (SinkUpsertMaterializer here, the same one Confluent names in its
     * UPSERT_AND_PRIMARY_KEYS_DIFFERENT advisory). Grouping on the
     * concatenation makes the upsert key a single column that matches the sink
     * primary key. account and ticker are constant within a group, so MAX()
     * over them is an identity, not an aggregation.
     *
     * Kept byte-for-byte equivalent to confluent/sql/optimized/statement_set.sql
     * so the AWS-vs-Confluent SQL comparison isolates the platform, not the query.
     */
    private static final String POSITION_BY_ACCOUNT_TICKER_CTE =
            "positions AS (\n"
            + "  SELECT CONCAT(JSON_VALUE(`val`, '$.account'), '|',"
            + " JSON_VALUE(`val`, '$.ticker')) AS acct_key,\n"
            + "         MAX(JSON_VALUE(`val`, '$.account')) AS account,\n"
            + "         MAX(JSON_VALUE(`val`, '$.ticker')) AS ticker,\n"
            + "         SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,\n"
            + "         MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of\n"
            + "  FROM deduped\n"
            + "  GROUP BY CONCAT(JSON_VALUE(`val`, '$.account'), '|',"
            + " JSON_VALUE(`val`, '$.ticker'))\n"
            + ")";

    private static final String POSITION_BY_TICKER_CTE =
            "positions AS (\n"
            + "  SELECT JSON_VALUE(`val`, '$.ticker') AS ticker,\n"
            + "         SUM(CAST(JSON_VALUE(`val`, '$.qty') AS BIGINT)) AS net_qty,\n"
            + "         MAX(CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT)) AS as_of\n"
            + "  FROM deduped\n"
            + "  GROUP BY JSON_VALUE(`val`, '$.ticker')\n"
            + ")";

    /**
     * Latest price per symbol.
     *
     * Default (sql.price.conflate.ms = 0): "keep last row" deduplication on a
     * processing-time attribute — the exact SQL equivalent of the DataStream
     * job's lastPriceCents value state, and it never stalls.
     *
     * sql.price.conflate.ms > 0 reproduces the Confluent statement set's
     * TUMBLE-based tick conflation for the like-for-like experiment. It is
     * event-time, so an idle price stream leaves the last tick stuck in an
     * unfired window — which is why it is not the default.
     */
    static String latestPriceCte(AppConfig params) {
        long conflateMs = params.getLong("sql.price.conflate.ms", 0L);
        if (conflateMs <= 0) {
            return "latest_price AS (\n"
                    + "  SELECT symbol, price, event_time FROM (\n"
                    + "    SELECT JSON_VALUE(`val`, '$.symbol') AS symbol,\n"
                    + "           CAST(JSON_VALUE(`val`, '$.price') AS DECIMAL(18, 2)) AS price,\n"
                    + "           CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT) AS event_time,\n"
                    + "           ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.symbol')\n"
                    // ORDER BY event_ts, NOT proc_ts. Ordering by processing time
                    // is last-ARRIVAL-wins, and arrival order is not event order:
                    // salted/adaptive price keys spread one symbol across
                    // partitions on purpose, so a stale tick can arrive after a
                    // newer one and win. That is the same defect fixed in the
                    // DataStream market-value operators in Phase 20.
                    + "                              ORDER BY `event_ts` DESC) AS rn\n"
                    + "    FROM `" + PRICES + "`\n"
                    + "  ) WHERE rn = 1\n"
                    + ")";
        }
        String seconds = BigDecimal.valueOf(conflateMs).movePointLeft(3).toPlainString();
        // Key salting (two-phase / local-global), identical to the salted CTE in
        // confluent/sql/optimized/statement_set.sql so the two SQL cells compare
        // platform rather than query. Conflating on symbol alone gives one worker
        // per symbol: with ten symbols only ten workers can ever be busy however
        // much compute is attached. Phase 1 partitions on (symbol, salt) to
        // multiply the key space, phase 2 reduces the shards back to one row per
        // symbol per window. Correct by associativity — the newest of the
        // per-shard newest IS the global newest.
        //
        // The salt must vary per RECORD; hashing the symbol gives a constant per
        // symbol and manufactures nothing. Both phases stay ROW_NUMBER rather
        // than GROUP BY because dedup preserves the time attribute, keeping `wt`
        // orderable downstream.
        int saltFactor = params.getInt("sql.price.salt.factor", 8);
        String phase1Salt = saltFactor > 1
                ? ",\n                                        MOD(CAST(JSON_VALUE(`val`,"
                        + " '$.event_time') AS BIGINT), " + saltFactor + ")"
                : "";
        return "conflated AS (\n"
                + "  SELECT `val`, wt FROM (\n"
                + "    SELECT `val`, wt, window_start, window_end, `event_ts`,\n"
                + "      ROW_NUMBER() OVER (PARTITION BY window_start, window_end,\n"
                + "                                      JSON_VALUE(`val`, '$.symbol')\n"
                + "                         ORDER BY `event_ts` DESC) AS rn2\n"
                + "    FROM (\n"
                + "      SELECT `val`, window_time AS wt, window_start, window_end, `event_ts`,\n"
                + "        ROW_NUMBER() OVER (PARTITION BY window_start, window_end,\n"
                + "                                        JSON_VALUE(`val`, '$.symbol')"
                + phase1Salt + "\n"
                + "                           ORDER BY `event_ts` DESC) AS rn\n"
                + "      FROM TABLE(TUMBLE(TABLE `" + PRICES + "`, DESCRIPTOR(`event_ts`),"
                + " INTERVAL '" + seconds + "' SECOND))\n"
                + "    ) WHERE rn = 1\n"
                + "  ) WHERE rn2 = 1\n"
                + "),\n"
                + "latest_price AS (\n"
                + "  SELECT symbol, price, event_time FROM (\n"
                + "    SELECT JSON_VALUE(`val`, '$.symbol') AS symbol,\n"
                + "           CAST(JSON_VALUE(`val`, '$.price') AS DECIMAL(18, 2)) AS price,\n"
                + "           CAST(JSON_VALUE(`val`, '$.event_time') AS BIGINT) AS event_time,\n"
                + "           ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(`val`, '$.symbol')\n"
                + "                              ORDER BY wt DESC) AS rn\n"
                + "    FROM conflated\n"
                + "  ) WHERE rn = 1\n"
                + ")";
    }

    // ------------------------------------------------------------------
    // The four INSERTs
    // ------------------------------------------------------------------

    static String positionByAccountTickerSql() {
        return "INSERT INTO `" + SINK_POSITION_ACCOUNT_TICKER + "`\n"
                + "WITH " + DEDUPED_CTE + ",\n" + POSITION_BY_ACCOUNT_TICKER_CTE + "\n"
                + "SELECT acct_key,\n"
                + "       JSON_OBJECT('account' VALUE account, 'ticker' VALUE ticker,\n"
                + "                   'net_qty' VALUE net_qty, 'as_of' VALUE as_of)\n"
                + "FROM positions";
    }

    static String positionByTickerSql() {
        return "INSERT INTO `" + SINK_POSITION_TICKER + "`\n"
                + "WITH " + DEDUPED_CTE + ",\n" + POSITION_BY_TICKER_CTE + "\n"
                + "SELECT ticker,\n"
                + "       JSON_OBJECT('ticker' VALUE ticker, 'net_qty' VALUE net_qty,\n"
                + "                   'as_of' VALUE as_of)\n"
                + "FROM positions";
    }

    static String mvByAccountTickerSql(AppConfig params) {
        return "INSERT INTO `" + SINK_MV_ACCOUNT_TICKER + "`\n"
                + "WITH " + DEDUPED_CTE + ",\n" + POSITION_BY_ACCOUNT_TICKER_CTE + ",\n"
                + latestPriceCte(params) + "\n"
                + "SELECT p.acct_key,\n"
                + "       JSON_OBJECT('account' VALUE p.account, 'ticker' VALUE p.ticker,\n"
                + "                   'net_qty' VALUE p.net_qty,\n"
                + "                   'price' VALUE CAST(lp.price AS STRING),\n"
                + "                   'mv' VALUE CAST(CAST(p.net_qty AS DECIMAL(18, 0)) * lp.price AS STRING),\n"
                + "                   'as_of' VALUE GREATEST(p.as_of, lp.event_time))\n"
                + "FROM positions p\n"
                + "JOIN latest_price lp ON p.ticker = lp.symbol";
    }

    static String mvByTickerSql(AppConfig params) {
        // No 'account' key: the DataStream model omits it for ticker-level rows.
        return "INSERT INTO `" + SINK_MV_TICKER + "`\n"
                + "WITH " + DEDUPED_CTE + ",\n" + POSITION_BY_TICKER_CTE + ",\n"
                + latestPriceCte(params) + "\n"
                + "SELECT p.ticker,\n"
                + "       JSON_OBJECT('ticker' VALUE p.ticker, 'net_qty' VALUE p.net_qty,\n"
                + "                   'price' VALUE CAST(lp.price AS STRING),\n"
                + "                   'mv' VALUE CAST(CAST(p.net_qty AS DECIMAL(18, 0)) * lp.price AS STRING),\n"
                + "                   'as_of' VALUE GREATEST(p.as_of, lp.event_time))\n"
                + "FROM positions p\n"
                + "JOIN latest_price lp ON p.ticker = lp.symbol";
    }

    // ------------------------------------------------------------------

    private static String options(Map<String, String> opts) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> e : opts.entrySet()) {
            sb.append("  ").append(literal(e.getKey())).append(" = ").append(literal(e.getValue()));
            sb.append(++i == opts.size() ? "\n" : ",\n");
        }
        return sb.toString();
    }

    /** SQL string literal — doubles embedded quotes so JAAS configs survive intact. */
    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
