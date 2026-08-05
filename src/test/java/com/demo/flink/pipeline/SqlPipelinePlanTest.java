package com.demo.flink.pipeline;

import com.demo.flink.common.AppConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Plans the SQL statement set without submitting it. This is the only local
 * check that catches a syntax error, a bad connector option or an unsupported
 * changelog mode before the job reaches a cluster — planning creates the
 * connectors but never talks to Kafka.
 */
class SqlPipelinePlanTest {

    private static AppConfig config(String... kv) throws IOException {
        return AppConfig.load(kv);
    }

    private static String plan(AppConfig params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SqlPipeline.configure(tableEnv, params);
        SqlPipeline.createTables(tableEnv, params);
        return SqlPipeline.buildStatementSet(tableEnv, params).explain();
    }

    @Test
    void statementSetPlansAllFourSinksAsOneFusedJob() throws Exception {
        String explained = plan(config());
        assertTrue(explained.contains(SqlPipeline.SINK_POSITION_ACCOUNT_TICKER), explained);
        assertTrue(explained.contains(SqlPipeline.SINK_POSITION_TICKER), explained);
        assertTrue(explained.contains(SqlPipeline.SINK_MV_ACCOUNT_TICKER), explained);
        assertTrue(explained.contains(SqlPipeline.SINK_MV_TICKER), explained);
        // Dedup keeps the first arrival per trade_id, exactly like DedupByTradeId.
        assertTrue(explained.contains("Deduplicate(keep=[FirstRow]"), explained);
        // Latest price is a keep-last deduplication, not a window — never stalls.
        assertTrue(explained.contains("Deduplicate(keep=[LastRow]"), explained);
        // One scan per topic, shared by all four outputs (no duplicated source).
        assertTrue(explained.contains("Reused(reference_id="), explained);
        // Money stays decimal all the way to the string.
        assertTrue(explained.contains("CAST(net_qty AS DECIMAL(18, 0)) * price"), explained);
    }

    @Test
    void statementSetPlansWithExactlyOnceAndTumbleConflation() throws Exception {
        String explained = plan(config(
                "--sink.delivery.guarantee", "exactly-once",
                "--sql.price.conflate.ms", "250",
                "--sql.minibatch.latency.ms", "200",
                "--sql.state.ttl.ms", "3600000"));
        assertTrue(explained.contains("WindowDeduplicate"), explained);
        assertTrue(explained.contains("size=[250 ms]"), explained);
        assertTrue(explained.contains("MiniBatchAssigner"), explained);
        assertTrue(explained.contains(SqlPipeline.SINK_MV_TICKER), explained);
    }
}
