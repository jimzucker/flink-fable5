package com.demo.flink.pipeline;

import com.demo.flink.common.AppConfig;
import com.demo.flink.common.JsonUtil;
import com.demo.flink.model.Position;
import com.demo.flink.model.Trade;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 2 walking skeleton:
 *   trades (Kafka) -> parse -> dedup(trade_id) -> position by account+ticker -> Kafka.
 *
 * Output is an idempotent per-key snapshot (upsert stream), so AT_LEAST_ONCE delivery
 * plus keyed partitioning gives correct end results without transactional consumers.
 */
public final class PositionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(PositionPipeline.class);

    public static void main(String[] args) throws Exception {
        AppConfig params = AppConfig.load(args);

        String bootstrap = params.get("kafka.bootstrap.servers", "localhost:29092");
        String tradesTopic = params.get("topic.trades", "trades");
        String positionsTopic = params.get("topic.position.account.ticker", "position-by-account-ticker");
        long checkpointIntervalMs = params.getLong("checkpoint.interval.ms", 10_000L);
        long dedupTtlMs = params.getLong("dedup.state.ttl.ms", 3_600_000L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (params.has("pipeline.parallelism")) {
            env.setParallelism(params.getInt("pipeline.parallelism", 2));
        }
        env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> tradesSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(tradesTopic)
                .setGroupId(params.get("kafka.group.id", "flink-demo-positions"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<Trade> trades = env
                .fromSource(tradesSource, WatermarkStrategy.noWatermarks(), "trades-source")
                .uid("trades-source")
                .flatMap((String json, Collector<Trade> out) -> {
                    Trade trade = JsonUtil.fromJson(json, Trade.class);
                    if (trade != null && trade.tradeId != null) {
                        out.collect(trade);
                    }
                })
                .returns(Trade.class)
                .name("parse-trade")
                .uid("parse-trade");

        DataStream<Trade> deduped = trades
                .keyBy(t -> t.tradeId)
                .process(new DedupByTradeId(dedupTtlMs))
                .name("dedup-by-trade-id")
                .uid("dedup-by-trade-id");

        DataStream<Position> positions = deduped
                .keyBy(t -> t.account + "|" + t.ticker)
                .process(new PositionAggregator())
                .name("position-by-account-ticker")
                .uid("position-by-account-ticker");

        KafkaSink<Position> positionSink = KafkaSink.<Position>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(new PositionKafkaSerializer(positionsTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        positions.sinkTo(positionSink)
                .name("position-account-ticker-sink")
                .uid("position-account-ticker-sink");

        LOG.info("Starting flink-demo positions pipeline against {}", bootstrap);
        env.execute("flink-demo-positions");
    }
}
