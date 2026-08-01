package com.demo.flink.pipeline;

import com.demo.flink.common.AppConfig;
import com.demo.flink.model.MarketValue;
import com.demo.flink.model.Position;
import com.demo.flink.model.PriceCents;
import com.demo.flink.model.TickerPosition;
import com.demo.flink.model.Trade;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 3 pipeline — all four required outputs:
 *
 *   trades  -> parse -> dedup(trade_id) -+-> position by account+ticker -+-> Kafka
 *                                        |                               +-> (join price) mv by account+ticker -> Kafka
 *                                        +-> position by ticker         -+-> Kafka
 *                                                                        +-> (join price) mv by ticker -> Kafka
 *   prices  -> parse (exact long cents) -> latest price per ticker (in the joins)
 *
 * Outputs are idempotent per-key snapshots (upsert streams), so AT_LEAST_ONCE
 * delivery plus keyed partitioning gives correct end results without
 * transactional consumers.
 */
public final class PositionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(PositionPipeline.class);

    public static void main(String[] args) throws Exception {
        AppConfig params = AppConfig.load(args);

        String bootstrap = params.get("kafka.bootstrap.servers", "localhost:29092");
        long checkpointIntervalMs = params.getLong("checkpoint.interval.ms", 10_000L);
        long dedupTtlMs = params.getLong("dedup.state.ttl.ms", 3_600_000L);
        long mvRevalIntervalMs = params.getLong("mv.reval.interval.ms", 250L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (params.has("pipeline.parallelism")) {
            env.setParallelism(params.getInt("pipeline.parallelism", 2));
        }
        env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);

        // --- Sources ---
        DataStream<Trade> trades = env
                .fromSource(
                        stringSource(params, params.get("topic.trades", "trades"), "flink-demo-trades"),
                        WatermarkStrategy.noWatermarks(), "trades-source")
                .uid("trades-source")
                .flatMap(new ParseTradeFunction())
                .name("parse-trade").uid("parse-trade");

        KeyedStream<PriceCents, String> prices = env
                .fromSource(
                        stringSource(params, params.get("topic.prices", "prices"), "flink-demo-prices"),
                        WatermarkStrategy.noWatermarks(), "prices-source")
                .uid("prices-source")
                .flatMap(new ParsePriceFunction())
                .name("parse-price").uid("parse-price")
                .keyBy(p -> p.symbol);

        // --- Dedup ---
        DataStream<Trade> deduped = trades
                .keyBy(t -> t.tradeId)
                .process(new DedupByTradeId(dedupTtlMs))
                .name("dedup-by-trade-id").uid("dedup-by-trade-id");

        // --- Output 1: position by account+ticker ---
        DataStream<Position> accountPositions = deduped
                .keyBy(t -> t.account + "|" + t.ticker)
                .process(new PositionAggregator())
                .name("position-by-account-ticker").uid("position-by-account-ticker");

        accountPositions
                .sinkTo(jsonSink(params, params.get("topic.position.account.ticker", "position-by-account-ticker"),
                        (Position p) -> p.account + "|" + p.ticker))
                .name("sink-position-account-ticker").uid("sink-position-account-ticker");

        // --- Output 2: position by ticker ---
        DataStream<TickerPosition> tickerPositions = deduped
                .keyBy(t -> t.ticker)
                .process(new TickerPositionAggregator())
                .name("position-by-ticker").uid("position-by-ticker");

        tickerPositions
                .sinkTo(jsonSink(params, params.get("topic.position.ticker", "position-by-ticker"),
                        (TickerPosition p) -> p.ticker))
                .name("sink-position-ticker").uid("sink-position-ticker");

        // --- Output 3: market value by account+ticker (position x latest price) ---
        DataStream<MarketValue> mvByAccount = accountPositions
                .keyBy(p -> p.ticker)
                .connect(prices)
                .process(new MarketValueByAccountTicker(mvRevalIntervalMs))
                .name("mv-by-account-ticker").uid("mv-by-account-ticker");

        mvByAccount
                .sinkTo(jsonSink(params, params.get("topic.mv.account.ticker", "mv-by-account-ticker"),
                        (MarketValue mv) -> mv.account + "|" + mv.ticker))
                .name("sink-mv-account-ticker").uid("sink-mv-account-ticker");

        // --- Output 4: market value by ticker ---
        DataStream<MarketValue> mvByTicker = tickerPositions
                .keyBy(p -> p.ticker)
                .connect(prices)
                .process(new MarketValueByTicker(mvRevalIntervalMs))
                .name("mv-by-ticker").uid("mv-by-ticker");

        mvByTicker
                .sinkTo(jsonSink(params, params.get("topic.mv.ticker", "mv-by-ticker"),
                        (MarketValue mv) -> mv.ticker))
                .name("sink-mv-ticker").uid("sink-mv-ticker");

        LOG.info("Starting flink-demo pipeline against {}", bootstrap);
        env.execute("flink-demo-positions");
    }

    private static KafkaSource<String> stringSource(AppConfig params, String topic, String defaultGroup) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:29092"))
                .setTopics(topic)
                .setGroupId(params.get("kafka.group.id", defaultGroup) + "-" + topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(params.kafkaProps())
                .build();
    }

    private static <T> KafkaSink<T> jsonSink(AppConfig params, String topic, JsonKafkaSerializer.KeyFn<T> keyFn) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:29092"))
                .setRecordSerializer(new JsonKafkaSerializer<>(topic, keyFn))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(params.kafkaProps())
                .build();
    }
}
