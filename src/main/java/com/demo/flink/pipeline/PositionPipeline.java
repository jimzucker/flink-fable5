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
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
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
 * Delivery guarantee is configurable and defaults to EXACTLY_ONCE.
 *
 * Outputs are idempotent per-key snapshots (upsert streams), so AT_LEAST_ONCE
 * would also give correct end results without transactional consumers — but
 * that is a deliberate weakening, not a free lunch, and it must be stated
 * whenever the pipeline is benchmarked: under EXACTLY_ONCE results are only
 * visible at transactional checkpoint commits, so end-to-end latency is
 * floored by checkpoint.interval.ms. Comparing an AT_LEAST_ONCE pipeline
 * against an exactly-once one measures the guarantee, not the engine.
 */
public final class PositionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(PositionPipeline.class);

    public static void main(String[] args) throws Exception {
        AppConfig params = AppConfig.load(args);

        // One jar, two implementations of the same contract: pipeline.mode=sql
        // runs the Flink SQL statement set instead of this DataStream graph.
        String mode = params.get("pipeline.mode", "datastream").trim().toLowerCase();
        if (mode.equals("sql")) {
            SqlPipeline.run(params);
            return;
        }
        if (!mode.equals("datastream")) {
            throw new IllegalArgumentException("pipeline.mode must be datastream or sql (was: " + mode + ")");
        }

        String bootstrap = params.get("kafka.bootstrap.servers", "localhost:29092");
        long checkpointIntervalMs = params.getLong("checkpoint.interval.ms", 10_000L);
        long dedupTtlMs = params.getLong("dedup.state.ttl.ms", 3_600_000L);
        // CR-1: output cadence is capped to human reading speed. Both knobs are
        // ceilings on OUTPUT rate per key; state always carries the newest value
        // and re-valuation happens at emit time, so an emitted record is never
        // staler than its own interval. 0 = per-event (lowest latency).
        //   position.emit.interval.ms — default 500 ms (positions)
        //   mv.emit.interval.ms       — default 1000 ms (market values)
        // mv.emit.interval.ms also governs price-driven re-valuation: one timer
        // serves both, so nothing is computed that is not emitted. The legacy
        // mv.reval.interval.ms is honoured as a fallback.
        long emitIntervalMs = params.getLong("position.emit.interval.ms",
                params.getLong("emit.interval.ms", 500L));
        long mvRevalIntervalMs = params.getLong("mv.emit.interval.ms",
                params.getLong("mv.reval.interval.ms", 1000L));

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

        DataStream<PriceCents> parsedPrices = env
                .fromSource(
                        stringSource(params, params.get("topic.prices", "prices"), "flink-demo-prices"),
                        WatermarkStrategy.noWatermarks(), "prices-source")
                .uid("prices-source")
                .flatMap(new ParsePriceFunction())
                .name("parse-price").uid("parse-price");

        // Optional phase 1 of two-phase (local-global) conflation. The
        // market-value stages must key on ticker so a tick meets its holders,
        // and with ten tickers only ten workers can ever be busy — every price
        // record funnels through ten subtasks and compute past ten idles. This
        // stage keys on (symbol, salt) first, multiplying the key space, and
        // forwards only the newest tick per shard. Downstream re-keys on symbol
        // and reduces the candidates; newest-of-newest is the global newest.
        // Off by default (factor <= 1 adds no operator) so the baseline
        // topology is unchanged unless the experiment asks for it.
        int saltFactor = params.getInt("price.salt.factor", 1);
        long localConflateMs = params.getLong("price.salt.conflate.ms", mvRevalIntervalMs);
        DataStream<PriceCents> pricesParsed = parsedPrices;
        if (saltFactor > 1) {
            pricesParsed = parsedPrices
                    .keyBy(p -> LocalPriceConflator.saltedKey(p, saltFactor))
                    .process(new LocalPriceConflator(localConflateMs))
                    .name("price-conflate-local").uid("price-conflate-local");
        }
        KeyedStream<PriceCents, String> prices = pricesParsed.keyBy(p -> p.symbol);

        // --- Dedup ---
        DataStream<Trade> deduped = trades
                .keyBy(t -> t.tradeId)
                .process(new DedupByTradeId(dedupTtlMs))
                .name("dedup-by-trade-id").uid("dedup-by-trade-id");

        // --- Output 1: position by account+ticker ---
        DataStream<Position> accountPositions = deduped
                .keyBy(t -> t.account + "|" + t.ticker)
                .process(new PositionAggregator(emitIntervalMs))
                .name("position-by-account-ticker").uid("position-by-account-ticker");

        accountPositions
                .sinkTo(jsonSink(params, params.get("topic.position.account.ticker", "position-by-account-ticker"),
                        (Position p) -> p.account + "|" + p.ticker))
                .name("sink-position-account-ticker").uid("sink-position-account-ticker");

        // --- Output 2: position by ticker ---
        DataStream<TickerPosition> tickerPositions = deduped
                .keyBy(t -> t.ticker)
                .process(new TickerPositionAggregator(emitIntervalMs))
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
        String guarantee = params.get("sink.delivery.guarantee", "exactly-once");
        DeliveryGuarantee dg;
        switch (guarantee) {
            case "exactly-once": dg = DeliveryGuarantee.EXACTLY_ONCE; break;
            case "at-least-once": dg = DeliveryGuarantee.AT_LEAST_ONCE; break;
            case "none": dg = DeliveryGuarantee.NONE; break;
            default: throw new IllegalArgumentException(
                    "sink.delivery.guarantee must be exactly-once | at-least-once | none, got: " + guarantee);
        }
        KafkaSinkBuilder<T> builder = KafkaSink.<T>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:29092"))
                .setRecordSerializer(new JsonKafkaSerializer<>(topic, keyFn))
                .setDeliveryGuarantee(dg)
                .setKafkaProducerConfig(params.kafkaProps());
        if (dg == DeliveryGuarantee.EXACTLY_ONCE) {
            // Each sink needs its own prefix, and the producer's transaction
            // timeout must stay under the broker's transaction.max.timeout.ms
            // (MSK default 15 min) or the transactions are rejected outright.
            builder.setTransactionalIdPrefix(
                    params.get("sink.transactional.id.prefix", "flink-demo") + "-" + topic);
        }
        return builder.build();
    }
}
