package com.demo.flink.generator;

import com.demo.flink.common.AppConfig;
import com.demo.flink.common.JsonUtil;
import com.demo.flink.model.Price;
import com.demo.flink.model.Trade;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Seeded mock producer for trades and ticking prices. All content (accounts, tickers,
 * quantities, price walk, duplicate injection) is deterministic for a given seed;
 * rates and universe sizes are pure configuration — no rebuild to change behavior.
 */
public final class DataGenerator {

    private static final String[] TICKER_UNIVERSE = {
            "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "JPM", "GS", "MS",
            "BAC", "C", "WFC", "V", "MA", "XOM", "CVX", "PFE", "JNJ", "UNH",
            "HD", "PG", "KO", "PEP", "DIS", "NFLX", "INTC", "AMD", "CRM", "ORCL"
    };

    public static void main(String[] args) throws Exception {
        AppConfig params = AppConfig.load(args);

        String bootstrap = params.get("kafka.bootstrap.servers", "localhost:29092");
        String tradesTopic = params.get("topic.trades", "trades");
        String pricesTopic = params.get("topic.prices", "prices");
        // "symbol" (default, unchanged) or "salted" — see the send() call below.
        String priceKeyMode = params.get("generator.price.key.mode", "symbol");
        // Skewed feed. Real tape is never uniform: on an IPO, an index rebalance
        // or a squeeze, ONE symbol can carry most of the volume. A uniform
        // benchmark hides the consequence entirely, because "one worker per key"
        // only looks like parallelism when the keys are evenly loaded.
        // hot.share = fraction of price ticks forced onto generator.hot.ticker
        // (index into the ticker array). 0 disables, restoring a uniform feed.
        double hotShare = params.getDouble("generator.hot.share", 0.0);
        int hotIdx = params.getInt("generator.hot.ticker", 0);
        int tradesPerSec = params.getInt("generator.trades.per.sec", 10);
        int pricesPerSec = params.getInt("generator.prices.per.sec", 20);
        int numAccounts = params.getInt("generator.accounts", 5);
        int numTickers = Math.min(params.getInt("generator.tickers", 10), TICKER_UNIVERSE.length);
        long seed = params.getLong("generator.seed", 42L);
        double duplicateRatio = params.getDouble("generator.duplicate.ratio", 0.05);
        long priceCentsOverride = params.getLong("generator.price.cents.override", -1L);
        // Trade ids are namespaced per run so a restarted generator produces NEW
        // trades instead of replaying ids that dedup (correctly) absorbs.
        // Pin it in config for fully reproducible runs.
        String runId = params.get("generator.run.id", "-1");
        if ("-1".equals(runId)) {
            runId = Long.toString(System.currentTimeMillis() / 1000, 36);
        }

        Random random = new Random(seed);

        List<String> accounts = new ArrayList<>();
        for (int i = 1; i <= numAccounts; i++) {
            accounts.add(String.format("ACC-%03d", i));
        }
        String[] tickers = new String[numTickers];
        long[] priceCents = new long[numTickers];
        for (int i = 0; i < numTickers; i++) {
            tickers[i] = TICKER_UNIVERSE[i];
            priceCents[i] = 1_000 + random.nextInt(49_000); // $10.00 - $500.00
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("linger.ms", "5");
        props.putAll(params.kafkaProps()); // e.g. MSK IAM auth on AWS

        // Idempotent topic creation (config-driven partitions) — replaces the
        // docker-compose kafka-init container when running against MSK.
        int partitions = params.getInt("topics.partitions", 0);
        if (partitions > 0) {
            Properties adminProps = new Properties();
            adminProps.put("bootstrap.servers", bootstrap);
            adminProps.putAll(params.kafkaProps());
            List<String> topics = List.of(
                    tradesTopic, pricesTopic,
                    params.get("topic.position.account.ticker", "position-by-account-ticker"),
                    params.get("topic.position.ticker", "position-by-ticker"),
                    params.get("topic.mv.account.ticker", "mv-by-account-ticker"),
                    params.get("topic.mv.ticker", "mv-by-ticker"));
            try (AdminClient admin = AdminClient.create(adminProps)) {
                if (params.getBoolean("topics.recreate", false)) {
                    try {
                        admin.deleteTopics(topics).all().get();
                        System.out.println("generator: deleted topics for clean recreate");
                        Thread.sleep(10_000); // let deletion propagate before recreate
                    } catch (ExecutionException e) {
                        if (!(e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)) {
                            throw e;
                        }
                    }
                }
                for (String topic : topics) {
                    try {
                        admin.createTopics(List.of(
                                new NewTopic(topic, Optional.of(partitions), Optional.empty()))).all().get();
                        System.out.println("generator: created topic " + topic);
                    } catch (ExecutionException e) {
                        if (!(e.getCause() instanceof TopicExistsException)) {
                            throw e;
                        }
                        // Existing topic: grow to the requested partition count if smaller
                        // (partitions can only increase in Kafka; shrink is ignored).
                        int current = admin.describeTopics(List.of(topic)).allTopicNames().get()
                                .get(topic).partitions().size();
                        if (current < partitions) {
                            admin.createPartitions(
                                    java.util.Map.of(topic, org.apache.kafka.clients.admin.NewPartitions
                                            .increaseTo(partitions))).all().get();
                            System.out.println("generator: grew topic " + topic
                                    + " " + current + " -> " + partitions + " partitions");
                        }
                    }
                }
            }
        }

        long tradeSeq = 0;
        long tradesSent = 0;
        long duplicatesSent = 0;
        long pricesSent = 0;
        long bytesSent = 0;
        long lastReport = System.currentTimeMillis();
        Deque<String> recentTrades = new ArrayDeque<>();

        System.out.printf("generator: %d trades/sec, %d prices/sec, %d accounts, %d tickers, seed=%d, dup=%.2f, run=%s -> %s%n",
                tradesPerSec, pricesPerSec, numAccounts, numTickers, seed, duplicateRatio, runId, bootstrap);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                long tickStart = System.currentTimeMillis();

                for (int i = 0; i < tradesPerSec; i++) {
                    String json;
                    String key;
                    if (!recentTrades.isEmpty() && random.nextDouble() < duplicateRatio) {
                        json = recentTrades.peekLast(); // resend an already-sent trade verbatim
                        key = JsonUtil.fromJson(json, Trade.class).tradeId;
                        duplicatesSent++;
                    } else {
                        tradeSeq++;
                        long qty = (long) (random.nextInt(200) + 1) * 10 * (random.nextBoolean() ? 1 : -1);
                        Trade trade = new Trade(
                                String.format("T-%s-%08d", runId, tradeSeq),
                                accounts.get(random.nextInt(accounts.size())),
                                tickers[random.nextInt(numTickers)],
                                qty,
                                System.currentTimeMillis());
                        json = JsonUtil.toJson(trade);
                        key = trade.tradeId;
                        recentTrades.addLast(json);
                        if (recentTrades.size() > 100) {
                            recentTrades.removeFirst();
                        }
                    }
                    producer.send(new ProducerRecord<>(tradesTopic, key, json));
                    tradesSent++;
                    bytesSent += json.length();
                }

                for (int i = 0; i < pricesPerSec; i++) {
                    int idx = (hotShare > 0 && random.nextDouble() < hotShare)
                            ? Math.min(hotIdx, numTickers - 1)
                            : random.nextInt(numTickers);
                    if (priceCentsOverride > 0) {
                        priceCents[idx] = priceCentsOverride; // Case 2: extreme price, config only
                    } else {
                        long delta = Math.round(priceCents[idx] * random.nextGaussian() * 0.001);
                        priceCents[idx] = Math.max(100, priceCents[idx] + delta);
                    }
                    String priceStr = BigDecimal.valueOf(priceCents[idx])
                            .divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP)
                            .toPlainString();
                    Price price = new Price(tickers[idx], priceStr, System.currentTimeMillis());
                    String json = JsonUtil.toJson(price);
                    // Price record key. Kafka's default partitioner hashes the KEY, so
                    // keying by symbol puts a 10-symbol feed into at most 10 partitions
                    // however many the topic has. That single fact caused both of the
                    // worst problems in this project:
                    //   * the Flink source could only read ~10-way regardless of
                    //     partition or CFU count, so query-side salting could not help
                    //     (a downstream PARTITION BY cannot widen a source), and a
                    //     Confluent pool capped at 20 CFU never drew more than 10;
                    //   * the empty partitions never advanced a watermark, so an
                    //     event-time TUMBLE never fired and the job consumed 119k
                    //     records/sec while writing zero rows.
                    // The key is not needed for correctness: every downstream query
                    // reads the symbol out of the VALUE, never the key. "salted"
                    // spreads records across all partitions so the source can read
                    // wide. Default stays "symbol" so existing runs are unchanged.
                    producer.send(new ProducerRecord<>(pricesTopic,
                            priceKey(priceKeyMode, price.symbol, pricesSent), json));
                    pricesSent++;
                    bytesSent += json.length();
                }

                long now = System.currentTimeMillis();
                if (now - lastReport >= 10_000) {
                    System.out.printf("generator: trades=%d (dups=%d) prices=%d volume=%.1f KB total%n",
                            tradesSent, duplicatesSent, pricesSent, bytesSent / 1024.0);
                    lastReport = now;
                }

                long elapsed = System.currentTimeMillis() - tickStart;
                if (elapsed < 1_000) {
                    Thread.sleep(1_000 - elapsed);
                }
            }
        }
    }

    /**
     * Partition-spreading key for price records.
     *
     * Kafka's default partitioner hashes the key, so a 10-symbol feed keyed by
     * symbol occupies at most 10 partitions no matter how wide the topic is.
     * "salted" appends a rotating suffix so records spread across every
     * partition, letting a Flink source read wide and keeping every partition
     * non-idle (idle partitions stall event-time watermarks). Downstream
     * queries read the symbol from the value, so the key is free to change.
     */
    static String priceKey(String mode, String symbol, long seq) {
        if ("salted".equalsIgnoreCase(mode)) {
            return symbol + "#" + (seq & 0x3F); // 64 buckets, >= any partition count used here
        }
        return symbol;
    }
}
