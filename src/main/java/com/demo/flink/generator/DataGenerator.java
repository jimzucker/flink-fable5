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
        // Volume distribution across symbols.
        //
        // A uniform feed is the least realistic thing a market-data benchmark can
        // do: it makes every symbol equally busy, so "one worker per key" looks
        // like parallelism and key starvation dominates every result. Real tape is
        // Pareto — a small head carries most of the messages while thousands of
        // names are quiet. Zipf is the discrete form: rank r gets weight 1/r^alpha.
        //
        //   distribution=zipf  alpha~1.0  -> top 10 ~ 20-30% of messages,
        //                                    top 100 ~ 60-70%, long tail beyond
        //   ipo.share=0.30                -> ONE symbol takes 30% of the tape on
        //                                    top of the baseline curve, which is
        //                                    the IPO / meme-squeeze day
        String distribution = params.get("generator.distribution", "uniform");
        double zipfAlpha = params.getDouble("generator.zipf.alpha", 1.0);
        double ipoShare = params.getDouble("generator.ipo.share", 0.0);
        int ipoIdx = params.getInt("generator.ipo.ticker", 0);
        // legacy crude switch, kept so earlier runs stay reproducible
        double hotShare = params.getDouble("generator.hot.share", 0.0);
        int hotIdx = params.getInt("generator.hot.ticker", 0);
        // adaptive mode: a symbol is hot once its share exceeds hotFactor x an
        // even share; hot symbols fan out across hotWidth partitions, the rest
        // keep a bare symbol key and their per-symbol ordering.
        double hotFactor = params.getDouble("generator.hot.factor", 2.0);
        int hotWidth = params.getInt("generator.hot.width", 48);
        int tradesPerSec = params.getInt("generator.trades.per.sec", 10);
        int pricesPerSec = params.getInt("generator.prices.per.sec", 20);
        int numAccounts = params.getInt("generator.accounts", 5);
        int numTickers = Math.min(params.getInt("generator.tickers", 10), TICKER_UNIVERSE.length);
        long seed = params.getLong("generator.seed", 42L);
        double duplicateRatio = params.getDouble("generator.duplicate.ratio", 0.05);
        long priceCentsOverride = params.getLong("generator.price.cents.override", -1L);
        // Fixed trade quantity. Correctness checking is far easier when the
        // arithmetic is trivial: with qty=1 and price=$1.00, a position is
        // simply the count of deduped trades for that key, and market value
        // equals that count in dollars. Any rounding, float creep, double-count
        // or dropped record shows up as an off-by-N a human can see, instead of
        // needing a second program to recompute 15,000 aggregates.
        // -1 keeps the random +/-10..2000 quantities used for realistic load.
        long qtyOverride = params.getLong("generator.qty.override", -1L);
        // Distinct, static price per symbol for correctness runs: symbol i is
        // priced at (i+1) dollars and never moves.
        //
        // A single fixed price for every symbol is too simple to be a test: if
        // all prices are $1.00 then a join that matched the WRONG symbol, or a
        // conflation that picked the wrong tick, produces exactly the same
        // answer as a correct one. Distinct prices make a mis-join show up as a
        // wrong multiple. Static prices make conflation lag impossible, so the
        // market-value check stays unambiguous without giving up that coverage.
        //
        // Market value then = (count of deduped trades) x (symbol index + 1)
        // dollars -- still arithmetic a human can verify by eye.
        boolean pricePerSymbol = params.getBoolean("generator.price.per.symbol", false);
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
            // Real US equities are ~8,000-11,000 tradeable symbols. The hardcoded
            // universe holds 30, so anything beyond it is synthesised. Symbol
            // COUNT is what matters for key-space behaviour, not the names.
            tickers[i] = i < TICKER_UNIVERSE.length
                    ? TICKER_UNIVERSE[i]
                    : String.format("SYM%04d", i);
            priceCents[i] = pricePerSymbol
                    ? (long) (i + 1) * 100          // symbol i -> $(i+1).00, static
                    : 1_000 + random.nextInt(49_000); // $10.00 - $500.00
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

        // Cumulative Zipf weights for O(log n) sampling across a wide universe.
        double[] zipfCdf = new double[numTickers];
        if ("zipf".equalsIgnoreCase(distribution)) {
            double acc = 0;
            for (int i = 0; i < numTickers; i++) {
                acc += 1.0 / Math.pow(i + 1, zipfAlpha);
                zipfCdf[i] = acc;
            }
            for (int i = 0; i < numTickers; i++) {
                zipfCdf[i] /= acc;
            }
        }
        long[] symbolCounts = new long[numTickers];
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
                        long qty = qtyOverride > 0
                                ? qtyOverride
                                : (long) (random.nextInt(200) + 1) * 10 * (random.nextBoolean() ? 1 : -1);
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
                    int idx;
                    double roll = random.nextDouble();
                    if (ipoShare > 0 && roll < ipoShare) {
                        idx = Math.min(ipoIdx, numTickers - 1);          // the hot listing
                    } else if (hotShare > 0 && roll < hotShare) {
                        idx = Math.min(hotIdx, numTickers - 1);          // legacy switch
                    } else if ("zipf".equalsIgnoreCase(distribution)) {
                        idx = zipfPick(zipfCdf, random.nextDouble());    // Pareto baseline
                    } else {
                        idx = random.nextInt(numTickers);                // uniform
                    }
                    if (pricePerSymbol) {
                        // leave it alone: the whole point is that it never moves
                    } else if (priceCentsOverride > 0) {
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
                    symbolCounts[idx]++;
                    String pk = "adaptive".equalsIgnoreCase(priceKeyMode)
                            ? adaptiveKey(price.symbol, symbolCounts[idx], pricesSent + 1,
                                          numTickers, hotFactor, hotWidth, pricesSent)
                            : priceKey(priceKeyMode, price.symbol, pricesSent);
                    producer.send(new ProducerRecord<>(pricesTopic, pk, json));
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
    /** Binary search a cumulative Zipf table: rank r has weight 1/r^alpha. */
    static int zipfPick(double[] cdf, double u) {
        int lo = 0, hi = cdf.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cdf[mid] < u) { lo = mid + 1; } else { hi = mid; }
        }
        return lo;
    }

    static String priceKey(String mode, String symbol, long seq) {
        if ("salted".equalsIgnoreCase(mode)) {
            return symbol + "#" + (seq & 0x3F); // 64 buckets, >= any partition count used here
        }
        return symbol;
    }

    /**
     * Adaptive key: salt ONLY the symbols that are actually hot.
     *
     * Salting every symbol works but is heavier than necessary in production —
     * it spreads quiet names across partitions that gain nothing, and it gives
     * up per-symbol ordering and log-compaction behaviour for the whole topic
     * rather than for the one name that needs it. Real feeds are Pareto: on any
     * given day a handful of symbols carry the tape and the rest are quiet.
     *
     * A symbol is "hot" once its share of recent ticks exceeds `hotFactor` times
     * an even share. Hot symbols fan out across `width` partitions; everything
     * else keeps a bare symbol key.
     *
     * Safe because this pipeline is order-independent by construction:
     * conflation selects MAX by event_time and dedup keys on trade_id, so
     * neither depends on a symbol's records sharing a partition or arriving in
     * order. The validation suite is the proof — it re-derives every output from
     * the raw topics and must pass unchanged.
     */
    static String adaptiveKey(String symbol, long symbolCount, long totalCount,
                              int numSymbols, double hotFactor, int width, long seq) {
        if (totalCount < 1000) {
            return symbol; // too early to judge; do not disturb the steady state
        }
        double evenShare = 1.0 / Math.max(1, numSymbols);
        double share = (double) symbolCount / totalCount;
        if (share > evenShare * hotFactor) {
            return symbol + "#" + (seq % width);
        }
        return symbol;
    }
}
