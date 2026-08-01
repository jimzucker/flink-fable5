package com.demo.flink.generator;

import com.demo.flink.common.AppConfig;
import com.demo.flink.common.JsonUtil;
import com.demo.flink.model.Price;
import com.demo.flink.model.Trade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.Random;

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
        int tradesPerSec = params.getInt("generator.trades.per.sec", 10);
        int pricesPerSec = params.getInt("generator.prices.per.sec", 20);
        int numAccounts = params.getInt("generator.accounts", 5);
        int numTickers = Math.min(params.getInt("generator.tickers", 10), TICKER_UNIVERSE.length);
        long seed = params.getLong("generator.seed", 42L);
        double duplicateRatio = params.getDouble("generator.duplicate.ratio", 0.05);
        long priceCentsOverride = params.getLong("generator.price.cents.override", -1L);

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

        long tradeSeq = 0;
        long tradesSent = 0;
        long duplicatesSent = 0;
        long pricesSent = 0;
        long bytesSent = 0;
        long lastReport = System.currentTimeMillis();
        Deque<String> recentTrades = new ArrayDeque<>();

        System.out.printf("generator: %d trades/sec, %d prices/sec, %d accounts, %d tickers, seed=%d, dup=%.2f -> %s%n",
                tradesPerSec, pricesPerSec, numAccounts, numTickers, seed, duplicateRatio, bootstrap);

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
                                String.format("T-%08d", tradeSeq),
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
                    int idx = random.nextInt(numTickers);
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
                    producer.send(new ProducerRecord<>(pricesTopic, price.symbol, json));
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
}
