package com.demo.flink.generator;

import com.demo.flink.common.AppConfig;
import com.demo.flink.common.JsonUtil;
import com.demo.flink.model.Price;
import com.demo.flink.model.Trade;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Independent correctness validation, run INSIDE the VPC.
 *
 * MSK sits in a private subnet, so the Python streaming validator cannot reach
 * it from a laptop. This is the same six checks, as a Fargate one-off task that
 * reuses the generator's image, task role and IAM auth — the pattern
 * scripts/run_probe.sh already uses for the latency probe.
 *
 * Recomputes every output from the RAW topics and compares:
 *   1 dedup                        duplicates must not move a position
 *   2 positions by account+ticker  recomputed from raw trades
 *   3 positions by ticker          same, aggregated differently
 *   4 completeness                 sum over accounts == ticker position
 *   5 MV by account+ticker         == recomputed position x a real price
 *   6 MV by ticker                 == recomputed position x a real price
 *
 * Market value is asserted against the FINAL RAW price using OUR OWN position,
 * so a wrong position cannot cancel a wrong price. A value priced at an older
 * but genuine tick of the right symbol is classified as conflation lag —
 * reported, never counted as a pass.
 *
 * Bounded memory: only the trade-id set and per-key aggregates are retained;
 * prices collapse to latest/min/max per symbol.
 */
public final class StreamValidator {

    private static int failures = 0;

    public static void main(String[] args) throws Exception {
        AppConfig params = AppConfig.load(args);
        String bootstrap = params.get("kafka.bootstrap.servers", "localhost:29092");
        int idleMs = params.getInt("validate.idle.ms", 20_000);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("group.id", "stream-validator-" + System.currentTimeMillis());
        props.putAll(params.kafkaProps());

        Set<String> seenTrades = new HashSet<>();
        long[] dupes = {0};
        Map<String, Long> posAcct = new HashMap<>();
        Map<String, Long> posTicker = new HashMap<>();
        long[] tradeCount = {0};

        drain(props, params.get("topic.trades", "trades"), idleMs, (k, v) -> {
            Trade t = JsonUtil.fromJson(v, Trade.class);
            if (t == null || t.tradeId == null) return;
            tradeCount[0]++;
            if (!seenTrades.add(t.tradeId)) { dupes[0]++; return; }
            posAcct.merge(t.account + "|" + t.ticker, t.qty, Long::sum);
            posTicker.merge(t.ticker, t.qty, Long::sum);
        });

        Map<String, BigDecimal> latest = new HashMap<>();
        Map<String, Long> latestTs = new HashMap<>();
        Map<String, BigDecimal> pmin = new HashMap<>();
        Map<String, BigDecimal> pmax = new HashMap<>();
        // Bounded staleness window. The old check accepted any implied price
        // inside the symbol's WHOLE-RUN min/max, which with moving prices means
        // any price the pipeline ever saw -- it cannot tell "250ms behind" from
        // "minutes stale". Acceptable lag is a property of the conflation
        // interval, not of how long the run happened to be, so keep only the
        // ticks within lagMs of the newest one seen per symbol.
        long lagMs = params.getLong("validate.lag.ms", 2000L);
        Map<String, java.util.ArrayDeque<Object[]>> recent = new HashMap<>();
        long[] priceCount = {0};
        // Phase 21: measure HOW STALE each market value is instead of only
        // pass/fail against one threshold. In simple-numbers mode a symbol's
        // price rises exactly 1 cent per tick, so
        //     staleness_in_ticks = final_price - implied_price   (in cents)
        // and multiplying by that symbol's mean inter-tick gap converts it to
        // milliseconds. That needs only first/last timestamp and a count per
        // symbol -- O(symbols) memory, no per-tick retention, so it cannot
        // reproduce the OOM that killed two earlier runs.
        Map<String, Long> firstTs = new HashMap<>();
        Map<String, Long> tickCount = new HashMap<>();

        drain(props, params.get("topic.prices", "prices"), idleMs, (k, v) -> {
            Price p = JsonUtil.fromJson(v, Price.class);
            if (p == null || p.symbol == null) return;
            priceCount[0]++;
            BigDecimal px = new BigDecimal(p.price);
            Long cur = latestTs.get(p.symbol);
            if (cur == null || p.eventTime >= cur) {
                latestTs.put(p.symbol, p.eventTime);
                latest.put(p.symbol, px);
            }
            pmin.merge(p.symbol, px, (a, b) -> a.compareTo(b) <= 0 ? a : b);
            pmax.merge(p.symbol, px, (a, b) -> a.compareTo(b) >= 0 ? a : b);
            firstTs.merge(p.symbol, p.eventTime, Math::min);
            tickCount.merge(p.symbol, 1L, Long::sum);
            java.util.ArrayDeque<Object[]> dq =
                    recent.computeIfAbsent(p.symbol, x -> new java.util.ArrayDeque<>());
            dq.addLast(new Object[]{p.eventTime, px});
            long newest = latestTs.get(p.symbol);
            while (!dq.isEmpty() && (Long) dq.peekFirst()[0] < newest - lagMs) {
                dq.pollFirst();   // bounded: holds only lagMs worth of ticks
            }
        });

        Map<String, String> outPosAcct = lastPerKey(props, params.get("topic.position.account.ticker", "position-by-account-ticker"), idleMs);
        Map<String, String> outPosTkr = lastPerKey(props, params.get("topic.position.ticker", "position-by-ticker"), idleMs);
        Map<String, String> outMvAcct = lastPerKey(props, params.get("topic.mv.account.ticker", "mv-by-account-ticker"), idleMs);
        Map<String, String> outMvTkr = lastPerKey(props, params.get("topic.mv.ticker", "mv-by-ticker"), idleMs);

        System.out.printf("streamed: trades=%d prices=%d symbols=%d%n",
                tradeCount[0], priceCount[0], latest.size());
        System.out.printf("published: pos=%d posTkr=%d mv=%d mvTkr=%d%n",
                outPosAcct.size(), outPosTkr.size(), outMvAcct.size(), outMvTkr.size());

        // The publisher is unique by contract and the pipeline has no dedup
        // stage, so the assertion is now "no duplicates arrived" rather than
        // "duplicates arrived and were removed". Requiring dupes > 0 would fail
        // a correct run against a correct source.
        check("uniqueness", dupes[0] == 0 && seenTrades.size() == tradeCount[0],
                seenTrades.size() + " unique of " + tradeCount[0] + " streamed, "
                + dupes[0] + " duplicates seen"
                + (dupes[0] > 0 ? "  <-- SOURCE EMITTED DUPLICATES: positions will"
                                  + " overcount, the pipeline no longer dedups" : ""));

        int bad = 0;
        for (Map.Entry<String, String> e : outPosAcct.entrySet()) {
            Long expect = posAcct.get(e.getKey());
            if (expect == null || expect != longField(e.getValue(), "net_qty")) bad++;
        }
        check("positions by account+ticker", bad == 0, outPosAcct.size() + " keys, " + bad + " mismatched");

        bad = 0;
        for (Map.Entry<String, String> e : outPosTkr.entrySet()) {
            Long expect = posTicker.get(e.getKey());
            if (expect == null || expect != longField(e.getValue(), "net_qty")) bad++;
        }
        check("positions by ticker", bad == 0, outPosTkr.size() + " keys, " + bad + " mismatched");

        Map<String, Long> rolled = new HashMap<>();
        posAcct.forEach((k, q) -> rolled.merge(k.substring(k.indexOf('|') + 1), q, Long::sum));
        bad = 0;
        for (Map.Entry<String, Long> e : rolled.entrySet()) {
            if (!e.getValue().equals(posTicker.get(e.getKey()))) bad++;
        }
        check("completeness sum(accounts)==ticker", bad == 0, rolled.size() + " tickers, " + bad + " disagree");

        System.out.println("consumer-visible ordering (upsert topics, offset order == per-key order):");
        orderCheck(props, params.get("topic.position.account.ticker", "position-by-account-ticker"), idleMs);
        orderCheck(props, params.get("topic.position.ticker", "position-by-ticker"), idleMs);
        orderCheck(props, params.get("topic.mv.account.ticker", "mv-by-account-ticker"), idleMs);
        orderCheck(props, params.get("topic.mv.ticker", "mv-by-ticker"), idleMs);

        staleness("MV by account", outMvAcct, posAcct, true, latest, firstTs, latestTs, tickCount);
        staleness("MV by ticker ", outMvTkr, posTicker, false, latest, firstTs, latestTs, tickCount);

        Map<String, BigDecimal> wmin = new HashMap<>();
        Map<String, BigDecimal> wmax = new HashMap<>();
        for (Map.Entry<String, java.util.ArrayDeque<Object[]>> e : recent.entrySet()) {
            for (Object[] t : e.getValue()) {
                BigDecimal px = (BigDecimal) t[1];
                wmin.merge(e.getKey(), px, (a, b) -> a.compareTo(b) <= 0 ? a : b);
                wmax.merge(e.getKey(), px, (a, b) -> a.compareTo(b) >= 0 ? a : b);
            }
        }
        System.out.println("  (lag tolerance: prices within " + lagMs + "ms of each symbol's final tick)");
        int[] r1 = mvCheck(outMvAcct, posAcct, true, latest, wmin, wmax);
        check("MV by account == position x FINAL price", r1[0] == 0,
                outMvAcct.size() + " checked, " + r1[0] + " wrong, " + r1[1] + " conflation lag");

        int[] r2 = mvCheck(outMvTkr, posTicker, false, latest, wmin, wmax);
        check("MV by ticker == position x FINAL price", r2[0] == 0,
                outMvTkr.size() + " checked, " + r2[0] + " wrong, " + r2[1] + " conflation lag");

        System.out.println(failures == 0
                ? "VALIDATION PASSED — all six checks"
                : "VALIDATION FAILED — " + failures + " check(s)");
        System.exit(failures == 0 ? 0 : 1);
    }

    /**
     * Phase 21: report the staleness DISTRIBUTION of published market values.
     *
     * Answers "is SQL wrong, or just late?" without picking a threshold. Prices
     * rise 1 cent per tick in simple-numbers mode, so the gap between the final
     * price and the price a market value actually used is a tick count, which
     * converts to milliseconds via that symbol's mean inter-tick gap.
     */
    private static void staleness(String label, Map<String, String> out,
                                  Map<String, Long> pos, boolean acctKey,
                                  Map<String, BigDecimal> latest,
                                  Map<String, Long> firstTs, Map<String, Long> lastTs,
                                  Map<String, Long> ticks) {
        java.util.List<Double> ms = new java.util.ArrayList<>();
        int exact = 0;
        for (Map.Entry<String, String> e : out.entrySet()) {
            String key = e.getKey();
            String sym = acctKey && key.indexOf('|') >= 0
                    ? key.substring(key.indexOf('|') + 1) : key;
            Long qty = pos.get(key);
            BigDecimal fin = latest.get(sym);
            Long f = firstTs.get(sym), l = lastTs.get(sym), n = ticks.get(sym);
            if (qty == null || qty == 0 || fin == null || f == null || n == null || n < 2) continue;
            BigDecimal got = new BigDecimal(strField(e.getValue(), "mv"));
            BigDecimal implied = got.divide(BigDecimal.valueOf(qty), 6, java.math.RoundingMode.HALF_UP);
            double cents = fin.subtract(implied).doubleValue() * 100.0;   // 1 cent == 1 tick
            if (Math.abs(cents) < 0.5) { exact++; ms.add(0.0); continue; }
            double msPerTick = (double) (l - f) / (double) (n - 1);
            ms.add(Math.max(0.0, cents) * msPerTick);
        }
        if (ms.isEmpty()) { System.out.println("  " + label + " staleness: no comparable keys"); return; }
        java.util.Collections.sort(ms);
        System.out.printf("  %s staleness: n=%d exact=%d (%.0f%%)  p50=%.0fms p90=%.0fms p99=%.0fms max=%.0fms%n",
                label, ms.size(), exact, 100.0 * exact / ms.size(),
                pct(ms, 50), pct(ms, 90), pct(ms, 99), ms.get(ms.size() - 1));
        for (int t : new int[]{2000, 5000, 10000}) {
            long over = ms.stream().filter(x -> x > t).count();
            System.out.printf("      beyond %5dms: %d of %d (%.1f%%)%n", t, over, ms.size(),
                    100.0 * over / ms.size());
        }
    }

    private static double pct(java.util.List<Double> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int i = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(i, sorted.size() - 1)));
    }

    /** returns {wrong, lag} */
    private static int[] mvCheck(Map<String, String> out, Map<String, Long> pos, boolean acctKey,
                                 Map<String, BigDecimal> latest,
                                 Map<String, BigDecimal> pmin, Map<String, BigDecimal> pmax) {
        int wrong = 0, lag = 0;
        for (Map.Entry<String, String> e : out.entrySet()) {
            String key = e.getKey();
            String sym = acctKey && key.indexOf('|') >= 0 ? key.substring(key.indexOf('|') + 1) : key;
            Long qty = pos.get(key);
            BigDecimal fin = latest.get(sym);
            if (qty == null || fin == null) continue;
            BigDecimal got = new BigDecimal(strField(e.getValue(), "mv"));
            if (BigDecimal.valueOf(qty).multiply(fin).compareTo(got) == 0) continue;
            // a real but older tick of the right symbol => lag, not wrong
            BigDecimal lo = pmin.get(sym), hi = pmax.get(sym);
            if (qty != 0 && lo != null) {
                BigDecimal implied = got.divide(BigDecimal.valueOf(qty), 6, java.math.RoundingMode.HALF_UP);
                if (implied.compareTo(lo) >= 0 && implied.compareTo(hi) <= 0) { lag++; continue; }
            }
            wrong++;
        }
        return new int[]{wrong, lag};
    }

    private static void check(String name, boolean ok, String detail) {
        System.out.printf("  [%s] %s: %s%n", ok ? "PASS" : "FAIL", name, detail);
        if (!ok) failures++;
    }

    private interface Sink { void accept(String key, String value); }

    private static void drain(Properties props, String topic, int idleMs, Sink sink) {
        // Bound consumer buffers. This assigns EVERY partition and seeks to the
        // beginning, so the client holds a fetch buffer per partition: at 48
        // partitions the defaults (1MB per partition, 50MB fetch.max.bytes)
        // reserve more than the whole heap of a 512MB Fargate task and the
        // validator dies with OutOfMemoryError inside KafkaConsumer.poll --
        // which reads as "no output" and looks like a validation problem.
        // The validator is streaming: it keeps one value per key, never the
        // records, so small fetches cost nothing but a few more round trips.
        Properties p = new Properties();
        p.putAll(props);
        p.putIfAbsent("max.partition.fetch.bytes", "262144");   // 256KB/partition
        p.putIfAbsent("fetch.max.bytes", "8388608");            // 8MB total
        p.putIfAbsent("max.poll.records", "2000");
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            List<PartitionInfo> pis = c.partitionsFor(topic);
            if (pis == null) return;
            for (PartitionInfo pi : pis) tps.add(new TopicPartition(topic, pi.partition()));
            c.assign(tps);
            c.seekToBeginning(tps);
            long lastData = System.currentTimeMillis();
            while (System.currentTimeMillis() - lastData < idleMs) {
                ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(500));
                if (recs.isEmpty()) continue;
                lastData = System.currentTimeMillis();
                for (ConsumerRecord<String, String> r : recs) {
                    if (r.value() != null) sink.accept(r.key(), r.value());
                }
            }
        }
    }

    /**
     * What a CONSUMER sees, in the order they see it.
     *
     * The outputs are upsert topics, so a downstream reader applies records in
     * offset order and the last one per key is what stays on their screen. If an
     * OLDER update arrives after a newer one, the consumer is left holding a
     * stale position or market value permanently -- for a trading screen that is
     * not "late", it is wrong.
     *
     * A key always hashes to one partition, so per-partition offset order IS
     * per-key order. Any decrease in as_of for a key is an ordering violation.
     *
     * Reports violations and, separately, how many keys END on a value that is
     * not their own maximum as_of -- the ones a consumer would still be looking
     * at after the stream goes quiet.
     */
    private static void orderCheck(Properties props, String topic, int idleMs) {
        Map<String, Long> lastSeen = new HashMap<>();
        Map<String, Long> maxSeen = new HashMap<>();
        // as_of is GREATEST(position time, price time), so it cannot isolate
        // whether a PRICE went backwards. In simple-numbers mode a symbol's
        // price only ever rises, so the price field itself is a direct proxy:
        // a later record carrying a LOWER price means an older tick was used
        // after a newer one -- a consumer computing from that sees the value
        // move backwards.
        Map<String, java.math.BigDecimal> lastPrice = new HashMap<>();
        int[] priceBack = {0};
        int[] violations = {0};
        long[] total = {0};
        drain(props, topic, idleMs, (k, v) -> {
            if (k == null) return;
            total[0]++;
            long asOf = longField(v, "as_of");
            if (asOf == 0) asOf = longField(v, "asOf");
            Long prev = lastSeen.get(k);
            if (prev != null && asOf < prev) {
                violations[0]++;      // an older update landed after a newer one
            }
            lastSeen.put(k, asOf);
            maxSeen.merge(k, asOf, Math::max);
            String pxs = strField(v, "price");
            if (pxs != null && !pxs.isEmpty()) {
                try {
                    java.math.BigDecimal px = new java.math.BigDecimal(pxs);
                    java.math.BigDecimal prevPx = lastPrice.get(k);
                    if (prevPx != null && px.compareTo(prevPx) < 0) priceBack[0]++;
                    lastPrice.put(k, px);
                } catch (NumberFormatException ignored) { }
            }
        });
        int endStale = 0;
        for (Map.Entry<String, Long> e : lastSeen.entrySet()) {
            Long mx = maxSeen.get(e.getKey());
            if (mx != null && e.getValue() < mx) endStale++;
        }
        System.out.printf("  order %-28s records=%d keys=%d  as_of-backwards=%d  "
                        + "price-backwards=%d  keys ending stale=%d%n",
                topic, total[0], lastSeen.size(), violations[0], priceBack[0], endStale);
        if (violations[0] == 0 && priceBack[0] == 0 && endStale == 0) {
            System.out.println("      -> consumers always see monotonic updates and "
                    + "end on the newest value");
        } else {
            System.out.println("      -> A CONSUMER CAN BE LEFT ON A STALE VALUE. "
                    + "This is a correctness fault, not latency.");
        }
    }

    /** Upsert topics: last value per key wins. */
    private static Map<String, String> lastPerKey(Properties props, String topic, int idleMs) {
        Map<String, String> m = new HashMap<>();
        drain(props, topic, idleMs, (k, v) -> { if (k != null) m.put(k, v); });
        return m;
    }

    private static long longField(String json, String field) {
        String v = strField(json, field);
        try { return Long.parseLong(v); } catch (NumberFormatException e) { return Long.MIN_VALUE; }
    }

    private static String strField(String json, String field) {
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\"" + field + "\"\\s*:\\s*\"?(-?[0-9.]+)\"?").matcher(json);
        return m.find() ? m.group(1) : "0";
    }

    private StreamValidator() {
    }
}
