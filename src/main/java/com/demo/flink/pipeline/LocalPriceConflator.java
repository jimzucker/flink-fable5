package com.demo.flink.pipeline;

import com.demo.flink.common.DemoMetrics;
import com.demo.flink.model.PriceCents;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Phase 1 of a two-phase (local-global) price conflation — the DataStream
 * equivalent of confluent/sql/optimized/salted_conflate.sql.
 *
 * WHY THIS EXISTS
 * Flink gives one worker per key. The market-value stages must key on ticker,
 * because a price tick has to meet the positions holding that ticker — and
 * with ten tickers only ten workers can ever be busy. Every price record in
 * the feed funnels through those ten subtasks, so compute past ten sits idle
 * and added parallelism buys nothing.
 *
 * This stage runs BEFORE that narrowing. It keys on (symbol, salt) instead of
 * symbol alone, multiplying the key space by the salt factor, and keeps only
 * the newest tick per shard per interval. Downstream then re-keys on symbol
 * and sees at most saltFactor candidates per interval instead of the raw feed.
 *
 * Correct by associativity: the newest of the per-shard newest IS the global
 * newest — the same argument that makes emission conflation safe. The
 * market-value operators already retain only the latest price, so receiving
 * several candidates and keeping the newest changes no result.
 *
 * The salt MUST vary per RECORD, not per key. Hashing the symbol would yield a
 * constant per symbol and manufacture no parallelism at all; event time modulo
 * the factor spreads consecutive ticks across shards evenly.
 *
 * saltFactor <= 1 disables the stage entirely (the operator is not added to
 * the graph), restoring the original single-phase path.
 */
public class LocalPriceConflator extends KeyedProcessFunction<String, PriceCents, PriceCents> {

    private final long intervalMs;
    private transient ValueState<PriceCents> newest;
    private transient ValueState<Boolean> emitPending;
    private transient Counter ticksConflated;

    public LocalPriceConflator(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    /** Shard key: symbol plus a per-record salt derived from event time. */
    public static String saltedKey(PriceCents p, int saltFactor) {
        return p.symbol + "#" + Math.floorMod(p.eventTime, saltFactor);
    }

    @Override
    public void open(OpenContext openContext) {
        newest = getRuntimeContext().getState(
                new ValueStateDescriptor<>("newest-price", Types.POJO(PriceCents.class)));
        emitPending = getRuntimeContext().getState(
                new ValueStateDescriptor<>("emit-pending", Types.BOOLEAN));
        ticksConflated = DemoMetrics.counter(
                getRuntimeContext().getMetricGroup(), "demoLocalTicksConflated");
    }

    @Override
    public void processElement(PriceCents price, Context ctx, Collector<PriceCents> out)
            throws Exception {
        PriceCents held = newest.value();
        if (held != null && held.eventTime > price.eventTime) {
            // Out-of-order tick: the one already held is newer, so this one
            // would never survive the global reduction either. Drop it.
            ticksConflated.inc();
            return;
        }
        if (held != null) {
            ticksConflated.inc();
        }
        newest.update(price);

        if (intervalMs <= 0) {
            // Conflation disabled: still shard the key space, but forward every
            // tick so latency is unchanged from the single-phase path.
            newest.clear();
            out.collect(price);
            return;
        }
        if (!Boolean.TRUE.equals(emitPending.value())) {
            emitPending.update(true);
            long fire = (ctx.timerService().currentProcessingTime() / intervalMs + 1) * intervalMs;
            ctx.timerService().registerProcessingTimeTimer(fire);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PriceCents> out)
            throws Exception {
        emitPending.clear();
        PriceCents held = newest.value();
        if (held != null) {
            newest.clear();
            out.collect(held);
        }
    }
}
