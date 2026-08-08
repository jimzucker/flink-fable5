package com.demo.flink.pipeline;

import com.demo.flink.common.DemoMetrics;
import com.demo.flink.model.MarketValue;
import com.demo.flink.model.Position;
import com.demo.flink.model.PriceCents;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Enrichment join keyed by ticker: latest price x per-account positions.
 *
 * Both inputs feed state; a single per-ticker processing-time timer decides
 * when that state reaches the output. Each price tick stores the latest price;
 * each position update stores the latest quantity; on the timer every holder
 * is re-valued once from the newest of both and emitted.
 *
 * That single timer serves two purposes at once (CR-1): it bounds price-driven
 * work at holders x (1000/interval) per ticker/sec regardless of tick rate,
 * AND it caps the output cadence at one update per account+ticker per
 * interval. Because the re-valuation happens AT emit time rather than being
 * computed early and held, the emitted value is always the freshest available
 * — max staleness is one interval, with no wasted intermediate re-valuations.
 *
 * intervalMs <= 0 restores per-event behaviour (lowest latency, unbounded
 * output rate). Note this supersedes the earlier "position updates emit
 * immediately" behaviour: under CR-1 a capped output rate necessarily delays
 * position-driven updates too, which is the trade the requirement asks for.
 *
 * Final state after quiesce is unchanged: position x latest price.
 */
public class MarketValueByAccountTicker extends KeyedCoProcessFunction<String, Position, PriceCents, MarketValue> {

    private final long revalIntervalMs;
    private transient MapState<String, Long> qtyByAccount;
    private transient ValueState<Long> lastPriceCents;
    private transient ValueState<Long> lastPriceTime;
    private transient ValueState<Boolean> revalPending;
    private transient Counter ticksConflated;
    private transient Counter staleTicksDropped;

    public MarketValueByAccountTicker(long revalIntervalMs) {
        this.revalIntervalMs = revalIntervalMs;
    }

    @Override
    public void open(OpenContext openContext) {
        qtyByAccount = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("qty-by-account", Types.STRING, Types.LONG));
        lastPriceCents = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-price-cents", Types.LONG));
        lastPriceTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-price-time", Types.LONG));
        revalPending = getRuntimeContext().getState(
                new ValueStateDescriptor<>("reval-pending", Types.BOOLEAN));
        ticksConflated = DemoMetrics.counter(getRuntimeContext().getMetricGroup(), "demoTicksConflated");
        staleTicksDropped = DemoMetrics.counter(getRuntimeContext().getMetricGroup(), "demoStaleTicksDropped");
    }

    @Override
    public void processElement1(Position position, Context ctx, Collector<MarketValue> out) throws Exception {
        qtyByAccount.put(position.account, position.netQty);
        Long price = lastPriceCents.value();
        if (price == null) {
            return;
        }
        if (revalIntervalMs <= 0) {
            out.collect(MarketValue.of(position.account, position.ticker, position.netQty, price, position.asOf));
            return;
        }
        scheduleEmit(ctx);
    }

    /** Ensure exactly one pending timer per key, so emission is capped per interval. */
    private void scheduleEmit(Context ctx) throws Exception {
        if (revalPending.value() == null) {
            revalPending.update(true);
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + revalIntervalMs);
        } else {
            ticksConflated.inc();
        }
    }

    @Override
    public void processElement2(PriceCents price, Context ctx, Collector<MarketValue> out) throws Exception {
        // Keep the NEWEST tick by event time, not the last one to arrive.
        //
        // Arrival order is not event order here. Salted/adaptive price keys put
        // one symbol's ticks on several partitions specifically to break the
        // hotspot, and Kafka only orders within a partition -- so the keyBy on
        // symbol merges several input channels and interleaves them freely.
        // The sinks are upsert (last write wins per key), so an unguarded
        // overwrite lets a stale tick become the published market value and stay
        // there: wrong numbers on a green, fast job.
        //
        // LocalPriceConflator already guards exactly this way; the guard was
        // simply missing on the operator that decides the published value.
        Long heldTime = lastPriceTime.value();
        if (heldTime != null && heldTime > price.eventTime) {
            staleTicksDropped.inc();
            return;
        }
        lastPriceCents.update(price.priceCents);
        lastPriceTime.update(price.eventTime);
        if (revalIntervalMs <= 0) {
            revalueAll(price.symbol, out);
            return;
        }
        scheduleEmit(ctx);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<MarketValue> out) throws Exception {
        revalPending.clear();
        revalueAll(ctx.getCurrentKey(), out);
    }

    private void revalueAll(String ticker, Collector<MarketValue> out) throws Exception {
        Long price = lastPriceCents.value();
        if (price == null) {
            return;
        }
        long asOf = lastPriceTime.value() == null ? 0L : lastPriceTime.value();
        for (var entry : qtyByAccount.entries()) {
            out.collect(MarketValue.of(entry.getKey(), ticker, entry.getValue(), price, asOf));
        }
    }
}
