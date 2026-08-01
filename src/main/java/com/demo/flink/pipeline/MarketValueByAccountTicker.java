package com.demo.flink.pipeline;

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
 * A position update re-values that account IMMEDIATELY — the order path never
 * waits. Price-driven re-valuation is CONFLATED: each tick stores the latest
 * price, and a per-ticker processing-time timer re-values every holder at most
 * once per revalIntervalMs, absorbing intermediate ticks (demoTicksConflated).
 * This bounds price-driven work at holders x (1000/interval) per ticker/sec
 * regardless of tick rate. revalIntervalMs <= 0 restores per-tick behavior.
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
        ticksConflated = getRuntimeContext().getMetricGroup().counter("demoTicksConflated");
    }

    @Override
    public void processElement1(Position position, Context ctx, Collector<MarketValue> out) throws Exception {
        qtyByAccount.put(position.account, position.netQty);
        Long price = lastPriceCents.value();
        if (price != null) {
            out.collect(MarketValue.of(position.account, position.ticker, position.netQty, price, position.asOf));
        }
    }

    @Override
    public void processElement2(PriceCents price, Context ctx, Collector<MarketValue> out) throws Exception {
        lastPriceCents.update(price.priceCents);
        lastPriceTime.update(price.eventTime);
        if (revalIntervalMs <= 0) {
            revalueAll(price.symbol, out);
            return;
        }
        if (revalPending.value() == null) {
            revalPending.update(true);
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + revalIntervalMs);
        } else {
            ticksConflated.inc();
        }
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
