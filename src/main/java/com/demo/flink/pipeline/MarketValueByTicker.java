package com.demo.flink.pipeline;

import com.demo.flink.model.MarketValue;
import com.demo.flink.model.PriceCents;
import com.demo.flink.model.TickerPosition;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Ticker-level market value with the same conflation contract as the
 * account-level join: position updates emit immediately; price-driven
 * re-valuation fires at most once per revalIntervalMs with the latest price.
 */
public class MarketValueByTicker extends KeyedCoProcessFunction<String, TickerPosition, PriceCents, MarketValue> {

    private final long revalIntervalMs;
    private transient ValueState<Long> netQty;
    private transient ValueState<Long> lastPriceCents;
    private transient ValueState<Long> lastPriceTime;
    private transient ValueState<Boolean> revalPending;
    private transient Counter ticksConflated;

    public MarketValueByTicker(long revalIntervalMs) {
        this.revalIntervalMs = revalIntervalMs;
    }

    @Override
    public void open(OpenContext openContext) {
        netQty = getRuntimeContext().getState(new ValueStateDescriptor<>("net-qty", Types.LONG));
        lastPriceCents = getRuntimeContext().getState(new ValueStateDescriptor<>("last-price-cents", Types.LONG));
        lastPriceTime = getRuntimeContext().getState(new ValueStateDescriptor<>("last-price-time", Types.LONG));
        revalPending = getRuntimeContext().getState(new ValueStateDescriptor<>("reval-pending", Types.BOOLEAN));
        ticksConflated = getRuntimeContext().getMetricGroup().counter("demoTicksConflated");
    }

    @Override
    public void processElement1(TickerPosition position, Context ctx, Collector<MarketValue> out) throws Exception {
        netQty.update(position.netQty);
        Long price = lastPriceCents.value();
        if (price != null) {
            out.collect(MarketValue.of(null, position.ticker, position.netQty, price, position.asOf));
        }
    }

    @Override
    public void processElement2(PriceCents price, Context ctx, Collector<MarketValue> out) throws Exception {
        lastPriceCents.update(price.priceCents);
        lastPriceTime.update(price.eventTime);
        if (revalIntervalMs <= 0) {
            revalue(price.symbol, out);
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
        revalue(ctx.getCurrentKey(), out);
    }

    private void revalue(String ticker, Collector<MarketValue> out) throws Exception {
        Long price = lastPriceCents.value();
        Long qty = netQty.value();
        if (price == null || qty == null) {
            return;
        }
        long asOf = lastPriceTime.value() == null ? 0L : lastPriceTime.value();
        out.collect(MarketValue.of(null, ticker, qty, price, asOf));
    }
}
