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
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Enrichment join keyed by ticker: latest price x per-account positions.
 * A position update re-values that account; a price tick re-values every
 * account holding the ticker. Price magnitude never changes the work done
 * (exact long-cents/BigDecimal math) — perf Case 2 by construction.
 */
public class MarketValueByAccountTicker extends KeyedCoProcessFunction<String, Position, PriceCents, MarketValue> {

    private transient MapState<String, Long> qtyByAccount;
    private transient ValueState<Long> lastPriceCents;

    @Override
    public void open(OpenContext openContext) {
        qtyByAccount = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("qty-by-account", Types.STRING, Types.LONG));
        lastPriceCents = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-price-cents", Types.LONG));
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
        for (var entry : qtyByAccount.entries()) {
            out.collect(MarketValue.of(entry.getKey(), price.symbol, entry.getValue(), price.priceCents, price.eventTime));
        }
    }
}
