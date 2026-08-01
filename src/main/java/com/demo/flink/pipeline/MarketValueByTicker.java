package com.demo.flink.pipeline;

import com.demo.flink.model.MarketValue;
import com.demo.flink.model.PriceCents;
import com.demo.flink.model.TickerPosition;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/** Ticker-level market value: aggregate position x latest price, keyed by ticker. */
public class MarketValueByTicker extends KeyedCoProcessFunction<String, TickerPosition, PriceCents, MarketValue> {

    private transient ValueState<Long> netQty;
    private transient ValueState<Long> lastPriceCents;

    @Override
    public void open(OpenContext openContext) {
        netQty = getRuntimeContext().getState(new ValueStateDescriptor<>("net-qty", Types.LONG));
        lastPriceCents = getRuntimeContext().getState(new ValueStateDescriptor<>("last-price-cents", Types.LONG));
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
        Long qty = netQty.value();
        if (qty != null) {
            out.collect(MarketValue.of(null, price.symbol, qty, price.priceCents, price.eventTime));
        }
    }
}
