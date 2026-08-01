package com.demo.flink.pipeline;

import com.demo.flink.model.TickerPosition;
import com.demo.flink.model.Trade;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Running net position per ticker across all accounts. */
public class TickerPositionAggregator extends KeyedProcessFunction<String, Trade, TickerPosition> {

    private transient ValueState<Long> netQty;

    @Override
    public void open(OpenContext openContext) {
        netQty = getRuntimeContext().getState(new ValueStateDescriptor<>("net-qty-ticker", Types.LONG));
    }

    @Override
    public void processElement(Trade trade, Context ctx, Collector<TickerPosition> out) throws Exception {
        long updated = (netQty.value() == null ? 0L : netQty.value()) + trade.qty;
        netQty.update(updated);
        out.collect(new TickerPosition(trade.ticker, updated, trade.eventTime));
    }
}
