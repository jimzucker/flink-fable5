package com.demo.flink.pipeline;

import com.demo.flink.model.Position;
import com.demo.flink.model.Trade;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Running net position per (account, ticker). O(1) state update per trade,
 * emits a full snapshot per update so the output topic is an upsert stream.
 */
public class PositionAggregator extends KeyedProcessFunction<String, Trade, Position> {

    private transient ValueState<Long> netQty;

    @Override
    public void open(OpenContext openContext) {
        netQty = getRuntimeContext().getState(new ValueStateDescriptor<>("net-qty", Types.LONG));
    }

    @Override
    public void processElement(Trade trade, Context ctx, Collector<Position> out) throws Exception {
        long updated = (netQty.value() == null ? 0L : netQty.value()) + trade.qty;
        netQty.update(updated);
        out.collect(new Position(trade.account, trade.ticker, updated, trade.eventTime));
    }
}
