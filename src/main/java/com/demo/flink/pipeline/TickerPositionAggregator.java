package com.demo.flink.pipeline;

import com.demo.flink.model.TickerPosition;
import com.demo.flink.model.Trade;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Running net position per ticker across all accounts. Same emission
 * conflation contract as PositionAggregator (0 = per-update).
 */
public class TickerPositionAggregator extends KeyedProcessFunction<String, Trade, TickerPosition> {

    private final long emitIntervalMs;

    private transient ValueState<Long> netQty;
    private transient ValueState<TickerPosition> pending;

    public TickerPositionAggregator() {
        this(0L);
    }

    public TickerPositionAggregator(long emitIntervalMs) {
        this.emitIntervalMs = emitIntervalMs;
    }

    @Override
    public void open(OpenContext openContext) {
        netQty = getRuntimeContext().getState(new ValueStateDescriptor<>("net-qty-ticker", Types.LONG));
        pending = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-emit-ticker", TypeInformation.of(TickerPosition.class)));
    }

    @Override
    public void processElement(Trade trade, Context ctx, Collector<TickerPosition> out) throws Exception {
        long updated = (netQty.value() == null ? 0L : netQty.value()) + trade.qty;
        netQty.update(updated);
        TickerPosition snapshot = new TickerPosition(trade.ticker, updated, trade.eventTime);
        if (emitIntervalMs <= 0) {
            out.collect(snapshot);
            return;
        }
        boolean timerPending = pending.value() != null;
        pending.update(snapshot);
        if (!timerPending) {
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + emitIntervalMs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TickerPosition> out) throws Exception {
        TickerPosition snapshot = pending.value();
        if (snapshot != null) {
            out.collect(snapshot);
            pending.clear();
        }
    }
}
