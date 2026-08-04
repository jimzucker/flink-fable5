package com.demo.flink.pipeline;

import com.demo.flink.model.Position;
import com.demo.flink.model.Trade;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Running net position per (account, ticker). O(1) state update per trade.
 *
 * Emission is either per-update (emitIntervalMs = 0, an upsert per applied
 * trade — lowest latency) or conflated (emitIntervalMs > 0: state updates
 * per trade, but at most one snapshot per key per interval reaches the
 * output — the Phase 7 timer pattern applied to emission). Final state
 * after quiesce is identical either way: the last timer always flushes the
 * latest snapshot, so the validation suite passes unchanged.
 */
public class PositionAggregator extends KeyedProcessFunction<String, Trade, Position> {

    private final long emitIntervalMs;

    private transient ValueState<Long> netQty;
    private transient ValueState<Position> pending;

    public PositionAggregator() {
        this(0L);
    }

    public PositionAggregator(long emitIntervalMs) {
        this.emitIntervalMs = emitIntervalMs;
    }

    @Override
    public void open(OpenContext openContext) {
        netQty = getRuntimeContext().getState(new ValueStateDescriptor<>("net-qty", Types.LONG));
        pending = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-emit", TypeInformation.of(Position.class)));
    }

    @Override
    public void processElement(Trade trade, Context ctx, Collector<Position> out) throws Exception {
        long updated = (netQty.value() == null ? 0L : netQty.value()) + trade.qty;
        netQty.update(updated);
        Position snapshot = new Position(trade.account, trade.ticker, updated, trade.eventTime);
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
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Position> out) throws Exception {
        Position snapshot = pending.value();
        if (snapshot != null) {
            out.collect(snapshot);
            pending.clear();
        }
    }
}
