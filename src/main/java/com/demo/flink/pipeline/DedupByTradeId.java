package com.demo.flink.pipeline;

import com.demo.flink.model.Trade;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Drops duplicate trades by trade_id. First occurrence wins.
 * State carries a TTL so the dedup index does not grow unbounded.
 */
public class DedupByTradeId extends KeyedProcessFunction<String, Trade, Trade> {

    private final long ttlMs;
    private transient ValueState<Boolean> seen;

    public DedupByTradeId(long ttlMs) {
        this.ttlMs = ttlMs;
    }

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-trade-id", Types.BOOLEAN);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.milliseconds(ttlMs))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Trade trade, Context ctx, Collector<Trade> out) throws Exception {
        if (seen.value() == null) {
            seen.update(true);
            out.collect(trade);
        }
    }
}
