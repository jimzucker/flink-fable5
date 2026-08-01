package com.demo.flink.pipeline;

import com.demo.flink.model.TickerPosition;
import com.demo.flink.model.Trade;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TickerPositionAggregatorTest {

    @Test
    void aggregatesAcrossAccounts() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, Trade, TickerPosition> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new TickerPositionAggregator()),
                        t -> t.ticker,
                        Types.STRING);
        h.open();
        try (h) {
            h.processElement(new StreamRecord<>(new Trade("T-1", "ACC-001", "AAPL", 100, 1000)));
            h.processElement(new StreamRecord<>(new Trade("T-2", "ACC-002", "AAPL", 50, 2000)));
            h.processElement(new StreamRecord<>(new Trade("T-3", "ACC-003", "AAPL", -20, 3000)));

            List<TickerPosition> out = h.extractOutputValues();
            assertEquals(100, out.get(0).netQty);
            assertEquals(150, out.get(1).netQty, "positions from different accounts accumulate");
            assertEquals(130, out.get(2).netQty);
        }
    }
}
