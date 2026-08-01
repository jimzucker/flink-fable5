package com.demo.flink.pipeline;

import com.demo.flink.model.Position;
import com.demo.flink.model.Trade;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PositionAggregatorTest {

    @Test
    void runningSumPerAccountTicker_handComputed() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, Trade, Position> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new PositionAggregator()),
                        t -> t.account + "|" + t.ticker,
                        Types.STRING);
        h.open();
        try (h) {
            h.processElement(new StreamRecord<>(new Trade("T-1", "ACC-001", "AAPL", 100, 1000)));
            h.processElement(new StreamRecord<>(new Trade("T-2", "ACC-001", "AAPL", -30, 2000)));
            h.processElement(new StreamRecord<>(new Trade("T-3", "ACC-002", "AAPL", 50, 3000)));
            h.processElement(new StreamRecord<>(new Trade("T-4", "ACC-001", "MSFT", 10, 4000)));
            h.processElement(new StreamRecord<>(new Trade("T-5", "ACC-001", "AAPL", 5, 5000)));

            List<Position> out = h.extractOutputValues();
            assertEquals(5, out.size(), "one snapshot per applied trade");

            // Hand-computed running sums
            assertEquals(100, out.get(0).netQty);   // ACC-001|AAPL: 100
            assertEquals(70, out.get(1).netQty);    // ACC-001|AAPL: 100 - 30
            assertEquals(50, out.get(2).netQty);    // ACC-002|AAPL: 50
            assertEquals(10, out.get(3).netQty);    // ACC-001|MSFT: 10
            assertEquals(75, out.get(4).netQty);    // ACC-001|AAPL: 70 + 5
            assertEquals(5000, out.get(4).asOf, "as_of carries the trade's event time");
        }
    }
}
