package com.demo.flink.pipeline;

import com.demo.flink.model.Trade;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DedupByTradeIdTest {

    private static KeyedOneInputStreamOperatorTestHarness<String, Trade, Trade> harness() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, Trade, Trade> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new DedupByTradeId(3_600_000L)),
                        t -> t.tradeId,
                        Types.STRING);
        h.open();
        return h;
    }

    @Test
    void firstOccurrencePasses_duplicatesDropped() throws Exception {
        try (var h = harness()) {
            Trade t1 = new Trade("T-1", "ACC-001", "AAPL", 100, 1000);
            Trade t1dup = new Trade("T-1", "ACC-001", "AAPL", 100, 1000);
            Trade t2 = new Trade("T-2", "ACC-002", "AAPL", 50, 2000);

            h.processElement(new StreamRecord<>(t1));
            h.processElement(new StreamRecord<>(t1dup));
            h.processElement(new StreamRecord<>(t1dup));
            h.processElement(new StreamRecord<>(t2));

            List<Trade> out = h.extractOutputValues();
            assertEquals(2, out.size(), "exactly one record per distinct trade_id");
            assertEquals("T-1", out.get(0).tradeId);
            assertEquals("T-2", out.get(1).tradeId);
        }
    }

    @Test
    void duplicateWithDifferentPayloadStillDropped_firstWins() throws Exception {
        try (var h = harness()) {
            h.processElement(new StreamRecord<>(new Trade("T-9", "ACC-001", "AAPL", 100, 1000)));
            h.processElement(new StreamRecord<>(new Trade("T-9", "ACC-001", "AAPL", 999, 2000)));

            List<Trade> out = h.extractOutputValues();
            assertEquals(1, out.size());
            assertEquals(100, out.get(0).qty, "first occurrence wins");
        }
    }
}
