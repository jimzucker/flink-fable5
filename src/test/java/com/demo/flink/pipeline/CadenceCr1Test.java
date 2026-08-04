package com.demo.flink.pipeline;

import com.demo.flink.model.MarketValue;
import com.demo.flink.model.Position;
import com.demo.flink.model.PriceCents;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CR-1: market values must not update more than once per key per interval,
 * and the one update that does escape must carry the newest state — both the
 * newest price and the newest quantity.
 */
class CadenceCr1Test {

    private KeyedTwoInputStreamOperatorTestHarness<String, Position, PriceCents, MarketValue> harness(long intervalMs)
            throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<String, Position, PriceCents, MarketValue> h =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new KeyedCoProcessOperator<>(new MarketValueByAccountTicker(intervalMs)),
                        p -> p.ticker, p -> p.symbol, Types.STRING);
        h.open();
        return h;
    }

    @Test
    void manyPricesAndPositionsInOneInterval_emitOnceWithNewestOfBoth() throws Exception {
        try (var h = harness(1000L)) {
            h.setProcessingTime(0);
            h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 10_000L, 100)));
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 10, 110)));
            // a burst of both inputs inside the same interval
            for (int i = 1; i <= 20; i++) {
                h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 10_000L + i, 120 + i)));
            }
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 55, 200)));
            assertTrue(h.extractOutputValues().isEmpty(), "nothing escapes inside the interval");

            h.setProcessingTime(1000);
            List<MarketValue> out = h.extractOutputValues();
            assertEquals(1, out.size(), "exactly one update per key per interval");
            assertEquals(55, out.get(0).netQty, "carries the NEWEST quantity");
            assertEquals("100.20", out.get(0).price, "carries the NEWEST price");
            assertEquals("5511.00", out.get(0).mv, "value computed at emit time from both");
        }
    }

    @Test
    void steadyInput_producesExactlyOneUpdatePerIntervalPerKey() throws Exception {
        try (var h = harness(1000L)) {
            h.setProcessingTime(0);
            h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 10_000L, 100)));
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 10, 100)));
            // five intervals of continuous input
            for (int interval = 1; interval <= 5; interval++) {
                for (int i = 0; i < 50; i++) {
                    h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 10_000L + i, 1000L * interval)));
                }
                h.setProcessingTime(1000L * interval);
            }
            assertEquals(5, h.extractOutputValues().size(),
                    "output rate is capped at one per interval regardless of input rate");
        }
    }

    @Test
    void intervalZero_restoresPerEventEmission() throws Exception {
        try (var h = harness(0L)) {
            h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 10_000L, 100)));
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 10, 110)));
            h.processElement1(new StreamRecord<>(new Position("ACC-002", "AAPL", 20, 120)));
            assertEquals(2, h.extractOutputValues().size(), "uncapped when the interval is 0");
        }
    }
}
