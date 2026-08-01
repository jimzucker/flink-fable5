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

/**
 * Conflated re-valuation semantics (Phase 7): under a price storm, holders are
 * re-valued at most once per interval, always at the LATEST price; position
 * updates are never delayed.
 */
class ConflationTest {

    private static final long INTERVAL = 250;

    private static KeyedTwoInputStreamOperatorTestHarness<String, Position, PriceCents, MarketValue> harness(long interval)
            throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<String, Position, PriceCents, MarketValue> h =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new KeyedCoProcessOperator<>(new MarketValueByAccountTicker(interval)),
                        p -> p.ticker,
                        pc -> pc.symbol,
                        Types.STRING);
        h.open();
        return h;
    }

    @Test
    void priceStormConflatesToOneRevaluationAtLatestPrice() throws Exception {
        try (var h = harness(INTERVAL)) {
            h.setProcessingTime(0);
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 100, 1000)));
            h.processElement1(new StreamRecord<>(new Position("ACC-002", "AAPL", -40, 1100)));
            assertEquals(0, h.extractOutputValues().size(), "no MV before any price");

            // 5 rapid ticks inside one interval
            for (long cents : new long[]{15_000, 15_100, 15_200, 15_300, 16_000}) {
                h.processElement2(new StreamRecord<>(new PriceCents("AAPL", cents, 2000)));
            }
            assertEquals(0, h.extractOutputValues().size(), "no emission until the timer fires");

            h.setProcessingTime(INTERVAL + 1); // fire the conflation timer
            List<MarketValue> out = h.extractOutputValues();
            assertEquals(2, out.size(), "exactly one re-valuation per holder for 5 ticks");
            for (MarketValue mv : out) {
                assertEquals("160.00", mv.price, "re-valued at the LATEST price, intermediates absorbed");
            }
        }
    }

    @Test
    void positionUpdatesEmitImmediatelyEvenWithTimerPending() throws Exception {
        try (var h = harness(INTERVAL)) {
            h.setProcessingTime(0);
            h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 15_000, 1000)));
            // timer now pending; the order path must not wait for it
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 100, 2000)));
            List<MarketValue> out = h.extractOutputValues();
            assertEquals(1, out.size(), "position-driven MV is immediate");
            assertEquals("15000.00", out.get(0).mv);
        }
    }

    @Test
    void quiescedFinalStateIsPositionTimesLatestPrice() throws Exception {
        try (var h = harness(INTERVAL)) {
            h.setProcessingTime(0);
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 100, 1000)));
            h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 15_000, 2000)));
            h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 17_500, 3000)));
            h.setProcessingTime(10_000); // drain past the interval
            List<MarketValue> out = h.extractOutputValues();
            MarketValue last = out.get(out.size() - 1);
            assertEquals("17500.00", last.mv, "final MV == position x latest price (validation invariant)");
        }
    }

    @Test
    void intervalZeroBehavesExactlyAsBefore() throws Exception {
        try (var h = harness(0)) {
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 100, 1000)));
            for (long cents : new long[]{15_000, 15_100, 15_200}) {
                h.processElement2(new StreamRecord<>(new PriceCents("AAPL", cents, 2000)));
            }
            assertEquals(3, h.extractOutputValues().size(), "per-tick re-valuation when conflation is off");
        }
    }
}
