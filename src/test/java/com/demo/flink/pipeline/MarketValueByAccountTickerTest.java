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

class MarketValueByAccountTickerTest {

    private static KeyedTwoInputStreamOperatorTestHarness<String, Position, PriceCents, MarketValue> harness()
            throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<String, Position, PriceCents, MarketValue> h =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new KeyedCoProcessOperator<>(new MarketValueByAccountTicker(0)),
                        p -> p.ticker,
                        pc -> pc.symbol,
                        Types.STRING);
        h.open();
        return h;
    }

    @Test
    void positionBeforePrice_emitsNothingUntilPriceArrives() throws Exception {
        try (var h = harness()) {
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 100, 1000)));
            assertEquals(0, h.extractOutputValues().size(), "no MV without a price");

            h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 15_000, 2000))); // $150.00
            List<MarketValue> out = h.extractOutputValues();
            assertEquals(1, out.size());
            assertEquals("150.00", out.get(0).price);
            assertEquals("15000.00", out.get(0).mv, "100 x $150.00");
        }
    }

    @Test
    void priceTickRevaluesEveryHolder() throws Exception {
        try (var h = harness()) {
            h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 15_000, 1000)));
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 100, 2000)));
            h.processElement1(new StreamRecord<>(new Position("ACC-002", "AAPL", -40, 3000)));
            h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 16_000, 4000))); // $160.00

            List<MarketValue> out = h.extractOutputValues();
            // 2 position-driven emissions + 2 from the re-valuing tick
            assertEquals(4, out.size());
            MarketValue last1 = out.get(2);
            MarketValue last2 = out.get(3);
            assertEquals("160.00", last1.price);
            long total = 0;
            for (MarketValue mv : List.of(last1, last2)) {
                total += Long.parseLong(mv.mv.replace(".00", ""));
            }
            assertEquals(100 * 160 + (-40) * 160, total, "all holders re-valued at the new price");
        }
    }

    @Test
    void extremePrice_exactMath_noOverflow() throws Exception {
        try (var h = harness()) {
            // $10,000,000,000,000.00 — perf/correctness Case 2: absurd price magnitude
            h.processElement2(new StreamRecord<>(new PriceCents("AAPL", 1_000_000_000_000_000L, 1000)));
            h.processElement1(new StreamRecord<>(new Position("ACC-001", "AAPL", 999_999, 2000)));

            List<MarketValue> out = h.extractOutputValues();
            assertEquals("10000000000000.00", out.get(0).price);
            assertEquals("9999990000000000000.00", out.get(0).mv,
                    "999,999 x 10^13 dollars — exact, no long overflow, no float drift");
        }
    }
}
