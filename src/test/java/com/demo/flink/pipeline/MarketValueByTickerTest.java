package com.demo.flink.pipeline;

import com.demo.flink.model.MarketValue;
import com.demo.flink.model.PriceCents;
import com.demo.flink.model.TickerPosition;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MarketValueByTickerTest {

    @Test
    void latestPositionTimesLatestPrice() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<String, TickerPosition, PriceCents, MarketValue> h =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new KeyedCoProcessOperator<>(new MarketValueByTicker()),
                        p -> p.ticker,
                        pc -> pc.symbol,
                        Types.STRING);
        h.open();
        try (h) {
            h.processElement2(new StreamRecord<>(new PriceCents("TSLA", 20_050, 1000))); // $200.50
            h.processElement1(new StreamRecord<>(new TickerPosition("TSLA", -140, 2000)));
            h.processElement2(new StreamRecord<>(new PriceCents("TSLA", 21_000, 3000))); // $210.00

            List<MarketValue> out = h.extractOutputValues();
            assertEquals(2, out.size());
            assertNull(out.get(0).account, "ticker-level MV has no account");
            assertEquals("-28070.00", out.get(0).mv, "-140 x $200.50");
            assertEquals("-29400.00", out.get(1).mv, "-140 x $210.00 after re-valuing tick");
        }
    }
}
