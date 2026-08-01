package com.demo.flink.pipeline;

import com.demo.flink.model.MarketValue;
import com.demo.flink.model.Position;
import com.demo.flink.model.PriceCents;
import com.demo.flink.model.TickerPosition;
import com.demo.flink.model.Trade;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end golden test: a small fixed dataset with hand-computed expected
 * outputs, run through the real operator chain (dedup -> aggregations -> MV
 * joins). This is the "validate calculations with simple inputs" gate:
 * every expected number below was computed by hand first.
 *
 * Dataset (in order):
 *   T-1  ACC-001 AAPL +100
 *   T-2  ACC-002 AAPL  +50
 *   T-1  (duplicate)          -> must be dropped
 *   T-3  ACC-001 MSFT  -30
 *   T-4  ACC-002 AAPL  -20
 *   T-5  ACC-001 AAPL  +10
 *   Final prices: AAPL $155.50, MSFT $410.25
 *
 * Expected final state:
 *   ACC-001|AAPL = 110      ACC-002|AAPL = 30      ACC-001|MSFT = -30
 *   AAPL = 140 (completeness: 110 + 30)            MSFT = -30
 *   MV ACC-001|AAPL = 17,105.00   MV ACC-002|AAPL = 4,665.00
 *   MV ACC-001|MSFT = -12,307.50
 *   MV AAPL = 21,770.00           MV MSFT = -12,307.50
 */
class GoldenPipelineTest {

    private static final List<Trade> TRADES = List.of(
            new Trade("T-1", "ACC-001", "AAPL", 100, 1000),
            new Trade("T-2", "ACC-002", "AAPL", 50, 2000),
            new Trade("T-1", "ACC-001", "AAPL", 100, 1000), // duplicate
            new Trade("T-3", "ACC-001", "MSFT", -30, 3000),
            new Trade("T-4", "ACC-002", "AAPL", -20, 4000),
            new Trade("T-5", "ACC-001", "AAPL", 10, 5000));

    private static final List<PriceCents> FINAL_PRICES = List.of(
            new PriceCents("AAPL", 15_550, 9000),
            new PriceCents("MSFT", 41_025, 9000));

    @Test
    void goldenDataset_allFourOutputs_handComputed() throws Exception {
        // --- dedup ---
        var dedup = new KeyedOneInputStreamOperatorTestHarness<String, Trade, Trade>(
                new KeyedProcessOperator<>(new DedupByTradeId(3_600_000L)), t -> t.tradeId, Types.STRING);
        dedup.open();
        for (Trade t : TRADES) {
            dedup.processElement(new StreamRecord<>(t));
        }
        List<Trade> deduped = dedup.extractOutputValues();
        dedup.close();
        assertEquals(5, deduped.size(), "one duplicate dropped");

        // --- positions ---
        var accountAgg = new KeyedOneInputStreamOperatorTestHarness<String, Trade, Position>(
                new KeyedProcessOperator<>(new PositionAggregator()), t -> t.account + "|" + t.ticker, Types.STRING);
        accountAgg.open();
        var tickerAgg = new KeyedOneInputStreamOperatorTestHarness<String, Trade, TickerPosition>(
                new KeyedProcessOperator<>(new TickerPositionAggregator()), t -> t.ticker, Types.STRING);
        tickerAgg.open();
        for (Trade t : deduped) {
            accountAgg.processElement(new StreamRecord<>(t));
            tickerAgg.processElement(new StreamRecord<>(t));
        }

        Map<String, Long> finalAccountPos = new LinkedHashMap<>();
        for (Position p : accountAgg.extractOutputValues()) {
            finalAccountPos.put(p.account + "|" + p.ticker, p.netQty);
        }
        Map<String, Long> finalTickerPos = new LinkedHashMap<>();
        for (TickerPosition p : tickerAgg.extractOutputValues()) {
            finalTickerPos.put(p.ticker, p.netQty);
        }

        // Output 1: position by account/ticker — hand-computed
        assertEquals(Map.of("ACC-001|AAPL", 110L, "ACC-002|AAPL", 30L, "ACC-001|MSFT", -30L), finalAccountPos);
        // Output 2: position by ticker — hand-computed
        assertEquals(Map.of("AAPL", 140L, "MSFT", -30L), finalTickerPos);

        // Completeness invariant: per-account positions sum to the ticker position
        Map<String, Long> summed = new LinkedHashMap<>();
        finalAccountPos.forEach((key, qty) -> summed.merge(key.split("\\|")[1], qty, Long::sum));
        assertEquals(finalTickerPos, summed, "sum(account positions) == ticker position");

        // --- market values from final positions + final prices ---
        var mvAccount = new KeyedTwoInputStreamOperatorTestHarness<String, Position, PriceCents, MarketValue>(
                new KeyedCoProcessOperator<>(new MarketValueByAccountTicker(0)), p -> p.ticker, pc -> pc.symbol, Types.STRING);
        mvAccount.open();
        for (Position p : accountAgg.extractOutputValues()) {
            mvAccount.processElement1(new StreamRecord<>(p));
        }
        for (PriceCents pc : FINAL_PRICES) {
            mvAccount.processElement2(new StreamRecord<>(pc));
        }
        Map<String, String> finalMvAccount = new LinkedHashMap<>();
        for (MarketValue mv : mvAccount.extractOutputValues()) {
            finalMvAccount.put(mv.account + "|" + mv.ticker, mv.mv);
        }
        // Output 3: MV by account/ticker — hand-computed (110x155.50, 30x155.50, -30x410.25)
        assertEquals(Map.of(
                "ACC-001|AAPL", "17105.00",
                "ACC-002|AAPL", "4665.00",
                "ACC-001|MSFT", "-12307.50"), finalMvAccount);

        var mvTicker = new KeyedTwoInputStreamOperatorTestHarness<String, TickerPosition, PriceCents, MarketValue>(
                new KeyedCoProcessOperator<>(new MarketValueByTicker(0)), p -> p.ticker, pc -> pc.symbol, Types.STRING);
        mvTicker.open();
        for (TickerPosition p : tickerAgg.extractOutputValues()) {
            mvTicker.processElement1(new StreamRecord<>(p));
        }
        for (PriceCents pc : FINAL_PRICES) {
            mvTicker.processElement2(new StreamRecord<>(pc));
        }
        Map<String, String> finalMvTicker = new LinkedHashMap<>();
        for (MarketValue mv : mvTicker.extractOutputValues()) {
            finalMvTicker.put(mv.ticker, mv.mv);
        }
        // Output 4: MV by ticker — hand-computed (140x155.50, -30x410.25)
        assertEquals(Map.of("AAPL", "21770.00", "MSFT", "-12307.50"), finalMvTicker);

        accountAgg.close();
        tickerAgg.close();
        mvAccount.close();
        mvTicker.close();
    }

    /** Determinism: running the identical input twice yields identical output. */
    @Test
    void sameInputTwice_identicalResults() throws Exception {
        for (int run = 0; run < 2; run++) {
            var dedup = new KeyedOneInputStreamOperatorTestHarness<String, Trade, Trade>(
                    new KeyedProcessOperator<>(new DedupByTradeId(3_600_000L)), t -> t.tradeId, Types.STRING);
            dedup.open();
            var agg = new KeyedOneInputStreamOperatorTestHarness<String, Trade, Position>(
                    new KeyedProcessOperator<>(new PositionAggregator()), t -> t.account + "|" + t.ticker, Types.STRING);
            agg.open();
            for (Trade t : TRADES) {
                dedup.processElement(new StreamRecord<>(t));
            }
            for (Trade t : dedup.extractOutputValues()) {
                agg.processElement(new StreamRecord<>(t));
            }
            Map<String, Long> result = new LinkedHashMap<>();
            for (Position p : agg.extractOutputValues()) {
                result.put(p.account + "|" + p.ticker, p.netQty);
            }
            assertEquals(Map.of("ACC-001|AAPL", 110L, "ACC-002|AAPL", 30L, "ACC-001|MSFT", -30L), result,
                    "run " + (run + 1) + " must match the golden result exactly");
            dedup.close();
            agg.close();
        }
    }
}
