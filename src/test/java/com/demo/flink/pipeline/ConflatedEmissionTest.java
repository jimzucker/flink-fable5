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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Emission conflation (emit.interval.ms > 0): many trades inside one
 * interval produce exactly ONE snapshot per key, carrying the final running
 * sum — so downstream (and the validation suite) see identical final state
 * with a fraction of the records.
 */
class ConflatedEmissionTest {

    @Test
    void manyTradesOneInterval_singleSnapshotWithFinalSum() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, Trade, Position> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new PositionAggregator(250L)),
                        t -> t.account + "|" + t.ticker,
                        Types.STRING);
        h.open();
        try (h) {
            h.setProcessingTime(0);
            h.processElement(new StreamRecord<>(new Trade("T-1", "ACC-001", "AAPL", 100, 1000)));
            h.processElement(new StreamRecord<>(new Trade("T-2", "ACC-001", "AAPL", -30, 2000)));
            h.processElement(new StreamRecord<>(new Trade("T-3", "ACC-001", "AAPL", 5, 3000)));
            assertTrue(h.extractOutputValues().isEmpty(), "nothing emitted inside the interval");

            h.setProcessingTime(250);
            List<Position> out = h.extractOutputValues();
            assertEquals(1, out.size(), "three trades conflated into one snapshot");
            assertEquals(75, out.get(0).netQty, "snapshot carries the final running sum");
            assertEquals(3000, out.get(0).asOf, "as_of is the last trade's event time");
        }
    }

    @Test
    void updatesAfterFlush_registerNewTimerAndFlushAgain() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, Trade, Position> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new PositionAggregator(250L)),
                        t -> t.account + "|" + t.ticker,
                        Types.STRING);
        h.open();
        try (h) {
            h.setProcessingTime(0);
            h.processElement(new StreamRecord<>(new Trade("T-1", "ACC-001", "AAPL", 100, 1000)));
            h.setProcessingTime(250);
            h.processElement(new StreamRecord<>(new Trade("T-2", "ACC-001", "AAPL", 50, 2000)));
            h.setProcessingTime(500);

            List<Position> out = h.extractOutputValues();
            assertEquals(2, out.size(), "one snapshot per interval that saw updates");
            assertEquals(100, out.get(0).netQty);
            assertEquals(150, out.get(1).netQty, "second flush carries the cumulative sum");
        }
    }

    @Test
    void intervalZero_behavesExactlyAsBefore() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, Trade, Position> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new PositionAggregator(0L)),
                        t -> t.account + "|" + t.ticker,
                        Types.STRING);
        h.open();
        try (h) {
            h.processElement(new StreamRecord<>(new Trade("T-1", "ACC-001", "AAPL", 100, 1000)));
            h.processElement(new StreamRecord<>(new Trade("T-2", "ACC-001", "AAPL", -30, 2000)));
            assertEquals(2, h.extractOutputValues().size(), "per-update emission when disabled");
        }
    }
}
