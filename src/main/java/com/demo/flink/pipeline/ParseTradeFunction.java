package com.demo.flink.pipeline;

import com.demo.flink.common.JsonUtil;
import com.demo.flink.model.Trade;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

/** Parses raw trade JSON; counts input volume (bytes, KB/sec via meter) and malformed records. */
public class ParseTradeFunction extends RichFlatMapFunction<String, Trade> {

    private transient Counter bytesIn;
    private transient Counter malformed;

    @Override
    public void open(OpenContext openContext) {
        MetricGroup group = getRuntimeContext().getMetricGroup();
        bytesIn = group.counter("demoBytesIn");
        group.meter("demoBytesInPerSecond", new MeterView(bytesIn));
        malformed = group.counter("demoMalformed");
    }

    @Override
    public void flatMap(String json, Collector<Trade> out) {
        bytesIn.inc(json.getBytes(StandardCharsets.UTF_8).length);
        Trade trade = JsonUtil.fromJson(json, Trade.class);
        if (trade == null || trade.tradeId == null || trade.account == null || trade.ticker == null) {
            malformed.inc();
            return;
        }
        out.collect(trade);
    }
}
