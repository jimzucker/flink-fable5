package com.demo.flink.pipeline;

import com.demo.flink.common.JsonUtil;
import com.demo.flink.model.Price;
import com.demo.flink.model.PriceCents;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/** Parses price JSON to exact long cents; counts input volume and malformed records. */
public class ParsePriceFunction extends RichFlatMapFunction<String, PriceCents> {

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
    public void flatMap(String json, Collector<PriceCents> out) {
        bytesIn.inc(json.getBytes(StandardCharsets.UTF_8).length);
        Price price = JsonUtil.fromJson(json, Price.class);
        if (price == null || price.symbol == null || price.price == null) {
            malformed.inc();
            return;
        }
        try {
            long cents = new BigDecimal(price.price).movePointRight(2).longValueExact();
            out.collect(new PriceCents(price.symbol, cents, price.eventTime));
        } catch (ArithmeticException | NumberFormatException e) {
            malformed.inc();
        }
    }
}
