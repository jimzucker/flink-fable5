package com.demo.flink.common;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

/**
 * Dual-scope custom metrics. Amazon Managed Service for Apache Flink only
 * exports user metrics registered under a "kinesisanalytics" metric group;
 * plain-scoped metrics are visible to Prometheus locally but silently dropped
 * by MSF. Registering in both scopes keeps one code path for both worlds
 * (local Prometheus names are unchanged).
 */
public final class DemoMetrics {

    private DemoMetrics() {
    }

    /** Counter registered in the plain scope (Prometheus) and the MSF scope (CloudWatch). */
    public static Counter counter(MetricGroup group, String name) {
        Counter local = group.counter(name);
        Counter cloud = group.addGroup("kinesisanalytics").counter(name);
        return new Counter() {
            @Override
            public void inc() {
                local.inc();
                cloud.inc();
            }

            @Override
            public void inc(long n) {
                local.inc(n);
                cloud.inc(n);
            }

            @Override
            public void dec() {
                local.dec();
                cloud.dec();
            }

            @Override
            public void dec(long n) {
                local.dec(n);
                cloud.dec(n);
            }

            @Override
            public long getCount() {
                return local.getCount();
            }
        };
    }

    /** Per-second meters over the counter, registered in both scopes. */
    public static void meter(MetricGroup group, String name, Counter counter) {
        group.meter(name + "PerSecond", new MeterView(counter));
        group.addGroup("kinesisanalytics").meter(name + "PerSecond", new MeterView(counter));
    }
}
