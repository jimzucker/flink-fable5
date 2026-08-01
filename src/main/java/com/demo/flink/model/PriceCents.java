package com.demo.flink.model;

/** Internal price representation: exact long cents, no floating point. */
public class PriceCents {
    public String symbol;
    public long priceCents;
    public long eventTime;

    public PriceCents() {
    }

    public PriceCents(String symbol, long priceCents, long eventTime) {
        this.symbol = symbol;
        this.priceCents = priceCents;
        this.eventTime = eventTime;
    }
}
