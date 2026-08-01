package com.demo.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Price {
    @JsonProperty("symbol")
    public String symbol;

    /** Decimal string, e.g. "184.52" — exact, no floating point drift on the wire. */
    @JsonProperty("price")
    public String price;

    @JsonProperty("event_time")
    public long eventTime;

    public Price() {
    }

    public Price(String symbol, String price, long eventTime) {
        this.symbol = symbol;
        this.price = price;
        this.eventTime = eventTime;
    }
}
