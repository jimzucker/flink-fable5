package com.demo.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TickerPosition {
    @JsonProperty("ticker")
    public String ticker;

    @JsonProperty("net_qty")
    public long netQty;

    @JsonProperty("as_of")
    public long asOf;

    public TickerPosition() {
    }

    public TickerPosition(String ticker, long netQty, long asOf) {
        this.ticker = ticker;
        this.netQty = netQty;
        this.asOf = asOf;
    }
}
