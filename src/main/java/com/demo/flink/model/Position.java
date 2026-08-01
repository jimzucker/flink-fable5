package com.demo.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Position {
    @JsonProperty("account")
    public String account;

    @JsonProperty("ticker")
    public String ticker;

    @JsonProperty("net_qty")
    public long netQty;

    /** Event time of the trade that produced this snapshot — keeps output deterministic. */
    @JsonProperty("as_of")
    public long asOf;

    public Position() {
    }

    public Position(String account, String ticker, long netQty, long asOf) {
        this.account = account;
        this.ticker = ticker;
        this.netQty = netQty;
        this.asOf = asOf;
    }
}
