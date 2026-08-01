package com.demo.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Trade {
    @JsonProperty("trade_id")
    public String tradeId;

    @JsonProperty("account")
    public String account;

    @JsonProperty("ticker")
    public String ticker;

    @JsonProperty("qty")
    public long qty;

    @JsonProperty("event_time")
    public long eventTime;

    public Trade() {
    }

    public Trade(String tradeId, String account, String ticker, long qty, long eventTime) {
        this.tradeId = tradeId;
        this.account = account;
        this.ticker = ticker;
        this.qty = qty;
        this.eventTime = eventTime;
    }
}
