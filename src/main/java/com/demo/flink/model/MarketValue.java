package com.demo.flink.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

/** Market value snapshot. account is null for ticker-level aggregates. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MarketValue {
    @JsonProperty("account")
    public String account;

    @JsonProperty("ticker")
    public String ticker;

    @JsonProperty("net_qty")
    public long netQty;

    /** Price as exact decimal string, e.g. "184.52". */
    @JsonProperty("price")
    public String price;

    /** market value = net_qty * price, exact decimal string. */
    @JsonProperty("mv")
    public String mv;

    @JsonProperty("as_of")
    public long asOf;

    public MarketValue() {
    }

    /** Exact math: BigDecimal from long cents — immune to overflow and float drift, any price magnitude. */
    public static MarketValue of(String account, String ticker, long netQty, long priceCents, long asOf) {
        MarketValue value = new MarketValue();
        value.account = account;
        value.ticker = ticker;
        value.netQty = netQty;
        BigDecimal priceDec = BigDecimal.valueOf(priceCents).movePointLeft(2);
        value.price = priceDec.toPlainString();
        value.mv = priceDec.multiply(BigDecimal.valueOf(netQty)).toPlainString();
        value.asOf = asOf;
        return value;
    }
}
