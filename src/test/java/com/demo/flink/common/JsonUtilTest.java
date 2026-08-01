package com.demo.flink.common;

import com.demo.flink.model.Trade;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class JsonUtilTest {

    @Test
    void roundTrip() {
        Trade t = new Trade("T-1", "ACC-001", "AAPL", -250, 12345);
        Trade back = JsonUtil.fromJson(JsonUtil.toJson(t), Trade.class);
        assertEquals("T-1", back.tradeId);
        assertEquals(-250, back.qty);
        assertEquals(12345, back.eventTime);
    }

    @Test
    void malformedReturnsNull() {
        assertNull(JsonUtil.fromJson("{not json", Trade.class));
        assertNull(JsonUtil.fromJson("", Trade.class));
    }

    @Test
    void unknownFieldsIgnored() {
        Trade t = JsonUtil.fromJson("{\"trade_id\":\"T-1\",\"account\":\"A\",\"ticker\":\"X\",\"qty\":1,\"event_time\":1,\"extra\":true}", Trade.class);
        assertEquals("T-1", t.tradeId);
    }
}
