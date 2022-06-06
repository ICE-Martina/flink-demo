package com.example.common;

import java.sql.Timestamp;

/**
 * @author liuwei
 * @date 2022/6/6 10:02
 */
public class OrderEvent {
    public String userId;
    public String orderId;
    public String eventType;
    public Long ts;

    public OrderEvent() {
    }

    public OrderEvent(String userId, String orderId, String eventType, Long ts) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}
