package com.example.common;

import java.sql.Timestamp;

/**
 * @author liuwei
 * @date 2022/6/1 17:15
 */
public class LoginEvent {
    public String userId;
    public String ipAddress;
    public String eventType;
    public Long ts;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ipAddress, String eventType, Long ts) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.eventType = eventType;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", eventType='" + eventType + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}
