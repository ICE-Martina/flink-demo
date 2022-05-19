package com.example.common;

import java.sql.Timestamp;

/**
 *
 * POJOs #
 * Java and Scala classes are treated by Flink as a special POJO data type if they fulfill the following requirements:
 * 1.The class must be public.
 * 2.It must have a public constructor without arguments (default constructor).
 * 3.All fields are either public or must be accessible through getter and setter functions. For a field called foo the getter and setter methods must be named getFoo() and setFoo().
 * 4.The type of a field must be supported by a registered serializer.
 *
 * @author liuwei
 * @date 2022/5/19 11:49
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
