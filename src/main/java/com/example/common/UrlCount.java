package com.example.common;

import java.sql.Timestamp;

/**
 * @author liuwei
 * @date 2022/5/24 14:55
 */
public class UrlCount {
    public String url;
    public Integer count;
    public Long start;
    public Long end;

    public UrlCount() {
    }

    public UrlCount(String url, Integer count, Long start, Long end) {
        this.url = url;
        this.count = count;
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return "UrlCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", start=" + new Timestamp(start) +
                ", end=" + new Timestamp(end) +
                '}';
    }
}
