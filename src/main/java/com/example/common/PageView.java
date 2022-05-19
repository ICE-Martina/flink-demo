package com.example.common;

/**
 * @author liuwei
 * @date 2022/5/19 15:38
 */
public class PageView {
    public String url;
    public Integer count;

    public PageView() {
    }

    public PageView(String url, Integer count) {
        this.url = url;
        this.count = count;
    }

    @Override
    public String toString() {
        return "PageView{" +
                "url='" + url + '\'' +
                ", count=" + count +
                '}';
    }
}
