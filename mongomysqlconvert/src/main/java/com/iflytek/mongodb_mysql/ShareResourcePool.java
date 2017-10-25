package com.iflytek.mongodb_mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ShareResourcePool<T> {
    private List<T> resources;
    private int size;
    private int cursor;

    public ShareResourcePool() {
        resources = new ArrayList<>();
        size = 0;
        cursor =0;
    }

    public void addResource(T resource) {
        resources.add(resource);
        size++;
    }

    public T getResource() {
        int i = (cursor++% size);
        return resources.get(i);
    }

    public List<T> getAllResource() {
        return resources;
    }

    public void clear() {
        resources.clear();
    }

    public int getSize() {
        return size;
    }


}
