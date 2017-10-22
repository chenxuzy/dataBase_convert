package com.iflytek.mongodb_mysql;

import java.util.ArrayList;
import java.util.List;

public class ShareResourcePool<T> {
    private List<T> resources;
    private int size;
    private volatile int cursor;

    public ShareResourcePool() {
        resources = new ArrayList<>();
        size = 0;
        cursor = 0;
    }

    public void addResource(T resource) {
        resources.add(resource);
        size++;
    }

    public T getResource() {
        cursor = (++cursor) % size;
        return resources.get(cursor);
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
