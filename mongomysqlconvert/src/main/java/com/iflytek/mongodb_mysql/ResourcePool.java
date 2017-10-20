package com.iflytek.mongodb_mysql;


import java.util.concurrent.LinkedBlockingDeque;


public class ResourcePool<T> {
    private LinkedBlockingDeque<T> resources;

    public ResourcePool() {
        resources = new LinkedBlockingDeque<>();
    }

    public boolean addResource(T resource) {
        resources.add(resource);
        return true;
    }

    public T getResource() throws InterruptedException {
        synchronized (resources) {
            return resources.takeFirst();
        }
    }

    public void setBackResource(T resource) {
        synchronized (resources) {
            resources.addLast(resource);
        }
    }
}
