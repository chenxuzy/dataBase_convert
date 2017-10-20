package com.iflytek.mongodb_mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

public class Worker {
    private LinkedBlockingDeque<Task> tasks;
    private List<Thread> workers;
    private int workerSize;
    private boolean runing;

    public Worker() {
        workerSize = 2;
        init();
    }

    public Worker(int size) {
        workerSize = size;
        init();
    }

    public void addTask(Task task) {
        tasks.addLast(task);
    }

    public void run() {
        for (Thread worker : workers)
            worker.start();
    }

    public void release() {
        this.runing = false;
    }

    private void init() {
        tasks = new LinkedBlockingDeque<>();
        workers = new ArrayList<>();
        runing = true;
        for (int i = 0; i < workerSize; i++) {
            workers.add(new Thread(new DoWork(i)));
        }

    }

    private class DoWork implements Runnable {

        private int index;

        public DoWork(int index) {
            this.index = index;
        }

        @Override
        public void run() {
            while (runing) {
                try {
                    Task task = tasks.takeFirst();
                    task.run();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
