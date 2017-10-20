package com.iflytek.mongodb_mysql;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingDeque;

public class Watcher {
    private static Watcher _watcher = null;
    private ArrayList<Consumer> workers;
    private LinkedBlockingDeque<Task> tasks;
    private static boolean running = false;


    static {
        _watcher = new Watcher();
    }

    private Watcher() {
        workers = new ArrayList<>();
        tasks = new LinkedBlockingDeque<>();
        running = true;
    }

    public static int getTaskSize() {
        return _watcher.tasks.size();
    }

    public static void close() {
        running = false;
        for (int i = 0, size = _watcher.workers.size(); i < size; i++) {
            if (_watcher.workers.get(i).isAlive())
                _watcher.workers.get(i).stop();
        }
    }

    public static void register(Consumer consumer) {
        synchronized (_watcher.workers) {
            _watcher.workers.add(consumer);
            consumer.start();
        }

    }

    public static void AddTask(Task task) {
        if (task.getFlag() == 2)
            _watcher.tasks.addFirst(task);
        else
            _watcher.tasks.add(task);
    }

    public static Task getTask() throws InterruptedException {
        return _watcher.tasks.takeFirst();
    }

    static public class Consumer extends Thread {
        @Override
        public void run() {
            boolean result = true;
            while (running && result) {
                try {
                    Task task = Watcher.getTask();
                    result = task.run();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
