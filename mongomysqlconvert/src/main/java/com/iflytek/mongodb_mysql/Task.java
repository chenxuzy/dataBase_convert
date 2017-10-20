package com.iflytek.mongodb_mysql;

public interface Task {
    int getFlag(); //1 一般任务， 0 结束任务 ， 2 紧急任务

    boolean run(); ///实现类必须返回 true
}
