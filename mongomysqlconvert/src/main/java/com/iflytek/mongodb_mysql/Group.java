package com.iflytek.mongodb_mysql;

import org.json.JSONObject;

public class Group {
    private String mysqlConfig;
    private String mongodbConifg;
    private int minDataNum;
    private int threads;
    private int step;
    private JSONObject mysql;
    private JSONObject mongodb;


    public Group(String mysqlConfig, String mongodbConifg, int minDataNum, int threads, int step) {
        this.mysqlConfig = mysqlConfig;
        this.mongodbConifg = mongodbConifg;
        this.minDataNum = minDataNum;
        this.threads = threads;
        this.step = step;
        init();
    }


    private void init() {
        mongodb = LoadConfig.getConfigFromJson(mongodbConifg);
        mysql = LoadConfig.getConfigFromJson(mysqlConfig);
    }

    public int getMinDataNum() {
        return minDataNum;
    }
    public int getThreads() {
        return threads;
    }
    public int getStep() {
        return step;
    }

    public JSONObject getMysqlConfig() {
        return mysql;
    }

    public JSONObject getMongodbConfig() {
        return mongodb;
    }
}
