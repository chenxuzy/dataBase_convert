package com.iflytek.mongodb_mysql;

import org.json.JSONObject;

public class DatabseFactary {
    private static final String MYSQL = "mysql";
    private static final String MONGODB = "mongodb";
    private static final String REDIS = "redis";
    private static final String SQL_SERVER = "sql_sever";
    private static final String DATABASE_TYPE = "type";

    public static DataBase getDatabase(JSONObject json) throws Exception {
        switch (json.getString(DATABASE_TYPE)) {
            case MYSQL:
                return null;
            case MONGODB:
                return null;
            case REDIS:
                return null;
            case SQL_SERVER:
                return null;
            default:
                throw new Exception("has not defiend " + json.getString(DATABASE_TYPE) + " database");
        }
    }
}
