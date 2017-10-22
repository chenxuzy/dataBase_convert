package com.iflytek.mongodb_mysql;

import org.json.JSONObject;

import java.sql.*;
import java.util.Map;

public class MySqlDB implements DataBase {
    private static final String MYSQL_STRING = "String";
    private static final String MYSQL_INTEGER = "Integer";
    private static final String MYSQL_LONG = "Long";
    private static final String MYSQL_DATE = "Date";
    private static final String MYSQL_TIME = "Time";
    private static final String MYSQL_DATETIME = "Datetime";
    private static final String MYSQL_BOOLEN = "Boolean";
    private static final String MYSQL_OBJECT = "Object";

    private final static String SERVER = "host";
    private final static String PORT = "port";
    private final static String DATABASE = "database";
    private final static String USERNAME = "username";
    private final static String PASSWORD = "password";

    private Connection conn = null;
    private JSONObject jsonConf;
    static private boolean loaded = false;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            loaded = true;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public MySqlDB(JSONObject json) throws ClassNotFoundException {
        this.jsonConf = json;
        init();
    }

    public static boolean set(PreparedStatement stmt, int index, Object object, String type) throws SQLException {
        try {
            switch (type) {
                case MYSQL_STRING:
                    stmt.setString(index, (String) object);
                    break;
                case MYSQL_INTEGER:
                    stmt.setInt(index, (int) (object));
                    break;
                case MYSQL_LONG:
                    stmt.setLong(index, (Long) (object));
                    break;
                case MYSQL_OBJECT:
                    stmt.setObject(index, object);
                    break;
                default:
                    stmt.setObject(index, object);

            }
        } catch (SQLException e) {
            throw e;
        }
        return true;

    }

    private void init() throws ClassNotFoundException {
        if (!loaded)
            throw new ClassNotFoundException("mysql Driver loaded failed");
        try {
            String url = "jdbc:mysql://" + this.jsonConf.getString(SERVER) + ":" + this.jsonConf.getInt(PORT) + "/" + this.jsonConf.getString(DATABASE) + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false&autoReconnect=true&rewriteBatchedStatements=true";

            conn = DriverManager.getConnection(url,
                    this.jsonConf.getString(USERNAME) == null ? "" : this.jsonConf.getString(USERNAME),
                    this.jsonConf.getString(PASSWORD) == null ? "" : this.jsonConf.getString(PASSWORD));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public Connection getConnecttion() {
        return conn;
    }

    public void close() { //close 之前关闭所有 Statement ；
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
