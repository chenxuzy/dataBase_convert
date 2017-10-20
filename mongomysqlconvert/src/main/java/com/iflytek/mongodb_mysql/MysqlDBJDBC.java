package com.iflytek.mongodb_mysql;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class MysqlDBJDBC {
    private final static String MYSQL="mysql_mv_ring";
    private final static String SERVER="server.host";
    private final static String PORT = "server.port";
    private final static String DATABASE="database";
    private final static String USERNAME = "username";
    private final static String PASSWORD = "password";

    private static  Connection conn = null;
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Map<String ,String> mysql = LoadConfig.getConfig(MYSQL);
            String url= "jdbc:mysql://"+mysql.get(SERVER)+"/"+mysql.get(DATABASE)+"?useUnicode=true&characterEncoding=UTF-8&useSSL=false&autoReconnect=true&rewriteBatchedStatements=true";
            conn = DriverManager.getConnection(
                    url, mysql.get(USERNAME), mysql.get(PASSWORD)) ;
           PreparedStatement stmt =  conn.prepareStatement("");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static Connection getConnecttion(){
        if(conn == null){
            throw new NullPointerException("mysql connection is null");
        }
        return conn;
    }

    public static void close(){ //close 之前关闭所有 Statement ；
        if(conn!=null){
            try{
                conn.close();
            }catch (SQLException e){
                e.printStackTrace();
            }

        }
    }
}
