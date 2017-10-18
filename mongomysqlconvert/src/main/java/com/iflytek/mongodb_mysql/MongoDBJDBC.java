package com.iflytek.mongodb_mysql;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoDBJDBC {
    private final static String MONGODB="mongodb_mv_ring";
    private final static String SERVER="server.host";
    private final static String PORT = "server.port";
    private final static String DATABASE="database";
    private final static String USERNAME = "username";
    private final static String PASSWORD = "password";

    private static MongoDatabase _mongoInstance = null;
    private static  MongoClient _client=null;

    static {
        try {
            Map<String ,String> mongodb = LoadConfig.getConfig(MONGODB);
            ServerAddress serverAddress = new ServerAddress(mongodb.get(SERVER),Integer.valueOf(mongodb.get(PORT)));
            List<ServerAddress> addrs = new ArrayList<>();
            addrs.add(serverAddress);
            List<MongoCredential> credentials = new ArrayList<>();
            if(mongodb.containsKey(USERNAME) && mongodb.containsKey(PASSWORD)) {
                MongoCredential credential = MongoCredential.createScramSha1Credential(
                        mongodb.get(USERNAME), mongodb.get(DATABASE), mongodb.get(PASSWORD).toCharArray());
                credentials.add(credential);
            }
            _client = new MongoClient(addrs,credentials);
            _mongoInstance = _client.getDatabase(mongodb.get(DATABASE));
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + " : " + e.getMessage());
        }
    }

    public static MongoDatabase getDatabase() {
        if (_mongoInstance == null) {
            throw new NullPointerException("_mongoInstance is null");
        }
        return _mongoInstance;
    }

    public static void close(){
        _client.close();
    }
}
