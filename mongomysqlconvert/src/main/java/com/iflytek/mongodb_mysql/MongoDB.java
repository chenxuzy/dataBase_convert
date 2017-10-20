package com.iflytek.mongodb_mysql;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class MongoDB implements DataBase {
    private final static String SERVER = "host";
    private final static String PORT = "port";
    private final static String DATABASE = "database";
    private final static String USERNAME = "username";
    private final static String PASSWORD = "password";
    private JSONObject jsonConf;
    private MongoDatabase _mongoInstance=null;
    public MongoDB(JSONObject json) {
        this.jsonConf = json;
        init();
    }

    public MongoDatabase getMongoInstance(){

        return this._mongoInstance;
    }

    private void init() {
        try {

            ServerAddress serverAddress = new ServerAddress(this.jsonConf.getString(SERVER), (this.jsonConf.getInt(PORT)));
            List<ServerAddress> addrs = new ArrayList<>();
            addrs.add(serverAddress);
            List<MongoCredential> credentials = new ArrayList<>();
            if (this.jsonConf.has(USERNAME) &&this.jsonConf.has(PASSWORD)) {
                MongoCredential credential = MongoCredential.createScramSha1Credential(
                        this.jsonConf.getString(USERNAME),this.jsonConf.getString(DATABASE), this.jsonConf.getString(PASSWORD).toCharArray());
                credentials.add(credential);
            }
            MongoClient client = new MongoClient(addrs, credentials);
            this._mongoInstance = client.getDatabase(this.jsonConf.getString(DATABASE));
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + " : " + e.getMessage());
        }
    }

}
