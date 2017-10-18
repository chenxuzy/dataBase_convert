package com.iflytek.mongodb_mysql;


import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.mongodb.client.model.Projections.exclude;


public class StartUp {
    private final static String MONGODb_FILTER = "server";
    private final static String MONGODB_KEY_FILTER = "mongodbKeyfilter";
    private final static String COLLECTION = "collection";

//    private final static  String columns ="id,title,ingid,ringname,userid,username,labels" +
//            ",url,audiourl,source,vipstate,duration,fire,commentcount,auditlevel,flower" +
//            ",score,desc,createdtime,updatedtime,status,sourceprice,type,calloutset,callinset" +
//            ",isprivate,collect";

    public static String getKeyMap(String key) {
        if (key.equals("id"))
            return "_id";
        return key;
    }

    public static void initialization() {

    }

    public static void main(String[] args) {
        Map<String, String> server = LoadConfig.getConfig(MONGODb_FILTER);

        MongoDatabase mongoDatabase = MongoDBJDBC.getDatabase();
        MongoCollection<Document> collection = mongoDatabase.getCollection(server.get(COLLECTION));
        Connection connection = MysqlDBJDBC.getConnecttion();
        int i = 0;
        int co = 0;

        while (true) {
            FindIterable<Document> findIterable = collection.find()
                    .projection(exclude(server.get(MONGODB_KEY_FILTER).split(",")))
                    .skip(i * 1000)
                    .limit(1000);
            MongoCursor<Document> mongoCursor = findIterable.iterator();

           List<JSONObject> mvRings = new ArrayList<>();

            if (!mongoCursor.hasNext()) {
                mongoCursor.close();
                break;
            }

            while (mongoCursor.hasNext()) {
                co++;
                try {
                    JSONObject json = new JSONObject();
                    Document doc = mongoCursor.next();

                    json.put("id", doc.getString("bizid"));
                    json.put("title", doc.getString("title"));
                    json.put("ringid", doc.getString("ringid"));
                    json.put("ringname", doc.getString("ringname"));
                    json.put("userid", doc.getString("userid"));
                    json.put("username", doc.getString("username"));
                    ArrayList<String> labels = doc.get("labels", ArrayList.class);
                    StringBuilder buffer = new StringBuilder();
                    int index = 0;
                    for (String str : labels) {
                        if (index > 0)
                            buffer.append(",");
                        buffer.append(str);
                        index++;
                    }
                    json.put("labels", buffer.toString());
                    json.put("url", doc.getString("url"));
                    json.put("audiourl", doc.getString("audiourl"));
                    json.put("source", Integer.valueOf(doc.getString("source")));
                    json.put("vipstate", doc.getInteger("vipstate"));
                    json.put("duration", doc.getLong("duration"));
                    json.put("fire", doc.getInteger("fire"));
                    json.put("commentcount", doc.getInteger("commentcount"));
                    json.put("auditlevel", doc.getInteger("auditlevel"));
                    json.put("flower", doc.getInteger("flower"));
                    json.put("score", doc.getInteger("score"));
                    json.put("desc", doc.getString("desc"));
                    json.put("createdtime", doc.getLong("createdtime"));
                    json.put("updatedtime", doc.getLong("updatedtime"));
                    json.put("status", doc.getInteger("status"));
                    json.put("sourceprice", doc.getInteger("sourceprice"));
                    json.put("type", doc.getInteger("type"));
                    json.put("calloutset", doc.getInteger("calloutset"));
                    json.put("callinset", doc.getInteger("callinset"));
                    json.put("isprivate", doc.getInteger("isprivate"));
                    json.put("collect", doc.getInteger("collect"));
                    mvRings.add(json);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            mongoCursor.close();
            try {
                MVRing.insertSql(mvRings);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            i++;
        }
        MongoDBJDBC.close();
        MysqlDBJDBC.close();
        System.out.println(co);


    }
}
