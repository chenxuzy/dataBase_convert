package com.iflytek.mongodb_mysql;


import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.mongodb.client.model.Projections.include;


public class StartUp {

    private final static String SERCVER = "server";
    private final static String MYSQL_CONFIG = "mysql_config";
    private final static String MONGODB_CONFIG = "mongodb_config";
    private final static String MONGODB_KEY_INCLUDE = "include";
    private final static String COLLECTION = "collection";
    private final static String NAME = "name";
    private static ArrayList<String> columns;
    private static JSONObject mysqlConfig;
    private static JSONObject mongodbConfig;
    private static ShareResourcePool<MongoDB> mongoDBResourcePool;
    private static ShareResourcePool<MySqlDB> mySqlDBResourcePool;
    private static int minDataNum = 50000;
    private static int threads = 2;
    private static int step = 50000;

    static {
        JSONObject serverConfig = LoadConfig.getConfigFromJson(SERCVER);
        mongodbConfig = LoadConfig.getConfigFromJson(serverConfig.getString(MONGODB_CONFIG));
        mysqlConfig = LoadConfig.getConfigFromJson(serverConfig.getString(MYSQL_CONFIG));
        minDataNum = serverConfig.getInt("minDataNum");
        threads = serverConfig.getInt("threads");
        step = serverConfig.getInt("step");
        JSONArray array = mongodbConfig.getJSONObject(COLLECTION).getJSONArray(MONGODB_KEY_INCLUDE);
        columns = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            columns.add(array.getString(i));
        }
        MVRing.SetInsertSql(columns);


    }

    public static void main(String[] args) throws NoSuchMethodException {
        Long end_time = java.util.Calendar.getInstance().getTimeInMillis();
        System.out.println("begin " + end_time);
        ///注意 int 最大值
        mongoDBResourcePool = new ShareResourcePool<>();
        mongoDBResourcePool.addResource(new MongoDB(mongodbConfig));
        long size = mongoDBResourcePool.getResource().getMongoInstance().getCollection(mongodbConfig.getJSONObject(COLLECTION).getString(NAME)).count();

        long burst = size / threads;
        if (minDataNum < step)
            minDataNum = step;
        while (burst < minDataNum) {
            if (threads > 1) {
                threads--;
                burst = size / threads;
            } else break;
        }



        for (int i = 1; i < threads; i++) {
            mongoDBResourcePool.addResource(new MongoDB(mongodbConfig));
        }

        mySqlDBResourcePool = new ShareResourcePool<>();
        for (int i = 0; i < threads; i++) {
            try {
                mySqlDBResourcePool.addResource(new MySqlDB(mysqlConfig));
            } catch (ClassNotFoundException e) {
                System.out.println("mysql Driver load error");
                e.printStackTrace();
            }
        }

        long count = burst / step;
        long start = 0;
        long total = count > 0 ? count * step : step;

        List<Thread> workThreads = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            if (i == threads - 1) {
                total = size - start;
            }
            if ((total) < Integer.MAX_VALUE && start < Integer.MAX_VALUE) {
                Thread thread = new Thread(new Proudoce((int) start, (int) total));
                thread.start();
                workThreads.add(thread);
                start = total + start;
            }

        }
        for (Thread thread : workThreads) {
            try {
                thread.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void insert(List<JSONObject> mvRings, WeakReference<ShareResourcePool<MySqlDB>> mysql) {
        Long begin_time = java.util.Calendar.getInstance().getTimeInMillis();
        try {
            if (mvRings.size() > 0) {
                Connection connection = mysql.get().getResource().getConnecttion();
                connection.setAutoCommit(false);
                PreparedStatement stmt = connection.prepareStatement(MVRing.insertSql);
                try {

                    for (JSONObject mvRing : mvRings) {
                        int i = 1;
                        for (String key : columns) {
                            if (key.equals("labels")) {
                                if (mvRing.has(key)) {
                                    String labels = mvRing.get(key).toString();
                                    stmt.setString(i, labels.substring(1, labels.length() - 1));
                                } else {
                                    stmt.setObject(i, null);
                                }
                            } else {
                                stmt.setObject(i, mvRing.has(key) ? mvRing.get(key) : null);
                            }
                            i++;
                        }
                        stmt.addBatch();
                    }
                    stmt.executeBatch();
                    connection.commit();
                    stmt.close();
                } catch (Exception e) {
                    stmt.close();
                    connection.setAutoCommit(true);
                    e.printStackTrace();
                }
                Long end_time = java.util.Calendar.getInstance().getTimeInMillis();
                System.out.println("current " + end_time + "  this one cost " + (end_time - begin_time));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static class Proudoce implements Runnable {
        private int start;
        private long total;

        public Proudoce(int offset, int total) {
            this.start = offset;
            this.total = total;
        }

        @Override
        public void run() {
            MongoCollection<Document> collection = mongoDBResourcePool.getResource().getMongoInstance().getCollection("mv_ring");
            int num =0;
            while (num < total) {
                FindIterable<Document> findIterable = collection.find()
                        .projection(include(columns))
                        .skip(start+num)
                        .limit(step);
                MongoCursor<Document> mongoCursor = findIterable.iterator();

                List<JSONObject> mvRings = new ArrayList<>();

                if (!mongoCursor.hasNext()) {
                    mongoCursor.close();
                    break;
                }
                while (mongoCursor.hasNext()) {
                    JSONObject json = new JSONObject();
                    Document doc = mongoCursor.next();
                    Set<String> keys = doc.keySet();
                    for (String key : keys)
                        json.put(key, doc.get(key));
                    mvRings.add(json);
                }
                mongoCursor.close();
                //Watcher.AddTask(new ImportMysqlTask(mvRings, columns, new WeakReference<>(mySqlDBResourcePool)));
                insert(mvRings, new WeakReference<>(mySqlDBResourcePool));
                num += step;
            }

        }
    }
//        Map<String, String> server = LoadConfig.getConfig(MONGODb_FILTER);
//        MongoDatabase mongoDatabase = MongoDBJDBC.getDatabase();
//        MongoCollection<Document> collection = mongoDatabase.getCollection(server.get(COLLECTION));
//
//        int i = 0;
//        int co = 0;
//        String[] columns = server.get(MONGODB_KEY_INCLUDE).split(",");
//        MVRing.SetInsertSql(columns);
//        Long begin_time = java.util.Calendar.getInstance().getTimeInMillis();
//
//        while (true) {
//            FindIterable<Document> findIterable = collection.find()
//                    .projection(include(columns))
//                    .skip(i * 50000)
//                    .limit(50000);
//            MongoCursor<Document> mongoCursor = findIterable.iterator();
//
//            List<JSONObject> mvRings = new ArrayList<>();
//
//            if (!mongoCursor.hasNext()) {
//                mongoCursor.close();
//                break;
//            }
//
//            while (mongoCursor.hasNext()) {
//                co++;
//                JSONObject json = new JSONObject();
//                Document doc = mongoCursor.next();
//                Set<String> keys = doc.keySet();
//                for (String key : keys)
//                    json.put(key, doc.get(key));
////                json.put("bizid", doc.getString("bizid"));
////                json.put("title", doc.getString("title"));
////                json.put("ringid", doc.getString("ringid"));
////                json.put("ringname", doc.getString("ringname"));
////                json.put("userid", doc.getString("userid"));
////                json.put("username", doc.getString("username"));
////                StringBuilder buffer = new StringBuilder();
////                if(doc.containsKey("labels")){
////                    ArrayList<String> labels = doc.get("labels", ArrayList.class);
////                    int index = 0;
////                    for (String str : labels) {
////                        if (index > 0)
////                            buffer.append(",");
////                        buffer.append(str);
////                        index++;
////                    }
////                }
////                json.put("labels", buffer.toString());
////                json.put("url", doc.getString("url"));
////                json.put("audiourl", doc.getString("audiourl"));
////                json.put("source", Integer.valueOf(doc.getString("source")));
////                json.put("vipstate", doc.getInteger("vipstate"));
////                json.put("duration", doc.getLong("duration"));
////                json.put("fire", doc.getInteger("fire"));
////                json.put("commentcount", doc.getInteger("commentcount"));
////                json.put("auditlevel", doc.getInteger("auditlevel"));
////                json.put("flower", doc.getInteger("flower"));
////                json.put("score", doc.getInteger("score"));
////                json.put("desc", doc.getString("desc"));
////                json.put("createdtime", doc.getLong("createdtime"));
////                json.put("updatedtime", doc.getLong("updatedtime"));
////                json.put("status", doc.getInteger("status"));
////                json.put("sourceprice", doc.getInteger("sourceprice"));
////                json.put("type", doc.getInteger("type"));
////                json.put("calloutset", doc.getInteger("calloutset"));
////                json.put("callinset", doc.getInteger("callinset"));
////                json.put("isprivate", doc.getInteger("isprivate"));
////                json.put("collect", doc.getInteger("collect"));
//                mvRings.add(json);
//
//
//            }
//            mongoCursor.close();
//            try {
//                MVRing.insertSql(mvRings, columns);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            i++;
//        }
//        Long end_time = java.util.Calendar.getInstance().getTimeInMillis();
//        System.out.println(end_time - begin_time);
//        MongoDBJDBC.close();
//        MysqlDBJDBC.close();
//        System.out.println(co);

}
