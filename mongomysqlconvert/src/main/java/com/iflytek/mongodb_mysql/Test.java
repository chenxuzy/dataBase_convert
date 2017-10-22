package com.iflytek.mongodb_mysql;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static com.mongodb.client.model.Projections.include;

public class Test {
    private final static String MONGODb_FILTER = "server";
    private final static String MONGODB_KEY_INCLUDE = "include";
    private final static String COLLECTION = "collection";
    private static ArrayList<String> columns;
    private static JSONObject mysqlConfig;
    private static JSONObject mongodbConfig;
    private static ShareResourcePool<MongoDB> mongoDBResourcePool;
    private static ShareResourcePool<MySqlDB> mySqlDBResourcePool;

    static {
        Map<String, String> server = LoadConfig.getConfig(MONGODb_FILTER);


        mongodbConfig = LoadConfig.getConfigFromJson("mongodb_mv_ring");
        mysqlConfig = LoadConfig.getConfigFromJson("mysql_mv_ring");
        JSONArray array = mongodbConfig.getJSONArray(MONGODB_KEY_INCLUDE);
        ArrayList<String> c = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            c.add(array.getString(i));
        }
        MVRing.SetInsertSql("mv_ring", columns);

        mongoDBResourcePool = new ShareResourcePool<>();

        for (int i = 0; i < 2; i++) {
            mongoDBResourcePool.addResource(new MongoDB(mongodbConfig));
        }


        mySqlDBResourcePool = new ShareResourcePool<>();
        for (int i = 0; i < 4; i++) {
            try {
                mySqlDBResourcePool.addResource(new MySqlDB(mysqlConfig));
            } catch (ClassNotFoundException e) {
                System.out.println("mysql Driver load error");
                e.printStackTrace();
            }
        }

        // worker = new Worker(4);
        // worker.run();
//        for (int i = 0; i < 4; i++) {
//            Watcher.register(new Watcher.Consumer());
//        }

    }

    ///注意 int 最大值
    public void run() throws InterruptedException {
        long size = mongoDBResourcePool.getResource().getMongoInstance().getCollection("mv_ring").count();


        long burst = size / 2;
        long count = burst / 50000;
        int start = 0;
        int total = (int) count * 50000;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Thread thread = new Thread(new Proudoce(start, total));
            thread.start();
            threads.add(thread);
            start = total;
            total = (int) size;
        }
        for (Thread thread : threads)
            thread.join();

    }

    public void insert(List<JSONObject> mvRings, WeakReference<ShareResourcePool<MySqlDB>> mysql) {
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

    class Proudoce implements Runnable {
        private int start;
        private long total;

        public Proudoce(int offset, int total) {
            this.start = offset;
            this.total = total;
        }

        @Override
        public void run() {
            MongoCollection<Document> collection = mongoDBResourcePool.getResource().getMongoInstance().getCollection("mv_ring");

            while (start < total) {
                FindIterable<Document> findIterable = collection.find()
                        .projection(include(columns))
                        .skip(start)
                        .limit(50000);
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
                start += 50000;
            }
        }
    }

}
