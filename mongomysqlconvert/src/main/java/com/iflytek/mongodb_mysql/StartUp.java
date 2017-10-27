package com.iflytek.mongodb_mysql;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class StartUp {

    private final static String SERCVER = "server";
    private final static String MYSQL_CONFIG = "mysql_config";
    private final static String MONGODB_CONFIG = "mongodb_config";
    private final static String MONGODB_KEY_INCLUDE = "include";
    private final static String COLLECTION = "collection";
    private final static String TABLE = "table";
    private final static String NAME = "name";

    private static ShareResourcePool<MongoDB> mongoDBResourcePool;
    private static ShareResourcePool<MySqlDB> mySqlDBResourcePool;
    private static List<Group> groups = new ArrayList<>();


    static {
        JSONObject serverConfig = LoadConfig.getConfigFromJson(SERCVER);
        JSONArray tasks = serverConfig.getJSONArray("convert");

        for (int i = 0, size = tasks.length(); i < size; i++) {
            JSONObject json = (JSONObject) tasks.get(i);
            groups.add(new Group(json.getString(MONGODB_CONFIG),
                    json.getString(MYSQL_CONFIG),
                    json.getInt("minDataNum"),
                    json.getInt("threads"),
                    json.getInt("step")));
        }

        ///初始化 mv_ring
        JSONObject mongodb_mv_ring = groups.get(0).getMongodbConfig();
        JSONObject mysql_mv_ring = groups.get(0).getMysqlConfig();
        JSONArray mv_ringarray = mongodb_mv_ring.getJSONObject(COLLECTION).getJSONArray(MONGODB_KEY_INCLUDE);
        for (int i = 0; i < mv_ringarray.length(); i++) {
            MVRing.columns.add(mv_ringarray.getString(i));
        }
        MVRing.table = mysql_mv_ring.getJSONObject(TABLE).getString(NAME);
        MVRing.SetInsertSql(MVRing.table, MVRing.columns);


        ///初始化 user

        JSONObject mongodb_user = groups.get(1).getMongodbConfig();
        JSONObject mysql_user = groups.get(1).getMysqlConfig();
        JSONArray mmongodb_userarray = mongodb_user.getJSONObject(COLLECTION).getJSONArray(MONGODB_KEY_INCLUDE);
        JSONArray mysql_userarray = mysql_user.getJSONObject(TABLE).getJSONArray(MONGODB_KEY_INCLUDE);

        for (int i = 0; i < mmongodb_userarray.length(); i++) {
            KuyinUser.mongo_clomuns.add(mmongodb_userarray.getString(i));
        }

        for (int i = 0; i < mysql_userarray.length(); i++) {
            KuyinUser.mysql_clomuns.add(mysql_userarray.getString(i));
        }

        KuyinUser.mongodbCollectiopn = mongodb_user.getJSONObject(COLLECTION).getString(NAME);
        ;
        KuyinUser.mysqlTables = mysql_mv_ring.getJSONObject(TABLE).getString(NAME);
        KuyinUser.SetInsertSql();

//        mongodbConfig = LoadConfig.getConfigFromJson(serverConfig.getString(MONGODB_CONFIG));
//        mysqlConfig = LoadConfig.getConfigFromJson(serverConfig.getString(MYSQL_CONFIG));
//        minDataNum = serverConfig.getInt("minDataNum");
//        threads = serverConfig.getInt("threads");
//        step = serverConfig.getInt("step");


//        MVRing.query.put("isprivate", 1);
//        MVRing.query.put("status", 1);


    }

    public static void main(String[] args) throws SQLException {
        Long begin_time = java.util.Calendar.getInstance().getTimeInMillis();
        System.out.println("begin " + begin_time);
        ///注意 int 最大值
        mongoDBResourcePool = new ShareResourcePool<>();
        mongoDBResourcePool.addResource(new MongoDB(groups.get(0).getMongodbConfig()));

        mySqlDBResourcePool = new ShareResourcePool<>();

        WeakReference<ShareResourcePool<MySqlDB>> weakReferenceMysql = new WeakReference<>(mySqlDBResourcePool);
        WeakReference<ShareResourcePool<MongoDB>> weakReferenceMongodb = new WeakReference<>(mongoDBResourcePool);
        List<Thread> workThreads = new ArrayList<>();
        try {
            mySqlDBResourcePool.addResource(new MySqlDB(groups.get(0).getMysqlConfig()));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("mysql driver loaded failed");
            return;
        }
        //// mv_ring
        {
            try {
                String delete = "delete from " + MVRing.table;
                Statement stmt = mySqlDBResourcePool.getResource().getConnecttion().createStatement();
                stmt.execute(delete);
                stmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            //String query = "{\"isprivate\":1,\"status\":1}";

            long size = mongoDBResourcePool.getResource().getMongoInstance()
                    .getCollection(groups.get(0).getMongodbConfig().getJSONObject(COLLECTION).getString(NAME)).count(MVRing.query);
            //避免当数据量很少时，还是开启多线程
            long burst = size / groups.get(0).getThreads();
            System.out.println("pull total " + size);
            int minDataNum = groups.get(0).getMinDataNum();
            if (minDataNum < groups.get(0).getStep())
                minDataNum = groups.get(0).getStep();
            int threads = groups.get(0).getThreads();
            while (burst < minDataNum) {
                if (threads > 1) {
                    threads--;
                    burst = size / threads;
                } else break;
            }


            for (int i = 1; i < threads; i++) {
                mongoDBResourcePool.addResource(new MongoDB(groups.get(0).getMongodbConfig()));
            }

            for (int i = 1; i < threads; i++) {
                try {
                    mySqlDBResourcePool.addResource(new MySqlDB(groups.get(0).getMysqlConfig()));
                } catch (ClassNotFoundException e) {
                    System.out.println("mysql Driver load error");
                    e.printStackTrace();
                    return;
                }
            }
            int step = groups.get(0).getStep();
            long count = burst / step;
            long start = 0;
            long total = count > 0 ? count * step : step;

            for (int i = 0; i < threads; i++) {
                if (i == threads - 1) {
                    total = size - start;
                }
                if ((total) < Integer.MAX_VALUE && start < Integer.MAX_VALUE) {
                    Thread thread = new Thread(new Proudoce(new ImportMysqlTask((int) start, (int) total, step, weakReferenceMysql, weakReferenceMongodb)));
                    thread.start();
                    workThreads.add(thread);
                    start = total + start;
                }
            }
        }

        ///user
        {
            try {
                String delete = "delete from " + KuyinUser.mysqlTables;
                Statement stmt = mySqlDBResourcePool.getResource().getConnecttion().createStatement();
                stmt.execute(delete);
                stmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            //String query = "{\"isprivate\":1,\"status\":1}";

            long size = mongoDBResourcePool.getResource().getMongoInstance()
                    .getCollection(groups.get(1).getMongodbConfig().getJSONObject(COLLECTION).getString(NAME)).count(MVRing.query);
            //避免当数据量很少时，还是开启多线程
            int threads = groups.get(1).getThreads();
            int minDataNum = groups.get(1).getMinDataNum();
            int step = groups.get(1).getStep();
            long burst = size / threads;
            System.out.println("pull total " + size);
            if (minDataNum < step)
                minDataNum = step;

            while (burst < minDataNum) {
                if (threads > 1) {
                    threads--;
                    burst = size / threads;
                } else break;
            }


            for (int i = 1; i < threads; i++) {
                mongoDBResourcePool.addResource(new MongoDB(groups.get(1).getMongodbConfig()));
            }

            for (int i = 1; i < threads; i++) {
                try {
                    mySqlDBResourcePool.addResource(new MySqlDB(groups.get(1).getMysqlConfig()));
                } catch (ClassNotFoundException e) {
                    System.out.println("mysql Driver load error");
                    e.printStackTrace();
                    return;
                }
            }

            long count = burst / step;
            long start = 0;
            long total = count > 0 ? count * step : step;
            for (int i = 0; i < threads; i++) {
                if (i == threads - 1) {
                    total = size - start;
                }
                if ((total) < Integer.MAX_VALUE && start < Integer.MAX_VALUE) {
                    Thread thread = new Thread(new Proudoce(new ImportUser((int) start, (int) total, step, weakReferenceMysql, weakReferenceMongodb)));
                    thread.start();
                    workThreads.add(thread);
                    start = total + start;
                }

            }

        }
        /// wait all task over
        for (Thread thread : workThreads) {
            try {
                thread.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        /////release source
        for (MongoDB mongoDB : mongoDBResourcePool.getAllResource()) {
            mongoDB.close();
        }
        mongoDBResourcePool.clear();
        for (MySqlDB mySqlDB : mySqlDBResourcePool.getAllResource()) {
            mySqlDB.close();
        }
        mySqlDBResourcePool.clear();
        Long end_time = java.util.Calendar.getInstance().getTimeInMillis();
        System.out.println("end_time  " + end_time + " cast " + (end_time - begin_time));
    }

//    public static void insert(List<JSONObject> mvRings, WeakReference<ShareResourcePool<MySqlDB>> mysql) {
//        // Long begin_time = java.util.Calendar.getInstance().getTimeInMillis();
//        try {
//            if (mvRings.size() > 0) {
//                Connection connection = mysql.get().getResource().getConnecttion();
//                connection.setAutoCommit(false);
//                PreparedStatement stmt = connection.prepareStatement(MVRing.insertSql);
//                try {
//
//                    for (JSONObject mvRing : mvRings) {
//                        int i = 1;
//                        for (String key : MVRing.columns) {
//                            if (key.equals("labels")) {
//                                if (mvRing.has(key)) {
//                                    String labels = mvRing.get(key).toString();
//                                    stmt.setString(i, labels.substring(1, labels.length() - 1));
//                                } else {
//                                    stmt.setObject(i, null);
//                                }
//                            } else {
//                                stmt.setObject(i, mvRing.has(key) ? mvRing.get(key) : null);
//                            }
//                            i++;
//                        }
//                        stmt.addBatch();
//                    }
//                    stmt.executeBatch();
//                    connection.commit();
//                    stmt.close();
//                } catch (Exception e) {
//                    stmt.close();
//                    connection.setAutoCommit(true);
//                    e.printStackTrace();
//                }
//                // Long end_time = java.util.Calendar.getInstance().getTimeInMillis();
//                //  System.out.println("current " + end_time + "  this one cost " + (end_time - begin_time));
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }

    static class Proudoce implements Runnable {
        private Task task;


        public Proudoce(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            task.run();
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
