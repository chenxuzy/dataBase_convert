package com.iflytek.mongodb_mysql;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.json.JSONObject;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ImportUser implements Task {
    private int start;
    private int total;
    private int step;
    private WeakReference<ShareResourcePool<MySqlDB>> mysql;
    private WeakReference<ShareResourcePool<MongoDB>> mongodb;

    public ImportUser(int start, int total, int step,
                      WeakReference<ShareResourcePool<MySqlDB>> mysql, WeakReference<ShareResourcePool<MongoDB>> mongodb) {
        this.start = start;
        this.total = total;
        this.step = step;
        this.mysql = mysql;
        this.mongodb = mongodb;
    }

    @Override
    public int getFlag() {
        return 1;
    }

    public void insert(List<JSONObject> mvRings) {
        // Long begin_time = java.util.Calendar.getInstance().getTimeInMillis();
        try {
            if (mvRings.size() > 0) {
                Connection connection = mysql.get().getResource().getConnecttion();
                connection.setAutoCommit(false);
                String sqlCharset = "set names utf8mb4";
                PreparedStatement stmt = connection.prepareStatement(MVRing.insertSql);
                stmt.execute(sqlCharset);
                try {
                    for (JSONObject mvRing : mvRings) {
                        int i = 1;
                        for (String key : MVRing.columns) {
                            stmt.setObject(i, mvRing.has(key) ? mvRing.get(key) : null);
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
                // Long end_time = java.util.Calendar.getInstance().getTimeInMillis();
                //  System.out.println("current " + end_time + "  this one cost " + (end_time - begin_time));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean run() {
        ///read mongo and insert mysql
        System.out.println("begin: " + start + "   total" + total);
        try {
            MongoCollection<Document> collection = mongodb.get().getResource().getMongoInstance().getCollection(KuyinUser.mysqlTables);
            int num = 0;
            while (num < total) {
                FindIterable<Document> findIterable = collection.find(KuyinUser.query)
                        .projection(Projections.include(KuyinUser.mongo_clomuns))
                        .skip(start + num)
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
                insert(mvRings);
                num += step;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }
}
