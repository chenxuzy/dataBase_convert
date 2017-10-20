package com.iflytek.mongodb_mysql;

import org.json.JSONObject;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class ImportMysqlTask implements Task {
    private List<JSONObject> mvRings;
    private String[] columns;
    private WeakReference<ShareResourcePool<MySqlDB>> mysql;

    public ImportMysqlTask(List<JSONObject> mvRings, String[] columns, WeakReference<ShareResourcePool<MySqlDB>> mysql) {
        this.mvRings = mvRings;
        this.columns = columns;
        this.mysql = mysql;
    }

    @Override
    public int getFlag() {
        return 1;
    }

    @Override
    public boolean run() {
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
        return true;
    }
}
