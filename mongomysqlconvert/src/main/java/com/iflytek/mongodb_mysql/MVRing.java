package com.iflytek.mongodb_mysql;


import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class MVRing {
//    private final static String[] columns = {"bizid", "title", "ringid", "ringname", "ringusername", "userid",
//            "username", "labels", "url", "audiourl", "source", "vipstate", "duration", "fire", "commentcount", "auditlevel",
//            "flower", "score", "desc", "createdtime", "updatedtime", "status", "sourceprice", "type",
//            "calloutset", "callinset", "isprivate", "collect"};


//    public static String insertSql = "replace into mv_ring ( `bizid`, `title`, `ringid`, `ringname`, `ringusername`,`userid`," +
//            "`username`, `labels`, `url`, `audiourl`, `source`, `vipstate`, `duration`, `fire`, `commentcount`," +
//            "`auditlevel`,`flower`, `score`, `desc`, `createdtime`, `updatedtime`, `status`, `sourceprice`, `type`," +
//            "`calloutset`, `callinset`, `isprivate`, `collect` )   values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    public static String insertSql = "";

//    public String id;
//    public String title;
//    public String ringid;
//    public String ringname;
//    public String userid;
//    public String username;
//    public String labels;
//    public String url;
//    public String audiourl;
//    public int source;
//    public boolean vipstate;
//    public long duration;
//    public int fire;
//    public int commentcount;
//    public int auditlevel;
//    public int flower;
//    public int score;
//    public String desc;
//    public long createdtime;
//    public long updatedtime;
//    public boolean status;
//    public int sourceprice;
//    public int type;
//    public int calloutset;
//    public int callinset;
//    public boolean isprivate;
//    public int collect;

    public MVRing() {

    }

    public static void SetInsertSql(List<String> keys) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("replace into mv_ring (");
        int index = 0;
        for (String key : keys) {
            if (index > 0)
                buffer.append(",");
            buffer.append("`");
            buffer.append(key);
            buffer.append("`");
            index++;
        }
        buffer.append(") values (");
        for (int i = 0, size = keys.size(); i < size; i++) {
            if (i > 0)
                buffer.append(",");
            buffer.append("?");
        }
        buffer.append(")");
        insertSql = buffer.toString();
        System.out.println(insertSql);
    }

    public static void insertSql(List<JSONObject> mvRings, String[] columns) throws SQLException {

        Long begin_time = java.util.Calendar.getInstance().getTimeInMillis();
        if (mvRings.size() > 0) {
            Connection connection = MysqlDBJDBC.getConnecttion();
            connection.setAutoCommit(false);
            PreparedStatement stmt = connection.prepareStatement(insertSql);
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
//                        stmt.setObject(1, mvRing.getString("id"));
//                        stmt.setObject(2, mvRing.getString("title"));
//                        stmt.setObject(3, mvRing.has("ringid") ? mvRing.getString("ringid") : null);
//                        stmt.setObject(4, mvRing.has("ringname") ? mvRing.getString("ringname") : null);
//                        stmt.setObject(5, mvRing.has("ringusername") ? mvRing.getString("ringusername") : null);
//                        stmt.setObject(6, mvRing.has("userid") ? mvRing.getString("userid") : null);
//                        stmt.setObject(7, mvRing.has("username") ? mvRing.getString("username") : null);
//                        stmt.setObject(8, mvRing.has("labels") ? mvRing.getString("labels") : null);
//                        stmt.setObject(9, mvRing.has("url") ? mvRing.getString("url") : null);
//                        stmt.setObject(10, mvRing.has("audiourl") ? mvRing.getString("audiourl") : null);
//                        stmt.setObject(11, mvRing.has("source") ? mvRing.getInt("source") : null);
//                        stmt.setObject(12, mvRing.has("vipstate") ? mvRing.getInt("vipstate") : null);
//                        stmt.setObject(13, mvRing.has("duration") ? mvRing.getLong("duration") : null);
//                        stmt.setObject(14, mvRing.has("fire") ? mvRing.getInt("fire") : null);
//                        stmt.setObject(15, mvRing.has("commentcount") ? mvRing.getInt("commentcount") : null);
//                        stmt.setObject(16, mvRing.has("auditlevel") ? mvRing.getInt("auditlevel") : null);
//                        stmt.setObject(17, mvRing.has("flower") ? mvRing.getInt("flower") : null);
//                        stmt.setObject(18, mvRing.has("score") ? mvRing.getInt("score") : null);
//                        stmt.setObject(19, mvRing.has("desc") ? mvRing.getString("desc") : null);
//                        stmt.setObject(20, mvRing.has("createdtime") ? mvRing.getLong("createdtime") : null);
//                        stmt.setObject(21, mvRing.has("updatedtime") ? mvRing.getLong("updatedtime") : null);
//                        stmt.setObject(22, mvRing.has("status") ? mvRing.getInt("status") : null);
//                        stmt.setObject(23, mvRing.has("sourceprice") ? mvRing.getInt("sourceprice") : null);
//                        stmt.setObject(24, mvRing.has("type") ? mvRing.getInt("type") : null);
//                        stmt.setObject(25, mvRing.has("calloutset") ? mvRing.getInt("calloutset") : null);
//                        stmt.setObject(26, mvRing.has("callinset") ? mvRing.getInt("callinset") : null);
//                        stmt.setObject(27, mvRing.has("isprivate") ? mvRing.getInt("isprivate") : null);
//                        stmt.setObject(28, mvRing.has("collect") ? mvRing.getInt("collect") : null)
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
            System.out.println(end_time - begin_time);

//            StringBuilder sql = new StringBuilder();
//            sql.append("replace into mv_ring ( "); //避免主键重复
//
//            int flag = 0;
//            for (String key : columns) {
//                if (flag > 0)
//                    sql.append(",");
//                sql.append("`");
//                sql.append(key);
//                sql.append("`");
//                flag++;
//            }
//
//            sql.append(") values ");
//
//
//            int total = 0;
//            for (JSONObject json : mvRings) {
//
//                if (total > 0)
//                    sql.append(",");
//                sql.append("(");
//
//                int index = 0;
//                for (String key : columns) {
//                    if (index > 0)
//                        sql.append(",");
//                    if (json.has(key))
//                        sql.append(json.get(key));
//                    else
//                        sql.append("null");
//                    index++;
//                }
//
//                sql.append(")");
//                total++;
//            }
//            return sql.toString();
        }

        //System.out.println(json.toString());
//                MVRing mvRing = new MVRing();
//                mvRing.id = doc.getString("_id");
//                mvRing.title = doc.getString("title");
//                mvRing.ringid = doc.getString("ringid");
//                mvRing.ringname = doc.getString("ringname");
//                mvRing.userid = doc.getString("userid");
//                mvRing.username = doc.getString("username");
//                mvRing.labels = doc.get("labels").toString();
//                mvRing.url = doc.getString("url");
//                mvRing.audiourl = doc.getString("audiourl");
//                mvRing.source = doc.getInteger("source");
//                mvRing.vipstate = doc.getBoolean("vipstate");
//                mvRing.duration = doc.getLong("audiourl");
//                mvRing.fire = doc.getInteger("fire");
//                mvRing.commentcount = doc.getInteger("commentcount");
//                mvRing.auditlevel = doc.getInteger("auditlevel");
//                mvRing.flower = doc.getInteger("flower");
//                mvRing.score = doc.getInteger("score");
//                mvRing.desc = doc.getString("desc");
//                mvRing.createdtime = doc.getLong("createdtime");
//                mvRing.updatedtime = doc.getLong("updatedtime");
//                mvRing.status = doc.getBoolean("status");
//                mvRing.sourceprice = doc.getInteger("sourceprice");
//                mvRing.type = doc.getInteger("type");
//                mvRing.calloutset = doc.getInteger("calloutset");
//                mvRing.callinset = doc.getInteger("callinset");
//                mvRing.isprivate = doc.getBoolean("isprivate");
//                mvRing.collect = doc.getInteger("collect");
    }
}