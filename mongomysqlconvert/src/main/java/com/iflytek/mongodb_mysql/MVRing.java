package com.iflytek.mongodb_mysql;

import org.json.JSONObject;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

public class MVRing {
    private final static String[] columns = {"id", "title", "ringid", "ringname", "userid",
            "username", "labels", "url", "audiourl", "source", "vipstate", "duration", "fire", "commentcount", "auditlevel",
            "flower", "score", "desc", "createdtime", "updatedtime", "status", "sourceprice", "type",
            "calloutset", "callinset", "isprivate", "collect"};
    private final static String[] emptyOptions = {"ringid", "ringname", "userid",
            "username", "labels", "url", "audiourl", "source", "vipstate", "duration", "fire", "commentcount", "auditlevel",
            "flower", "score", "desc", "createdtime", "updatedtime", "status", "sourceprice", "type",
            "calloutset", "callinset", "isprivate", "collect"};
    private static List<String> empty = new ArrayList<>();


    public  static final String insertSql = "replace into mv_ring ( `id`, `title`, `ringid`, `ringname`, `userid`," +
            "`username`, `labels`, `url`, `audiourl`, `source`, `vipstate`, `duration`, `fire`, `commentcount`," +
            "`auditlevel`,`flower`, `score`, `desc`, `createdtime`, `updatedtime`, `status`, `sourceprice`, `type`," +
            "`calloutset`, `callinset`, `isprivate`, `collect` )   values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    static {
        for (String str : emptyOptions)
            empty.add(str);
    }

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

    public static void insertSql(List<JSONObject> mvRings) throws SQLException {

        Long begin_time = java.util.Calendar.getInstance().getTimeInMillis();
        if (mvRings.size() > 0) {
            Connection connection = MysqlDBJDBC.getConnecttion();
            connection.setAutoCommit(false);
            PreparedStatement stmt = connection.prepareStatement(insertSql);

            int count = 0;
            for (JSONObject mvRing : mvRings) {
                try {
//                    int i =1;
//                    for(String key :columns) {
//                        stmt.setObject(i,mvRing.has(key)?mvRing.get(key):null);
//                        i++;
//                    }
                    stmt.setString(1, mvRing.getString("id"));
                    stmt.setString(2, mvRing.getString("title"));
                    stmt.setString(3, mvRing.has("ringid") ? mvRing.getString("ringid") : null);
                    stmt.setString(4, mvRing.has("ringname") ? mvRing.getString("ringname") : null);
                    stmt.setString(5, mvRing.has("userid") ? mvRing.getString("userid") : null);
                    stmt.setString(6, mvRing.has("username") ? mvRing.getString("username") : null);
                    stmt.setString(7, mvRing.has("labels") ? mvRing.getString("labels") : null);
                    stmt.setString(8, mvRing.has("url") ? mvRing.getString("url") : null);
                    stmt.setString(9, mvRing.has("audiourl") ? mvRing.getString("audiourl") : null);
                    stmt.setInt(10, mvRing.has("source") ? mvRing.getInt("source") : null);
                    stmt.setInt(11, mvRing.has("vipstate") ? mvRing.getInt("vipstate") : null);
                    stmt.setLong(12, mvRing.has("duration") ? mvRing.getLong("duration") : null);
                    stmt.setInt(13, mvRing.has("fire") ? mvRing.getInt("fire") : null);
                    stmt.setInt(14, mvRing.has("commentcount") ? mvRing.getInt("commentcount") : null);
                    stmt.setInt(15, mvRing.has("auditlevel") ? mvRing.getInt("auditlevel") : null);
                    stmt.setInt(16, mvRing.has("flower") ? mvRing.getInt("flower") : null);
                    stmt.setInt(17, mvRing.has("score") ? mvRing.getInt("score") : null);
                    stmt.setString(18, mvRing.has("desc") ? mvRing.getString("desc") : null);
                    stmt.setLong(19, mvRing.has("createdtime") ? mvRing.getLong("createdtime") : null);
                    stmt.setLong(20, mvRing.has("updatedtime") ? mvRing.getLong("updatedtime") : null);
                    stmt.setInt(21, mvRing.has("status") ? mvRing.getInt("status") : null);
                    stmt.setInt(22, mvRing.has("sourceprice") ? mvRing.getInt("sourceprice") : null);
                    stmt.setInt(23, mvRing.has("type") ? mvRing.getInt("type") : null);
                    stmt.setInt(24, mvRing.has("calloutset") ? mvRing.getInt("calloutset") : null);
                    stmt.setInt(25, mvRing.has("callinset") ? mvRing.getInt("callinset") : null);
                    stmt.setInt(26, mvRing.has("isprivate") ? mvRing.getInt("isprivate") : null);
                    stmt.setInt(27, mvRing.has("collect") ? mvRing.getInt("collect") : null);
                    stmt.addBatch();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            stmt.executeBatch();
            connection.commit();
            stmt.close();

            Long end_time = java.util.Calendar.getInstance().getTimeInMillis();
            System.out.println(end_time -begin_time );

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